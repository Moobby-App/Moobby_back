"""Fixed spawn target. Only explicitly listed synthetic jobs; no database imports."""

import os
import select
import signal
import socket
import time

from mooby.api.deadline_policy import Deadline
from mooby.api import process_protocol as wire


def immediate(job, deadline, sock):
    return job.text


def delay(job, deadline, sock):
    end = Deadline.after(job.delay_ms / 1000)
    while not end.expired():
        if deadline.expired():
            raise TimeoutError()
        ready, _, _ = select.select([sock], [], [], min(.01, end.remaining_seconds()))
        if ready and not sock.recv(1):
            raise InterruptedError()
    return job.text


def controlled_error(job, deadline, sock):
    raise ValueError("synthetic exception; never exported")


def abrupt_exit(job, deadline, sock):
    os._exit(23)


def unresponsive(job, deadline, sock):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        time.sleep(60)


def oversized_output(job, deadline, sock):
    return "x" * wire.MAX_RESPONSE_BYTES


FUNCTIONS = {
    wire.JobType.SUCCESS: immediate,
    wire.JobType.DELAY: delay,
    wire.JobType.ERROR: controlled_error,
    wire.JobType.CRASH: abrupt_exit,
    wire.JobType.UNRESPONSIVE: unresponsive,
    wire.JobType.OVERSIZE_OUTPUT: oversized_output,
}


def worker(sock: socket.socket):
    # Spawn still initially inherits environment; never mutate the parent's.
    os.environ.clear()
    null = os.open(os.devnull, os.O_WRONLY)
    os.dup2(null, 1)
    os.dup2(null, 2)
    os.close(null)
    try:
        sock.settimeout(5)
        identity = wire.receive_sync(sock, wire.MAX_REQUEST_BYTES)
        job = wire.validate_request(identity)
        # Same-host Python monotonic domain; IPC transfer cannot restart budget.
        deadline = Deadline(min(identity["expires_at_seconds"],
                                time.monotonic() + identity["budget_ms"] / 1000))
        if job.kind is wire.JobType.BAD_VERSION:
            sock.sendall(wire.encode({"v": 2, "id": identity["id"], "type": identity["type"],
                                     "status": "success", "output": "", "error": "none"},
                                    wire.MAX_RESPONSE_BYTES))
            return
        if job.kind is wire.JobType.OVERSIZE_FRAME:
            sock.sendall(wire.HEADER.pack(wire.MAX_RESPONSE_BYTES + 1))
            return
        if job.kind is wire.JobType.BROKEN_IPC:
            return
        if job.kind is wire.JobType.PARTIAL_FRAME:
            sock.sendall(wire.HEADER.pack(64) + b"{")
            return unresponsive(job, deadline, sock)
        try:
            if deadline.expired():
                raise TimeoutError()
            text = FUNCTIONS[job.kind](job, deadline, sock)
            data = wire.response(identity, text=text)
        except wire.PayloadTooLarge:
            data = wire.response(identity, error="payload_too_large")
        except TimeoutError:
            data = wire.response(identity, error="deadline_expired")
        except Exception:
            data = wire.response(identity, error="execution_error")
        sock.sendall(data)
    except BaseException:
        # No raw exception/traceback, framework logging, or arbitrary fields.
        pass
    finally:
        sock.close()
