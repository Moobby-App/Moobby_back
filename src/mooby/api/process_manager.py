"""Inactive bounded spawn supervision. Synthetic-only, no HTTP/database integration."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import StrEnum
import math
import multiprocessing
import socket
import time
from threading import Event, Thread
from uuid import uuid4

from mooby.api.admission import AdmissionController, AdmissionOutcome
from mooby.api.deadline_policy import Deadline, DeadlinePolicy
from mooby.api import process_protocol as wire
from mooby.api.process_protocol import JobType, SyntheticJob
from mooby.api.synthetic_process import worker

MAX_EXECUTION_SLOTS = 2
POLL_SECONDS = .01


class Outcome(StrEnum):
    SUCCESS = "success"
    CHILD_ERROR = "child_execution_error"
    CHILD_CRASH = "child_crash"
    DEADLINE_EXPIRED = "deadline_expired"
    CANCELLED = "request_cancelled"
    PROTOCOL_ERROR = "ipc_protocol_violation"
    PAYLOAD_TOO_LARGE = "ipc_payload_too_large"
    UNAVAILABLE = "process_unavailable"
    RETIREMENT_INCOMPLETE = "retirement_incomplete"
    CLOSED = "manager_closed"
    QUEUE_FULL = "admission_queue_full"
    ADMISSION_EXPIRED = "admission_wait_expired"


class Lifecycle(StrEnum):
    STARTING = "starting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLING = "cancelling"
    TERMINATING = "terminating"
    REAPED = "reaped"
    QUARANTINED = "quarantined"


@dataclass(frozen=True)
class ExecutionResult:
    outcome: Outcome
    output: str | None = None
    reaped: bool = False
    cause: Outcome | None = None


@dataclass(frozen=True)
class ManagerState:
    active_process_slots: int
    starting: int
    running: int
    reaped_count: int
    unreaped: int
    available_capacity: int
    closed: bool
    waiting_requests: int
    supervisory_threads: int


@dataclass(frozen=True)
class ShutdownResult:
    complete: bool
    state: ManagerState


@dataclass(frozen=True)
class _Report:
    reaped: bool
    exitcode: int | None = None
    forced: bool = False


@dataclass(eq=False)
class _Slot:
    job_id: str
    job: SyntheticJob
    deadline: Deadline
    parent_socket: socket.socket
    child_socket: socket.socket
    started: asyncio.Future
    finished: asyncio.Future
    stop: Event = field(default_factory=Event)
    phase: Lifecycle = Lifecycle.STARTING
    process: object = None
    thread: Thread | None = None
    task: asyncio.Task | None = None
    report: _Report | None = None
    cancelled: bool = False
    retirement: Deadline | None = None
    attempt: object = field(default_factory=object)


class ProcessManager:
    """Explicit local use only. One job/child/thread per occupied slot, no pool queue."""

    def __init__(self, policy: DeadlinePolicy | None = None):
        self.policy = policy or DeadlinePolicy()
        if self.policy.max_active_jobs > MAX_EXECUTION_SLOTS:
            raise ValueError("process capacity exceeds hard limit")
        self._admission = AdmissionController(self.policy)
        self._context = multiprocessing.get_context("spawn")
        self._slots: dict[str, _Slot] = {}
        self._loop = None
        self._closed = False
        self._reaped = 0

    def _bind(self):
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("process manager belongs to another event loop")
        return loop

    def snapshot(self) -> ManagerState:
        self._bind()
        slots = tuple(self._slots.values())
        return ManagerState(len(slots), sum(s.phase is Lifecycle.STARTING for s in slots),
                            sum(s.phase is Lifecycle.RUNNING for s in slots), self._reaped,
                            sum(s.phase is Lifecycle.QUARANTINED for s in slots),
                            self.policy.max_active_jobs - len(slots), self._closed,
                            self._admission.snapshot().waiting_jobs,
                            sum(s.thread is not None and s.thread.is_alive() for s in slots))

    async def execute(self, job: SyntheticJob, parent: Deadline) -> ExecutionResult:
        loop = self._bind()
        if self._closed:
            return ExecutionResult(Outcome.CLOSED)
        if parent.expired():
            return ExecutionResult(Outcome.DEADLINE_EXPIRED)
        deadline = parent.child(self.policy.feed_prepare_ms / 1000)
        job_id = uuid4().hex
        try:
            if type(job) is not SyntheticJob:
                raise wire.ProtocolError("invalid job")
            wire.request(job, job_id, 300000, deadline.expires_at_seconds)
        except wire.PayloadTooLarge:
            return ExecutionResult(Outcome.PAYLOAD_TOO_LARGE)
        except (wire.ProtocolError, TypeError, ValueError):
            return ExecutionResult(Outcome.PROTOCOL_ERROR)
        async with self._admission.admit(deadline) as admission:
            if not admission.accepted:
                mapping = {AdmissionOutcome.CLOSED: Outcome.CLOSED,
                           AdmissionOutcome.PARENT_EXPIRED: Outcome.DEADLINE_EXPIRED,
                           AdmissionOutcome.WAIT_EXPIRED: Outcome.ADMISSION_EXPIRED,
                           AdmissionOutcome.QUEUE_FULL: Outcome.QUEUE_FULL}
                return ExecutionResult(mapping[admission.outcome])
            if self._closed:
                return ExecutionResult(Outcome.CLOSED)
            # Admission leases count callers; these slots independently retain
            # ownership of quarantined/retiring children after callers depart.
            if len(self._slots) >= self.policy.max_active_jobs or job_id in self._slots:
                return ExecutionResult(Outcome.UNAVAILABLE)
            if deadline.expired():
                return ExecutionResult(Outcome.DEADLINE_EXPIRED)
            parent_sock = child_sock = None
            try:
                parent_sock, child_sock = socket.socketpair()
                parent_sock.setblocking(False)
            except OSError:
                for sock in (parent_sock, child_sock):
                    if sock is not None:
                        sock.close()
                return ExecutionResult(Outcome.UNAVAILABLE)
            slot = _Slot(job_id, job, deadline, parent_sock, child_sock,
                         loop.create_future(), loop.create_future())
            self._slots[job_id] = slot
            slot.task = loop.create_task(self._drive(slot), name="mooby-synthetic-supervision")
            try:
                result = await asyncio.shield(slot.task)
            except asyncio.CancelledError:
                slot.cancelled = True
                self._request_stop(slot, interrupt=True)
                # Preserve the original exception, including repeated cancellation
                # during cleanup. The independently owned task retains the slot.
                await self._settle({slot.task}, self.policy.retirement_ms / 1000 + .05,
                                   preserve_cancellation=True)
                raise
        # Admission scope cleanup and result delivery can also consume time.
        return self._delivery_result(result, deadline)

    @staticmethod
    def _delivery_result(result, deadline):
        if result.outcome is Outcome.SUCCESS and deadline.expired():
            return ExecutionResult(Outcome.DEADLINE_EXPIRED, reaped=result.reaped)
        return result

    def _request_stop(self, slot, *, interrupt=False):
        if slot.retirement is None:
            slot.retirement = Deadline.after(self.policy.retirement_ms / 1000)
        slot.stop.set()
        if interrupt and slot.task is not None and not slot.task.done() and slot.thread is not None:
            # Closing an fd alone need not wake an already registered sock_recv.
            # Cancel our owned IO task, never the caller; it handles retirement.
            slot.task.cancel()
        try:
            slot.parent_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        if not interrupt:
            slot.parent_socket.close()

    async def _settle(self, tasks: set[asyncio.Task], seconds: float, *, preserve_cancellation=False):
        end = Deadline.after(seconds)
        pending = {task for task in tasks if not task.done()}
        while pending and not end.expired():
            try:
                _, pending = await asyncio.wait(pending, timeout=end.remaining_seconds())
            except asyncio.CancelledError:
                # Used only under an already propagating cancellation or shutdown.
                # Absolute cleanup end is never restarted.
                if not preserve_cancellation:
                    raise

    def _post(self, callback, *args):
        try:
            self._loop.call_soon_threadsafe(callback, *args)
        except RuntimeError:
            pass  # Closing loop cannot stop thread-side deadline/retirement.

    def _phase(self, slot, phase, attempt):
        if slot.attempt is attempt and slot.phase is not Lifecycle.QUARANTINED:
            slot.phase = phase

    def _started(self, slot, success, attempt):
        if slot.attempt is not attempt:
            return
        if not slot.started.done():
            slot.started.set_result(success)
        if success:
            self._phase(slot, Lifecycle.RUNNING, attempt)

    def _finished(self, slot, report, attempt):
        # Publication is the terminal linearization point for this attempt.
        # The report and completion future are installed atomically on the loop.
        if (self._slots.get(slot.job_id) is not slot or slot.attempt is not attempt
                or slot.finished.done()):
            return
        slot.report = report
        slot.finished.set_result(report)

    def _supervise(self, slot, existing, attempt):
        """Only this bounded thread touches multiprocessing lifecycle methods."""
        process = slot.process
        report = _Report(False)
        try:
            if not existing:
                process = self._context.Process(target=worker, args=(slot.child_socket,), daemon=True)
                slot.process = process
                if slot.stop.is_set() or slot.deadline.expired():
                    process.close()
                    report = _Report(True)
                    self._post(self._started, slot, False, attempt)
                    return
                process.start()  # May block: never run on the asyncio loop.
                slot.child_socket.close()
                self._post(self._started, slot, True, attempt)
            retirement = None
            terminated = killed = False
            while True:
                code = process.exitcode
                if code is not None:
                    process.join(timeout=0)
                    process.close()
                    report = _Report(True, code, terminated or killed)
                    break
                if slot.stop.is_set() or slot.deadline.expired():
                    if retirement is None:
                        retirement = slot.retirement or Deadline.after(self.policy.retirement_ms / 1000)
                        self._post(self._phase, slot, Lifecycle.CANCELLING, attempt)
                    remaining = retirement.remaining_seconds()
                    elapsed = self.policy.retirement_ms / 1000 - remaining
                    grace = self.policy.cancellation_grace_ms / 1000
                    if elapsed >= grace and not terminated:
                        self._post(self._phase, slot, Lifecycle.TERMINATING, attempt)
                        process.terminate()
                        terminated = True
                    if elapsed >= grace + (self.policy.retirement_ms / 1000 - grace) / 2 and not killed:
                        process.kill()
                        killed = True
                    if retirement.expired():
                        break
                # No unbounded joins or per-operation executor submissions.
                if slot.stop.is_set():
                    time.sleep(POLL_SECONDS)
                else:
                    slot.stop.wait(POLL_SECONDS)
        except BaseException:
            # A failed/blocked startup is uncertain: retain its slot rather than
            # assuming that no child was created and spawning replacements.
            self._post(self._started, slot, False, attempt)
            if process is None:
                report = _Report(True)  # Construction failed before any start attempt.
        finally:
            slot.child_socket.close()
            self._post(self._finished, slot, report, attempt)

    async def _drive(self, slot):
        outcome, output = Outcome.UNAVAILABLE, None
        if slot.stop.is_set() or slot.deadline.expired():
            self._request_stop(slot)
            slot.child_socket.close()
            self._slots.pop(slot.job_id)
            return ExecutionResult(Outcome.CANCELLED if slot.cancelled else Outcome.DEADLINE_EXPIRED)
        try:
            slot.thread = Thread(target=self._supervise, args=(slot, False, slot.attempt),
                                 name="mooby-synthetic-supervisor", daemon=True)
            slot.thread.start()
            async with asyncio.timeout(slot.deadline.remaining_seconds()):
                if not await asyncio.shield(slot.started):
                    outcome = Outcome.DEADLINE_EXPIRED if slot.deadline.expired() else Outcome.UNAVAILABLE
                else:
                    budget = min(300000, math.floor(slot.deadline.remaining_seconds() * 1000))
                    if budget <= 0:
                        raise TimeoutError()
                    data = wire.request(slot.job, slot.job_id, budget, slot.deadline.expires_at_seconds)
                    await self._loop.sock_sendall(slot.parent_socket, data)
                    value = await wire.receive_async(slot.parent_socket, wire.MAX_RESPONSE_BYTES)
                    value = wire.validate_response(value, slot.job_id, slot.job.kind.value)
                    mapping = {"none": Outcome.SUCCESS, "execution_error": Outcome.CHILD_ERROR,
                               "deadline_expired": Outcome.DEADLINE_EXPIRED,
                               "payload_too_large": Outcome.PAYLOAD_TOO_LARGE}
                    outcome = mapping[value["error"]]
                    output = value["output"] if outcome is Outcome.SUCCESS else None
                    if slot.deadline.expired():
                        outcome, output = Outcome.DEADLINE_EXPIRED, None
            slot.phase = Lifecycle.COMPLETED if outcome is Outcome.SUCCESS else Lifecycle.FAILED
        except TimeoutError:
            outcome = Outcome.DEADLINE_EXPIRED
        except wire.PayloadTooLarge:
            outcome = Outcome.PAYLOAD_TOO_LARGE
        except (wire.ProtocolError, OSError, ValueError):
            outcome = Outcome.PROTOCOL_ERROR
        except asyncio.CancelledError:
            outcome = Outcome.CANCELLED
            slot.cancelled = True
        except Exception:
            outcome = Outcome.UNAVAILABLE
        finally:
            self._request_stop(slot)
            if slot.thread is None or slot.thread.ident is None:
                slot.child_socket.close()
                self._finished(slot, _Report(True), slot.attempt)  # No supervisory thread ever started.
        # Independent cleanup timeout: never borrow the future DB cleanup budget.
        end = slot.retirement
        while (slot.thread is not None and (slot.thread.is_alive() or not slot.finished.done())
               and not end.expired()):
            try:
                await asyncio.sleep(min(POLL_SECONDS, end.remaining_seconds()))
            except asyncio.CancelledError:
                slot.cancelled = True
                outcome = Outcome.CANCELLED
        if slot.cancelled:
            outcome, output = Outcome.CANCELLED, None
        if (slot.report is not None and not slot.report.forced
                and slot.report.exitcode not in (None, 0) and outcome in {
                Outcome.SUCCESS, Outcome.PROTOCOL_ERROR}):
            outcome, output = Outcome.CHILD_CRASH, None
        if self._finalize(slot):
            return self._delivery_result(ExecutionResult(outcome, output, True), slot.deadline)
        slot.phase = Lifecycle.QUARANTINED
        return ExecutionResult(Outcome.RETIREMENT_INCOMPLETE, cause=outcome)

    def _finalize(self, slot):
        if slot.task is not None and not slot.task.done() and asyncio.current_task() is not slot.task:
            return False
        if ((slot.thread is not None and slot.thread.is_alive()) or not slot.finished.done()
                or slot.report is None or not slot.report.reaped):
            return False
        if self._slots.pop(slot.job_id, None) is not None:
            slot.phase = Lifecycle.REAPED
            self._reaped += int(slot.report.exitcode is not None)
        return True

    async def reap_quarantined(self) -> ShutdownResult:
        """Explicit bounded retry, same slots/process handles; never spawn replacements."""
        self._bind()
        attempts = []
        for slot in tuple(self._slots.values()):
            if slot.phase is not Lifecycle.QUARANTINED or (slot.thread and slot.thread.is_alive()):
                continue
            if self._finalize(slot):
                continue
            if not slot.finished.done():
                continue  # Thread exit alone does not publish its terminal report.
            slot.attempt = object()
            slot.finished = self._loop.create_future()
            slot.report = None
            slot.retirement = Deadline.after(self.policy.retirement_ms / 1000)
            slot.phase = Lifecycle.TERMINATING  # Claim retry ownership before yielding.
            slot.thread = Thread(target=self._supervise, args=(slot, True, slot.attempt),
                                 name="mooby-synthetic-supervisor", daemon=True)
            attempts.append(slot)
            try:
                slot.thread.start()
            except RuntimeError:
                self._finished(slot, _Report(False), slot.attempt)
                slot.phase = Lifecycle.QUARANTINED
        try:
            end = Deadline.after(self.policy.retirement_ms / 1000)
            while any(s.thread and (s.thread.is_alive() or not s.finished.done())
                      for s in self._slots.values()) and not end.expired():
                await asyncio.sleep(min(POLL_SECONDS, end.remaining_seconds()))
        finally:
            for slot in tuple(self._slots.values()):
                if slot in attempts or slot.phase is Lifecycle.QUARANTINED:
                    if self._finalize(slot):
                        continue
                    slot.phase = Lifecycle.QUARANTINED
        return ShutdownResult(not self._slots, self.snapshot())

    async def shutdown(self) -> ShutdownResult:
        self._bind()
        self._closed = True
        self._admission.close()
        for slot in tuple(self._slots.values()):
            self._request_stop(slot, interrupt=True)
        tasks = {s.task for s in self._slots.values() if s.task is not None}
        await self._settle(tasks, self.policy.retirement_ms / 1000 + .05)
        for slot in tuple(self._slots.values()):
            self._finalize(slot)
        return ShutdownResult(not self._slots, self.snapshot())
