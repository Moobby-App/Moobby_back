"""Versioned, bounded JSON framing for trusted local synthetic processes only."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
import json
import math
import re
import socket
import struct

VERSION = 1
MAX_REQUEST_BYTES = 8192
MAX_RESPONSE_BYTES = 16384
HEADER = struct.Struct("!I")


class ProtocolError(ValueError):
    pass


class PayloadTooLarge(ProtocolError):
    pass


class JobType(StrEnum):
    SUCCESS = "success"
    DELAY = "delay"
    ERROR = "error"
    CRASH = "crash"
    UNRESPONSIVE = "unresponsive"
    OVERSIZE_OUTPUT = "oversize_output"
    BAD_VERSION = "bad_version"
    OVERSIZE_FRAME = "oversize_frame"
    BROKEN_IPC = "broken_ipc"
    PARTIAL_FRAME = "partial_frame"


ERRORS = frozenset({"none", "execution_error", "deadline_expired", "payload_too_large"})


@dataclass(frozen=True)
class SyntheticJob:
    kind: JobType
    text: str = "synthetic"
    delay_ms: int = 0

    def validate(self):
        if not isinstance(self.kind, JobType) or type(self.text) is not str:
            raise ProtocolError("invalid synthetic job")
        if len(self.text) > MAX_REQUEST_BYTES:
            raise PayloadTooLarge("request too large")
        if type(self.delay_ms) is not int or not 0 <= self.delay_ms <= 60000:
            raise ProtocolError("invalid synthetic duration")


def encode(value: dict, maximum: int) -> bytes:
    data = json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")
    if len(data) > maximum:
        raise PayloadTooLarge("frame too large")
    return HEADER.pack(len(data)) + data


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError("duplicate field")
        result[key] = value
    return result


def decode(data: bytes, maximum: int) -> dict:
    if len(data) > maximum:
        raise PayloadTooLarge("frame too large")
    try:
        result = json.loads(data.decode("ascii"), object_pairs_hook=_pairs,
                            parse_constant=lambda _: (_ for _ in ()).throw(ProtocolError("invalid number")))
    except (ValueError, UnicodeError, RecursionError):
        raise ProtocolError("invalid JSON") from None
    if type(result) is not dict:
        raise ProtocolError("invalid envelope")
    return result


def _identity(value: dict):
    if type(value.get("v")) is not int or value["v"] != VERSION:
        raise ProtocolError("invalid version")
    if type(value.get("id")) is not str or re.fullmatch(r"[0-9a-f]{32}", value["id"]) is None:
        raise ProtocolError("invalid job identifier")
    if type(value.get("type")) is not str or value["type"] not in {kind.value for kind in JobType}:
        raise ProtocolError("invalid job type")


def request(job: SyntheticJob, job_id: str, budget_ms: int, expires_at_seconds: float) -> bytes:
    job.validate()
    value = {"v": VERSION, "id": job_id, "type": job.kind.value, "budget_ms": budget_ms,
             "expires_at_seconds": expires_at_seconds,
             "input": {"text": job.text, "delay_ms": job.delay_ms}}
    validate_request(value)
    return encode(value, MAX_REQUEST_BYTES)


def validate_request(value: dict) -> SyntheticJob:
    if set(value) != {"v", "id", "type", "budget_ms", "expires_at_seconds", "input"}:
        raise ProtocolError("invalid request fields")
    _identity(value)
    if type(value["budget_ms"]) is not int or not 1 <= value["budget_ms"] <= 300000:
        raise ProtocolError("invalid execution budget")
    try:
        valid_expiry = (type(value["expires_at_seconds"]) in (int, float)
                        and math.isfinite(value["expires_at_seconds"]))
    except OverflowError:
        valid_expiry = False
    if not valid_expiry:
        raise ProtocolError("invalid monotonic expiry")
    payload = value["input"]
    if type(payload) is not dict or set(payload) != {"text", "delay_ms"}:
        raise ProtocolError("invalid input fields")
    job = SyntheticJob(JobType(value["type"]), payload["text"], payload["delay_ms"])
    job.validate()
    return job


def response(identity: dict, *, text: str = "", error: str = "none") -> bytes:
    value = {"v": VERSION, "id": identity["id"], "type": identity["type"],
             "status": "success" if error == "none" else "failure", "output": text, "error": error}
    validate_response(value, identity["id"], identity["type"])
    return encode(value, MAX_RESPONSE_BYTES)


def validate_response(value: dict, job_id: str, job_type: str) -> dict:
    if set(value) != {"v", "id", "type", "status", "output", "error"}:
        raise ProtocolError("invalid response fields")
    _identity(value)
    if value["id"] != job_id or value["type"] != job_type:
        raise ProtocolError("response identity mismatch")
    if type(value["output"]) is not str or type(value["error"]) is not str or value["error"] not in ERRORS:
        raise ProtocolError("invalid response payload")
    if value["status"] != ("success" if value["error"] == "none" else "failure"):
        raise ProtocolError("inconsistent response status")
    if value["error"] != "none" and value["output"]:
        raise ProtocolError("unexpected failure payload")
    return value


def frame_size(header: bytes, maximum: int) -> int:
    size = HEADER.unpack(header)[0]
    if size > maximum:
        raise PayloadTooLarge("frame too large")
    if size == 0:
        raise ProtocolError("empty frame")
    return size


async def receive_async(sock: socket.socket, maximum: int) -> dict:
    loop = asyncio.get_running_loop()

    async def exact(size):
        data = bytearray()
        while len(data) < size:
            chunk = await loop.sock_recv(sock, size - len(data))
            if not chunk:
                raise ProtocolError("incomplete frame")
            data.extend(chunk)
        return bytes(data)

    size = frame_size(await exact(HEADER.size), maximum)
    return decode(await exact(size), maximum)


def receive_sync(sock: socket.socket, maximum: int) -> dict:
    def exact(size):
        data = bytearray()
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                raise ProtocolError("incomplete frame")
            data.extend(chunk)
        return bytes(data)

    size = frame_size(exact(HEADER.size), maximum)
    return decode(exact(size), maximum)
