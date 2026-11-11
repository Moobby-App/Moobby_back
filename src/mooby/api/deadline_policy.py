"""Inactive deadline-budget foundation. No environment reads or execution hooks."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, fields
from math import isfinite
import re
from time import monotonic


class DeadlinePolicyError(ValueError):
    """Invalid policy; messages contain controlled field names, never input values."""


Clock = Callable[[], float]
MAX_ALLOWANCE_MS = 300_000
PSYCOPG_MIN_CONNECT_SECONDS = 2


def _finite_number(value: float, name: str) -> float:
    if type(value) not in (int, float):
        raise DeadlinePolicyError(f"{name} must be finite")
    try:
        result = float(value)
    except OverflowError:
        raise DeadlinePolicyError(f"{name} must be finite") from None
    if not isfinite(result):
        raise DeadlinePolicyError(f"{name} must be finite")
    return result


def _duration(value: float) -> float:
    result = _finite_number(value, "duration_seconds")
    if result < 0:
        raise DeadlinePolicyError("duration_seconds must be nonnegative")
    return result


@dataclass(frozen=True)
class Deadline:
    """Absolute monotonic deadline in seconds; children retain this clock domain."""

    expires_at_seconds: float
    clock: Clock = field(default=monotonic, repr=False, compare=False)

    def __post_init__(self):
        _finite_number(self.expires_at_seconds, "expires_at_seconds")
        if not callable(self.clock):
            raise DeadlinePolicyError("clock must be callable")

    @classmethod
    def after(cls, duration_seconds: float, *, clock: Clock = monotonic) -> Deadline:
        duration = _duration(duration_seconds)
        if not callable(clock):
            raise DeadlinePolicyError("clock must be callable")
        now = _finite_number(clock(), "clock")
        return cls(now + duration, clock)

    def remaining_seconds(self) -> float:
        now = _finite_number(self.clock(), "clock")
        if now >= self.expires_at_seconds:
            return 0.0
        return _finite_number(self.expires_at_seconds - now, "remaining_seconds")

    def expired(self) -> bool:
        return self.remaining_seconds() == 0.0

    def child(self, allowance_seconds: float) -> Deadline:
        duration = _duration(allowance_seconds)
        now = _finite_number(self.clock(), "clock")
        end = _finite_number(now + duration, "expires_at_seconds")
        return Deadline(min(self.expires_at_seconds, end), self.clock)


def _integer(value: object, name: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise DeadlinePolicyError(f"{name} is outside its integer range")
    return value


@dataclass(frozen=True)
class DeadlinePolicy:
    """Provisional millisecond allowances, not live API configuration.

    Construction checks internal arithmetic. validate_for_database() must also be
    called with the actual ApiSettings timeouts before any future integration.
    """

    enabled: bool = False
    admission_wait_ms: int = 100
    max_active_jobs: int = 2
    max_waiting_jobs: int = 4
    dispatch_ms: int = 400
    aggregate_connect_ms: int = 3000
    database_transport_margin_ms: int = 250
    database_operation_ms: int = 3250
    response_construction_ms: int = 500
    cleanup_ms: int = 1000  # Per connection, inside preparation.
    preparation_slack_ms: int = 500
    feed_prepare_ms: int = 12_000
    readiness_prepare_ms: int = 16_000
    response_send_ms: int = 1000  # Outside preparation; whole response, not per chunk.
    cancellation_grace_ms: int = 250
    retirement_ms: int = 1500  # Total after cancellation starts, including grace.

    def __post_init__(self):
        if type(self.enabled) is not bool or self.enabled:
            raise DeadlinePolicyError("deadline activation is unsupported")
        for item in fields(self):
            if item.name.endswith("_ms"):
                minimum = 0 if item.name in {"admission_wait_ms", "cancellation_grace_ms"} else 1
                _integer(getattr(self, item.name), item.name, minimum, MAX_ALLOWANCE_MS)
        _integer(self.max_active_jobs, "max_active_jobs", 1, 64)
        _integer(self.max_waiting_jobs, "max_waiting_jobs", 0, 1024)
        if (self.max_waiting_jobs == 0) != (self.admission_wait_ms == 0):
            raise DeadlinePolicyError("admission_wait_ms and max_waiting_jobs conflict")
        if self.database_operation_ms <= self.database_transport_margin_ms:
            raise DeadlinePolicyError("database_operation_ms must exceed transport margin")
        if self.feed_prepare_ms < self.minimum_preparation_ms(connection_count=1):
            raise DeadlinePolicyError("feed_prepare_ms cannot fit stage allowances")
        if self.readiness_prepare_ms < self.minimum_preparation_ms(connection_count=2):
            raise DeadlinePolicyError("readiness_prepare_ms cannot fit stage allowances")
        if self.retirement_ms < self.cancellation_grace_ms + self.cleanup_ms:
            raise DeadlinePolicyError("retirement_ms cannot fit cancellation and cleanup")

    @classmethod
    def from_mapping(cls, values: Mapping[str, object]) -> DeadlinePolicy:
        """Parse explicit policy keys only, never environment variables or aliases.

        Integers accept int or canonical ASCII decimal strings. No whitespace,
        signs, leading zeros, floating point, interpolation or truthy coercion.
        """
        allowed = {item.name for item in fields(cls)}
        if set(values) - allowed:
            raise DeadlinePolicyError("unknown deadline policy field")
        parsed = {}
        for name, value in values.items():
            if name == "enabled":
                if value is False or (type(value) is str and value == "false"):
                    parsed[name] = False
                else:
                    raise DeadlinePolicyError("deadline activation is unsupported")
            elif type(value) is str:
                if len(value) > 6 or re.fullmatch(r"0|[1-9][0-9]*", value, flags=re.ASCII) is None:
                    raise DeadlinePolicyError(f"{name} must be a canonical integer")
                parsed[name] = int(value)
            else:
                parsed[name] = value
        return cls(**parsed)

    def minimum_preparation_ms(self, *, connection_count: int) -> int:
        """Both current DB paths have two SELECTs; readiness has two connections."""
        _integer(connection_count, "connection_count", 1, 2)
        return (self.admission_wait_ms + self.dispatch_ms
                + connection_count * self.aggregate_connect_ms
                + 2 * self.database_operation_ms + self.response_construction_ms
                + connection_count * self.cleanup_ms + self.preparation_slack_ms)

    def validate_for_database(
        self, *, connect_timeout_seconds: int, statement_timeout_ms: int
    ) -> None:
        """Compatibility check only; never adjusts existing PostgreSQL settings.

        The aggregate connect allowance includes all attempts and DNS. It is not
        a claim that psycopg itself enforces that wall-clock limit.
        """
        _integer(connect_timeout_seconds, "connect_timeout_seconds", 1, 15)
        _integer(statement_timeout_ms, "statement_timeout_ms", 100, 10_000)
        required_connect = max(PSYCOPG_MIN_CONNECT_SECONDS, connect_timeout_seconds) * 1000
        if self.aggregate_connect_ms < required_connect:
            raise DeadlinePolicyError("aggregate_connect_ms is incompatible with PostgreSQL timeout")
        if self.database_operation_ms < statement_timeout_ms + self.database_transport_margin_ms:
            raise DeadlinePolicyError("database_operation_ms is incompatible with PostgreSQL timeout")
