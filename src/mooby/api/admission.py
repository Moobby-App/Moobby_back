"""Standalone asyncio admission control; not connected to API execution.

All operations belong to one event loop. There are no locks, execution tasks,
threads or processes. Capacity is owned lexically through ``async with admit``.
"""

from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import AsyncIterator

from mooby.api.deadline_policy import Deadline, DeadlinePolicy


class AdmissionOutcome(StrEnum):
    ACCEPTED = "accepted"
    QUEUE_FULL = "queue_full"
    WAIT_EXPIRED = "wait_expired"
    PARENT_EXPIRED = "parent_deadline_expired"
    CLOSED = "controller_closed"
    CANCELLED = "request_cancelled"


class AdmissionCancelled(asyncio.CancelledError):
    """Controlled cancellation which retains asyncio's cancellation semantics."""

    outcome = AdmissionOutcome.CANCELLED


@dataclass(frozen=True)
class AdmissionState:
    active_jobs: int
    waiting_jobs: int
    available_active_capacity: int
    closed: bool


class AdmissionLease:
    """One active slot. Release is synchronous, idempotent and loop-affine."""

    def __init__(self, controller: AdmissionController):
        self._controller = controller
        self._released = False

    @property
    def released(self) -> bool:
        return self._released

    def release(self) -> bool:
        return self._controller._release(self)


@dataclass(frozen=True)
class AdmissionResult:
    outcome: AdmissionOutcome
    lease: AdmissionLease | None = None

    @property
    def accepted(self) -> bool:
        return self.outcome is AdmissionOutcome.ACCEPTED


@dataclass(eq=False)
class _Waiter:
    future: asyncio.Future[AdmissionResult]
    parent: Deadline
    deadline: Deadline
    timer: asyncio.TimerHandle | None = None
    queued: bool = True


class AdmissionController:
    def __init__(self, policy: DeadlinePolicy):
        self._policy = policy
        self._loop: asyncio.AbstractEventLoop | None = None
        self._active: set[AdmissionLease] = set()
        self._waiters: deque[_Waiter] = deque()
        self._closed = False

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("admission controller belongs to another event loop")
        return loop

    def snapshot(self) -> AdmissionState:
        self._bind_loop()
        return AdmissionState(len(self._active), len(self._waiters),
                              self._policy.max_active_jobs - len(self._active), self._closed)

    def close(self) -> bool:
        """Reject/wake waiters; active owners must still exit their contexts."""
        self._bind_loop()
        if self._closed:
            return False
        self._closed = True
        for waiter in tuple(self._waiters):
            self._finish(waiter, AdmissionResult(AdmissionOutcome.CLOSED))
        return True

    @asynccontextmanager
    async def admit(self, parent: Deadline) -> AsyncIterator[AdmissionResult]:
        """Yield an internal result; always release accepted capacity on exit.

        Cancellation is re-raised as a CancelledError subtype, never returned as
        an ordinary result. A caller must check result.accepted before doing work.
        """
        try:
            result = await self._acquire(parent)
            try:
                yield result
            finally:
                if result.lease is not None:
                    result.lease.release()
        except asyncio.CancelledError:
            raise AdmissionCancelled() from None

    def _new_lease(self) -> AdmissionResult:
        lease = AdmissionLease(self)
        result = AdmissionResult(AdmissionOutcome.ACCEPTED, lease)
        self._active.add(lease)
        return result

    def _release(self, lease: AdmissionLease) -> bool:
        self._bind_loop()
        if lease not in self._active:
            return False
        self._active.remove(lease)
        lease._released = True
        self._drain()
        return True

    @staticmethod
    def _expiry(waiter: _Waiter) -> AdmissionOutcome | None:
        # Parent wins ties, including after a permit was handed off but before
        # its task resumed. All deadline calculations retain the parent's clock.
        if waiter.parent.expired():
            return AdmissionOutcome.PARENT_EXPIRED
        if waiter.deadline.expired():
            return AdmissionOutcome.WAIT_EXPIRED
        return None

    def _remove(self, waiter: _Waiter):
        if waiter.queued:
            self._waiters.remove(waiter)
            waiter.queued = False
        if waiter.timer is not None:
            waiter.timer.cancel()
            waiter.timer = None

    def _finish(self, waiter: _Waiter, result: AdmissionResult):
        self._remove(waiter)
        if not waiter.future.done():
            waiter.future.set_result(result)

    def _arm_timer(self, waiter: _Waiter):
        waiter.timer = self._bind_loop().call_later(
            waiter.deadline.remaining_seconds(), self._on_timeout, waiter)

    def _on_timeout(self, waiter: _Waiter):
        if not waiter.queued:
            return
        outcome = AdmissionOutcome.CLOSED if self._closed else self._expiry(waiter)
        if outcome is not None:
            self._finish(waiter, AdmissionResult(outcome))
        else:
            # Event-loop timers may fire early. Recheck the absolute clock;
            # neither an early wakeup nor a new stage grants a fresh allowance.
            self._arm_timer(waiter)

    def _drain(self):
        # Prune cancelled futures even while all active slots remain occupied.
        for waiter in tuple(self._waiters):
            if waiter.future.done():
                self._remove(waiter)
        while not self._closed and self._waiters and len(self._active) < self._policy.max_active_jobs:
            waiter = self._waiters[0]
            outcome = self._expiry(waiter)
            result = AdmissionResult(outcome) if outcome is not None else self._new_lease()
            self._finish(waiter, result)

    async def _acquire(self, parent: Deadline) -> AdmissionResult:
        loop = self._bind_loop()
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise asyncio.CancelledError()
        if self._closed:
            return AdmissionResult(AdmissionOutcome.CLOSED)
        if parent.expired():
            return AdmissionResult(AdmissionOutcome.PARENT_EXPIRED)
        deadline = parent.child(self._policy.admission_wait_ms / 1000)
        self._drain()
        if not self._waiters and len(self._active) < self._policy.max_active_jobs:
            return self._new_lease()
        if len(self._waiters) >= self._policy.max_waiting_jobs:
            return AdmissionResult(AdmissionOutcome.QUEUE_FULL)
        waiter = _Waiter(loop.create_future(), parent, deadline)
        self._waiters.append(waiter)
        try:
            self._arm_timer(waiter)
            result = await waiter.future
            if task is not None and task.cancelling():
                raise asyncio.CancelledError()
            if result.accepted:
                outcome = AdmissionOutcome.CLOSED if self._closed else self._expiry(waiter)
                if outcome is not None:
                    result.lease.release()
                    return AdmissionResult(outcome)
            return result
        except BaseException:
            # A released slot may already have been reserved for us before
            # cancellation is delivered. Recover that lease as well as waiters.
            if waiter.future.done() and not waiter.future.cancelled():
                result = waiter.future.result()
                if result.lease is not None:
                    result.lease.release()
            raise
        finally:
            self._remove(waiter)
