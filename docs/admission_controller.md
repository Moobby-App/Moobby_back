# R06-02: inactive bounded admission controller

`src/mooby/api/admission.py` is a standalone asyncio component. No API route,
supervisor, repository or R01/R02 logger imports or activates it. It creates no
execution tasks, threads, processes or database connections. R06-01's `enabled`
flag remains false; there is no runtime activation switch in this package.

Explicit construction uses the existing immutable `DeadlinePolicy`. Provisional
defaults are two active jobs, four waiting jobs and 100 ms admission wait. These
are not measured capacity limits. The controller creates a bounded FIFO deque
of futures, not a task queue or executor. Each queued waiter owns one cancellable
event-loop timer. No threading primitives or background polling are used.

## Ownership and outcomes

Future callers must use the lexical context manager and inspect its result:

```python
controller = AdmissionController(policy)
async with controller.admit(parent_deadline) as result:
    if result.accepted:
        # Future execution belongs here; none is implemented by this module.
        pass
    else:
        # Internal outcome only; no HTTP mapping exists yet.
        pass
```

Normal internal outcomes are `accepted`, `queue_full`, `wait_expired`,
`parent_deadline_expired` and `controller_closed`. Native `asyncio.CancelledError`
propagates without replacement or suppression, both while queued and inside an
accepted scope. An enclosing `asyncio.timeout()` can therefore translate its own
cancellation into `TimeoutError`; external task cancellation remains cancellation.
Cancellation is never converted into a normal result. The former custom
`AdmissionCancelled` exception has been removed: no active routes used it, and it
interfered with Python 3.12 timeout handling.

`AdmissionOutcome.CANCELLED` (`request_cancelled`) remains a classification label
for future callers catching native cancellation, not a raised exception or a
returned admission result. Callers must re-raise cancellation after classifying
it. Native exception messages are preserved, so diagnostics must use this fixed
label and never log the exception or its message. This module does not log.

Accepted results own a lease. Context exit releases it on success, exception or
cancellation, without awaiting cleanup. Explicit `lease.release()` is idempotent:
the first call releases, later calls return false. Do not release early while work
still uses the resource. Do not manually drive or abandon a context manager;
there is deliberately no garbage-collection or task-finalizer release mechanism.

All public operations, including snapshots and release, must run on the owning
event loop. The controller binds on first use and rejects another loop. It is not
thread-safe and cannot be shared across processes. No operation awaits in the
middle of counter updates or a permit handoff.

## Deadlines and race handling

A waiting deadline is derived once as `parent.child(admission_wait_ms / 1000)`.
The same R06-01 clock is retained. Relative event-loop timers wake the controller,
but expiry is checked against the absolute deadline; early timers are rearmed
with remaining time, never a fresh full allowance. Tests can inject the parent
clock and control timer delivery without arbitrary sleeps.

Expired parents are rejected even when capacity is free. Immediate admission
checks the parent again after child-deadline creation and queue draining, directly
before granting the lease, with no intervening await. Clock advancement during
those operations cannot bypass this final check. Otherwise, free capacity admits
immediately; zero-waiting mode rejects occupied capacity as `queue_full`.
Waiting is bounded by the earlier of parent expiry and admission expiry. Parent
expiry wins ties. Closure takes precedence over expiry for queued requests.

FIFO applies to live, unexpired queued waiters. Cancelled waiters are removed;
new callers cannot bypass existing waiters. A released slot is reserved before its
waiter resumes. If that waiter is cancelled, expires or observes closure before
entry, the reserved lease is reclaimed and passed onward. Context exit releases
capacity if cancellation occurs after entry. Late timer callbacks cannot revoke
a delivered lease. The clock can still advance after the final decision; there
is no atomic real-time clock/slot operation. Normal asyncio scheduling latency
still applies; this is not a hard real-time deadline or an execution deadline
after admission.

`close()` is synchronous and idempotent. It prevents new admissions, wakes all
queued waiters as closed and cancels their timers. Already entered active scopes
remain counted until their owners exit. It does not stop or pretend to finish
active jobs, and provides no forced-shutdown execution mechanism. Callers own
active-job cancellation/draining. Undelivered grants are reclaimed when their
waiting coroutines resume or are cancelled.

## Safe state and limitations

`snapshot()` returns an immutable `AdmissionState`: `active_jobs`, `waiting_jobs`,
`available_active_capacity`, and `closed`. It contains no request/user identifiers,
station selections, credentials or payloads. A slot reserved for a waiter counts
as active even before its coroutine resumes. Cancelled futures are pruned during
their cleanup or the next admission/release. The bounded deque permits bounded
linear removal; there is no unbounded storage or executor submission.

The existing deadline policy requires a monotonic, valid clock. Production callers
must not substitute a frozen test clock. The controller does not cancel backend
execution, enforce PostgreSQL timeouts, integrate diagnostics, or map rejection to
HTTP 503. Those require separately authorized packages. No API configuration or
HTTP behavior changes here.

## Local verification and rollback

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=tests .venv/bin/python -B -m unittest test_admission -v
```

Tests use isolated event loops, events, controlled clocks and timer callbacks;
one short timer test checks actual loop timeout delivery. They need no service or
network access. Rollback is removal of this module, its focused test and this
document after reviewing the diff. Preserve all earlier work; there is no service,
environment, database, migration or configuration rollback.
