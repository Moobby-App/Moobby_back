# R06-03: inactive bounded synthetic process manager

This component is standalone and unactivated. No API routes, repositories,
authentication, supervisor configuration, SQL or R01/R02 logging schema import or
use it. It accepts only explicitly enumerated synthetic jobs. It is not a worker
CLI, model runner, database executor or HTTP endpoint. No service installation or
activation command is provided.

## Architecture and ownership

`process_manager.py` combines the existing R06-02 `AdmissionController` with an
independent bounded registry of occupied process slots. `process_protocol.py`
defines the wire contract, and `synthetic_process.py` is the fixed importable
spawn target and function allowlist. No caller may supply a callable, module,
process identifier or entry-point name.

`ProcessManager(policy)` creates no threads or processes. Explicit
`await manager.execute(SyntheticJob(...), parent_deadline)` performs admission,
then creates one disposable child and one supervisory thread for that slot.
Children are not persistent pool workers; no process replacement occurs
automatically. The supervisor owns **all** multiprocessing lifecycle operations:
construction, `start`, exit polling, `terminate`, `kill`, `join(timeout=0)` and
`close`. The event loop handles bounded nonblocking Unix-socket IPC and deadlines.
This thread is necessary because `Process.start()` can block independently of
coroutine cancellation. There is no thread executor or second submission queue.

Creation uses `multiprocessing.get_context("spawn")`, never fork. This avoids
copying live parent database connections and thread state. Python's spawn
machinery uses trusted local pickle internally for its fixed target/socket
descriptor; job/result IPC never deserializes pickle. Standard-library spawn may
also start its runtime resource-tracker helper. That helper is separate from the
execution-slot cap and exits with its owning Python runtime; no custom external
supervisor or persistent service is created.

Each manager binds to a single event loop. Future integration must own one
manager per intended capacity domain; multiple instances/Uvicorn workers would
multiply aggregate capacity. This package does not implement a global OS quota.

## API and capacity

- `execute(job, parent)` returns an immutable `ExecutionResult`.
- Cancelling the task awaiting `execute` requests retirement and re-raises native
  cancellation; there is no arbitrary-PID cancellation API.
- `snapshot()` returns immutable numeric capacity/lifecycle data.
- `shutdown()` rejects new work, wakes admission waiters, requests retirement and
  returns `ShutdownResult(complete, state)` after a bounded wait.
- `reap_quarantined()` explicitly collects late completion or retries retirement
  of the **same** tracked child. It never starts replacement children.

Policy defaults are two active admission leases, four waiters and 100 ms waiting.
The process manager imposes an additional hard maximum of **two execution slots**;
policies above two are rejected. One is supported for tests/smaller use. The
R06-01 policy and its operational timeout settings remain unchanged.

The admission lease counts the caller, not proof of child exit. A cancelled caller
can release its lease while its process slot remains occupied. Starting, retiring,
and quarantined slots all count against the process cap. If all slots remain
occupied, new admitted callers receive `process_unavailable`. No second waiting
queue is introduced. Slot capacity returns only after confirmed reaping and
supervisory-thread exit, or confirmed failure before any thread/process start.

Lifecycle states are starting, running, completed, failed, cancelling, terminating,
reaped and quarantined. Reaped slots are removed; no unbounded job history is kept.
The cumulative reaped counter counts actual exit-code-confirmed child retirements,
not validation rejections or construction failures.

## Version 1 IPC

Communication uses one anonymous Unix socket pair per slot, not a TCP listener.
Each frame is a four-byte unsigned network-order length followed by ASCII JSON
(`ensure_ascii=True`, compact separators, no NaN). Maximum serialized JSON sizes:

| Direction | JSON bytes | Additional framing |
|---|---:|---:|
| Request | 8192 | 4 bytes |
| Response | 16384 | 4 bytes |

Receivers validate the length **before reading/allocating the body**. Partial
headers/bodies are asynchronously cancellable in the parent. Empty/oversize
frames, duplicate keys, invalid JSON, unknown fields, types, versions, identities,
statuses and error categories are rejected. Object shape and byte bounds are both
checked. Failure responses cannot carry output text.

Request fields: `v`, `id` (server-created random 32-character hex), `type`,
`budget_ms`, `expires_at_seconds`, `input` (`text`, `delay_ms`). The worst-case
request is size-checked before spawning; the actual frame cannot have a larger
budget field. Input is synthetic text only, with a bounded character precheck
and a stricter serialized-byte check. Delay is an integer 0–60000 ms.

Response fields: `v`, matching `id` and `type`, `status` (`success`/`failure`),
`output`, `error`. Error values are fixed: `none`, `execution_error`,
`deadline_expired`, `payload_too_large`. Oversize worker output produces a small
controlled failure, never truncation or an oversized transmission.

Allowed jobs are success, cooperative delay, controlled exception, abrupt exit,
unresponsive execution, oversized output, and explicit protocol-fault jobs
(bad version, oversize frame, broken IPC, partial frame). Protocol-fault branches
send only fixed synthetic faults for validation tests. They are not network API
features. No functions, imports or arbitrary code are accepted through messages.

## Deadlines and cancellation

The execution deadline is a child of the supplied parent, capped by the policy's
feed-preparation allowance (12000 ms by default), beginning **before admission**.
Admission waiting and startup therefore consume that same absolute budget. The
manager does not restart a timeout for each stage. Success is rechecked against
the same absolute deadline after required reaping/thread cleanup and again at
public delivery, after admission-scope cleanup. Equality with expiry is expired.
An expired successful result becomes `deadline_expired`, preserving reaping status
and discarding output. No await separates the final expiry check and return
decision. Cleanup is still required even when the parent has expired.

Real executions require Python's same-host system monotonic clock domain. Both
absolute expiry and remaining budget are transmitted. The child clamps to the
earlier absolute expiry and its received remaining budget, so time spent sending
IPC cannot extend the parent. Custom clock epochs are suitable only for isolated
pre-launch tests, not real cross-process execution. Synthetic unresponsive jobs
deliberately ignore cooperative expiry to test forced retirement.

Native `CancelledError` propagates, preserving exception identity/message where
applicable. `asyncio.timeout()` can translate its own cancellation to TimeoutError.
The manager signals the supervisor, cancels its owned IPC task to unblock reads,
and waits only a bounded cleanup period. Repeated cancellation during that period
does not reset the cleanup deadline or replace the original caller exception.
Cancellation while admission is queued has no child to retire. Cancelling
shutdown or explicit reaping also propagates natively; already tracked work is
not abandoned.

## Retirement, quarantine and shutdown

The first stop request fixes one absolute retirement deadline. Default total:
1500 ms. For an active process the supervisor allows 250 ms cancellation grace,
then calls that owned process's `terminate()`. If needed, it calls `kill()` halfway
through the remaining retirement interval. It polls exit without indefinite joins;
only after an exit code exists does it join with timeout zero and close the handle.
The future 1000 ms database connection-cleanup allowance is **not used** for these
synthetic jobs. Normal completion also requires confirmed reaping before success.

Blocked startup, blocked lifecycle calls or failed retirement can outlive this
attempt. The result is `retirement_incomplete` with a controlled cause, and the
slot becomes quarantined. It retains its process handle, supervisory thread (if
still alive) and capacity. Do not equate a returned timeout/cancellation with
process termination. A supervisor which returns late remains explicitly tracked.
Explicit collection/retry is bounded, serialized per slot, and never substitutes
a new child. Cancellation of a retry restores quarantine accounting.

Every supervision/retirement attempt has a fresh opaque ownership token, a
completion future and one immutable terminal report. A supervisor captures its
token when started; callbacks carry that token. Report publication linearizes in
`_finished()` on the owning event loop: validate slot/token ownership, install the
report and resolve its future without yielding. Duplicate terminal reports and
reports from stale attempts are ignored. Confirmed successful reaping cannot be
replaced by a later failure report.

Thread exit alone is insufficient to start a retry. An unpublished terminal
report retains capacity, and collection waits only its bounded allowance. A new
attempt is permitted only after prior publication and thread exit, with a known
unsuccessful report. Successful publication instead leads to finalization, never
another read of the closed process handle. A failure to start a retry thread
publishes a failed terminal report so future explicit retries remain possible.

Slot release linearizes at registry removal in `_finalize()`, requiring the
current report's resolved future, confirmed reaping and supervisory-thread exit
(and no unfinished drive owned by another task). It increments the reaped count
at most once. Shutdown during pending publication reports incomplete; cancellation
does not discard the slot or authorize a premature retry. If the event loop can
never accept a terminal callback, state remains unknown and capacity stays held.

Shutdown is incomplete while any occupied slot remains. Active processes and
queued callers are signalled; there is no claim that shutting down the manager
forcibly terminates OS-uninterruptible work. Supervisory threads are daemon threads
so Python cannot forcibly join them; normal tested paths explicitly finish them.
Manager owners must call/await shutdown, inspect its result, and retain incomplete
managers. Abrupt interpreter/OS termination, suspended clocks, kernel stalls,
startup internals that fail after partially launching, and hard memory/RSS limits
are outside this guarantee. Uncertain startup is conservatively quarantined.

Terminating a local process does **not** prove remote PostgreSQL work has ended.
This package runs no database operations. It makes no hard real-time guarantee.

## Results, diagnostics and security

Internal outcomes include success, child execution error/crash, deadline expiry,
request cancellation, protocol violation, payload too large, process unavailable,
retirement incomplete, manager closed, admission queue full and admission expiry.
Caller cancellation uses native propagation; `request_cancelled` is also used for
manager-owned cancellation outcomes. There is no HTTP mapping.

Snapshot fields are occupied slots, starting/running counts, cumulative reaped
count, quarantined/unreaped count, available capacity, closed state, admission
waiters and supervisory-thread count. Occupied slots include reserved startup
capacity, not just children with a PID. No payload, identity, PID or exception
message appears in snapshots. No R01/R02 schema changes or new logging exist.

IPC peers are trusted local processes. Never submit credentials, DSNs, bearer
tokens, user bodies or social-media content even as synthetic text. Spawn initially
inherits OS environment; the child clears its own environment at worker entry
without modifying the parent's. Child stdout/stderr are discarded before job
handling; caught exceptions become fixed error categories, without traceback or
raw messages. There is no sandbox against hostile Python code or a hostile local
user. Byte bounds are not an OS-level memory quota.

## Local verification and future integration

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=tests .venv/bin/python -B -m unittest test_process_manager -v
```

Tests use real spawn children for ordinary lifecycle/IPC/deadline paths and gated
lifecycle doubles for blocked startup, failed retirement, delayed publication and
cleanup races. Every test attempts owned-resource cleanup in `finally`, even after
a shutdown exception or assertion failure. Fallback termination is restricted to
handles collected from that test's managers, not arbitrary new child processes.
Cleanup waits are bounded; unresolved resources are reported without suppressing
the original test failure. These are not live database/service tests.

R06-04 may later extract the existing response preparation without changing its
contract. R06-05 must separately design real database cancellation and provenance
transfer; this synthetic allowlist must not become an arbitrary-callable executor.
Route integration, secrets delivery, overall response limits and service ownership
need separately approved work. No such integrations are implemented here.

Rollback: review and remove only the four new modules/test files plus this document
(`process_manager.py`, `process_protocol.py`, `synthetic_process.py`,
`test_process_manager.py`, `process_manager.md`). Preserve earlier work. There is
no configuration, service, SQL, migration or deployment rollback to perform.
