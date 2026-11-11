# R02: opt-in local diagnostic sink

Implemented locally, tested with temporary files, and **not activated or deployed**.
No plist change, service restart, ngrok change, upload, database or Redis operation
is part of this package. This is Development instrumentation, not an availability
or durability guarantee.

## Data flow and isolation

R01 creates an allowlisted record. If the optional sink is active, it validates
that record, encodes compact UTF-8 JSON and attempts `put_nowait` into a bounded
queue. One daemon writer owns all diagnostic directory/file I/O and rotation.
Request threads never create, read, chmod, rotate, write or flush diagnostic files.
Validation/encoding and short queue/counter locks do run on the submitting thread.
There is no queue-space wait and no disk operation under a request-side lock.

Only the direct R01 submission path is attached: no handlers are added to root,
Uvicorn, ngrok, or any other logger. Raw child stdout/stderr remain discarded.
Without opt-in, the existing R01 INFO logger behavior is preserved. Repeated app
creation does not start threads; lifespan startup shares one process-local sink,
and nested lifespans release it only when their last user exits. A writer that is
still blocked after shutdown is not replaced with another thread in that process.
An exclusive private lock file also prevents two processes rotating this location.

## Schema and limits

Only the exact [R01 schema](request_observability.md), including FIX01 transport
fields, is accepted. Stage events require the two timing fields. Extra fields,
unknown routes/methods/categories, invalid UUIDv4 IDs, non-UTC timestamps, non-finite
durations and malformed types are rejected. Records are normalized and limited to
4096 encoded bytes. No stringification of arbitrary objects, exceptions or raw
logging records occurs. Bodies, headers, tokens, DB URLs, station selections,
source text and authors never enter the sink. No metric labels are introduced.

Defaults and permitted configuration:

| Setting | Default | Range |
|---|---|---|
| `capacity` | 1024 records | 1–8192 |
| `max_bytes` | 1,000,000 bytes per file | 4096–100,000,000 |
| `backups` | 3 | 1–10 |
| `flush_seconds` | 0.5 seconds | 0–2 |

The queue holds at most capacity × 4096 payload bytes (4 MiB by default), plus
bounded queue/object overhead and one in-flight payload. Producer-local encoding
uses additional temporary memory. This is not a process-wide RSS limit.
On overflow the **newest record is dropped**. No API response fails because of a
drop. There is no background retry buffer or recursive diagnostic logging.

## Files, permissions and retention

The fixed location relative to this repository is:

```text
.local/services/diagnostics/api-requests.jsonl
.local/services/diagnostics/api-requests.jsonl.1
.local/services/diagnostics/api-requests.jsonl.2
.local/services/diagnostics/api-requests.jsonl.3
.local/services/diagnostics/writer.lock
```

The diagnostic directory is mode 0700; files/lock are 0600, owned by the API user.
Parents are created as 0700 if missing. Existing parent permissions are not widened;
foreign-owned or group/world-writable parent components are rejected. Descriptor-
relative opens reject symlinks, non-regular files and multiply linked files.
No diagnostic path can be supplied through JSON or environment configuration.

Rotation occurs **before** a record would exceed the configured size. `.1` is the
newest backup; the oldest is removed. Only this fixed filename and its numbered
backups are managed. Reducing backup count prunes known surplus backups 1–10 on
next startup. Existing files from a larger historical size setting can remain
larger until they rotate out; lowering a limit does not truncate their content.
Default newly managed retention is approximately four million bytes, plus lock
and filesystem overhead. Retention is size-based, not day-based.

No uploads or fsync durability promise: `written` means the complete record was
accepted by OS writes, not that it survives power failure. Rotation can remove
previously written records. A failed partial write can leave an incomplete last
line; next startup refuses to append to such a file rather than combining records.
An operator must review/archive that file while the service is stopped before
retrying. Do not edit operational files automatically or print raw OS errors.

## Internal status and failure behavior

`mooby.api.diagnostic_sink.diagnostic_status()` and
`app.state.diagnostic_status()` return safe process-local snapshots. These are
internal callables, not HTTP endpoints. Running them in a new Python process does
**not** inspect the running API. No public metrics route is added.

Counters:

- `submitted`: submissions to the configured sink, including rejected records.
- `written`: complete successful writes; never incremented for dropped records.
- `dropped`: invalid, full-queue, failed-writer and shutdown-rejected submissions.
- `invalid_records`, `queue_full`: reasons for drops, subsets of `dropped`.
- `sink_failures`, `initialization_failures`: writer/initialization failures.
- `shutdown_timeouts`: bounded joins that ended while the writer remained alive.
- `pending`, `inflight`: queue depth and current write; snapshots can be transient.
- `writer_state`: starting/running/draining/stopped/failed; `writer_alive` and
  `accepting` describe lifecycle separately.
- `enabled`: a lifespan currently owns the sink; after shutdown the last sink's
  snapshot remains available with enabled false.
- `configuration_failures`: rejected opt-in/limits in the current process.

Sink counters reset on a new sink lifecycle (including process restart);
configuration failures reset on process restart. They are not written as R01 events.
A positive `written` count does not prove all records were retained or delivered.
At quiescence after shutdown, submitted = written + dropped, unless work remains
in-flight/pending. Missing terminal events can also indicate a drop or process
termination, not necessarily an unfinished request. Counter export/alerting is
future work; do not infer absence of data loss from files alone.

Invalid configuration disables the sink without failing startup. Directory/open/
rotation/write failures stop the writer, count failure/drop outcomes and reject
later submissions cheaply until a future application lifecycle/restart. There is
no automatic retry loop. Sink problems do not affect liveness, readiness, auth or
data-serving responses. API and PostgreSQL dependency behavior is unchanged.

On shutdown, acceptance stops and the writer drains within the configured join
budget. A stuck filesystem call cannot be killed safely: the daemon may remain
blocked, its pending/in-flight work is reported, and those records may be lost
when the process exits. A later unblocked write may complete; it is not counted
as dropped preemptively. SIGKILL/crash can lose all queued records and counters.

## Manual activation — not performed

After review/deployment approval, add this optional member to the **existing
ignored** `ops/launchd/development-api.json`, preserving all other settings:

```json
"diagnostics": {
  "enabled": true,
  "capacity": 1024,
  "max_bytes": 1000000,
  "backups": 3,
  "flush_seconds": 0.5
}
```

The supervisor forwards only enabled/numeric diagnostic settings to the API child
as `MOOBY_DIAGNOSTIC_*` variables. Local `.env` diagnostic flags/paths and inherited
logging variables are not forwarded. Invalid nested settings set a safe disabled
configuration indicator; they do not invalidate the database/token configuration.
The tunnel child still receives only its previous base environment.

No installation or plist update is needed. The owner must separately authorize
and perform the normal API LaunchAgent restart after deploying reviewed source;
this package has not done so. Do not start a duplicate manual API or restart worker/
tunnel merely to activate diagnostics. Direct manual API use can opt in with
`MOOBY_DIAGNOSTIC_ENABLED=1` and the numeric environment variables corresponding
to the table, but running that service is outside local preparation.

Once activated and after test requests, review only the sanitized diagnostic file:

```bash
cd /Users/naufal/Moobby_back
tail -n 50 -f .local/services/diagnostics/api-requests.jsonl
```

Absence of a file can mean disabled/invalid config, failed initialization or no
successful startup. Inspect safe in-process counters through an approved local
debugging mechanism when available; do not print `.env` or enable raw access logs.

## Tests and rollback

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=tests .venv/bin/python -B -m unittest \
  test_diagnostic_sink test_request_observability test_mock_feed \
  test_api_diagnostic test_api_launchd test_original_text \
  test_automatic_publication -v
```

Tests use temporary files, controlled writer doubles and in-memory ASGI. No live
API, LaunchAgent, ngrok, model or database operation is invoked.

Rollback: set `diagnostics.enabled` to false/remove the optional member, then use a
separately authorized API restart. Existing files remain private and bounded; no
database migration or data rollback is involved. For source rollback, reverse only
R02 changes to supervisor/observability and remove R02-specific module/tests/docs;
preserve R01 and FIX01. Do not delete the lock file while a writer is running.
