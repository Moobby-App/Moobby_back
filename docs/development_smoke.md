# One-item Development worker smoke with unconditional rollback

`scripts/smoke_one_item_development.py` processes a historical snapshot UUID supplied
at runtime, with the existing worker login and genuine local classifier/embedding
models. It invokes the real `enqueue_recent_snapshots()`, `claim_batch()`, processor,
`process_and_finish_attempt()`, `finish_attempt()`, and event persistence functions.
It never runs the scheduler, opens the app, or changes worker rules, model locks,
migrations, grants, membership, database objects, or collection data.

## Isolated queue scope and time simulation

A fresh random `development-rollback-smoke-<UUID>` pipeline is checked empty in the
worker-readable queue. The existing enqueue helper gets that pipeline, exactly the
requested snapshot ID, and `batch_size=1`. Its INSERT names only
`source_snapshot_id` and `pipeline_version`, as allowed by migration 013. The
harness never INSERTs status, attempt count, or lease columns. The existing claim
helper performs the normal authorized UPDATE, filtered to that same unique pipeline
and `batch_size=1`. Both the enqueue result and claimed identity/owner are asserted.
No existing pipeline is used, so unrelated queued/expired work cannot be claimed.

The dispatcher retains its real database-clock/captured-at filter. To include the
one historical snapshot, the harness passes a lookback of its capture age rounded
up plus 60 seconds. Snapshot-ID filtering remains mandatory; this is not broad
historical discovery. A missing/future capture timestamp or enqueue mismatch fails.

Only the NLP input copy is time-simulated: `dataclasses.replace` sets publication
time to database-now minus one minute and capture time to database-now in memory.
Raw text, source IDs, source digest, and publication basis remain unchanged. Nothing
updates collection timestamps, parser rules, model thresholds, or path effective
dates. Existing processor gates must still accept the item and create a valid draft.
Unsupported inputs fail; no rejected event is forced into publication.

## Worker checks inside the rollback transaction

One `conn.transaction(force_rollback=True)` contains enqueue, claim, processing,
embedding, persistence and worker checks. All helper transactions are nested
savepoints; they cannot commit the outer scope. Models are local-only and real;
no artifact download is attempted.

The worker lacks general event/run/evidence inspection access and cannot read
`app_api.published_disruptions`. The narrow conflict-target SELECT grants recorded
in migration 017 (`events.event_id`, the three `event_sources` conflict columns,
and `event_evidence.evidence_id`) were already applied manually in Development,
as reported by the project owner; see [embedding operations](embedding_operations.md#worker-conflict-target-select-grants).
They do not allow the event/vector/run readback needed for full inspection.
The smoke transaction therefore checks:

- Exactly one successful processing-run, event, version, source and evidence INSERT,
  each with driver `rowcount == 1` (conflicts/no-op inserts fail this smoke).
- The actual bound embedding has 384 finite normalized components; model digest,
  model/input versions and event/run linkage match processor provenance.
- The unchanged event INSERT includes `verified`/`published` and database-transaction
  timestamps. Successful execution means database constraints accepted it.
- This unique queue contains exactly one item and finishes as `succeeded`.

These are **successful-write/payload/constraint checks**, not independent SQL
readback of event state or proof of gateway visibility. Only the queue is read by
the worker. Diagnostic instrumentation is harness-only and does not grant access,
change SQL in the worker, add RETURNING clauses, or switch roles.

## Rollback and independent residue inspection

Even full success rolls back the outer worker transaction. Then the worker checks
that its queue namespace is absent. Only **after the outer rollback**, the harness
opens a separate existing read-only NLP inspection connection, with a default
read-only session and a read-only, rollback-only transaction. It counts queue,
run, notes, event, version, source and evidence for the unique namespace/event;
all seven counts must be zero. The inspection connection is closed afterwards.

The inspector cannot see uncommitted smoke data and is never used to validate
in-transaction persistence or gateway visibility. No connection queries `app_api`
in this harness. The inspector needs existing SELECT access to the relevant NLP
objects only; it needs no worker writes or source text. No inspector grants are
created. If approved inspection credentials are unavailable, configure them through
the project owner separately; the harness fails closed rather than weakening the
worker or claiming that all residue was checked. Missing inspector configuration
is rejected before opening the worker connection.

Exceptions trigger the same outer rollback and post-rollback inspection. A failed
inspection produces `rollback_verified: false`, never success. Interrupts unwind
the rollback scope and close the worker connection; they may not produce a final
verification report. Abrupt disconnection cannot produce a successful check claim.

## API verification is separate

The API role cannot see uncommitted rows from the worker connection. No grants,
role changes, new view/RPC, or transaction commits are used to bypass that boundary.
The rollback smoke intentionally makes **no API visibility claim**.

Run the existing offline API contract test separately:

```bash
PYTHONPATH=tests .venv/bin/python -m unittest \
  test_automatic_publication.AutomaticPublicationTests.test_existing_projection_and_api_allowlist -v
```

It checks projection eligibility rules, station-ID/affected-range filtering and
response privacy using a mocked repository row; it is not a live integration test.
For a separately authorized live read-only gateway check, use already committed,
eligible data. Rolled-back smoke events must never appear there. The unchanged
latest-version, verified/published, location and 24-hour filters still apply.

## Manual use — Development only

Independently confirm the exact Development host/database and both logins. Target
flags are an owner-supplied allowlist; the harness cannot infer Production from
host naming. Both DSNs must explicitly match the same expected Development
host/database. `service`, `hostaddr` and multi-host settings are rejected.

Supply privately, through local secret management:

- `SMOKE_DEV_DATABASE_URL`: the existing worker login connection, inheriting the
  existing queue/source/reference/persistence rights. Its PostgreSQL `current_user`
  and `session_user` must both be `mooby_nlp_worker_login_dev` from migration 008.
  Elevated/inspection logins are not accepted for processing. The DSN username may
  include a pooler suffix; it is validated separately from the PostgreSQL role.
- `SMOKE_DEV_INSPECTION_DATABASE_URL`: an existing read-only NLP inspector for the
  **post-rollback** checks. Its DSN and PostgreSQL role are independently checked.

There is no fallback to generic database, API or worker environment URLs. Neither
DSN is printed or placed in command arguments. Pooler connection usernames and SQL
roles may differ; both `current_user` and `session_user` must match the corresponding
expected SQL role. The connected host and `current_database()` are also checked.
No SET ROLE or grants are performed. Required migrations/privileges must already
be deployed; the harness applies none.

Load the existing worker configuration locally if needed:

```bash
cd /Users/naufal/Moobby_back
set -a
source .env
set +a
```

With both dedicated DSNs supplied separately, replace placeholders below:

```bash
.venv/bin/python scripts/smoke_one_item_development.py \
  --snapshot-id HISTORICAL_SNAPSHOT_UUID \
  --expected-dev-host YOUR_REVIEWED_DEVELOPMENT_HOST \
  --expected-dev-database YOUR_DEVELOPMENT_DATABASE \
  --expected-dev-user YOUR_WORKER_CONNECTION_USERNAME_WITH_POOLER_SUFFIX \
  --expected-dev-session-user mooby_nlp_worker_login_dev \
  --expected-dev-inspection-user YOUR_READ_ONLY_INSPECTOR_CONNECTION_USERNAME \
  --expected-dev-inspection-session-user YOUR_READ_ONLY_INSPECTOR_POSTGRES_ROLE \
  --confirm-development-rollback
```

The result reports worker persistence/payload checks, dimension, rollback status
and the seven zero residual counts. It contains no raw text, quotes, snapshot IDs,
model vectors, connection URLs, SQL text or parameters. Error diagnostics allow only
stage, SQLSTATE and optional driver schema/table/column names; diagnostic messages,
detail/hint/context and exception text are excluded. The first driver error is
preserved if helpers convert it to an outcome or an aborted-transaction error
follows. A separate `rollback_check_error` can describe inspection failure, and
`rollback_verified` is false on that path.

## Stage labels and local tests

| Operation | Stage |
|---|---|
| Worker identity | `check_database_session_identity` |
| Timeouts | `set_statement_timeout`, `set_lock_timeout` |
| Empty namespace check | `preflight_worker_queue_count` |
| Source/clock/catalog | `load_source_snapshot`, `read_database_clock`, `load_station_catalog`, `load_confirmed_paths` |
| Existing helper enqueue | `insert_smoke_queue` |
| Existing helper claim | `claim_smoke_queue` |
| Queue completion | `finish_queue_update` |
| Audit inserts | `insert_processing_run`, `insert_processing_note` |
| Event inserts | `insert_event`, `insert_event_version`, `insert_event_source`, `insert_event_evidence` |
| Completed queue | `inspect_completed_smoke_queue` |
| Worker queue after rollback | `post_rollback_worker_queue_count` |
| Inspector initialization | `post_rollback_inspector_read_only`, `post_rollback_inspector_identity` |
| Inspector residue counts | `post_rollback_row_counts.<schema>.<table>` |

Local tests use transaction/SQL doubles, actual queue/processor/finish/persistence
code, and model doubles. They assert unrelated queue work is not claimed, least-
privilege enqueue columns, worker-readable-only queries, rollback on success and
failure, and inspector use only after rollback. They do not connect to a database.

```bash
PYTHONPATH=tests .venv/bin/python -m unittest test_development_smoke -v
```

The harness was **not run against a database** during implementation.
