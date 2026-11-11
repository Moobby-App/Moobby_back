# R01 request observability

R01 adds request-local diagnostics only. HTTP bodies, headers, status codes,
authentication, query text/parameters and database-operation counts are unchanged.
There is no Redis, exporter, log handler, new configuration or service operation.
Existing error-contract gaps are intentionally left for R03.

## Correlation and event records

`ObservedFastAPI.build_middleware_stack()` wraps the existing Starlette stack with
pure ASGI `RequestObservabilityMiddleware`. Its placement outside the existing
server-error middleware observes the actual generated 500 response while retaining
the original exception re-raise. Non-HTTP scopes pass through untouched.

Each admitted HTTP request receives a random UUIDv4 rendered as 32 lowercase hex
characters. Incoming headers never supply this identifier. No diagnostic header or
request parameter is introduced. `current_request_id()` exposes it internally.
A ContextVar holds a separate mutable state object per request; FastAPI/AnyIO's
copied sync-thread context shares that request's state, not another request's state.
The context is reset in `finally`. State updates shared across threads use a lock.

Without a configured sink, logger `mooby.api.observability` receives INFO-level
JSON records. R01 does not configure handlers or levels. R02 adds an optional
[bounded file sink](diagnostic_sink.md) which receives validated records directly
instead of forwarding them to other logging handlers. It is disabled by default.
The LaunchAgent still discards raw child output; no operational activation has
been performed. The diagnostic file is distinct from supervisor lifecycle logs.

Events:

- `request_admitted`: before routing; route/status/category can be null.
- `request_stage`: measured stage completion, including exceptional exits.
- Exactly one terminal event on ordinary stack unwinding: `request_completed`,
  `request_failed`, `request_cancelled`, or `request_aborted`.

`request_failed` includes completed HTTP errors (401/404/422/503) as well as
unexpected failures. It does not mean that every failure was an unhandled exception.
`request_aborted` identifies an incomplete response, observed disconnect or failed
send. Cancellation is re-raised after diagnostics; existing exceptions are not
converted into new HTTP responses.

The first observed failure remains in `failure_stage` / `exception_category`.
Transport outcome is independent: any exception from the awaited ASGI send sets
`transport_aborted` and retains a controlled `transport_exception_category`.
A handled database failure followed by a failed 503 body send therefore emits
one `request_aborted` terminal event, preserving the database failure and attempted
503 status while identifying the transport failure separately. A successfully sent
503 remains `request_failed` with no transport abort. A later send failure after
partial body delivery is also an abort, never a successful completion.
Cancellation still propagates with `request_cancelled`; if it occurs during send,
the independent transport-abort fields are also set. No exception messages are
stored, and send failures do not imply an observed `http.disconnect` message.

All records contain this fixed field set:

| Field | Meaning |
|---|---|
| `event` | Controlled event name |
| `request_id` | Server UUID, correlation only; never a metric label |
| `timestamp` | UTC ISO timestamp at emission |
| `route` | One of the four known route templates, otherwise null |
| `method` | Standard method or `OTHER` |
| `status` | ASGI response-start status, or null if not observed |
| `category` | Controlled response/outcome category, or null before known |
| `duration_ms` | Monotonic elapsed time since admission |
| `failure_stage` | First observed, controlled stage name, or null |
| `exception_category` | Fixed classification, never an exception message/class name |
| `response_started` | A response-start send was attempted at this boundary |
| `disconnect_observed` | Application-requested receive returned `http.disconnect` |
| `transport_aborted` | An awaited ASGI send raised; independent of the primary failure |
| `transport_exception_category` | Controlled category of the first send exception, otherwise null |

Stage records additionally contain `stage` and `stage_duration_ms`. UTC admission
and terminal timestamps indicate processing start/end. Duration is independent of
wall-clock adjustment. Nested durations overlap and must not be added together.

Response categories are `results`, `no_match`, `source_unavailable`, `available`,
`authentication_failure`, `parameter_validation_failure`, `http_error`,
`unexpected_failure`, `cancelled`, and `aborted`.

Exception categories are `configuration_error`, `connection_error`,
`database_error`, `validation_error`, `mapping_error`, `dependency_error`,
`transport_error`, `http_error`, `unexpected_error`, and `cancellation`.
Framework-generated validation failures may have a null exception category: no
exception object is inspected or synthesized merely to provide a classification.

## Timing coverage

| Stage | Measurement |
|---|---|
| `authentication` | Existing configuration/Bearer dependency execution |
| `endpoint_execution` | Sync endpoint function body, including its nested work |
| `feed_read` | Normal feed repository call |
| `database_connect` | Existing psycopg connect call |
| `database_session` | Connection scope including connect, operations and cleanup |
| `database_timestamp` | Timestamp execute + fetchone |
| `database_feed` | Feed execute + fetchall |
| `database_readiness` / `projection_readiness` | Existing readiness SELECT + fetch |
| `row_mapping` | Each existing `_item` call, including nested model validation |
| `response_validation` | Construction of the DisruptionFeed model |
| `response_send` | Each awaited ASGI send, without examining its body/headers |

`parameter_validation`, `fixture_read`, and `application` are also available as
failure locations. Framework parameter validation is classified from status 422;
its independent execution duration is not claimed. The innermost failure remains
recorded when existing code catches it and returns a controlled response.

Database durations are client-observed operations, not pure PostgreSQL server
execution. Buffered transfer/decoding cannot be isolated from execute/fetch timings.
No extra SQL is issued. Connection/session cleanup failures are attributed to
`database_session` unless an earlier operation already failed.

## Privacy and failure isolation

The module constructs allowlisted primitive fields. It never serializes request,
response, exception, SQL, parameters, headers, body, query string or row objects.
Unknown routes are null, rather than raw paths. Unknown stage/category inputs
cannot introduce arbitrary values. Tokens, database URLs, station selections,
post/source text and author data are absent. The previous class-only feed warnings
are replaced by correlated diagnostics with fixed classifications.

Logging/formatting/handler exceptions are swallowed without recursive logging.
Records can be lost if the logger is disabled or fails; this does not change the
HTTP response. Abrupt process termination can prevent a terminal record. See the
R02 sink documentation for queue drops, per-process counters and bounded shutdown.

## Limits intentionally deferred

- No overall deadline, database cancellation, pool, admission limit or metrics
  exporter is added.
- Endpoint timing excludes thread-pool waiting. Authentication and endpoint
  execution are timed separately; their sum is not a request scheduling metric.
- Automatic FastAPI response revalidation/JSON serialization is included in total
  elapsed time but not independently instrumented. No global encoder patch is used.
- Normal feed handlers may never call receive. No background task polls it;
  `disconnect_observed: false` means not observed, not proof of connection health.
- Successful ASGI send does not prove ngrok delivery or iOS decoding.
- No client attempt/device identifier or public correlation header is added.
- These diagnostics alone cannot identify a request that failed before FastAPI
  admission, and they do not establish the original incident's root cause.

## Local verification and rollback

Tests use in-memory ASGI and driver doubles; no sockets, services or real models:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=tests .venv/bin/python -B -m unittest \
  test_request_observability test_mock_feed test_api_diagnostic \
  test_api_launchd test_original_text test_automatic_publication -v
```

Rollback is a source-only reversal of the R01 imports/wrappers in `app.py` and
`repository.py` and removal of the new observability module/tests/documentation.
Review/reverse only the R01 diff; do not reset unrelated changes. There is no schema
or configuration rollback. Applying source changes to a running service would
require a separately authorized deployment/restart.
