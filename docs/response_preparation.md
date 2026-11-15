# R06-04: synchronous response-preparation extraction

This is a behavior-preserving extraction, not process-isolation activation.
`response_preparation.py` has no FastAPI request/response objects, environment
lookup, request IDs, logging, timers, execution workers or connection creation.
All routes remain synchronous with their existing FastAPI/thread-pool execution.
No whole-request deadline is enforced and database work is not independently
cancelled or terminated.

## Characterized baseline before extraction

| Concern | Existing behavior retained |
|---|---|
| Normal feed | One PostgreSQL connection, timestamp SELECT and feed SELECT |
| Feed filtering | Supplied station-array overlap, no sorting/deduplication or route expansion in Python |
| Ordering | Repository order: freshness anchor descending, public event ID tie-break |
| Pagination | Repository receives requested limit and fetches limit + 1; only limit rows are mapped |
| Envelope | Existing Pydantic model, 24-hour window, exact messages/fields, database generated_at |
| Lookahead | Extra row determines has_more but is not mapped/validated as an item |
| Errors | Existing READ_ERRORS family becomes controlled 503 at the feed route; unexpected errors propagate |
| Readiness | Timestamp query then full feed projection/filter/order using LIMIT 0, up to two connections |
| Short circuit | Unavailable database skips the projection probe |
| Partial readiness | Database may remain available while projection is unavailable |
| HTTP | Existing 200/401/422/503 schemas, headers and authentication precedence |

The original 225-test suite was run successfully before changes. No live database
or services were inspected to establish this baseline. SQL and connection behavior
are verified with existing definitions and driver doubles, not live measurements.

## Types and function boundaries

- `FeedQuery` is frozen data: ordered station-ID tuple, limit, offset. Values are
  already validated by the existing route. It does not introduce another parser,
  normalization policy or stricter validation. Direct internal callers must supply
  validated values. Existing query bounds remain 1–30 stations, limit 1–50 and
  offset 0–1000, plus the unchanged station-ID pattern.
- `load_feed(query, reader=...)` calls a typed `FeedReader` once. It forwards a
  list of station IDs and the original pagination values, without adding one to
  the limit itself. Its intermediate result is the existing `FeedPage`.
- `prepare_item(row)` maps explicitly selected projection fields to the existing
  `DisruptionItem`, including `StationOrSegment` and `AppSource` validation.
- `prepare_feed(query, page, item_mapper=..., feed_builder=...)` maps the requested
  rows and validates the existing `DisruptionFeed`. It never sorts, filters again,
  reconstructs timestamps, catches errors or fabricates a successful empty feed.
- `probe_readiness(database_probe=..., projection_probe=..., on_failure=...)`
  coordinates the existing ordered probes and READ_ERRORS boundary. Its frozen
  `ReadinessStatus` contains two availability flags and an optional controlled
  internal failure-stage label, with no exception or connection object.
- `prepare_readiness(status)` constructs the existing `Readiness` model. Model
  construction remains outside the probe exception boundary, as before.

Outputs preserve existing UUID, datetime, URL, nullable-field and serialization
semantics. `model_dump(mode="json")` / `model_dump_json()` provide the existing
JSON representation; no separate response serializer is introduced. No output
size limit, content rewriting or truncation is added. Approved original_text is
preserved exactly, including whitespace and Unicode; absent legacy text stays
null. Internal raw_text, evidence quotes, author data, snapshot IDs, embeddings
and queue metadata are not mapped into output.

Pagination also retains existing edge behavior: next_offset may exceed 1000 when
a lookahead row exists at the final allowed offset. This package does not change
that pre-existing contract or claim the resulting later page is requestable.

## Repository and HTTP adapters

`app.py` supplies local `functools.partial` adapters for existing repository
functions. Only these local dependencies bind database URL and timeout settings;
they are not part of input/output data and must never be serialized as jobs.
No open database connection or FastAPI object is captured in the data contracts.
The preparation module depends on the existing repository's FeedPage/READ_ERRORS
definitions; importing it performs no database operation.

`repository.py`, SQL strings, statement count, parameter order, connection
contexts, `connect_timeout` and startup `statement_timeout` options are unchanged.
The existing APP_API timeout defaults/ranges remain unchanged as well.

Authentication, station validation, query parsing, HTTP status/headers, JSONResponse
creation, configuration-error pagination handling, ASGI sending and request
lifecycle remain in the HTTP adapter. `source_unavailable` is still HTTP 503,
never a false healthy `no_match`. Readiness keeps its distinct response schema;
`failure_stage` is internal and not emitted to the app. Liveness and mock-route
logic remain unchanged.

## Error and observability compatibility

Feed loading, row mapping and envelope validation propagate original exception
types to the unchanged route-level READ_ERRORS handler. Readiness catches only
that same family and preserves the successful first-probe flag on a second-probe
failure. RuntimeError and other unexpected programming failures are not suppressed.

The optional readiness observer receives the original escaped exception only
transiently. The adapter passes existing `record_failure`; it is not a diagnostic
payload or stored result. Probes returning False do not cause a new observer call,
preserving repository-owned first-failure diagnostics. No raw error messages,
credentials or source content are logged by preparation.

Existing wrappers remain in app.py: `row_mapping` surrounds item preparation,
`response_validation` surrounds `_feed`, and `feed_read` surrounds the delegated
repository read. The last excludes row mapping/envelope construction, just as
before. Delegation introduces ordinary function-call overhead but no new measured
stage or expanded database timing boundary. Request IDs, stage names, error
classification, exactly-one terminal event and FIX01 transport-abort behavior are
unchanged. Automatic FastAPI response validation/serialization still occurs in
the existing framework path. R01/R02 code and record schemas are untouched.

## Future isolation boundary and limitations

R06-03 remains synthetic: no new job type, process-manager import or real database
worker exists here. R06-05/R06-06 need separately approved dependency provisioning,
wire validation, credentials delivery, deadline/cancellation ownership and route
orchestration. The local reader/probe/mapper/builder/observer callables are not wire
representations, and credentials must not be placed in future job payloads.

The synthetic protocol limits requests to 8192 JSON bytes and responses to 16384.
Normal feed text is not bounded by those limits; even one original_text can exceed
the response cap. Therefore existing feed responses cannot be safely plugged into
that protocol unchanged. This remains a blocker for later IPC integration until
an explicit transport/size policy is approved. Do not silently truncate, drop
items, replace live data with fixtures or weaken validation to fit the protocol.

This extraction does not improve connection reuse, cancel synchronous PostgreSQL
work, enforce an HTTP deadline, or resolve the original iOS loading incident.

## Local verification and rollback

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=tests .venv/bin/python -B -m unittest test_response_preparation -q
```

Tests use deterministic data, injected callables, in-memory ASGI requests and
driver doubles. Database/network/process creation is forbidden in standalone
preparation tests. Only the separate existing process-manager regression suite
spawns its owned synthetic children.

Rollback: review and reverse only the R06-04 app.py delegation changes and remove
the new preparation module, focused test and this document. Preserve every earlier
R06 file, including untracked files. No SQL, migration, dependency, runtime config,
service or deployment rollback is needed. A runtime restart is not part of this
package and was not performed.
