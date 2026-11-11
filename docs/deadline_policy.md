# R06-01: inactive deadline-budget foundation

`src/mooby/api/deadline_policy.py` contains pure calculations and explicit-mapping
validation only. API routes, ApiSettings, repositories and the LaunchAgent do not
import it. It reads neither `.env` nor `os.environ`, starts no timers, threads or
processes, and changes no HTTP behavior. No service activation is provided.
`enabled` defaults to false; requesting true is rejected, not merely ignored.

These are proposed R05 engineering allowances, not approved operational deadlines
or measured performance. No new environment variables are exposed. Mapping keys
below are Python policy fields, not aliases for existing API environment variables.

| Field | Proposed default | Meaning |
|---|---:|---|
| `enabled` | false | Only false is supported |
| `max_active_jobs` | 2 | Proposed execution capacity; creates nothing |
| `max_waiting_jobs` | 4 | Proposed bounded waiting capacity |
| `admission_wait_ms` | 100 | Inside preparation |
| `dispatch_ms` | 400 | Inside preparation |
| `aggregate_connect_ms` | 3000 | DNS and all connection attempts, per connection |
| `database_transport_margin_ms` | 250 | Additional allowance beyond statement timeout |
| `database_operation_ms` | 3250 | Each SELECT including transfer/retrieval |
| `response_construction_ms` | 500 | Mapping, validation, serialization |
| `cleanup_ms` | 1000 | Per connection, inside preparation |
| `preparation_slack_ms` | 500 | Inside preparation |
| `feed_prepare_ms` | 12000 | Admission through prepared response/normal cleanup |
| `readiness_prepare_ms` | 16000 | Same boundary, two connections |
| `response_send_ms` | 1000 | Outside preparation; entire response, not per chunk |
| `cancellation_grace_ms` | 250 | Inside retirement allowance |
| `retirement_ms` | 1500 | Total after cancellation starts, including grace/cleanup |

All `_ms` fields are integer milliseconds, bounded above by 300000. Required
allowances must be positive; admission and cancellation grace may be zero.
Active capacity is 1–64 and waiting capacity 0–1024. Zero waiting capacity requires
zero admission wait, and vice versa. These are validation ceilings, not sizing
recommendations. Neither capacity is used by the live application.

Both current database paths execute two SELECTs. Minimum preparation is:

```text
admission_wait + dispatch + connection_count * aggregate_connect
  + 2 * database_operation + response_construction
  + connection_count * cleanup + preparation_slack
```

Connection count is one for feed and two for readiness. The totals must fit their
respective preparation allowances. Retirement must fit cancellation grace plus
cleanup. Sending is separate and cannot consume a new preparation allowance.
The arithmetic reserves allowances; it does not schedule or enforce them.

`DeadlinePolicy.from_mapping()` accepts only known field names and integer values
or canonical ASCII decimal strings. It rejects signs, surrounding whitespace,
leading zeros, floats, booleans as integers, unknown fields and interpolation.
`enabled` accepts boolean false or the exact string `false`. Error messages do
not echo supplied values or unknown keys. Mapping and environment are unchanged.

## Explicit PostgreSQL compatibility check

Construction validates internal combinations only. Before future integration,
the caller must separately validate against the actual existing ApiSettings:

```python
policy = DeadlinePolicy.from_mapping(proposed_policy_values)
policy.validate_for_database(
    connect_timeout_seconds=settings.connect_timeout_seconds,
    statement_timeout_ms=settings.statement_timeout_ms,
)
```

There is no automatic environment/settings lookup. Required aggregate connection
allowance is `max(2, connect_timeout_seconds) * 1000`, reflecting the inspected
psycopg minimum. Required operation allowance is statement timeout plus transport
margin. Insufficient allowances are rejected; none are silently increased.
Larger PostgreSQL timeouts may require larger policy totals. Recheck the driver
minimum when changing psycopg/libpq versions.

Existing settings remain untouched: `APP_API_CONNECT_TIMEOUT_SECONDS` defaults to
3 seconds, range 1–15; `APP_API_STATEMENT_TIMEOUT_MS` defaults to 3000 milliseconds,
range 100–10000. Existing parsing grammar and supervisor forwarding are unchanged.
The stricter policy grammar does not apply to those environment variables.

Connection timeout applies per attempt and does not independently cover all DNS
work. Statement timeout is a PostgreSQL statement-stage bound, not an overall HTTP
deadline. Proposed aggregate allowances need future supervision/process isolation
to become enforceable. Killing local work would not prove remote query termination.

## Pure absolute deadlines

`Deadline.after(seconds, clock=...)` samples an injectable monotonic clock once.
`remaining_seconds()` clamps to zero, and `expired()` is true at exact expiry.
`child(seconds)` uses `min(parent_expiry, now + allowance)` and retains the same
clock; deriving nested stages never extends a parent. Zero durations are valid.
Negative/nonfinite durations, invalid clocks and nonfinite timestamps are rejected.
The clock must be monotonic; arbitrary clock epochs are supported. The module does
not detect clock rollback or promise hard real-time scheduling. Do not transfer
absolute timestamps to another clock domain without an explicit protocol.

## Verification, dependencies and rollback

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=tests .venv/bin/python -B -m unittest test_deadline_policy -v
```

Future admission control, execution isolation, cancellation, payload bounds, send
supervision and R01/R02 diagnostics require separate authorization and packages.
None exists in R06-01. In particular, policy compatibility does not prove live
pooler support or bounded cleanup. Owner approval is still needed for activation,
capacity, deadlines and overload/deadline HTTP semantics.

Rollback consists solely of removing this package's new module, focused test and
document after reviewing its diff. Do not reset existing R01–R04 work. There is no
runtime configuration, service, SQL or migration rollback.
