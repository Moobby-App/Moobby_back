# App API Development Contract

This backend slice implements the first, route-scoped **Path A** read endpoint. It does not publish drafts, access `collection`, run Path B vector search, or start ngrok automatically. For the local Development tunnel and Mac migration steps, see the repository [README](../README.md).

## Route

`GET /api/v1/disruptions?station_ids=open-krl:THB&station_ids=open-krl:PLM&limit=20&offset=0`

- `station_ids`: 1–30 canonical `reference.krl_stations.station_id` values in the selected route; current accepted form is `open-krl:<UPPERCASE_CODE>`.
- `limit`: 1–50, default 20.
- `offset`: 0–1000, default 0.
- Authorization: `Bearer` token configured server-side as `APP_API_BEARER_TOKEN`. It must be at least 32 characters. A shared token is for private Development smoke tests only; do not embed it in a distributable app.

Example response shape (synthetic values):

```json
{
  "availability": "results",
  "generated_at": "2026-11-06T10:00:00Z",
  "window_hours": 24,
  "message": "Laporan gangguan yang cocok ditemukan.",
  "items": [
    {
      "public_event_id": "00000000-0000-4000-8000-000000000001",
      "title": "Gangguan layanan KRL di Stasiun Contoh",
      "summary": "Ringkasan kurasi dari field terstruktur.",
      "station_or_segment": {
        "kind": "station",
        "line": null,
        "station": "Stasiun Contoh",
        "station_id": "open-krl:CONTOH",
        "start_station": null,
        "start_station_id": null,
        "end_station": null,
        "end_station_id": null,
        "direction": null,
        "affected_station_ids": ["open-krl:CONTOH"]
      },
      "severity": null,
      "event_time": null,
      "event_time_certainty": "unknown",
      "freshness": "fresh",
      "certainty": null,
      "source": {
        "display_name": "Web",
        "url": "https://example.invalid/source",
        "published_at": "2026-11-06T09:00:00Z"
      }
    }
  ],
  "limit": 20,
  "offset": 0,
  "next_offset": null
}
```

`no_match` is a successful response with an empty `items` array. `source_unavailable` is returned with HTTP 503 when the API configuration or database dependency cannot provide a reliable result. Authentication errors remain HTTP 401. Location IDs come from the canonical station reference and the view's confirmed-path expansion; the app can use `affected_station_ids` to place events on its selected-route timeline without matching names. The response does not expose `path_id`; it intentionally omits incident category until the taxonomy is defined.

## Eligibility and data boundary

Migration `014_app_api_read_contract.sql` adds nullable, constrained `event_versions.severity` and creates a latest-version projection. The view returns only the latest event version when it is `verified` and `published`, and only while the database-clock freshness anchor is within the preceding 24 hours. Freshness is `fresh` through 8 hours and `stale` after 8 through 24 hours. It projects curated event fields, canonical location names and IDs, confirmed-path affected station IDs, platform-level source display name, HTTPS source URL, and source publication time. Migration 018 extends this projection with approved nullable `original_text`. Internal quote fields/offsets, separate author metadata, snapshot IDs, model output, notes, path IDs and queue data remain excluded.

The API login should use `mooby_app_api_login_dev`, whose only application data access is through `mooby_app_api_gateway_dev` and the single view. Set that login password out of band in Development and store its URL in the server-only `APP_API_DATABASE_URL`. Never reuse the worker or migration URL. The role setup assumes the existing `moobby_incident_readonly` role; stop if that prerequisite is absent. Migration 014 has not been applied by this code change.

New processor outcomes are publication-accepted only when `result_status="succeeded"` and a valid `draft_event` is present. The unchanged processor intake/provenance, keyword, station/confirmed-path, relevance/severity thresholds, structural, and exact-evidence checks still apply. Queue success alone is insufficient; classification may succeed without a valid draft. Skip/rejection/error/non-disruption/threshold-failure/no-draft outcomes do not publish.

The worker inserts accepted new versions as `verified`/`published` with both timestamps set to `transaction_timestamp()`, atomically with the queue outcome, run, provenance, and evidence. `verified` means machine-accepted, not officially confirmed. A compatible 384-dimensional embedding is created automatically after all processor gates and before the atomic INSERT; no human review, separate verifier, or correlation/cluster prerequisite is introduced. Embedding failures produce `transient_embedding_error` and retry/fail without an event. Existing drafts remain untouched on conflict. The API remains read-only and keeps its field allowlist, station-ID filtering, affected range, freshness, and latest-version rules.

Deployment prerequisites (not applied here): migration 004 requires vectors and the worker now supplies them with a versioned derived-text input. The tracked migration 015 relaxes NOT NULL; its application state cannot be inferred from Git. Forward migration `016_required_event_embedding_publication.sql` restores/reasserts NOT NULL and grants only INSERT on `embedding`, `embedding_input_version`, `verification_status`, `publication_status`, `verified_at`, `published_at`. Null historical rows block that migration rather than being changed. No roles, membership, RLS, ownership, or collection objects change. See [embedding operations](embedding_operations.md) for manual Development steps and read-only verification. Local tests use provider/transaction doubles; actual database readiness and real model inference are not established by those tests.

For a short, explicitly enabled local UI mock test, `GET /api/v1/mock/disruptions` supports Development-only synthetic fixtures. `GET /api/v1/disruptions` always reads the normal database feed, even when mock flags are enabled. It runs the CSV text through the processor in memory and marks each returned title/source as `SIMULASI`; it does not write to PostgreSQL. Enable it only with `APP_API_ENVIRONMENT=development`, `APP_API_SYNTHETIC_FIXTURE_MODE=development`, and a 1–240 minute `APP_API_SYNTHETIC_FIXTURE_TTL_MINUTES`. The mode ends automatically after that process TTL. It is not part of the normal published-event feed.

## Local configuration and launch

Set these server-side values in the ignored local `.env` or a secret manager:

```dotenv
APP_API_DATABASE_URL=postgresql://<app-api-login>:<password>@<development-db-host>:5432/postgres
APP_API_BEARER_TOKEN=<private-development-token-at-least-32-characters>
APP_API_CONNECT_TIMEOUT_SECONDS=3
APP_API_STATEMENT_TIMEOUT_MS=3000
```

Load them into the shell and start the local API from the repository root:

```bash
set -a
source .env
set +a
.venv/bin/uvicorn mooby.api.app:app --host 127.0.0.1 --port 8000
```

The app's base URL must be configured per build environment in the iOS project, not hard-coded here. A reserved NGROK domain can keep the Development URL stable; expose the tunnel only after the API authentication flow has been configured. Do not put the NGROK authtoken, database URL, password, or shared server bearer token into the app bundle. The current bearer implementation is a private Development gate, not production user authentication.

The iOS app must map its selected route stations to canonical `open-krl:*` IDs before calling this endpoint. No display-name or array-position fallback is implemented. Path B/free-text search is not part of this slice.

## Optional macOS service supervision

See [API/ngrok LaunchAgents](development_api_launchd.md) for the opt-in localhost
API and stable-domain tunnel setup. The supervisor reads only the two API secrets
from local `.env`, keeps all credentials out of arguments/plists/logs, and refuses
to create a random-domain tunnel when no stable API domain is configured. No agent
is installed or started by the prepared setup.

## Read-only diagnosis: count succeeds but API/readiness returns 503

A successful `count(*) = 0` is an empty result, not a readiness failure. With the
repository view contract, an empty feed returns `200 no_match`; projection
readiness also succeeds when its SELECT returns no row. Runtime configuration
validation occurs before authentication/database queries and can independently
produce a 503. The API process may have settings different from a terminal or the
latest saved `.env`.

Exact repository query stages:

| Stage | Query/connection behavior |
|---|---|
| All three repository connections | `psycopg.connect` with `APP_API_DATABASE_URL`, configured connect timeout, dict rows, and startup `options="-c statement_timeout=<configured-ms>"` (default 3000) |
| Database readiness | `SELECT transaction_timestamp()` |
| Projection readiness | Full `_SEARCH_SQL` SELECT/filter/ordering with pagination replaced by `LIMIT 0`; bind an empty station array; fetch no rows |
| Feed timestamp | `SELECT transaction_timestamp() AS generated_at`, then fetch `generated_at` |
| Feed rows | `_SEARCH_SQL`: the 22 view fields listed below plus computed freshness; affected-station array overlap, deterministic ordering, limit+1/offset |
| Feed response | `_item()` validates/maps selected rows to the typed response; empty rows become `no_match` |

The 22 referenced view columns are `public_event_id`, `event_kind`, `title`,
`summary`, `original_text`, `severity`, `event_at`, `event_time_certainty`, `certainty_label`,
`freshness_anchor_at`, `line_name`, `station_name`, `station_id`,
`start_station_name`, `start_station_id`, `end_station_name`, `end_station_id`,
`direction`, `affected_station_ids`, `source_display_name`, `source_url`, and
`source_published_at`. Migration 014 defines the initial fields; forward migration 018 appends `original_text`. Freshness is computed
in the repository by comparing the anchor against `transaction_timestamp()` minus
8 hours. The request predicate is `v.affected_station_ids && %s::text[]`; ordering
is `v.freshness_anchor_at DESC, v.public_event_id`; pagination is `LIMIT %s OFFSET
%s`, with parameters `(station_ids, limit + 1, offset)`.

The view itself selects the latest event version, requires verified/published,
applies the database-clock 24-hour window, and validates confirmed segment paths.
Its affected-station IDs are arrays built from text IDs. The current repository query expects the view after migration 018; before this
extension, migration 014 matched the original repository query. This does not
establish that the deployed view equals that migration, or that a manual count
used the same startup options, timeouts, process environment, or query plan.

The readiness body can narrow the failure before running further checks:

- `database: unavailable`: settings, connection/startup-options or clock probe
  failed. Configuration failures now use the same readiness-shaped 503 body.
- `database: available` with `app_api_projection: unavailable`: the complete
  zero-row feed projection/filter/order probe failed.
- Readiness passes but feed fails: inspect runtime query timeout, concurrent
  database/schema changes or actual row/response validation. The zero-row probe
  validates column/operator availability, not real row contents or feed latency.
  The independent mock endpoint can fail separately.

### R03 error contract

The normal feed keeps its existing success schema, station-ID overlap, ordering
and pagination. It still performs exactly two SELECTs per repository call. Mapping
and feed-envelope validation are inside the guarded read boundary and produce
`503 source_unavailable`, empty items and null `generated_at`/`next_offset` on the
recognized driver, OS, validation or mapping failures. Unexpected programming
errors are not indiscriminately converted to dependency failures.

Configuration is still checked before auth/query validation. Its feed-shaped 503
preserves each valid requested limit/offset; absent or invalid values independently
fall back to 20/0. Integer parsing uses the same Pydantic rules and bounds as the
endpoint; duplicate scalar parameters use the last value. Invalid pagination with
valid server configuration still returns the existing 422 response. Missing/wrong
Bearer credentials still return the existing 401 response when settings are valid.

Readiness uses the same recognized error categories and returns only its existing
`api`, `database`, and `app_api_projection` fields, including configuration errors.
It first probes the database clock, then resolves the actual feed SELECT, overlap
operator and ordering with an empty station array and `LIMIT 0`. Empty views are
healthy. No model/data rows are fetched by this probe. Readiness still uses at
most two connections and one SELECT on each, with the same timeout mechanism.
Liveness remains unauthenticated and independent of database/configuration.

The manual diagnostic below uses that same zero-row SQL for its projection and
schema stages; the repeated diagnostic probes are not part of normal feed reads.
R03 does not apply migrations, change permissions, enable diagnostic files,
introduce caching/pooling, or implement an overall request deadline.

### Manual diagnostic — not run during implementation

`scripts/diagnose_api_feed.py` reproduces the query stages read-only. It first
connects without the API's explicit startup options, sets the same timeout via
transaction-local SQL, then tests the clock, count, minimal readiness SELECT,
full feed fields with LIMIT 0, exact feed query, and response mapping. It repeats
using the API's exact startup `options` argument. Each outer transaction is read-only
and forced to roll back; individual query probes use rollback-only savepoints.
No writes, permission changes, worker, HTTP requests or service startup occur.

Default settings are parsed from local `.env` without shell execution or variable
interpolation. `--environment` instead uses the caller's already exported API
settings. Use whichever matches how the actual API was launched; the diagnostic
cannot inspect the environment of a different running process. The supervisor
setup forwards only the two API secrets and therefore uses default timeouts;
a manually sourced shell can additionally supply timeout overrides.

Run manually only when a read-only Development database connection is authorized:

```bash
cd /Users/naufal/Moobby_back
.venv/bin/python -B scripts/diagnose_api_feed.py --station-id open-krl:BOO
```

Repeat `--station-id` and optionally set `--limit`/`--offset` to match the failing
request. No supplied station values or pagination values are printed. Output has
exactly `stage`, `exception_type`, and `sqlstate`. Successful stages have null
exception type/SQLSTATE. No row data, row counts, SQL, parameters, credentials,
driver messages or exception tracebacks are printed.

If `without_api_options.*` passes but `api_options.connect` fails, the difference
is connection startup-option handling (for example a pooler's acceptance of
`options`), not evidence that the view needs another grant. A column error such as
`UndefinedColumn`/`42703` instead identifies schema drift at the failed projection
stage. Other SQLSTATEs distinguish privilege/query cancellation/connection errors;
do not change schema or permissions based on a count alone. If every stage passes,
compare the API's actual loaded configuration/connection target and process version;
this script is not proof that a separately running service has that same state.

Local tests only:

```bash
PYTHONPATH=tests .venv/bin/python -B -m unittest test_api_diagnostic -v
```

Tests mock the database driver and verify empty-view success, exact query reuse,
startup-option isolation and restricted error reporting. The diagnostic itself was
not run against a database during implementation.

## Concurrent mock endpoint

`GET /api/v1/mock/disruptions` uses the same bearer authentication, canonical
`station_ids`, limit/offset bounds, location model and response field allowlist as
the normal feed. It runs existing synthetic fixtures in memory, with SIMULASI
labels and no database/queue/event writes. The endpoint does not query the database;
API URL/token configuration is still required by the shared authentication settings.

It is disabled by default (404), requires the existing Development environment/mode
flags and a valid TTL of 1–240 minutes, and becomes 404 when the TTL expires. Invalid
mode/environment/TTL or unavailable fixture processing fails closed with 503. It
never falls back to the real database, and the normal route never falls back to
mock data. Mock authentication failures remain 401 and request validation remains
422. Normal readiness continues to test database/projection readiness independently.

For the existing LaunchAgent, set optional local JSON `mock_enabled: true` and
`mock_ttl_minutes: 60`; no new port, tunnel or worker is needed. See
[service instructions](development_api_launchd.md#mock-test-pada-port-dan-tunnel-yang-sama).
An API process restart is needed to load changed code/configuration. No app code
is changed here; the app must explicitly select the mock path when testing.

Mock fixtures require the saved NLP thresholds and trusted publication-basis
settings. The LaunchAgent forwards these three non-secret processor settings only
when mock is enabled, without forwarding worker connection credentials. The
fixture processor retains its original behavior.

## Original source text

Every item now includes `original_text: string | null`, alongside curated `title`
and `summary`. This explicitly approved field contains the unmodified posting
text, including original whitespace, not reconstructed text from the evidence.
It applies to both `/api/v1/disruptions` and `/api/v1/mock/disruptions`; mock uses
the same synthetic input and SIMULASI labels. No CSV/model/threshold changes.

```json
{
  "title": "Gangguan layanan KRL di Bogor",
  "summary": "Laporan gangguan layanan KRL menyebut Stasiun Bogor.",
  "original_text": "CONTOH SINTETIS: KRL tertahan di Stasiun Bogor karena gangguan sinyal."
}
```

Migration `018_app_api_original_text.sql` is prepared, NOT applied. It adds nullable
`nlp.event_versions.original_text`, grants INSERT on that column to the existing
worker and SELECT to the existing view owner, then appends the field to the existing
security-barrier view. Development inspection reported a membership granted by
`supabase_admin` to `postgres` with ADMIN true, INHERIT false and SET false.
Membership alone cannot authorize SET ROLE. Migration 018 uses that existing ADMIN
option to add a separate temporary membership explicitly `GRANTED BY postgres`,
with INHERIT false, SET true and ADMIN false. After replacing the view, it revokes
only that grant, explicitly `GRANTED BY postgres RESTRICT`, preserving the original
supabase_admin grant and its options. Both statements are in the same transaction;
any failure rolls them back with the DDL. A preflight requires the postgres session,
the existing direct ADMIN option, and no pre-existing postgres-granted membership;
otherwise it stops instead of overwriting/removing an unrelated grant. It still
requires permission to assume the NLP owner. No permanent membership, collection access, data,
RLS, existing API-role privileges or eligibility filters are changed. Existing
view SELECT access covers the appended field. No direct collection query is added.

The worker writes the exact draft text atomically with event/embedding/provenance/
evidence for new accepted versions only. Legacy events retain null; no automatic
backfill or promotion occurs. A missing legacy field in an adapter row also maps
to null. Internal evidence fields/offsets, normalized text, separate author
metadata, embeddings, model metadata and queue state are not exposed. Full original
text may naturally contain names/mentions or the same words as an internal quote.
Do not print original_text to operational logs or render it as executable markup.

Deployment order is manual: apply migration 018 in the intended Development
project using the established owner/migration process, then restart the API and
worker to pick up the changed code when authorized. New API SQL/worker INSERTs
require that migration; deploying them first can produce database column errors.
Older worker code remains compatible after the migration (its new text column is
null). Do not infer the migration has run merely because this file exists.
This implementation does not apply SQL, connect to a database, or restart services.
