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

Migration `014_app_api_read_contract.sql` adds nullable, constrained `event_versions.severity` and creates a latest-version projection. The view returns only the latest event version when it is `verified` and `published`, and only while the database-clock freshness anchor is within the preceding 24 hours. Freshness is `fresh` through 8 hours and `stale` after 8 through 24 hours. It projects curated event fields, canonical location names and IDs, confirmed-path affected station IDs, platform-level source display name, HTTPS source URL, and source publication time. It excludes source text, exact quotes, author data, source snapshot IDs, model output, internal notes, path IDs, and queue data.

The API login should use `mooby_app_api_login_dev`, whose only application data access is through `mooby_app_api_gateway_dev` and the single view. Set that login password out of band in Development and store its URL in the server-only `APP_API_DATABASE_URL`. Never reuse the worker or migration URL. The role setup assumes the existing `moobby_incident_readonly` role; stop if that prerequisite is absent. Migration 014 has not been applied by this code change.

New processor outcomes are publication-accepted only when `result_status="succeeded"` and a valid `draft_event` is present. The unchanged processor intake/provenance, keyword, station/confirmed-path, relevance/severity thresholds, structural, and exact-evidence checks still apply. Queue success alone is insufficient; classification may succeed without a valid draft. Skip/rejection/error/non-disruption/threshold-failure/no-draft outcomes do not publish.

The worker inserts accepted new versions as `verified`/`published` with both timestamps set to `transaction_timestamp()`, atomically with the queue outcome, run, provenance, and evidence. `verified` means machine-accepted, not officially confirmed. No human review, separate verifier, embedding, or correlation/cluster prerequisite is introduced. Existing drafts remain untouched on conflict. The API remains read-only and keeps its field allowlist, station-ID filtering, affected range, freshness, and latest-version rules.

Deployment prerequisites (not applied here): migration 004 currently requires embeddings which this processor does not produce. Prepared migration `015_optional_event_embeddings.sql` makes the embedding/input-version pair optional without changing old rows or access policies. Separately, migrations 012/014 do not grant worker INSERT on `verification_status`, `publication_status`, `verified_at`, or `published_at`. The new INSERT therefore remains blocked for that role until database access is separately authorized; this change deliberately does not alter grants, roles, RLS, or credentials. Do not deploy expecting successful writes with only grants 001–014. Local tests use transaction fakes and do not establish live database readiness.

For a short, explicitly enabled local UI smoke test, the server also supports a Development-only synthetic fixture mode. It runs the CSV text through the processor in memory and marks each returned title/source as `SIMULASI`; it does not write to PostgreSQL. Enable it only with `APP_API_ENVIRONMENT=development`, `APP_API_SYNTHETIC_FIXTURE_MODE=development`, and a 1–240 minute `APP_API_SYNTHETIC_FIXTURE_TTL_MINUTES`. The mode ends automatically after that process TTL. It is not part of the normal published-event feed.

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
