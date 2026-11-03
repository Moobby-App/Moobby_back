# KRL Service Disruption rIntelligence — Technical Specification

**Status:** Draft for implementation planning  
**Version:** 0.1  
**Date:** 2026-11-03  
**Language:** English

## 1. Purpose

This specification defines a system that turns news and user-generated rail reports already stored in the existing `collection` database schema into versioned, structured, evidence-backed disruption events. A scheduled Python worker performs collection access, text processing, station resolution, event correlation, verification, and publication. A FastAPI gateway exposes an app-safe read interface through `app_api`; the iOS application never connects directly to PostgreSQL.

The system supports two request paths:

- **Path A — location or route:** resolve a user's GPS position, route, or station name to reference stations, then use structured SQL filters.
- **Path B — free text:** embed the query and search event embeddings with pgvector. Apply available station and time constraints before similarity ranking. A 24-hour time bound is mandatory even when no station or time can be extracted from the query.

The system must distinguish a real lack of matching reports from unavailable source data. It must not report “normal service” merely because a search returned no results.

## 2. Goals

1. Keep raw source text immutable and preserve exact quotations for evidence.
2. Move expensive classification and event embedding work to scheduled batch processing.
3. Make GPS, route, and station-based lookups deterministic and explainable.
4. Use vector similarity for free-text retrieval and duplicate candidates, never as proof that reports are true or describe the same event.
5. Publish only events that pass validation, verification, and publication policy.
6. Keep private source records separate from NLP processing outputs and app-facing data.
7. Make worker processing repeatable, idempotent, auditable, versioned, and recoverable.
8. Prevent the iOS application from holding database credentials or reading private schemas.
9. Evaluate quality against manually reviewed examples and real disruption cases.

## 3. Non-goals

- Replacing the existing collector or changing its ownership of `collection`.
- Treating report volume, embedding similarity, or model confidence as independent truth verification.
- Generating incident coordinates from text. Coordinates belong to station reference data and are used to resolve a user's GPS position.
- Allowing an LLM to create facts, locations, durations, or quotations absent from the source evidence.
- Allowing the iOS application to query PostgreSQL directly.
- Automatically accepting fuzzy station suggestions returned by `rapidfuzz`.

## 4. Architecture and trust boundaries

```text
Collector
  └─ writes private source snapshots → collection

Scheduled Worker (Python 3.12)
  ├─ claims snapshot IDs with FOR UPDATE SKIP LOCKED
  ├─ preserves raw text; derives normalized processing text
  ├─ classifies, extracts, resolves stations, parses time, embeds
  ├─ correlates and deduplicates candidate events using rules
  ├─ validates, verifies, and applies publication policy
  └─ writes immutable, versioned results and processing records → nlp

FastAPI / Uvicorn Gateway (Python 3.12, reached through the configured NGROK tunnel)
  ├─ reads app-facing views/RPCs → app_api
  ├─ runs structured SQL lookup for GPS/routes
  └─ runs bounded pgvector retrieval for free text

iOS application
  └─ calls the gateway; has no database credentials and no direct database connection
```

`collection` is private source input. `nlp` is the trusted processing and event layer. `app_api` is the narrow app-facing read contract. The gateway is the only app-facing database client and uses a read-only database role. Database role credentials are held server-side, never in the iOS bundle.

The API may read trusted `nlp` data internally only where required by an approved query implementation. The app-facing contract remains `app_api`; raw source text, author data, and private collection fields must never be returned to the app.

## 5. Technology baseline

| Area | Required baseline | Responsibility |
|---|---|---|
| Database | Supabase Postgres 17 | Existing `collection`, new `nlp`, and new `app_api` schemas; transactions, queue, SQL filters, views/RPCs |
| Vector search | pgvector | Event and query embeddings; candidate retrieval for Path B and duplicate candidate generation |
| Python runtime | Python 3.12 | Worker and API |
| Database driver | psycopg 3 | Transactional database access from worker and API |
| Migrations | Ordered SQL files | Create schemas, tables, indexes, views/RPCs, roles, and grants in repeatable deployment order |
| Worker scheduling | `launchd` or cron | Periodically start batch processing; one scheduler configuration is selected per deployment |
| Baseline classifier | scikit-learn | Character TF-IDF n-grams + logistic regression; baseline relevance and category metrics |
| Fine-tuned classifier | IndoBERTweet with `transformers` and PyTorch MPS | Relevance and category classification; use MPS when available and a supported fallback where not |
| Event embeddings | `sentence-transformers` + `multilingual-e5-small` | Embeddings for semantic search and duplicate candidates |
| Station matching | Python alias dictionary + normalization + `rapidfuzz` | Deterministic alias matching; fuzzy matches are suggestions for review only |
| Time parsing | Python regex and rules | Parse explicit and relative Indonesian times, such as `08.15`, `tadi pagi`, and `barusan`; relative expressions are anchored to source post publication time in Asia/Jakarta |
| Summary construction | Python templates | Compose a curated title and one-sentence impact conclusion from structured fields; do not copy source-post wording |
| Optional constrained rewriting | Ollama + `qwen2.5:7b-instruct` | Optional phrasing cleanup from structured fields only, using a JSON schema; must not receive or reproduce raw post text |
| API | FastAPI + Uvicorn | App-facing gateway and query orchestration |
| API validation | Pydantic | Request/response contracts and validation of structured model output |
| Query embeddings | `fastembed` or ONNX Runtime | Low-latency query embedding inside the API process using the same embedding model family/version as event vectors |
| Tunnel | NGROK | Development/debug tunnel to the gateway; does not replace API authentication or database access controls |
| Quality checks | pytest | Unit/integration coverage for station matching, time parsing, validation, queue/idempotency, feed adapter, query filters, and response behavior |
| Evaluation data | Manually reviewed JSONL gold set | Recall and precision evaluation, regression checks, and known-disruption coverage |
| Version control | Git | Version code, migrations, rules, model/evaluation metadata, and reviewed evaluation results |

Dependency versions and model artifacts must be pinned in deployment manifests/lock files. The exact production choice between `fastembed` and ONNX Runtime, classifier model, category taxonomy, and model artifact digest are deployment configuration, not implicit runtime guesses.

## 6. Data ownership and schema contracts

### 6.1 Schema access matrix

| Schema | Contents | Writer | Reader | Exposure |
|---|---|---|---|---|
| `collection` (existing) | Original text, source/post ID, author, URL, source metadata, and trusted post publication timestamp when available. Private. | Collector | Batch worker only | Never exposed through app API |
| `nlp` (new) | Processing queue/runs, structured event versions, embeddings, station/segment matches, internal exact evidence quotes, verification/publication state, skip/error notes, model version and digest | Batch worker | Worker and trusted API implementation | Internal trusted schema; not directly granted to iOS |
| `app_api` (new) | Views/RPCs containing only app-visible fields and only eligible published events | No direct application/API writes | Gateway/API using read-only login | Sole app-facing database contract |

The `app_api` contract should contain only the fields required by the application: title, curated summary/conclusion, affected station or segment, category, time, certainty, and a `source` object (for example, display name, URL, and publication time). It must not return the exact evidence quote or raw post text. Add a stable public event identifier only if the app needs it for identity or pagination; do not expose internal source IDs or private metadata without a documented need.

### 6.2 Logical data entities

Exact table names may be selected during migration design, but the implementation must represent these logical records:

- **Reference station master:** reuse the existing `reference.krl_stations` as the canonical station source where available. It contains `station_id`, `source_station_code`, `canonical_name`, `aliases`, station coordinates, `line_memberships`, and `source_sha256`. Do not duplicate the station master or silently modify its live rows. Coordinates describe stations only.
- **Line/path catalog:** represent a line separately from its physical route paths. A line can have multiple branches or corridor variants, such as Cikarang services via Manggarai versus Pasar Senen, or a Bogor-line branch to Nambo. Store a stable path/variant ID, line identity, configured direction, endpoints or terminus, optional reviewed `via` label, source feed/version digest, and effective date.
- **Ordered path stations:** use an ordered relation such as `line_path_stations(path_id, seq, station_id)` or an equivalent relation carrying line, path variant, direction, sequence, and station. A single flat `line_stations(line_id, seq, station_id, direction)` sequence is insufficient when a line branches or has distinct physical paths. A trip's stop pattern is not itself a canonical physical path: local, short-turn, and skip-stop services can omit stations while following the same corridor.
- **Processing queue:** source snapshot ID, state, attempt count, lease/claim metadata, next retry time, and last error category. Queue records are operational metadata; source payload remains in `collection`.
- **Processing run/version:** source snapshot ID, run ID/version, code/rule/model versions, source digest, start/end times, and processing outcome.
- **Structured event version:** event identity, version, event kind (`station` or `segment`), category, station/line or segment associations, event time, source publication/first-seen/freshness timestamps, certainty, verification status, publication status, curated title/summary, and provenance to one or more source snapshots.
- **Segment disruption:** line ID, start station ID, end station ID, and direction, plus a resolved path variant when needed to distinguish branches. Resolve the affected ordered station range using the applicable ordered path; do not reduce a segment incident to one station or infer a unique path where endpoints are ambiguous.
- **Evidence:** source snapshot reference, exact internal quote copied from the immutable original text, and source metadata. Store the quote in `nlp`, validate it as an exact substring of the original, and use it for audit/publication validation only. The app receives source metadata, not the quote.
- **Embedding:** vector associated with an event version and embedding model/version. The vector dimension must match the pinned model artifact.
- **Skip/error record:** source snapshot ID, reason code, processing stage, model/rule version, and timestamp. Keep malformed model output, valid policy rejection, unmapped station, irrelevant content, and transient infrastructure failure as different reason categories.
- **Correlation/deduplication membership:** event-cluster identity, related event versions, applied deterministic rules, and the embedding candidate score for audit. The score is evidence for candidate generation only.

Use database constraints for required identifiers and uniqueness. A new processing version must be appended or linked as a new version; it must not silently overwrite an older result. A current/published pointer or equivalent may identify the active version without deleting history.

### 6.3 Reference-data requirements

- Reuse `reference.krl_stations` for canonical stations, source station codes, aliases, coordinates, and line memberships where available. Preserve `station_id` as the internal identifier and map external feed stop IDs through an explicit, audited crosswalk (for the inspected Open-KRL sample, GTFS `stop_id` matched `source_station_code`; verify the full feed before loading).
- `line_memberships` only states membership; it does not encode station order, direction, or physical path. Do not infer ordering from array position, station coordinates, station names, or membership order.
- Maintain line path variants and ordered path-station relations with direction and sequence. Distinguish physical path variants from trip stop patterns. A trip pattern that skips stops or terminates early must not automatically create a new physical path or remove omitted stations from the canonical corridor.
- Treat GTFS `trip_headsign` as a destination hint, not a canonical direction identifier. `service_id` describes a service calendar, not direction. Group and inspect trips using their route, headsign, service, and `stop_sequence`, then review differences before deriving canonical path variants.
- Record the route-feed source, license/attribution, feed digest, effective date, and reference-data version. The initial implementation scope is the five lines present in `reference.krl_stations`; `MERAK` remains out of scope until separately decided. The Open-KRL v2 crosswalk audit found 84 exact `stop_id`–`source_station_code` matches out of 94 GTFS stops. Nine unmatched stops are used by the `MERAK` route; `SG` (`SERANG`) is present in `stops.txt` but not referenced by this feed's `stop_times.txt`. Keep these statuses explicit; do not assign them to one of the five lines.
- The inspected Open-KRL v2 build reported 0 itinerary trips and 1,143 reconstructed trips. The five in-scope routes produced 65 distinct ordered stop sequences. Treat those sequences as feed candidates, not as automatically authoritative line paths. Official KAI Commuter materials confirm the broad network topology: the Nambo branch connects through the Citayam–Cibinong corridor, and Cikarang operates alternative loop corridors via Manggarai and via Pasar Senen, rejoining at Jatinegara. The observed Bogor-line branch sequence Citayam → Pondok Rajeg → Cibinong → Nambo is consistent with KAI Commuter's confirmation that Pondok Rajeg lies between Citayam and Cibinong and was reactivated for commuter service. The Depok–Nambo trip is a short-turn on this branch, not a separate physical path. The two Kampung Bandan–Bekasi sequences (13 and 15 stops) traverse different corridors and are not skip-stop variants. See the [KAI Commuter 2024 route map and line descriptions](https://kci.id/files/download/annual_report/AR%20KCI%2019082024%20LR%20-%20LK%20under%2010MB.pdf), the [KAI Commuter Pondok Rajeg announcement](https://www.kci.id/informasi-publik/berita/pengguna-commuter-line-sudah-bisa-naik-dan-turun-di-stasiun-pondok-rajeg-uji-coba-operasional-commuter-line-di-stasiun-pondok-rajeg-sebanyak-24-perjalanan), and its [Cikarang loop-operation notice](https://www.kci.id/index.php/informasi-publik/berita/kai-commuter-persiapkan-perubahan-pola-operasi-perjalanan-krl-terkait-pelaksanaan-so-5-stasiun). These sources validate corridor topology; exact canonical ordered sequences still need a trusted, date-matched operational source.
- **Tangerang pattern status:** the official May 2026 timetable confirms the Tangerang branch order Tangerang → Tanah Tinggi → Batu Ceper → Poris → Kalideres → Rawa Buaya → Bojong Indah → Taman Kota → Pesing → Grogol → Duri, matching the longest feed pattern. The same timetable says train 2013A (Tangerang–Duri) continues as train 2014B to Manggarai. Therefore the observed Duri → Tanah Abang → Sudirman Baru → Sudirman → Manggarai pattern is **confirmed as a through-service continuation at the destination level**, not a new physical Tangerang path; model its Tangerang branch and shared-corridor continuation as separate path segments/service legs. The reverse Manggarai → Duri pattern has not yet been matched to a specific official train number, so mark it **unconfirmed at trip level** and do not treat it as a separate physical route. See the [KAI Commuter May 2026 timetable](https://kci.id/files/download/documents/Jadwal%20Commuter%20Line%20Jabodetabek%20-%20Update%20Mei%202026.pdf).
- **Sudirman Baru (`SUDB`) status:** this is not simply a bad station match. KAI Commuter announced that passenger boarding/alighting service previously at Karet was transferred to Sudirman Baru/BNI City from 2026-09-01 during integration works scheduled through 2026-12-31. This period overlaps the inspected GTFS captures (2026-09-24 and 2026-10-11). Mark the transfer as **confirmed temporary passenger-service substitution**. Do not infer from that notice alone that Sudirman Baru is a permanent physical stop or a separate permanent path station; retain its effective dates and the unresolved physical/path identity. See the [KAI Commuter 2026 integration notice](https://www.kci.id/informasi-publik/berita/pekerjaan-integrasi-stasiun-karet-stasiun-sudirman-baru-bni-city-dimulai-per1-september-2026-layanan-naik-dan-turun-pengguna-commuter-line-dialihkan-ke-stasiun-sudirman-baru-bni-city).
- **Rangkasbitung ordering:** the feed's longest pattern contains Jatake between Cicayur and Parung Panjang. KAI Commuter confirms Jatake is on the Tanah Abang–Rangkasbitung line and lies between Cicayur and Parung Panjang; the station entered service on 2026-01-28. This validates that ordered segment ([KAI Commuter Jatake notice](https://www.kci.id/informasi-publik/berita/stasiun-jatake-setelah-empat-hari-beroperasi-telah-melayani-lima-ribu-lebih-pengguna-commuter-line)). The remaining feed stop sequence matches the broad order in KAI Commuter's 2024 route map; retain the feed digest/effective date in any imported path version.
- **Tanjung Priok ordering:** the official 2026-04-15 timetable lists the base path Jakarta Kota → Kampung Bandan → Ancol → Tanjung Priok and the reverse. KAI Commuter then announced limited JIS service beginning 2026-06-22 from the 13:00 trip; JIS is an effective-dated stop/service variant, not a timeless assumption that every trip calls there. The feed's JIS insertion is compatible with the later service period, but its pattern CSV does not retain per-trip times, so it cannot prove that all listed trips stop at JIS. See the [April 2026 timetable](https://kci.id/files/download/documents/Jadwal%20Commuter%20Line%20Jabodetabek%20-%20Update%2015%20April%202026.pdf) and the [KAI Commuter JIS service notice](https://kci.id/informasi-publik/berita/menhub-ri-dudy-purwagandhi-dan-gubernur-jakarta-pramono-resmikan-stasiun-jis).
- Official route maps/timetables now support the broad ordered corridors for all five in-scope lines, including the two Cikarang loop alternatives and the Bogor/Nambo branch. Remaining uncertainty is trip-level/effective-dated service behavior, not whether those broad physical corridors exist. Preserve service stop substitutions and through-running as dated trip/service patterns rather than inventing permanent physical paths. Re-run this assessment whenever the source feed changes; observed counts and findings are a discovery snapshot, not permanent constraints.
- The inspected Open-KRL v2 export digest is `b7070274011d1be1da0919e1210f8728df454d0d5964276d79320df26e40ab17`. Its route/headsign groups contain multiple stop patterns. Re-run this quality assessment whenever the source feed changes; observed counts and findings are a discovery snapshot, not permanent constraints.
- The current audit found all 84 station rows unique with valid coordinates, but all alias arrays empty. Exact canonical-name/code matching can work; aliases must be curated before relying on abbreviations or colloquial station names. Revalidate these counts and alias coverage when importing a new reference version.
- Normalize reference names and IDs during import, and detect leading/trailing whitespace, case/character confusables, malformed coordinates, and duplicate aliases.
- `rapidfuzz` may return candidate stations and scores to a review workflow. It must never assign a station automatically based on fuzzy score alone.
- If a post names a station but has no coordinates, resolve the station by exact/alias rules when possible. Do not require incident coordinates.
- If the station is unresolved, record the post and an explicit reason; do not invent coordinates or infer a station silently.

## 7. Batch worker specification

### 7.1 Scheduling and queue semantics

The worker is started periodically by either `launchd` or cron. It claims work in short transactions with PostgreSQL `FOR UPDATE SKIP LOCKED`, allowing multiple worker processes without claiming the same snapshot concurrently.

Required behavior:

1. Claim a stable source snapshot ID and create/update its processing-queue lease atomically.
2. Commit the claim before long-running model inference; do not keep a database row lock open while loading/running a model.
3. Record attempt count, lease expiry, start time, and an outcome category. Recover expired leases after process crashes.
4. Make processing idempotent for a given snapshot ID and pipeline/model/rule version. A retry must not create duplicate event versions or duplicate evidence rows.
5. When code, rules, or a model changes, create a new processing version and retain the previous version for audit and evaluation.
6. Distinguish transient failures (database/network/model service unavailable) from terminal data/model failures and policy outcomes. Apply bounded retries to transient failures; preserve terminal failure reasons.
7. Never mutate the source record in `collection`.

### 7.2 Processing sequence

1. **Read snapshot:** load the original source record from `collection` by immutable snapshot/post ID. Persist a cryptographic source-content digest in `nlp` processing metadata.
2. **Preserve evidence text:** retain the original text exactly. Do not normalize, rewrite, or truncate the evidence source used for quotations.
3. **Create a derived processing copy:** apply versioned Python normalization only to the derived text used for classification, extraction, and embeddings. Store the normalization/rules version.
4. **Classify relevance and category:** use a configured classifier. The scikit-learn character TF-IDF + logistic-regression model is the baseline. IndoBERTweet fine-tuning is the higher-capability option for relevant/not-relevant and category labels. Store model artifact version/digest and prediction metadata. A non-relevant post is recorded as skipped with a reason rather than disappearing.
5. **Extract structured fields:** identify station mentions, line, category, event time, and evidence spans. Use versioned rules and model output validation. Never treat untrusted model text as executable instructions.
6. **Resolve station references and paths:** use the canonical station master, audited source-code crosswalk, aliases, line memberships, and reviewed path variants. Coordinates are not required for a named station. Fuzzy matching is advisory only. A non-matching station creates an explicit unmapped outcome and skip/review record. If multiple paths remain possible, retain the ambiguity rather than selecting one silently.
7. **Parse time:** use regex/rules for explicit values such as `08.15` and relative phrases such as `tadi pagi` and `barusan`. Resolve relative phrases against the source post's publication timestamp in `Asia/Jakarta`, never against the worker's processing time. Retain the raw phrase, post publication time, parsed value, time zone, parsing method/rule version, and certainty. If the publication timestamp is missing or the phrase is ambiguous, preserve that uncertainty; do not fabricate precision.
8. **Generate event embedding:** create a vector with the pinned `multilingual-e5-small` artifact from the approved derived text. Record the model name/version/digest and input version. Use the same compatible embedding model for query vectors.
9. **Correlate and deduplicate:** use vector similarity only to produce candidates. Confirm cluster membership using deterministic, versioned rules that require the same resolved station, a compatible time window, and a compatible category. The time window and category compatibility map are configuration data and must be evaluated. Do not use number of posts as verification.
10. **Validate structured output:** validate required fields and types with Pydantic/JSON Schema before persistence. Classify malformed output separately from a valid event rejected by policy. A malformed response may be retried or recorded as a format failure; a legitimate rejection is recorded with its policy reason.
11. **Verify and apply publication policy:** set explicit verification and publication states. Only eligible, verified/policy-approved versions may appear in `app_api`. Similarity, duplicate count, or model confidence alone cannot publish an event.
12. **Persist an immutable version:** write event version, associations, exact evidence quote, embedding, source/model/rule digests, and processing outcome to `nlp`. Keep prior versions. Record all skipped source snapshots and reasons.
13. **Update queue state:** mark completed, retryable, skipped, rejected, or failed with timestamps and reason codes. Queue completion and output persistence must be coordinated transactionally where possible.

### 7.3 Evidence and output integrity

- Every internal evidence quote must be an exact substring of the original source text stored in `collection` (or an exact copied immutable field in `nlp`).
- Do not derive internal quotes from normalized text, translation, model rewrites, or summaries. Validate exact substring equality before publication. Quotes are internal audit evidence and a publication prerequisite; they are never displayed in the app response.
- Summaries and conclusions are composed from structured fields through templates and must not copy source-post text. Optional LLM rewriting may receive structured fields only, must return the declared JSON schema, and must pass deterministic validation without reproducing source wording.
- If optional LLM output is malformed, unsupported, or adds facts, reject the rewrite and use the deterministic template or return a controlled error. Do not label that a policy rejection unless it is actually a policy decision.
- Do not invent station, line, location, duration, cause, certainty, or current-service status.

## 8. Event correlation and deduplication

Correlation is a batch step that prevents repeated source posts about one disruption from appearing as repeated independent events.

1. Restrict candidate comparison to posts with compatible resolved station, event-time window, and category.
2. Use pgvector similarity on the compatible subset to find likely related reports. Similarity produces candidate pairs/clusters only.
3. Apply deterministic, versioned event-identity rules to accept or reject a merge. Store the ruleset version and the evidence used for the decision.
4. Preserve individual source snapshots, source objects, and internal exact quotes under a correlated event. Deduplication must not discard provenance.
5. A count of related posts may be displayed only as a count of reports if the product chooses to show it; it must not be described as verification or proof.
6. For station events, require compatible resolved station, time window, and category. For segment events, compare line, ordered endpoints, direction, and path variant when resolved, plus time window and compatible category. Do not merge segments on different branches merely because they share a line label.
7. If no station/segment, time, or category combination can be resolved safely, do not auto-merge. Keep separate records or route them to review according to policy.

## 9. Station and route resolution

### 9.1 Station reference matching

Normalization and aliases may be used to match station names from source text and user queries. The alias inventory should cover official names, abbreviations, common colloquial names, and known spelling variants. All alias changes should be versioned/reviewable.

Resolution order should be deterministic and auditable, for example:

1. Canonical station ID explicitly present in trusted structured source data.
2. Exact canonical station name.
3. Exact normalized alias.
4. Approved deterministic alias rule.
5. Fuzzy candidate from `rapidfuzz` for human review only.

A fuzzy candidate is not an accepted match. Unresolved station references remain unresolved and are logged with reason and source snapshot ID.

### 9.2 GPS and routes

- Use station coordinates only to map a user's GPS location to the nearest eligible reference station.
- Do not treat a source post's coordinates as an incident location unless separately validated and explicitly approved by product policy.
- Model a disruption affecting a stretch of line (for example, Tanah Abang–Rangkasbitung not operating) as a segment event with `line_id`, `start_station_id`, `end_station_id`, and `direction`; include a path ID/variant when branches make it necessary. Resolve the covered station range from the applicable ordered path; do not store the incident as a single-station event.
- Route requests must use ordered path stations and direction. Resolve route station IDs and any applicable path variant first, then query station events and segment events that overlap the route in the correct direction. If endpoints do not identify a unique path, preserve that ambiguity instead of claiming a precise range.
- If a user asks by station name without GPS coordinates, station-name matching is sufficient.
- If a source event has a station but no coordinates, it remains eligible for station-based search if other publication rules pass.
- If an event's station does not map to the reference table, do not guess. Record an unmapped station outcome; do not make it appear under a guessed station. If the source describes a line segment, preserve its endpoints and direction only when each can be resolved; otherwise record the unresolved fields and do not claim a precise affected range.

## 10. Time semantics and freshness

All request-window and freshness decisions use the database clock, not an iOS/device clock or an untrusted client timestamp.

### 10.1 Timestamps

Persist the trusted source post publication timestamp, extracted event time when confidently parsed, `first_seen_at`/collection time, and any later trusted verification/update time. Store absolute times in a timezone-aware database type. Display time in the product's configured local timezone; the deployment should explicitly set the intended timezone (assumed Asia/Jakarta unless product configuration says otherwise). If `collection` has no trusted publication timestamp for a post, relative phrases remain unresolved; do not substitute worker processing time.

For relative phrases in a source post (for example, “tadi pagi” or “barusan”), calculate the event time relative to that post's publication time, interpreted in `Asia/Jakarta`. Processing time must never be the reference for a relative phrase. Retain the source publication timestamp and the parsing rule/version used.

Define a `freshness_anchor_at` policy and version it. Recommended default: use confidently parsed event/observation time when available; otherwise fall back to first-seen database time. Do not present a guessed time as exact. The team must confirm this anchor before production because it affects freshness labels. This fallback is separate from parsing relative language, which must use post publication time.

### 10.2 Request-time rules

Compute age as database current time minus `freshness_anchor_at`:

- **Fresh:** age is at most 8 hours.
- **Stale:** age is more than 8 hours and at most 24 hours.
- **Expired:** age is more than 24 hours.

The normal retrieval window is the preceding 24 hours, based on database time. Expired events are excluded from normal results. If Path B cannot extract station or time from the query, vector retrieval still runs but the 24-hour database-time filter remains mandatory. When a specific query time/window is provided, its interpretation and maximum allowed range must be versioned and constrained by the 24-hour policy unless product explicitly changes it.

Freshness is metadata, not truth verification. A fresh unverified event is not publishable merely because it is recent.

## 11. Request and retrieval behavior

### 11.1 Shared API contract

FastAPI/Uvicorn exposes authenticated, validated endpoints to the app. Pydantic defines request and response types. All app-facing reads go through `app_api` views/RPCs using a server-side read-only database identity. The endpoint must return only fields allowed by the `app_api` contract.

Use a top-level availability state to distinguish:

- `results`: one or more eligible matching events are available.
- `no_match`: the source is reachable and the query completed, but no eligible event matches. The response must not say or imply that service is normal.
- `source_unavailable`: the database/feed/API dependency is unavailable or cannot provide a trustworthy result. Do not convert dependency failures into an empty successful result.

A response should include freshness and certainty for each result and a `source` object for each curated conclusion. It must not include `evidence_quote` or raw post text. Error details returned to the app must not expose credentials, SQL, raw private metadata, or internal stack traces.

### 11.2 Path A — GPS, route, or station lookup

1. Receive coordinates, route, or station-name query from the app through the gateway.
2. Resolve GPS to nearest station using reference station coordinates, or expand a route using the applicable ordered path variant and its station sequence.
3. If only a station name is supplied, use exact/alias resolution; GPS is not mandatory.
4. If no station maps, return an explicit unresolved/no-match state and do not substitute a nearby or guessed station.
5. Query eligible event versions using SQL and structured station IDs. Apply the database-clock 24-hour window, verification/publication status, and freshness rules before ranking.
6. Return curated conclusions and approved source objects through `app_api` only. Keep the exact evidence quote internal to `nlp`.

### 11.3 Path B — free-text semantic search

1. Receive text through the gateway and derive a query representation without modifying stored source evidence.
2. Attempt station/alias and time extraction. Preserve which constraints were confidently resolved.
3. Compute query embedding using `fastembed` or ONNX Runtime with the exact compatible `multilingual-e5-small` version used for stored vectors.
4. Apply station constraints if one was resolved. Apply the mandatory database-time 24-hour window even if no station or time was resolved. Apply publication and verification constraints.
5. Run pgvector similarity only over the filtered eligible subset; then apply a versioned ranking strategy.
6. Return only `app_api`-approved fields and source objects; never return the raw text or internal exact quote.

A mismatch between event-vector and query-vector model versions must fail closed or use a separately versioned compatible index; it must not silently compare incompatible vectors.

### 11.4 Ranking

Ranking may combine semantic similarity, time/freshness, station/route match, category match, and configured certainty. The formula and thresholds must be versioned and evaluated against the gold set. Do not use post count as truth confidence. Structured Path A should primarily rank by deterministic relevance and event time; semantic similarity is not required for GPS/route lookups.

## 12. Answer generation and result states

The default answer is produced by deterministic Python templates using structured event fields. The answer includes, as applicable:

- Title.
- One-sentence curated impact conclusion composed from structured fields. It must not copy source-post text.
- Station and/or line.
- Event/report time and freshness label.
- Certainty/confidence label with its basis.
- Source object with approved attribution/link metadata (for example, publisher/display name, URL, and publication time). The exact quote remains internal.

The system must distinguish:

1. **Results found:** display the curated conclusion, approved source object, and freshness. Keep source text and exact internal quotes hidden.
2. **No matching results:** explicitly say that no matching report was found in the searched window. Do not say “normal,” “no disruption,” or equivalent.
3. **Source unavailable:** report that data could not be checked. Do not represent an outage as an empty search result.

The optional Ollama `qwen2.5:7b-instruct` path is strictly a constrained phrasing pass over structured fields. It must not receive raw post text or exact quotes, must conform to a Pydantic/JSON Schema contract, and must be rejected if it copies source wording or adds unsupported facts. The LLM is not the authority for station resolution, event identity, truth, verification, freshness, publication, or final facts.

## 13. `app_api` exposure contract

`app_api` consists only of views and/or RPCs designed for the app. They must:

- Return only approved display columns: title, curated summary/conclusion, station or affected segment, category, time, certainty, and a `source` object (publisher/display name, URL, and publication time as approved). Do not return `evidence_quote`, original post text, or normalized post text.
- Include only events whose current version is eligible for publication and passes current request-time filters.
- Enforce the mandatory 24-hour boundary and freshness calculation using database time.
- Hide source author, raw post payload, normalized text, internal evidence quote, internal notes, model prompts, queue state, embeddings, and private IDs.
- Expose bounded query parameters, result limits, and safe pagination. Reject invalid coordinates, oversized text, unsupported time ranges, and malformed requests.
- Use read-only access. The gateway/API cannot mutate `collection`, `nlp`, or published event state through the app-facing role.
- Ensure no iOS client has a database URL, password, service-role key, or direct Supabase database credential.

If PostgreSQL functions need privileged access to `nlp`, use a narrowly scoped, reviewed function/view boundary with fixed search path and explicit column projection. Do not grant broad schema access to compensate for an underspecified view.

## 14. Security and privacy

- Keep `collection` private and grant the batch worker only the minimum read permissions needed.
- Keep database credentials in server-side environment/secret storage; never commit secrets to Git or bundle them in the iOS application.
- Use separate least-privilege roles for Collector, worker, migrations, and gateway/API.
- Gateway/API production identity is read-only for app-facing operations. Migration credentials are not runtime credentials.
- NGROK is a transport/tunnel for development/debugging, not authentication or authorization. Protect the gateway with the configured authentication controls and restrict tunnel exposure.
- Parameterize all SQL through psycopg 3. Do not build SQL from user strings.
- Apply request-size limits, query timeouts, result caps, and rate controls appropriate to deployment.
- Logs must not include secrets or unnecessary private source text. Use snapshot IDs and digests for audit correlation.
- Document retention and deletion policy for raw text, derived events, quotes, and model outputs. Preserve provenance and audit history consistent with product and legal requirements.

### 14.1 Database and role ownership boundary

The initial development target is the existing Supabase **development database** that already contains `collection`. This development database is separate from the live database. Apply development migrations only to this development database; never apply them to live as part of this implementation phase.

- `collection` remains an existing, read-only source boundary for this project. The worker may read the minimum required source columns, but migrations and application code must not alter its schema objects, data, roles, grants, or ownership.
- Create new owner roles dedicated to the new `nlp` and `app_api` schemas in development. Assign ownership only to those new schemas and their new objects.
- Keep migrations additive and scoped to explicitly named new schemas and objects. Do not transfer, rename, broaden, or revoke privileges from existing roles, and do not change ownership or grants on `collection` objects.
- Runtime worker and gateway roles must not be schema-owner roles. Use separate least-privilege runtime roles. Any source-read permission needed by the worker must be provided without changing existing `collection` role definitions or ownership; if that is not possible, stop and resolve the access boundary before applying migrations.
- Production rollout topology and timing remain a separate future decision. No migration in this implementation phase is authorized for the live database. If a later production plan places the new schemas alongside `collection`, it must preserve the same no-change boundary for all existing `collection` objects, data, roles, grants, and ownership.

## 15. SQL migration requirements

Migrations are ordered, checked into Git, and applied in sequence. Each migration should have one clear responsibility and be safe to inspect and reproduce in a clean environment.

The migration set must cover:

1. Required extensions (including pgvector) and schema creation.
2. New development-only role and ownership boundaries, with least-privilege grants scoped to new schemas and objects. Do not alter existing `collection` objects, data, roles, grants, or ownership. Production deployment requires a separately reviewed plan.
3. Reference tables/indexes or approved read boundaries for canonical stations, aliases, lines, path variants, and ordered path stations. Reuse `reference.krl_stations` when available; do not make a duplicate station master. Reconcile feed-to-station mapping and route scope before loading paths.
4. `nlp` queue, run/version, event, evidence, embedding, correlation, skip/error, and evaluation metadata structures.
5. Constraints and uniqueness rules supporting snapshot idempotency and immutable versions.
6. Vector dimension/index configuration aligned to the pinned embedding model.
7. SQL indexes for event time, station associations, publication/verification state, route lookup, and queue claim patterns.
8. `app_api` views/RPCs and column-level exposure rules.
9. Grants for new worker, migration, and gateway/API roles on explicitly named new schemas and objects. Do not change grants, roles, or ownership associated with `collection` or the Collector.
10. Migration/version bookkeeping and documented rollback or forward-repair procedure.

Do not put model inference or policy logic into opaque triggers. Keep workflow decisions in versioned Python code unless a narrowly scoped SQL function is required for safe atomic query behavior.

## 16. Evaluation, tests, and quality gates

### 16.1 Gold set

Maintain a manually reviewed JSONL set containing representative Indonesian posts and queries, including:

- Relevant and irrelevant posts.
- Station aliases, abbreviations, colloquial names, spelling errors, and ambiguous names.
- Posts naming a station without coordinates.
- Unmapped stations that must not be guessed.
- Explicit and relative time expressions.
- Multiple reports about one event and reports that are similar but describe different events.
- Internal exact-quote selection and cases where normalized text differs from the original.
- Segment disruptions with line, start/end stations, direction, and route-order resolution.
- Relative-time expressions with known source publication timestamps.
- Fresh, stale, and expired timestamps.
- Empty search and dependency/source-unavailable behavior.

The set should contain several hundred examples, at least about 100 positive/relevant examples, and many hard negatives (for example, ordinary service chatter, stale reports, unrelated station mentions, and semantically similar but different incidents). When the natural positive rate is below 15%, increase the set size as needed to retain at least about 100 positives without dropping representative negatives. Keep a natural-prevalence evaluation slice in addition to any stratified development slice.

Split training/development/evaluation data by time and by content group (such as source-post/event cluster), so reports from the same incident or copied/near-duplicate content cannot leak across splits. Freeze and version split manifests. Each gold entry should record expected relevance/category, station or segment resolution (line, endpoints, direction), source publication timestamp, time interpretation where determinable, event identity/grouping, publication expectation, expected internal evidence substring, expected app `source` object, and result-state expectation. Manual review changes must be versioned and attributed.

### 16.2 Metrics

Track at minimum:

- Recall on known/real disruptions.
- Precision and recall for relevance/category classification.
- Station-resolution accuracy and unresolved rate.
- Duplicate-candidate precision and accepted-cluster accuracy.
- Exact-quote validation pass rate.
- Count/rate of skipped posts by reason.
- Malformed-format failure rate, separate from legitimate rejection rate.
- Freshness-state correctness at the 8-hour and 24-hour boundaries.
- API latency and error rate for SQL and vector paths.
- Empty-result versus source-unavailable classification accuracy.

Do not create a single aggregate score that hides failures in critical measures. Set numeric release thresholds after establishing a baseline with the initial gold set; keep them in version control.

### 16.3 pytest coverage

Automated tests should include:

- Alias/canonical station matching, fuzzy suggestion non-acceptance, and unmapped outcomes.
- Route expansion over ordered path variants, including branches, alternative corridors, short-turn services, and skip-stop trip patterns.
- Time parser examples (`08.15`, `tadi pagi`, `barusan`), relative-time anchoring to source publication time in Asia/Jakarta, ambiguity, timezone handling, and database-clock boundaries.
- Normalized processing copy versus immutable original, internal exact-quote validation, and proof that app responses never include the quote.
- Curated summaries composed from structured fields without copying source-post wording.
- Segment events with line, endpoints, direction, and ordered path-variant expansion, including ambiguous endpoints and branch-specific behavior.
- Classifier adapter output validation and malformed-versus-policy-rejected handling.
- Embedding model/version compatibility and vector dimension.
- Correlation rules for same/different station, time windows, and category compatibility.
- Queue claiming with `FOR UPDATE SKIP LOCKED`, lease recovery, retry safety, and idempotent reprocessing.
- Version retention and no-overwrite behavior.
- `app_api` field allowlist, approved source-object contract, exclusion of ineligible events/private fields, and exclusion of internal evidence quotes.
- Path A SQL filters and Path B mandatory 24-hour filter before similarity ranking.
- No-match versus source-unavailable responses.
- FastAPI/Pydantic request and response contracts.
- Feed/database adapter behavior under transient failures.

Tests must run without real production credentials or mutation of production data.

## 17. Operations and observability

- The scheduler must report run start/end, claimed/completed/retried/skipped counts, and fatal errors.
- Expose a health check that distinguishes API process health, database reachability, and source/feed availability.
- Monitor queue depth, oldest pending age, expired leases, retries, model load failures, embedding failures, and database/vector query errors.
- Monitor evaluation metrics: recall on known disruptions, number of skipped posts, and format failure rate. Keep format failures separate from valid policy rejections.
- Alert when source data becomes unavailable or stale, not merely when a query returns no results.
- Make every event traceable to its source snapshot ID, processing run, rules/model versions, and digest.
- Store operational timestamps using the database clock where they govern leases, eligibility, and freshness.
- Ensure model loading does not happen for every post if the worker can keep a process warm; measure memory and startup behavior before choosing worker batch size.

## 18. Configuration

Configuration should be explicit, versioned, and validated at startup. It should include:

- Database connection settings per component/role, secret references, connect/read timeouts, and pool limits.
- Queue batch size, lease duration, retry limits, scheduler interval, and worker concurrency.
- Active classifier, classifier artifact/version/digest, category taxonomy version, and thresholds.
- Embedding model name/version/digest, vector dimension, and query runtime (`fastembed` or ONNX Runtime).
- Station alias/reference-data version, fuzzy suggestion threshold for review only, route/path variant data version, GTFS source feed digest/effective date, crosswalk version, and direction conventions.
- Time parsing rules version, display timezone, freshness anchor policy, stale threshold (8h), expiration/request window (24h).
- Correlation/deduplication rules version, compatible categories, and time-window settings.
- Verification and publication policy version.
- Optional Ollama URL/model, JSON schema version, timeout, and whether constrained rewriting is enabled.
- API result limit, authentication settings, tunnel URL configuration, query timeout, and rate controls.

Secrets and environment-specific URLs must not be stored in the repository. Changes to model/rule thresholds must include evaluation results and Git history.

## 19. Recommended implementation sequence

1. Use the existing Supabase development database that contains `collection`; confirm its project identity and separation from live. Inspect and document the source schema and stable snapshot/post ID. New migrations target development only and must not change any `collection` objects, data, roles, grants, ownership, or Collector behavior.
2. Reconcile the audited stop crosswalk, curate aliases, and review candidate path variants and ordered station sequences for the five-line scope; add data-quality checks. Keep `MERAK` out of scope and `SG` catalog-only unless a later scope decision changes that.
3. Add ordered SQL migrations for `nlp`, roles, queue, event-version structures, evidence, embeddings, skip/error records, and indexes.
   - Vector storage and pgvector search are required by this Spec but are deferred from migrations 001–003. A future ordered migration must add event-version embedding storage with model/version provenance, pgvector support, and vector dimension/index configuration aligned to the pinned model artifact. It must support filtered semantic retrieval and embedding-based duplicate candidate generation; finalize the artifact dimension and index configuration before designing that migration.
4. Implement the Python 3.12 worker adapter and `FOR UPDATE SKIP LOCKED` claim/lease flow; prove retry safety and immutable versioning.
5. Implement raw-text preservation, normalized derived text, classifier adapters (scikit-learn baseline first, IndoBERTweet as a later model option), station/segment/time extraction, and internal exact-quote validation.
6. Add `multilingual-e5-small` batch embedding, vector storage, query-compatible embedding adapter, and filtered pgvector retrieval.
7. Implement deterministic correlation/deduplication rules and record embedding candidates separately from accepted event identity.
8. Implement output validation, separate malformed-format and legitimate-rejection paths, verification/publication state, and auditable skip reasons.
9. Create `app_api` views/RPCs with strict field projection and eligibility filters. Apply database-clock 24-hour and 8/24-hour freshness rules.
10. Build FastAPI/Uvicorn endpoints for Path A and Path B; expose them through the configured NGROK tunnel for debug use. Verify the iOS app has no database credential or direct database route.
11. Add deterministic response templates and explicit `results`, `no_match`, and `source_unavailable` states. Add optional Ollama rewriting only after template path and validation are stable.
12. Create the manually reviewed JSONL gold set, pytest suite, and baseline evaluation report; add CI/release thresholds after baseline measurement.
13. Deploy worker scheduling with either `launchd` or cron, document operations and recovery, and monitor queue/evaluation metrics.

## 20. Acceptance criteria

The implementation is acceptable when all of the following are demonstrated:

- Existing news is read from `collection`; no second collector is introduced for the same data.
- Original text is immutable and every internal evidence quote validates against it exactly before publication; no exact quote or raw post text is returned to the app.
- Normalized text is a derived copy used only for classification/extraction/embedding, never as the evidence source.
- Relative time expressions are resolved from source post publication time in Asia/Jakarta, not worker processing time.
- A snapshot can be retried safely; outputs are versioned and old versions remain auditable.
- Each event version records model version and digest; every skipped post has a reason.
- Malformed output and valid policy rejection are separately represented and measured.
- Duplicate candidates are constrained by station, time window, and compatible category; embedding similarity alone cannot merge or verify events.
- Station-only posts without coordinates can resolve; unmapped station posts are not guessed and are recorded.
- Segment events retain line, start/end stations, direction, and a path variant when needed; ordered path-station data determines the affected range. Ambiguous branch/path resolution is not presented as precise.
- Verification/publication is enforced before an event becomes visible to `app_api`.
- GPS/route lookups use structured SQL; free-text uses pgvector with station/time filters applied before similarity where available and a mandatory 24-hour DB-time bound always present.
- Freshness is computed from the database clock: fresh through 8 hours, stale after 8 and through 24, expired after 24.
- Empty matches do not imply normal service; source unavailability is a different response state.
- The app reads only through the FastAPI gateway/NGROK path and `app_api`; it contains no database credentials and cannot access `collection` or raw `nlp` tables. `app_api` returns a curated conclusion and source object, not source-post text or `evidence_quote`.
- The manually reviewed gold set has several hundred examples, about 100 or more positive examples, many hard negatives, and time/content-grouped splits.
- Development migrations run only in the Supabase development database containing `collection`. They do not alter `collection` objects, data, roles, grants, or ownership, and are never applied to live during this implementation phase. Any later live rollout requires a separately reviewed plan and new schema-scoped roles/objects while preserving every existing `collection` role and object owner.
- The gold set and pytest suite cover station matching, time parsing, output validation, adapter behavior, idempotency, API boundaries, and both retrieval paths.
- Monitoring reports recall on known disruptions, skipped-post count, and format failure rate separately from legitimate rejection rate.

## 21. Open decisions to resolve before production

1. What exact stable ID in `collection` identifies an immutable post snapshot? If source rows can change, how is a new snapshot represented?
2. Which timestamp is authoritative for freshness when extracted event time conflicts with source publication time? Confirm the `freshness_anchor_at` fallback policy. Relative language itself must use source publication time in Asia/Jakarta.
3. What is the approved alias governance workflow, segment endpoint/direction convention, path-variant model, and behavior for events affecting multiple stations or lines? The initial scope is the five lines in `reference.krl_stations`; `MERAK` is deferred and `SG` is catalog-only in the inspected feed. Official KAI Commuter maps and 2026 timetables validate the broad ordered corridors, including Bogor/Nambo, Cikarang via Manggarai/Pasar Senen, the current Tangerang stop order and through-running continuation, Rangkasbitung with Jatake, and Tanjung Priok with a limited JIS service variant. The Open-KRL v2 feed remains reconstructed candidate data for per-trip patterns. Any route reference load must use source-version and effective-date fields, distinguish physical paths from through-running/short-turn/temporary-stop service patterns, and keep unresolved trip-level behavior explicit.
4. What verification process and evidence are required for an event to become publishable? Is approval automated, human, or mixed?
5. What time-window size and category-compatibility rules define duplicate candidates? These must be measured and versioned.
6. Which classifier is the initial production model: scikit-learn baseline or fine-tuned IndoBERTweet? What thresholds satisfy the gold-set quality gate?
7. Which query embedding runtime is selected (`fastembed` or ONNX Runtime), and how are compatible model artifacts deployed on each host?
8. Is optional Ollama rewriting enabled in production, and what is its timeout/fallback policy? The default should remain deterministic templates until explicitly approved.
9. What authentication mechanism protects FastAPI through NGROK and the production gateway? NGROK alone is not authorization.
10. What retention/deletion policy applies to source text, quotes, processing versions, embeddings, and logs?
11. What user-visible wording and localization are required for stale results, unresolved stations, no matching results, and source unavailability?
12. What numeric baseline thresholds are required for recall, precision, exact-quote validation, and format failure rate before publication?
13. What production rollout topology and approval process will be used for `nlp` and `app_api`? The implementation target is already decided: the existing Supabase development database containing `collection`, separate from live. Development migrations must not be applied to live, and must not change existing `collection` objects, data, roles, grants, or ownership.

## Appendix A — Suggested response shape

The exact names may change with the app contract, but responses should encode availability separately from the result list:

```json
{
  "availability": "results",
  "generated_at": "database-derived timestamp",
  "items": [
    {
      "title": "Structured title",
      "summary": "Curated conclusion composed from structured fields",
      "station_or_segment": {
        "kind": "segment",
        "line": "Canonical line name",
        "start_station": "Start station",
        "end_station": "End station",
        "direction": "Configured direction value"
      },
      "category": "Configured category",
      "event_time": "Parsed or source-supported time",
      "freshness": "fresh",
      "certainty": "Configured certainty value",
      "source": {
        "display_name": "Approved publisher/source name",
        "url": "Approved source URL",
        "published_at": "Source publication timestamp"
      }
    }
  ]
}
```

The exact evidence quote is retained and validated internally in `nlp`; it is not part of this response. The displayed summary must be a curated conclusion composed from structured fields, not copied post text. For an empty but successful search, return `availability: "no_match"` with an empty `items` array and wording that says no matching report was found. For dependency failure, return `availability: "source_unavailable"` (or an equivalent typed error contract); never return an empty successful result that could be mistaken for no disruption.
