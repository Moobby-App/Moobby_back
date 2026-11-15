# Redis cache for the normal disruption feed

The synchronous authenticated `/api/v1/disruptions` route uses a cache-aside Redis
response cache. PostgreSQL remains the source of truth. Mock feeds and live/ready
health probes bypass Redis. Authentication, configuration and query validation run
before any cache lookup; 401/422/503 responses are never stored.

Configuration (read when `create_app()` runs; restart the API after changing it):

```dotenv
APP_API_REDIS_URL=redis://127.0.0.1:6379/0
APP_API_REDIS_TTL_SECONDS=5
```

The repository example leaves the URL empty to avoid assuming an installed Redis
server. Set it to your actual server to enable caching; no Redis service is started
by the application. Use `rediss://` for TLS. URL query parameters are rejected so
they cannot override bounded timeouts. TTL must be 1–60 seconds. An empty/invalid
configuration, missing optional runtime client, unavailable Redis, full pool or
bad payload makes caching bypass to the existing DB path. `redis==6.4.0` is declared
in project dependencies. No Redis credentials or cache payloads are logged.

## Behavior and bounds

- Cache keys use a versioned SHA-256 identity of database URL, ordered station IDs
  (including duplicates), limit and offset. Credentials are not exposed in keys.
- Only successfully prepared and validated `results` or `no_match` responses are
  written, preserving original text, nullable fields, ordering and lookahead result.
- `generated_at` remains the original PostgreSQL timestamp of the cached snapshot;
  it is not refreshed on cache hit. New publications/removals/freshness changes may
  take up to the configured TTL to appear. No stale-after-expiry fallback is served.
- Values have both Redis expiry (`SET ... EX`) and an application expiry check.
  Reads use bounded `GETRANGE`; responses larger than 1 MiB bypass caching, with no
  truncation or contract change. Fresh cached data can serve during a DB outage;
  readiness still checks the database directly and can return 503 independently.
- Each app has a thread-safe Redis connection pool, maximum 8 connections,
  100 ms connect and socket timeouts, and zero retries. Failed get/set operations
  add bounded socket waits before falling back. DNS resolution is OS-controlled;
  use a local/IP Redis endpoint where a strict DNS-independent bound is needed.
- API shutdown closes its Redis client. No background cache thread is started.
- Concurrent misses may read PostgreSQL independently. There is no distributed
  stampede lock and no worker-side publication invalidation in this implementation.
  Redis should have its own `maxmemory` and eviction policy (e.g. `allkeys-lru`);
  expiry bounds key lifetime, not server memory under arbitrary request volume.

## Observability and rollback

The diagnostic schema and terminal event count are unchanged. Cache hits still run
response validation and set the correct response availability category. Database
and row-mapping stages occur only on misses; those stages are not fabricated on
hits. Total endpoint time includes cache operations. Existing correlation IDs and
transport-abort reporting stay in the outer middleware. No cache-specific response
headers are introduced.

Disable by clearing `APP_API_REDIS_URL` and restarting the API. No schema migration,
SQL, worker, model, authentication or process-isolation changes are required.

Tests: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:tests .venv/bin/python -B -m unittest test_redis_feed_cache -q`.
Tests use in-memory Redis/ASGI doubles, not a database or production Redis.

Redis client connection/pooling reference:
https://redis.io/docs/latest/develop/clients/redis-py/connect/
