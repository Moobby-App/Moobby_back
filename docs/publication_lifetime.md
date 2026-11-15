# Severity-specific feed visibility

The normal app API feed now uses light = 30 minutes, moderate = 60 minutes,
severe = 180 minutes. Legacy null/unknown severity gets 30 minutes, never 24h.
At `anchor + lifetime == transaction_timestamp()` the event is excluded.
Future anchors are excluded as well.

The anchor is the existing projection's `freshness_anchor_at`, defined by
`COALESCE(event_at, freshness_anchor_at, first_seen_at)`. Reading a feed, restarting
the API, caching, or changing the model does not reset the anchor. Expiry hides
events from this feed; it does not delete historical records or assert recovery.

Filtering happens in PostgreSQL before ordering, limit+1 and offset, so expired
rows do not create short pages or incorrect lookahead. The existing restricted
view still imposes its older 24h upper bound, which is wider than every new
lifetime. No database migration is needed for this narrower API filter. Direct
consumers of the view itself retain its 24h behavior.

`window_hours` changes from 24 to 3 as the feed's maximum window. Individual rows
have shorter severity windows. The empty-result message no longer claims 24h.
Each API item now includes `expires_at` as an ISO-8601 timestamp from the exact SQL
expiry calculation. iOS must stop displaying it at that timestamp, including when
offline; it must not restart the lifetime on download. This field is nullable for
legacy/injected items without an expiry, while normal SQL feed rows supply it.
The development mock endpoint remains synthetic, not an operational expiry test.

Redis uses a new cache namespace, bypassing old 24h feed entries. Cached pages
expire no later than the earliest row expiry, including the lookahead row; the
application checks expiry even if the Redis key still exists. Worker/model config,
SQL grants and stored rows are unchanged.

Restart only the API to load the filter:
`launchctl kickstart -k "gui/$(id -u)/dev.mooby.api"`.
No service restart or live database operation is performed by the implementation tests.
