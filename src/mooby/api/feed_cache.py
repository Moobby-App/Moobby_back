"""Optional bounded Redis response cache; failures always fall back to the DB.

Only validated, successful normal feeds are cached. Redis never decides auth or
readiness. Keys include the database identity, ordered station IDs and pagination.
No tokens, DSNs, original text or Redis error messages are logged here.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from mooby.api.models import DisruptionFeed
from mooby.api.response_preparation import FeedQuery


@dataclass
class RedisFeedCache:
    client: Any = field(default=None, repr=False)
    ttl_seconds: int = 5
    max_bytes: int = 1_048_576

    @classmethod
    def from_env(cls) -> RedisFeedCache:
        url = os.environ.get('APP_API_REDIS_URL', '').strip()
        if not url:
            return cls()
        try:
            parsed = urlsplit(url)
            # Query parameters in redis-py can override bounded socket settings.
            if parsed.scheme not in {'redis', 'rediss'} or not parsed.hostname or parsed.query:
                return cls()
            ttl = int(os.environ.get('APP_API_REDIS_TTL_SECONDS', '5'))
            if not 1 <= ttl <= 60:
                return cls()
            from redis import Redis
            from redis.backoff import NoBackoff
            from redis.retry import Retry

            client = Redis.from_url(
                url, socket_connect_timeout=0.1, socket_timeout=0.1,
                max_connections=8, retry=Retry(NoBackoff(), 0),
                retry_on_timeout=False, decode_responses=False,
            )
            return cls(client=client, ttl_seconds=ttl)
        except Exception:
            # Optional cache configuration/dependency failure must not break API.
            return cls()

    @staticmethod
    def key(database_url: str, query: FeedQuery) -> str:
        identity = json.dumps(
            [database_url, query.station_ids, query.limit, query.offset],
            ensure_ascii=True, separators=(',', ':'),
        )
        return 'moobby:feed:v3:item-expiry:' + hashlib.sha256(identity.encode()).hexdigest()

    def get(self, database_url: str, query: FeedQuery) -> DisruptionFeed | None:
        if self.client is None:
            return None
        try:
            # Bound bytes read even if Redis contains a malformed/oversized value.
            payload = self.client.getrange(self.key(database_url, query), 0, self.max_bytes)
            if not payload or len(payload) > self.max_bytes:
                return None
            envelope = json.loads(payload)
            expires = envelope['expires_at']
            now = time.time()
            if not isinstance(expires, (int, float)) or not now < expires <= now + self.ttl_seconds:
                return None
            feed = DisruptionFeed.model_validate(envelope['feed'])
            if not self._eligible(feed, query):
                return None
            return feed
        except Exception:
            return None

    @staticmethod
    def _eligible(feed: DisruptionFeed, query: FeedQuery) -> bool:
        return (
            feed.availability in {'results', 'no_match'}
            and feed.generated_at is not None
            and feed.limit == query.limit and feed.offset == query.offset
            and len(feed.items) <= query.limit
            and (feed.availability == 'results') == bool(feed.items)
            and feed.next_offset in {None, query.offset + query.limit}
        )

    def put(self, database_url: str, query: FeedQuery, feed: DisruptionFeed,
            *, valid_until: float | None = None) -> None:
        if self.client is None or not self._eligible(feed, query):
            return
        try:
            now = time.time()
            expiry = min(now + self.ttl_seconds, valid_until) if valid_until is not None else now + self.ttl_seconds
            if expiry <= now:
                return
            payload = json.dumps({
                'expires_at': expiry,
                'feed': feed.model_dump(mode='json'),
            }, ensure_ascii=False, separators=(',', ':')).encode()
            if len(payload) <= self.max_bytes:
                self.client.set(self.key(database_url, query), payload, ex=self.ttl_seconds)
        except Exception:
            pass

    def close(self) -> None:
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
