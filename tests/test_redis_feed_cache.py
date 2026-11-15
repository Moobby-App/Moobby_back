"""Redis cache tests with deterministic in-memory Redis and ASGI doubles."""
import asyncio
import json
import unittest
from unittest.mock import patch

import psycopg

from mooby.api.app import create_app
from mooby.api.feed_cache import RedisFeedCache
from mooby.api.models import DisruptionFeed
from mooby.api.repository import FeedPage
from mooby.api.response_preparation import FeedQuery, prepare_feed
from test_request_observability import ENV, NOW, ROW, ObservabilityTestCase, exchange


class FakeRedis:
    def __init__(self):
        self.data = {}
        self.writes = []
        self.reads = 0
        self.closed = False

    def getrange(self, key, start, end):
        self.reads += 1
        return self.data.get(key, b'')[start:end+1]

    def set(self, key, value, ex):
        self.data[key] = value
        self.writes.append((key, ex))

    def close(self):
        self.closed = True


class RedisCacheUnitTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.cache = RedisFeedCache(self.redis)
        self.query = FeedQuery(('open-krl:SRP',), 1, 0)
        self.dsn = 'postgresql://private'
        self.feed = prepare_feed(self.query, FeedPage(NOW, [dict(ROW), dict(ROW)]))

    def test_round_trip_preserves_text_timestamp_nulls_and_lookahead(self):
        self.cache.put(self.dsn, self.query, self.feed)
        self.assertEqual(self.cache.get(self.dsn, self.query), self.feed)
        self.assertEqual(self.cache.get(self.dsn, self.query).next_offset, 1)
        self.assertEqual(self.redis.writes[0][1], 5)
        self.assertNotIn('private', self.redis.writes[0][0])

    def test_key_separates_database_filter_pagination_and_order(self):
        cases = [(self.dsn, self.query), ('postgresql://other', self.query),
                 (self.dsn, FeedQuery(('open-krl:BOO',), 1, 0)),
                 (self.dsn, FeedQuery(('open-krl:SRP',), 2, 0)),
                 (self.dsn, FeedQuery(('open-krl:SRP',), 1, 1)),
                 (self.dsn, FeedQuery(('open-krl:SRP','open-krl:BOO'))),
                 (self.dsn, FeedQuery(('open-krl:BOO','open-krl:SRP')))]
        self.assertEqual(len({self.cache.key(*case) for case in cases}), len(cases))

    def test_expired_and_future_payloads_are_misses(self):
        with patch('mooby.api.feed_cache.time.time', return_value=100):
            self.cache.put(self.dsn, self.query, self.feed)
        for now in (99, 105, 106):
            with patch('mooby.api.feed_cache.time.time', return_value=now):
                self.assertIsNone(self.cache.get(self.dsn, self.query))

    def test_corrupt_wrong_pagination_and_oversized_values_are_misses(self):
        key = self.cache.key(self.dsn, self.query)
        for value in (b'not json', b'{}', b'x'*(self.cache.max_bytes+2)):
            self.redis.data[key] = value
            self.assertIsNone(self.cache.get(self.dsn, self.query))
        self.cache.put(self.dsn, self.query, self.feed)
        value = json.loads(self.redis.data[key]); value['feed']['offset'] = 1
        self.redis.data[key] = json.dumps(value).encode()
        self.assertIsNone(self.cache.get(self.dsn, self.query))

    def test_empty_feed_is_cached_but_unavailable_is_not(self):
        empty = prepare_feed(self.query, FeedPage(NOW, []))
        self.cache.put(self.dsn, self.query, empty)
        self.assertEqual(self.cache.get(self.dsn, self.query), empty)
        unavailable = DisruptionFeed(availability='source_unavailable', generated_at=None,
                                     message='Unavailable', limit=1, offset=0)
        self.cache.put(self.dsn, self.query, unavailable)
        self.assertEqual(len(self.redis.writes), 1)

    def test_read_write_and_close_failures_are_optional(self):
        with patch.object(self.redis, 'getrange', side_effect=OSError('PRIVATE')):
            self.assertIsNone(self.cache.get(self.dsn, self.query))
        with patch.object(self.redis, 'set', side_effect=OSError('PRIVATE')):
            self.cache.put(self.dsn, self.query, self.feed)
        with patch.object(self.redis, 'close', side_effect=OSError('PRIVATE')):
            self.cache.close()

    def test_oversized_response_is_not_written(self):
        self.cache.max_bytes = 10
        self.cache.put(self.dsn, self.query, self.feed)
        self.assertEqual(self.redis.writes, [])

    def test_disabled_and_invalid_settings_do_not_create_connections(self):
        for env in ({}, {'APP_API_REDIS_URL':'https://wrong'},
                    {'APP_API_REDIS_URL':'redis://localhost?socket_timeout=99'},
                    {'APP_API_REDIS_URL':'redis://localhost','APP_API_REDIS_TTL_SECONDS':'0'}):
            with patch.dict('os.environ', env, clear=True):
                self.assertIsNone(RedisFeedCache.from_env().client)

    def test_client_pool_timeouts_and_retry_configuration(self):
        with patch.dict('os.environ', {'APP_API_REDIS_URL':'redis://127.0.0.1:6379/0'}, clear=True):
            cache = RedisFeedCache.from_env()
        self.assertIsNotNone(cache.client)
        settings = cache.client.connection_pool.connection_kwargs
        self.assertEqual(settings['socket_connect_timeout'], 0.1)
        self.assertEqual(settings['socket_timeout'], 0.1)
        self.assertEqual(cache.client.connection_pool.max_connections, 8)
        self.assertEqual(settings['retry'].get_retries(), 0)
        cache.close()


class RedisCacheApiTests(ObservabilityTestCase):
    def setUp(self):
        super().setUp()
        self.redis = FakeRedis()
        self.cache = RedisFeedCache(self.redis)
        with patch('mooby.api.app.RedisFeedCache.from_env', return_value=self.cache):
            self.app = create_app()

    def test_hit_is_identical_and_avoids_database_with_single_terminal(self):
        with patch('mooby.api.app.fetch_disruptions', return_value=FeedPage(NOW,[dict(ROW)])) as fetch:
            first = self.call(app=self.app)
            self.events.clear()
            second = self.call(app=self.app)
        self.assertEqual(first, second)
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(self.terminal()['category'], 'results')
        self.assertEqual(len(self.redis.writes), 1)

    def test_auth_and_input_validation_precede_cache(self):
        with patch('mooby.api.app.fetch_disruptions') as fetch:
            self.assertEqual(self.call(app=self.app,token=None)[0]['status'],401)
            self.assertEqual(self.call(app=self.app,query={'station_ids':'bad'})[0]['status'],422)
            fetch.assert_not_called()
        self.assertEqual(self.redis.reads,0)

    def test_redis_failure_falls_back_and_db_failure_stays_503_not_cached(self):
        with patch.object(self.redis,'getrange',side_effect=OSError('PRIVATE')):
            with patch('mooby.api.app.fetch_disruptions', return_value=FeedPage(NOW,[])):
                self.assertEqual(self.call(app=self.app)[0]['status'],200)
            with patch('mooby.api.app.fetch_disruptions',side_effect=psycopg.OperationalError('PRIVATE')):
                start,raw,error = self.call(app=self.app)
                self.assertEqual(start['status'],503)
                self.assertIsNone(error)
                self.assertNotIn('PRIVATE',raw.decode())
        self.assertEqual(len(self.redis.writes),1)

    def test_health_routes_never_use_cache(self):
        self.assertEqual(self.call(app=self.app,path='/health/live')[0]['status'],200)
        with patch('mooby.api.app.check_database',return_value=False):
            self.assertEqual(self.call(app=self.app,path='/health/ready')[0]['status'],503)
        self.assertEqual(self.redis.reads,0)

    def test_shutdown_closes_client(self):
        async def run():
            async with self.app.router.lifespan_context(self.app):
                pass
        asyncio.run(run())
        self.assertTrue(self.redis.closed)
