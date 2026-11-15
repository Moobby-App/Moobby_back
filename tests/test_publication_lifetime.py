"""Severity visibility SQL and cache boundary checks; no live database."""
import unittest
from datetime import timedelta
import json
from unittest.mock import patch

from mooby.api.repository import _SEARCH_SQL, _PROJECTION_SQL, FeedPage
from mooby.api.response_preparation import FeedQuery, prepare_feed
from mooby.api.feed_cache import RedisFeedCache
from test_redis_feed_cache import FakeRedis
from test_request_observability import NOW, ROW, ObservabilityTestCase


class PublicationLifetimeTests(unittest.TestCase):
    def test_item_exposes_sql_expiry_without_recomputing_from_nullable_event_time(self):
        query = FeedQuery(('open-krl:SRP',))
        expiry = NOW + timedelta(minutes=17)
        feed = prepare_feed(query, FeedPage(NOW, [dict(ROW, cache_expires_at=expiry)]))
        self.assertIsNone(feed.items[0].event_time)
        self.assertEqual(feed.items[0].expires_at, expiry)
        self.assertEqual(feed.model_dump(mode='json')['items'][0]['expires_at'],
                         expiry.isoformat().replace('+00:00','Z'))

    def test_sql_applies_lifetimes_before_order_and_pagination(self):
        for label, minutes in [('light',30),('moderate',60),('severe',180)]:
            self.assertIn(f"WHEN '{label}' THEN interval '{minutes} minutes'", _SEARCH_SQL)
        self.assertIn("ELSE interval '30 minutes'", _SEARCH_SQL)
        condition = ') > transaction_timestamp()'
        self.assertIn(condition, _SEARCH_SQL)
        self.assertLess(_SEARCH_SQL.index(condition), _SEARCH_SQL.index('LIMIT %s OFFSET %s'))
        self.assertIn('v.freshness_anchor_at <= transaction_timestamp()', _SEARCH_SQL)
        self.assertEqual(_SEARCH_SQL.count('%s'),3)
        self.assertEqual(_PROJECTION_SQL,_SEARCH_SQL.replace('LIMIT %s OFFSET %s','LIMIT 0'))

    def test_cached_item_expires_at_exact_boundary_not_full_redis_ttl(self):
        cache = RedisFeedCache(FakeRedis(),ttl_seconds=5)
        query = FeedQuery(('open-krl:SRP',))
        feed = prepare_feed(query,FeedPage(NOW,[dict(ROW)]))
        with patch('mooby.api.feed_cache.time.time',return_value=100):
            cache.put('postgresql://test',query,feed,valid_until=100.5)
        with patch('mooby.api.feed_cache.time.time',return_value=100.49):
            self.assertEqual(cache.get('postgresql://test',query),feed)
        with patch('mooby.api.feed_cache.time.time',return_value=100.5):
            self.assertIsNone(cache.get('postgresql://test',query))

    def test_already_expired_page_is_not_cached(self):
        redis = FakeRedis(); cache = RedisFeedCache(redis)
        query = FeedQuery(('open-krl:SRP',))
        feed = prepare_feed(query,FeedPage(NOW,[dict(ROW)]))
        with patch('mooby.api.feed_cache.time.time',return_value=100):
            cache.put('postgresql://test',query,feed,valid_until=100)
        self.assertEqual(redis.writes,[])

    def test_old_cache_namespace_is_not_reused(self):
        self.assertTrue(RedisFeedCache.key('db',FeedQuery(('open-krl:SRP',))).startswith(
            'moobby:feed:v3:item-expiry:'))


class ExpiryResponseTests(ObservabilityTestCase):
    def test_normal_api_serializes_each_sql_expiry(self):
        rows = [dict(ROW, severity=severity, cache_expires_at=NOW+timedelta(minutes=minutes))
                for severity, minutes in [('light',30),('moderate',60),('severe',180)]]
        with patch('mooby.api.app.fetch_disruptions',return_value=FeedPage(NOW,rows)):
            start,raw,error=self.call()
        self.assertEqual(start['status'],200)
        self.assertIsNone(error)
        items=json.loads(raw)['items']
        for item,row in zip(items,rows):
            self.assertEqual(item['expires_at'],row['cache_expires_at'].isoformat().replace('+00:00','Z'))
