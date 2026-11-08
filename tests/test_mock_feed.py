"""In-memory ASGI requests with fake data, no sockets, DB or model calls."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import urlencode
from uuid import uuid4

from mooby.api.app import create_app
from mooby.api.models import AppSource, DisruptionItem, StationOrSegment
from mooby.api.repository import FeedPage
from mooby.api.synthetic_fixtures import SyntheticFixtureUnavailable

NOW = datetime.now(timezone.utc)
ENV = {'APP_API_DATABASE_URL':'postgresql://unused', 'APP_API_BEARER_TOKEN':'x'*32,
       'APP_API_ENVIRONMENT':'development', 'APP_API_SYNTHETIC_FIXTURE_MODE':'development',
       'APP_API_SYNTHETIC_FIXTURE_TTL_MINUTES':'60'}


async def request(path, token='x'*32, query=None):
    messages = []
    async def receive():
        return {'type':'http.request', 'body':b'', 'more_body':False}
    async def send(message): messages.append(message)
    headers = [(b'authorization', ('Bearer '+token).encode())] if token else []
    await create_app()({'type':'http', 'asgi':{'version':'3.0'}, 'http_version':'1.1',
        'method':'GET', 'scheme':'http', 'path':path, 'raw_path':path.encode(),
        'root_path':'', 'query_string':urlencode(query or {'station_ids':'open-krl:BOO'}).encode(),
        'headers':headers, 'client':('127.0.0.1',1), 'server':('127.0.0.1',8000)}, receive, send)
    status = next(m['status'] for m in messages if m['type']=='http.response.start')
    body = b''.join(m.get('body',b'') for m in messages if m['type']=='http.response.body')
    return status, json.loads(body)


class MockFeedTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict('os.environ', ENV, clear=True); self.env.start(); self.addCleanup(self.env.stop)
        self.db = patch('mooby.api.app.fetch_disruptions', return_value=FeedPage(NOW, []))
        self.fetch = self.db.start(); self.addCleanup(self.db.stop)
        item = DisruptionItem(public_event_id=uuid4(), title='SIMULASI — Gangguan layanan KRL di Bogor',
            summary='Ringkasan sintetis.', station_or_segment=StationOrSegment(kind='station',
                station='Bogor', station_id='open-krl:BOO', affected_station_ids=['open-krl:BOO']),
            severity='light', freshness='fresh', source=AppSource(display_name='SIMULASI · dataset sintetis',
                url='https://example.invalid/synthetic/test', published_at=NOW))
        self.page = SimpleNamespace(generated_at=NOW, items=[SimpleNamespace(item=item)])
        self.fixture = patch('mooby.api.app.synthetic_fixture_page', return_value=self.page)
        self.synthetic = self.fixture.start(); self.addCleanup(self.fixture.stop)

    def test_normal_feed_always_uses_database_even_with_mock_enabled(self):
        code, body = asyncio.run(request('/api/v1/disruptions'))
        self.assertEqual(code, 200); self.assertEqual(body['availability'], 'no_match')
        self.fetch.assert_called_once(); self.synthetic.assert_not_called()

    def test_mock_feed_has_same_location_contract_and_no_database_or_private_fields(self):
        code, body = asyncio.run(request('/api/v1/mock/disruptions'))
        self.assertEqual(code, 200); self.assertEqual(body['availability'], 'results')
        self.assertTrue(body['items'][0]['title'].startswith('SIMULASI'))
        self.assertEqual(body['items'][0]['station_or_segment']['affected_station_ids'], ['open-krl:BOO'])
        self.fetch.assert_not_called()
        for key in ['raw_text','evidence_quote','author','source_snapshot_id','embedding','result_metadata']:
            self.assertNotIn(key, json.dumps(body))
        self.synthetic.assert_called_once_with(station_ids=['open-krl:BOO'], limit=20, offset=0)

    def test_mock_requires_authentication(self):
        for token in [None, 'wrong']:
            self.assertEqual(asyncio.run(request('/api/v1/mock/disruptions', token=token))[0], 401)
        self.synthetic.assert_not_called(); self.fetch.assert_not_called()

    def test_disabled_or_expired_mock_is_404_without_database_fallback(self):
        with patch.dict('os.environ', {'APP_API_SYNTHETIC_FIXTURE_MODE':'disabled'}):
            self.assertEqual(asyncio.run(request('/api/v1/mock/disruptions'))[0], 404)
        with patch('mooby.api.app._PROCESS_STARTED_AT', NOW-timedelta(hours=5)):
            self.assertEqual(asyncio.run(request('/api/v1/mock/disruptions'))[0], 404)
        self.fetch.assert_not_called(); self.synthetic.assert_not_called()

    def test_non_development_invalid_ttl_or_fixture_error_fail_closed(self):
        for override in [{'APP_API_ENVIRONMENT':'production'}, {'APP_API_SYNTHETIC_FIXTURE_TTL_MINUTES':'invalid'}]:
            with patch.dict('os.environ', override):
                self.assertEqual(asyncio.run(request('/api/v1/mock/disruptions'))[0], 503)
        self.synthetic.side_effect = SyntheticFixtureUnavailable('private message')
        code, body = asyncio.run(request('/api/v1/mock/disruptions'))
        self.assertEqual(code, 503); self.assertEqual(body['availability'],'source_unavailable')
        self.assertNotIn('private message', json.dumps(body)); self.fetch.assert_not_called()

    def test_mock_station_ids_and_pagination_are_validated(self):
        for query in [{'station_ids':'Bogor'}, {'station_ids':'open-krl:BOO','limit':51}]:
            self.assertEqual(asyncio.run(request('/api/v1/mock/disruptions', query=query))[0], 422)
        self.fetch.assert_not_called(); self.synthetic.assert_not_called()
        self.page.items *= 2
        code, body = asyncio.run(request('/api/v1/mock/disruptions',
            query={'station_ids':'open-krl:BOO','limit':1,'offset':0}))
        self.assertEqual(code, 200); self.assertEqual(len(body['items']),1)
        self.assertEqual(body['next_offset'],1)

