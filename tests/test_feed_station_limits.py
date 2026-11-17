"""Long-route request validation and short copy; no external services."""
from types import SimpleNamespace
from unittest.mock import patch
import json

from mooby.api import messages
from mooby.api.repository import FeedPage
from mooby.api.response_preparation import prepare_item
from test_request_observability import ObservabilityTestCase, ROW, NOW

ROUTE_CODES = ('RK CTR MJ CKY TGS TEJ DAR CJT PRP JTK CC CSK SRP RU SDM JMU PDJ KBY '
               'PLM THB SUDB SUD MRI TEB CW DRN PSMB PSM TNT LNA UP UI POC DPB DP CTA BJD CLT BOO').split()


class FeedStationLimitTests(ObservabilityTestCase):
    def test_full_39_station_route_and_100_ids_reach_both_readers_without_truncation(self):
        route = ['open-krl:'+code for code in ROUTE_CODES]
        self.assertEqual(len(route),39)
        fixture = SimpleNamespace(generated_at=NOW,items=[])
        for ids in (route, [f'open-krl:S{i}' for i in range(100)]):
            with self.subTest(stations=len(ids)), \
                 patch('mooby.api.app.fetch_disruptions',return_value=FeedPage(NOW,[])) as fetch, \
                 patch('mooby.api.app._synthetic_fixture_mode_enabled',return_value=True), \
                 patch('mooby.api.app.synthetic_fixture_page',return_value=fixture) as mock:
                for path,reader in (('/api/v1/disruptions',fetch),('/api/v1/mock/disruptions',mock)):
                    start,raw,error=self.call(path=path,query={'station_ids':ids,'limit':7,'offset':3})
                    self.assertEqual(start['status'],200)
                    self.assertIsNone(error)
                    self.assertEqual(reader.call_args.kwargs['station_ids'],ids)
                    body=json.loads(raw)
                    self.assertEqual((body['limit'],body['offset']),(7,3))

    def test_101_ids_rejected_before_any_feed_reader(self):
        ids=['open-krl:BOO']*101
        with patch('mooby.api.app.fetch_disruptions') as fetch, \
             patch('mooby.api.app.synthetic_fixture_page') as fixture:
            for path in ('/api/v1/disruptions','/api/v1/mock/disruptions'):
                start,raw,error=self.call(path=path,query={'station_ids':ids})
                self.assertEqual(start['status'],422)
                self.assertIsNone(error)
                self.assertEqual(json.loads(raw)['detail'][0]['ctx']['max_length'],100)
            fetch.assert_not_called();fixture.assert_not_called()

    def test_short_copy_on_live_and_mock_success_empty_and_failure(self):
        for rows,live_message,mock_message in (
            ([],messages.FEED_NO_MATCH,messages.MOCK_NO_MATCH),
            ([dict(ROW)],messages.FEED_RESULTS,messages.MOCK_RESULTS),
        ):
            fixture=SimpleNamespace(generated_at=NOW,items=[SimpleNamespace(item=prepare_item(r)) for r in rows])
            with patch('mooby.api.app.fetch_disruptions',return_value=FeedPage(NOW,rows)), \
                 patch('mooby.api.app.synthetic_fixture_page',return_value=fixture), \
                 patch('mooby.api.app._synthetic_fixture_mode_enabled',return_value=True):
                for path,message in (('/api/v1/disruptions',live_message),('/api/v1/mock/disruptions',mock_message)):
                    self.assertEqual(json.loads(self.call(path=path)[1])['message'],message)
        from mooby.api.synthetic_fixtures import SyntheticFixtureUnavailable
        import psycopg
        with patch('mooby.api.app.fetch_disruptions',side_effect=psycopg.OperationalError('PRIVATE')), \
             patch('mooby.api.app.synthetic_fixture_page',side_effect=SyntheticFixtureUnavailable('PRIVATE')), \
             patch('mooby.api.app._synthetic_fixture_mode_enabled',return_value=True):
            for path,message in (('/api/v1/disruptions',messages.FEED_UNAVAILABLE),
                                 ('/api/v1/mock/disruptions',messages.MOCK_UNAVAILABLE)):
                start,raw,error=self.call(path=path)
                self.assertEqual(start['status'],503)
                self.assertEqual(json.loads(raw)['message'],message)
        for text in (messages.FEED_RESULTS,messages.FEED_NO_MATCH,messages.FEED_UNAVAILABLE,
                     messages.MOCK_RESULTS,messages.MOCK_NO_MATCH,messages.MOCK_UNAVAILABLE):
            self.assertEqual(text.count('.'),2)
            self.assertLessEqual(len(text.split()),12)
