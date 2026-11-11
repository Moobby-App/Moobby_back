"""R03 error/health contracts with ASGI and driver doubles; no external access."""
import json
from types import SimpleNamespace
from unittest.mock import patch

import psycopg

from mooby.api import repository
from mooby.api.models import DisruptionFeed
from test_request_observability import ObservabilityTestCase, DriverDouble, ROW, NOW, PRIVATE


class ProjectionConnection(DriverDouble):
    def __init__(self, error=None):
        super().__init__()
        self.error = error

    def execute(self, sql, params=()):
        if sql == repository._PROJECTION_SQL:
            self.queries.append((sql, params))
            if self.error is not None:
                raise self.error
            return SimpleNamespace(fetchall=lambda: [])
        return super().execute(sql, params)


class ErrorReadinessTests(ObservabilityTestCase):
    def assert_feed_failure(self, raw, *, limit=20, offset=0):
        payload = json.loads(raw)
        self.assertEqual(payload, {
            "availability":"source_unavailable", "generated_at":None, "window_hours":24,
            "message":"Informasi gangguan belum dapat diperiksa. Coba lagi.",
            "items":[], "limit":limit, "offset":offset, "next_offset":None,
        })
        DisruptionFeed.model_validate(payload)
        self.assertNotIn(PRIVATE, raw.decode())

    def test_configuration_failure_preserves_pagination_on_normal_and_mock(self):
        with patch.dict("os.environ", {"APP_API_BEARER_TOKEN":""}):
            for path in ("/api/v1/disruptions", "/api/v1/mock/disruptions"):
                start, raw, error = self.call(path=path, query={"station_ids":"open-krl:SRP", "limit":7, "offset":3})
                self.assertEqual(start["status"], 503); self.assertIsNone(error)
                self.assert_feed_failure(raw, limit=7, offset=3)
        self.connect.assert_not_called()

    def test_configuration_failure_defaults_only_invalid_pagination_fields(self):
        cases = [({},20,0), ({"limit":"bad","offset":9},20,9),
                 ({"limit":8,"offset":-1},8,0), ({"limit":51,"offset":1001},20,0),
                 ({"limit":"7.0","offset":"3.0"},7,3),
                 ({"limit":["2","5"],"offset":["1","4"]},5,4)]
        with patch.dict("os.environ", {"APP_API_BEARER_TOKEN":""}):
            for params, limit, offset in cases:
                with self.subTest(params=params):
                    start, raw, error = self.call(query={"station_ids":"open-krl:SRP", **params})
                    self.assertEqual(start["status"], 503); self.assertIsNone(error)
                    self.assert_feed_failure(raw, limit=limit, offset=offset)

    def test_configuration_failure_does_not_change_auth_validation_precedence(self):
        with patch.dict("os.environ", {"APP_API_BEARER_TOKEN":""}):
            start, _, error = self.call(token=None, query={})
            self.assertEqual(start["status"], 503); self.assertIsNone(error)
        self.assertEqual(self.call(token=None)[0]["status"], 401)
        self.assertEqual(self.call(query={"station_ids":"bad"})[0]["status"], 422)
        self.connect.assert_not_called()

    def test_invalid_feed_envelope_is_controlled_on_empty_and_nonempty_pages(self):
        for rows in ([], [ROW]):
            self.events.clear()
            with patch("mooby.api.app.fetch_disruptions", return_value=repository.FeedPage(PRIVATE, rows)):
                start, raw, error = self.call(query={"station_ids":"open-krl:SRP","limit":9,"offset":2})
            self.assertEqual(start["status"], 503); self.assertIsNone(error)
            self.assert_feed_failure(raw, limit=9, offset=2)
            self.assertEqual(self.terminal()["failure_stage"], "response_validation")
            self.assertEqual(self.terminal()["exception_category"], "validation_error")
            self.assertNotIn(PRIVATE, json.dumps(self.events))

    def test_bad_row_remains_controlled_and_private(self):
        with patch("mooby.api.app.fetch_disruptions", return_value=repository.FeedPage(NOW, [{**ROW,"severity":"unsupported"}])):
            start, raw, error = self.call()
        self.assertEqual(start["status"], 503); self.assertIsNone(error)
        self.assert_feed_failure(raw)
        self.assertEqual(self.terminal()["failure_stage"], "row_mapping")

    def test_unexpected_programming_errors_are_not_silently_swallowed(self):
        failure = RuntimeError(PRIVATE)
        with patch("mooby.api.app.fetch_disruptions", side_effect=failure):
            start, raw, error = self.call()
        self.assertEqual(start["status"], 500)
        self.assertIs(error, failure)
        self.assertNotIn(PRIVATE, raw.decode())

    def test_normal_feed_still_issues_only_two_selects_and_preserves_pagination(self):
        driver = DriverDouble()
        self.connect.side_effect = None; self.connect.return_value = driver
        start, raw, error = self.call(query={"station_ids":["open-krl:SRP","open-krl:THB"],"limit":7,"offset":3})
        self.assertEqual(start["status"], 200); self.assertIsNone(error)
        payload = json.loads(raw)
        self.assertEqual((payload["availability"],payload["limit"],payload["offset"]), ("results",7,3))
        self.assertEqual(len(driver.queries), 2)
        self.assertEqual(driver.queries[1], (repository._SEARCH_SQL,(["open-krl:SRP","open-krl:THB"],8,3)))
        self.assertTrue(driver.closed)

    def test_normal_no_match_preserves_success_contract(self):
        with patch("mooby.api.app.fetch_disruptions", return_value=repository.FeedPage(NOW, [])):
            start, raw, error = self.call()
        self.assertEqual(start["status"], 200); self.assertIsNone(error)
        payload = DisruptionFeed.model_validate_json(raw)
        self.assertEqual(payload.availability, "no_match")
        self.assertEqual(payload.items, [])

    def test_readiness_configuration_failure_uses_readiness_schema(self):
        with patch.dict("os.environ", {"APP_API_BEARER_TOKEN":""}):
            start, raw, error = self.call(path="/health/ready")
        self.assertEqual(start["status"], 503); self.assertIsNone(error)
        self.assertEqual(json.loads(raw), {"api":"available","database":"unavailable","app_api_projection":"unavailable"})
        self.connect.assert_not_called()

    def test_liveness_remains_independent_of_configuration_and_database(self):
        with patch.dict("os.environ", {"APP_API_BEARER_TOKEN":"","APP_API_DATABASE_URL":""}):
            start, raw, error = self.call(path="/health/live",token=None)
        self.assertEqual(start["status"], 200); self.assertIsNone(error)
        self.assertEqual(json.loads(raw), {"api":"available"})
        self.connect.assert_not_called()

    def test_readiness_empty_projection_checks_all_feed_columns_without_rows(self):
        clock, projection = DriverDouble(), ProjectionConnection()
        self.connect.side_effect = [clock, projection]
        start, raw, error = self.call(path="/health/ready")
        self.assertEqual(start["status"], 200); self.assertIsNone(error)
        self.assertEqual(json.loads(raw), {"api":"available","database":"available","app_api_projection":"available"})
        self.assertEqual(self.connect.call_count, 2)
        self.assertEqual(len(clock.queries), 1)
        self.assertEqual(projection.queries, [(repository._PROJECTION_SQL,([],))])
        self.assertTrue(repository._PROJECTION_SQL.strip().endswith("LIMIT 0"))
        self.assertIn("v.original_text", repository._PROJECTION_SQL)
        self.assertIn("v.affected_station_ids", repository._PROJECTION_SQL)
        self.assertEqual(repository._PROJECTION_SQL,
                         repository._SEARCH_SQL.replace("LIMIT %s OFFSET %s", "LIMIT 0"))
        self.assertIn("v.affected_station_ids && %s::text[]", repository._PROJECTION_SQL)
        self.assertIn("ORDER BY v.freshness_anchor_at DESC, v.public_event_id", repository._PROJECTION_SQL)
        self.assertTrue(clock.closed and projection.closed)

    def test_missing_feed_column_fails_readiness_and_preserves_clock_status(self):
        clock, projection = DriverDouble(), ProjectionConnection(psycopg.errors.UndefinedColumn(PRIVATE))
        self.connect.side_effect = [clock, projection]
        start, raw, error = self.call(path="/health/ready")
        self.assertEqual(start["status"], 503); self.assertIsNone(error)
        self.assertEqual(json.loads(raw), {"api":"available","database":"available","app_api_projection":"unavailable"})
        self.assertEqual(self.terminal()["failure_stage"], "projection_readiness")
        self.assertTrue(clock.closed and projection.closed)
        self.assertNotIn(PRIVATE, json.dumps(self.events))

    def test_readiness_connection_error_categories_are_controlled(self):
        for exception in (OSError, psycopg.OperationalError, TypeError, ValueError, KeyError):
            self.events.clear()
            self.connect.side_effect = exception(PRIVATE)
            start, raw, error = self.call(path="/health/ready")
            self.assertEqual(start["status"], 503); self.assertIsNone(error)
            self.assertEqual(json.loads(raw)["database"], "unavailable")
            self.assertEqual(self.terminal()["failure_stage"], "database_connect")

    def test_readiness_projection_error_categories_are_controlled(self):
        for exception in (OSError, psycopg.OperationalError, TypeError, ValueError, KeyError):
            self.events.clear()
            self.connect.side_effect = [DriverDouble(),ProjectionConnection(exception(PRIVATE))]
            start, raw, error = self.call(path="/health/ready")
            self.assertEqual(start["status"], 503); self.assertIsNone(error)
            self.assertEqual(json.loads(raw)["database"], "available")
            self.assertEqual(json.loads(raw)["app_api_projection"], "unavailable")
            self.assertEqual(self.terminal()["failure_stage"], "projection_readiness")

    def test_readiness_boundary_handles_oserror_and_skips_unnecessary_probe(self):
        with patch("mooby.api.app.check_database",side_effect=OSError(PRIVATE)), \
             patch("mooby.api.app.check_app_api_projection") as projection:
            start, raw, error = self.call(path="/health/ready")
        self.assertEqual(start["status"],503); self.assertIsNone(error)
        self.assertEqual(json.loads(raw)["database"],"unavailable")
        projection.assert_not_called()
