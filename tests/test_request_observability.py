"""R01: in-memory ASGI and driver doubles only; no sockets or local secrets."""

import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from threading import Barrier
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import urlencode
from uuid import UUID

import psycopg

from mooby.api.app import create_app
from mooby.api.models import DisruptionFeed
from mooby.api import observability as obs
from mooby.api.repository import FeedPage, _SEARCH_SQL


NOW = datetime.now(timezone.utc)
PRIVATE = "DO_NOT_LOG_secret_source_author_password"
TOKEN = "private_bearer_" + "x" * 32
ENV = {"APP_API_DATABASE_URL": "postgresql://" + PRIVATE,
       "APP_API_BEARER_TOKEN": TOKEN}
ROW = dict(public_event_id="00000000-0000-0000-0000-000000000001",
           title=PRIVATE, summary=PRIVATE, original_text=PRIVATE,
           event_kind="station", station_name=PRIVATE, station_id="open-krl:SRP",
           affected_station_ids=["open-krl:SRP"], severity=None, event_at=None,
           event_time_certainty=None, freshness="fresh", certainty_label=None,
           source_display_name=PRIVATE, source_url="https://example.invalid/post",
           source_published_at=None)
TERMINALS = {"request_completed", "request_failed", "request_aborted", "request_cancelled"}


async def exchange(app=None, *, query=None, token=TOKEN, path="/api/v1/disruptions"):
    messages = []
    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}
    async def send(message):
        messages.append(message)
    headers = [(b"x-request-id", PRIVATE.encode())]
    if token is not None:
        headers.append((b"authorization", ("Bearer " + token).encode()))
    caught = None
    try:
        await (app or create_app())({
            "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
            "method": "GET", "scheme": "http", "path": path, "raw_path": path.encode(),
            "root_path": "", "query_string": urlencode(
                {"station_ids": "open-krl:SRP"} if query is None else query,
                doseq=True).encode(), "headers": headers,
            "client": ("127.0.0.1", 123), "server": ("127.0.0.1", 8000),
        }, receive, send)
    except Exception as exc:
        caught = exc
    start = next(m for m in messages if m["type"] == "http.response.start")
    raw = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
    return start, raw, caught


class DriverDouble:
    def __init__(self, failure=None):
        self.failure = failure
        self.queries = []
        self.ids = []
        self.closed = False
        self.exited_with = None

    def __enter__(self):
        return self

    def __exit__(self, typ, value, tb):
        self.closed = True
        self.exited_with = typ

    @contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=()):
        self.ids.append(obs.current_request_id())
        self.queries.append((sql, params))
        if self.failure == "clock" and "transaction_timestamp() AS generated_at" in sql:
            raise psycopg.OperationalError(PRIVATE)
        if self.failure == "feed" and sql == _SEARCH_SQL:
            raise psycopg.errors.QueryCanceled(PRIVATE)
        return SimpleNamespace(fetchone=lambda: {"generated_at": NOW},
                               fetchall=lambda: [dict(ROW)])


class ObservabilityTestCase(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.env = patch.dict("os.environ", ENV, clear=True)
        self.env.start(); self.addCleanup(self.env.stop)
        self.logs = patch.object(obs.logger, "info", side_effect=lambda payload:
                                 self.events.append(json.loads(payload)))
        self.logs.start(); self.addCleanup(self.logs.stop)
        # Fail closed if any test accidentally reaches a real database.
        self.network = patch("psycopg.connect", side_effect=AssertionError("network forbidden"))
        self.connect = self.network.start(); self.addCleanup(self.network.stop)

    def terminal(self):
        values = [e for e in self.events if e["event"] in TERMINALS]
        self.assertEqual(len(values), 1)
        return values[0]

    def call(self, **kwargs):
        return asyncio.run(exchange(**kwargs))


class RequestObservabilityTests(ObservabilityTestCase):
    def test_unique_server_ids_context_reset_and_unchanged_headers(self):
        with patch("mooby.api.app.fetch_disruptions", return_value=FeedPage(NOW, [])):
            for _ in range(2):
                start, raw, error = self.call()
                self.assertEqual(start["status"], 200)
                self.assertIsNone(error)
                self.assertEqual(set(k for k, _ in start["headers"]),
                                 {b"content-type", b"content-length"})
                self.assertEqual(json.loads(raw)["availability"], "no_match")
        admitted = [e for e in self.events if e["event"] == "request_admitted"]
        self.assertEqual(len({e["request_id"] for e in admitted}), 2)
        for event in admitted:
            value = UUID(hex=event["request_id"])
            self.assertEqual(value.version, 4)
            self.assertEqual(value.hex, event["request_id"])
        self.assertIsNone(obs.current_request_id())

    def test_concurrent_sync_endpoint_context_is_isolated(self):
        barrier = Barrier(2)
        seen = []
        def fetch(**kwargs):
            before = obs.current_request_id()
            barrier.wait(timeout=3)
            seen.append((before, obs.current_request_id()))
            return FeedPage(NOW, [])
        async def run():
            app = create_app()
            return await asyncio.gather(exchange(app), exchange(app))
        with patch("mooby.api.app.fetch_disruptions", side_effect=fetch):
            results = asyncio.run(run())
        self.assertTrue(all(r[0]["status"] == 200 and r[2] is None for r in results))
        self.assertEqual(len({pair[0] for pair in seen}), 2)
        self.assertTrue(all(before == after and before is not None for before, after in seen))
        for request_id in {pair[0] for pair in seen}:
            events = [e for e in self.events if e["request_id"] == request_id]
            self.assertEqual(sum(e["event"] in TERMINALS for e in events), 1)
            self.assertIn("authentication", {e.get("stage") for e in events})
            self.assertIn("endpoint_execution", {e.get("stage") for e in events})

    def test_results_and_all_supported_timing_boundaries_with_exact_query_count(self):
        driver = DriverDouble()
        self.connect.side_effect = None; self.connect.return_value = driver
        start, raw, error = self.call()
        self.assertEqual(start["status"], 200); self.assertIsNone(error)
        self.assertEqual(json.loads(raw)["items"][0]["original_text"], PRIVATE)
        self.assertEqual(self.terminal()["category"], "results")
        stages = {e.get("stage") for e in self.events}
        self.assertTrue({"authentication", "endpoint_execution", "database_connect",
                         "database_timestamp", "database_feed", "database_session",
                         "row_mapping", "response_validation", "response_send"} <= stages)
        self.assertEqual(len(driver.queries), 2)
        self.assertEqual(driver.queries[1], (_SEARCH_SQL, (["open-krl:SRP"], 21, 0)))
        self.assertTrue(driver.closed)
        self.assertEqual(set(driver.ids), {self.terminal()["request_id"]})
        for event in self.events:
            self.assertGreaterEqual(event["duration_ms"], 0)
            self.assertEqual(datetime.fromisoformat(event["timestamp"]).utcoffset().total_seconds(), 0)
            if "stage" in event:
                self.assertGreaterEqual(event["stage_duration_ms"], 0)

    def test_no_match_lifecycle_accounting(self):
        with patch("mooby.api.app.fetch_disruptions", return_value=FeedPage(NOW, [])):
            start, raw, error = self.call()
        self.assertEqual(start["status"], 200)
        self.assertIsNone(error)
        self.assertEqual(json.loads(raw)["items"], [])
        self.assertEqual(self.terminal()["event"], "request_completed")
        self.assertEqual(self.terminal()["category"], "no_match")
        self.assertEqual(sum(e["event"] == "request_admitted" for e in self.events), 1)

    def test_auth_failures(self):
        for token in (None, PRIVATE):
            with self.subTest(token_present=token is not None):
                self.events.clear()
                start, _, error = self.call(token=token)
                self.assertEqual(start["status"], 401); self.assertIsNone(error)
                self.assertEqual(self.terminal()["category"], "authentication_failure")
                self.assertEqual(self.terminal()["failure_stage"], "authentication")
        self.connect.assert_not_called()

    def test_configuration_error_is_observed_without_contract_fix(self):
        with patch.dict("os.environ", {"APP_API_BEARER_TOKEN": ""}):
            start, raw, error = self.call(query={"station_ids": "open-krl:SRP", "limit": 7})
        self.assertEqual(start["status"], 503); self.assertIsNone(error)
        self.assertEqual(json.loads(raw)["limit"], 20)  # Existing R03 gap is unchanged.
        self.assertEqual(self.terminal()["exception_category"], "configuration_error")
        self.assertEqual(self.terminal()["failure_stage"], "authentication")

    def test_framework_and_endpoint_parameter_validation(self):
        for query in ({}, {"station_ids": PRIVATE}, {"station_ids": "open-krl:SRP", "limit": 51}):
            self.events.clear()
            start, _, error = self.call(query=query)
            self.assertEqual(start["status"], 422); self.assertIsNone(error)
            self.assertEqual(self.terminal()["category"], "parameter_validation_failure")
            self.assertEqual(self.terminal()["failure_stage"], "parameter_validation")
        self.connect.assert_not_called()

    def test_connection_and_query_failure_stages_and_cleanup(self):
        for failure, stage in (("connect", "database_connect"), ("clock", "database_timestamp"),
                               ("feed", "database_feed")):
            self.events.clear()
            driver = DriverDouble(failure)
            self.connect.side_effect = psycopg.OperationalError(PRIVATE) if failure == "connect" else None
            self.connect.return_value = driver
            start, raw, error = self.call()
            self.assertEqual(start["status"], 503); self.assertIsNone(error)
            self.assertEqual(json.loads(raw)["availability"], "source_unavailable")
            event = self.terminal()
            self.assertEqual(event["event"], "request_failed")
            self.assertEqual(event["failure_stage"], stage)
            self.assertEqual(event["exception_category"],
                             "connection_error" if failure == "connect" else "database_error")
            if failure != "connect":
                self.assertTrue(driver.closed)
                self.assertIsNotNone(driver.exited_with)

    def test_mapping_failure_is_handled_and_retains_inner_stage(self):
        with patch("mooby.api.app.fetch_disruptions", return_value=FeedPage(NOW, [{**ROW, "title": None}])):
            start, _, error = self.call()
        self.assertEqual(start["status"], 503); self.assertIsNone(error)
        self.assertEqual(self.terminal()["failure_stage"], "row_mapping")
        self.assertEqual(self.terminal()["exception_category"], "validation_error")

    def test_unhandled_envelope_validation_still_returns_existing_500(self):
        with patch("mooby.api.app.fetch_disruptions", return_value=FeedPage(PRIVATE, [])):
            start, _, error = self.call()
        self.assertEqual(start["status"], 500)
        self.assertIsNotNone(error)
        event = self.terminal()
        self.assertEqual(event["failure_stage"], "response_validation")
        self.assertEqual(event["status"], 500)
        self.assertTrue(event["response_started"])

    def test_automatic_fastapi_response_validation_failure_is_classified(self):
        app = create_app()
        @app.get("/probe", response_model=DisruptionFeed)
        async def invalid_response():
            return {"availability": PRIVATE}
        start, _, error = self.call(app=app, path="/probe")
        self.assertEqual(start["status"], 500)
        self.assertIsNotNone(error)
        self.assertEqual(self.terminal()["failure_stage"], "response_validation")
        self.assertEqual(self.terminal()["exception_category"], "validation_error")
        self.assertIsNone(self.terminal()["route"])
        self.assertNotIn(PRIVATE, json.dumps(self.events))

    def test_unexpected_exception_is_not_swallowed_or_exposed(self):
        class PrivateException(RuntimeError):
            pass
        with patch("mooby.api.app.fetch_disruptions", side_effect=PrivateException(PRIVATE)):
            start, _, error = self.call()
        self.assertEqual(start["status"], 500)
        self.assertIsInstance(error, PrivateException)
        self.assertEqual(self.terminal()["exception_category"], "unexpected_error")
        self.assertNotIn("PrivateException", json.dumps(self.events))

    def test_strict_event_allowlist_and_sensitive_data_redaction(self):
        driver = DriverDouble(); self.connect.side_effect = None; self.connect.return_value = driver
        self.call()
        self.call(query={"station_ids": PRIVATE}, path="/private/" + PRIVATE)
        encoded = json.dumps(self.events)
        for value in (PRIVATE, TOKEN, ENV["APP_API_DATABASE_URL"], "open-krl:SRP", "example.invalid"):
            self.assertNotIn(value, encoded)
        allowed = {"event", "request_id", "timestamp", "route", "method", "status", "category",
                   "duration_ms", "failure_stage", "exception_category", "response_started",
                   "disconnect_observed", "transport_aborted", "transport_exception_category",
                   "stage", "stage_duration_ms"}
        self.assertTrue(all(set(e) <= allowed for e in self.events))
        self.assertTrue(all(e["route"] in {None, "/api/v1/disruptions"} for e in self.events))

    def test_logger_failure_does_not_change_success_or_handled_error(self):
        for failure, status in ((None, 200), (psycopg.OperationalError(PRIVATE), 503)):
            with patch.object(obs.logger, "info", side_effect=RuntimeError(PRIVATE)), \
                 patch("mooby.api.app.fetch_disruptions", return_value=FeedPage(NOW, []), side_effect=failure):
                start, _, error = self.call()
            self.assertEqual(start["status"], status)
            self.assertIsNone(error)
        self.assertIsNone(obs.current_request_id())

    def test_invalid_categories_and_stage_names_cannot_inject_values(self):
        def fetch(**kwargs):
            obs.set_category(PRIVATE)
            obs.record_failure(PRIVATE, RuntimeError(PRIVATE))
            with obs.timed_stage(PRIVATE):
                pass
            return FeedPage(NOW, [])
        with patch("mooby.api.app.fetch_disruptions", side_effect=fetch):
            self.call()
        self.assertNotIn(PRIVATE, json.dumps(self.events))
        self.assertEqual(self.terminal()["failure_stage"], "application")

    def test_readiness_driver_failure_is_observed_without_extra_queries(self):
        self.connect.side_effect = psycopg.OperationalError(PRIVATE)
        start, raw, error = self.call(path="/health/ready")
        self.assertEqual(start["status"], 503); self.assertIsNone(error)
        self.assertEqual(json.loads(raw)["database"], "unavailable")
        self.assertEqual(self.terminal()["failure_stage"], "database_connect")
        self.assertEqual(self.connect.call_count, 1)


class ASGIBoundaryTests(ObservabilityTestCase):
    async def run_boundary(self, app, *, send=None, receive=None):
        async def default_send(message):
            pass
        async def default_receive():
            return {"type": "http.request", "body": b""}
        await obs.RequestObservabilityMiddleware(app)(
            {"type": "http", "method": "GET"}, receive or default_receive, send or default_send)

    def test_cancelled_request_has_one_terminal_and_reraises(self):
        async def app(scope, receive, send):
            raise asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(self.run_boundary(app))
        self.assertEqual(self.terminal()["event"], "request_cancelled")
        self.assertEqual(self.terminal()["exception_category"], "cancellation")
        self.assertIsNone(obs.current_request_id())

    def test_disconnect_observation_does_not_read_ahead(self):
        calls = []
        async def receive():
            calls.append(1)
            return {"type": "http.disconnect"}
        async def app(scope, observed_receive, send):
            self.assertEqual(calls, [])
            self.assertEqual((await observed_receive())["type"], "http.disconnect")
        asyncio.run(self.run_boundary(app, receive=receive))
        self.assertEqual(calls, [1])
        self.assertEqual(self.terminal()["event"], "request_aborted")
        self.assertTrue(self.terminal()["disconnect_observed"])

    def test_send_failure_and_incomplete_response(self):
        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"private"})
        async def fail_send(message):
            raise OSError(PRIVATE)
        with self.assertRaises(OSError):
            asyncio.run(self.run_boundary(app, send=fail_send))
        self.assertEqual(self.terminal()["failure_stage"], "response_send")
        self.assertEqual(self.terminal()["event"], "request_aborted")
        self.assertFalse(self.terminal()["disconnect_observed"])
        self.assertTrue(self.terminal()["transport_aborted"])
        self.assertEqual(self.terminal()["transport_exception_category"], "transport_error")
        self.events.clear()
        async def incomplete(scope, receive, send):
            await send({"type": "http.response.start", "status": 200})
        asyncio.run(self.run_boundary(incomplete))
        self.assertEqual(self.terminal()["event"], "request_aborted")

    def test_non_http_scope_is_passed_through_without_events(self):
        received = []
        async def app(scope, receive, send):
            received.append(scope["type"])
        asyncio.run(obs.RequestObservabilityMiddleware(app)({"type": "lifespan"}, None, None))
        self.assertEqual(received, ["lifespan"])
        self.assertEqual(self.events, [])


class CompoundFailureTests(ObservabilityTestCase):
    async def run_api(self, send):
        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}
        await create_app()({
            "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
            "method": "GET", "scheme": "http", "path": "/api/v1/disruptions",
            "raw_path": b"/api/v1/disruptions", "root_path": "",
            "query_string": b"station_ids=open-krl:SRP",
            "headers": [(b"authorization", ("Bearer " + TOKEN).encode())],
            "client": ("127.0.0.1", 1), "server": ("127.0.0.1", 8000),
        }, receive, send)

    def failing_driver(self):
        driver = DriverDouble("feed")
        self.connect.side_effect = None
        self.connect.return_value = driver
        return driver

    def test_handled_database_error_then_body_send_failure(self):
        driver = self.failing_driver()
        sent = []
        failure = OSError(PRIVATE)
        async def send(message):
            if message["type"] == "http.response.body":
                raise failure
            sent.append(message)
        with self.assertRaises(OSError) as caught:
            asyncio.run(self.run_api(send))
        self.assertIs(caught.exception, failure)
        self.assertEqual(sent[0]["status"], 503)
        terminal = self.terminal()
        self.assertEqual(terminal["event"], "request_aborted")
        self.assertEqual(terminal["category"], "aborted")
        self.assertEqual(terminal["status"], 503)
        self.assertTrue(terminal["response_started"])
        self.assertEqual(terminal["failure_stage"], "database_feed")
        self.assertEqual(terminal["exception_category"], "database_error")
        self.assertTrue(terminal["transport_aborted"])
        self.assertEqual(terminal["transport_exception_category"], "transport_error")
        self.assertFalse(terminal["disconnect_observed"])
        send_events = [e for e in self.events if e.get("stage") == "response_send"]
        self.assertFalse(send_events[0]["transport_aborted"])
        self.assertTrue(send_events[-1]["transport_aborted"])
        self.assertEqual({e["request_id"] for e in self.events}, {terminal["request_id"]})
        self.assertEqual(len(driver.queries), 2)
        self.assertTrue(driver.closed)
        self.assertIsNone(obs.current_request_id())
        self.assertNotIn(PRIVATE, json.dumps(self.events))

    def test_partial_body_then_non_oserror_send_failure(self):
        failure = RuntimeError(PRIVATE)
        sent = []
        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": PRIVATE.encode(), "more_body": True})
            await send({"type": "http.response.body", "body": PRIVATE.encode(), "more_body": False})
        async def send(message):
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                raise failure
            sent.append(message)
        with self.assertRaises(RuntimeError) as caught:
            asyncio.run(obs.RequestObservabilityMiddleware(app)(
                {"type": "http", "method": "GET"}, None, send))
        self.assertIs(caught.exception, failure)
        self.assertEqual(len(sent), 2)
        self.assertTrue(sent[1]["more_body"])
        terminal = self.terminal()
        self.assertEqual(terminal["event"], "request_aborted")
        self.assertEqual(terminal["status"], 200)
        self.assertEqual(terminal["failure_stage"], "response_send")
        self.assertTrue(terminal["transport_aborted"])
        self.assertEqual(terminal["transport_exception_category"], "unexpected_error")
        self.assertNotIn(PRIVATE, json.dumps(self.events))
        self.assertIsNone(obs.current_request_id())
        self.connect.assert_not_called()

    def test_handled_database_error_with_successful_response_is_unchanged(self):
        driver = self.failing_driver()
        sent = []
        async def send(message):
            sent.append(message)
        asyncio.run(self.run_api(send))
        self.assertEqual(sent[0]["status"], 503)
        body = json.loads(b"".join(m.get("body", b"") for m in sent))
        self.assertEqual(body["availability"], "source_unavailable")
        terminal = self.terminal()
        self.assertEqual(terminal["event"], "request_failed")
        self.assertEqual(terminal["category"], "source_unavailable")
        self.assertEqual(terminal["failure_stage"], "database_feed")
        self.assertEqual(terminal["exception_category"], "database_error")
        self.assertFalse(terminal["transport_aborted"])
        self.assertIsNone(terminal["transport_exception_category"])
        self.assertEqual(len(driver.queries), 2)
        self.assertTrue(driver.closed)

    def test_logger_failure_does_not_replace_compound_send_exception(self):
        driver = self.failing_driver()
        sent = []
        failure = OSError(PRIVATE)
        async def send(message):
            if message["type"] == "http.response.body":
                raise failure
            sent.append(message)
        with patch.object(obs.logger, "info", side_effect=RuntimeError("synthetic logger failure")):
            with self.assertRaises(OSError) as caught:
                asyncio.run(self.run_api(send))
        self.assertIs(caught.exception, failure)
        self.assertEqual(sent[0]["status"], 503)
        self.assertEqual(len(driver.queries), 2)
        self.assertTrue(driver.closed)
        self.assertIsNone(obs.current_request_id())

    def test_cancellation_during_send_preserves_cancelled_lifecycle(self):
        failure = asyncio.CancelledError()
        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
        async def send(message):
            raise failure
        with self.assertRaises(asyncio.CancelledError) as caught:
            asyncio.run(obs.RequestObservabilityMiddleware(app)(
                {"type": "http", "method": "GET"}, None, send))
        self.assertIs(caught.exception, failure)
        terminal = self.terminal()
        self.assertEqual(terminal["event"], "request_cancelled")
        self.assertEqual(terminal["category"], "cancelled")
        self.assertTrue(terminal["transport_aborted"])
        self.assertEqual(terminal["transport_exception_category"], "cancellation")
        self.assertIsNone(obs.current_request_id())


if __name__ == "__main__":
    unittest.main()
