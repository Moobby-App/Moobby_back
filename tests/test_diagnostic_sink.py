"""R02: temporary files, in-memory ASGI, fake writers. No external services."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import stat
import tempfile
from threading import Event, get_ident
from time import monotonic
import unittest
from unittest.mock import patch
from uuid import uuid4
from types import SimpleNamespace

import psycopg

from mooby.api import diagnostic_sink as ds
from mooby.api import observability as obs
from mooby.api.app import create_app
from mooby.api.repository import FeedPage
from test_request_observability import ENV, exchange
from test_api_launchd import REF, URL, TOKEN
import launchd_api


def record():
    return dict(event="request_completed", request_id=uuid4().hex,
        timestamp=datetime.now(timezone.utc).isoformat(), route="/api/v1/disruptions",
        method="GET", status=200, category="no_match", duration_ms=1.5,
        failure_stage=None, exception_category=None, response_started=True,
        disconnect_observed=False, transport_aborted=False, transport_exception_category=None)


def wait_for(predicate):
    deadline = monotonic() + 2
    while not predicate():
        if monotonic() >= deadline:
            raise AssertionError("test writer did not reach expected state")
        Event().wait(0.005)


class DiagnosticSinkTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.directory = self.root / ".local/services/diagnostics"
        for name, value in (("ROOT", self.root), ("_sink", None), ("_users", 0),
                            ("_configuration_failures", 0)):
            p = patch.object(ds, name, value); p.start(); self.addCleanup(p.stop)
        p = patch.dict("os.environ", ENV, clear=True); p.start(); self.addCleanup(p.stop)
        p = patch("psycopg.connect", side_effect=AssertionError("network forbidden"))
        p.start(); self.addCleanup(p.stop)

    def sink(self, **kwargs):
        sink = ds.DiagnosticSink(ds.SinkSettings(**kwargs), root=self.root).start()
        self.addCleanup(sink.close)
        return sink

    def test_valid_records_persist_with_private_permissions_and_survive_restart(self):
        sink = self.sink()
        value = record(); sink.submit(value); sink.close()
        self.assertEqual(sink.snapshot()["written"], 1)
        self.assertEqual(sink.snapshot()["dropped"], 0)
        path = self.directory / ds._FileWriter.name
        self.assertEqual(json.loads(path.read_text()), value)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.directory.stat().st_mode), 0o700)
        again = self.sink(); again.submit(record()); again.close()
        self.assertEqual(len(path.read_text().splitlines()), 2)

    def test_rotation_retention_and_size_bounds(self):
        sink = self.sink(capacity=128, max_bytes=4096, backups=2)
        for _ in range(50): sink.submit(record())
        sink.close()
        files = list(self.directory.glob("api-requests.jsonl*"))
        self.assertEqual(len(files), 3)
        for path in files:
            self.assertLessEqual(path.stat().st_size, 4096)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            for line in path.read_text().splitlines():
                ds.encode_record(json.loads(line))
        self.assertEqual(sink.snapshot()["written"], 50)

    def test_reduced_retention_prunes_only_known_backups(self):
        writer = ds._FileWriter(self.root, ds.SinkSettings(backups=3))
        writer.close()
        (self.directory / "api-requests.jsonl.3").write_text("")
        unrelated = self.directory / "other.txt"; unrelated.write_text("keep")
        sink = self.sink(backups=1); sink.close()
        self.assertFalse((self.directory / "api-requests.jsonl.3").exists())
        self.assertEqual(unrelated.read_text(), "keep")

    def test_invalid_records_are_rejected_without_serializing_secrets(self):
        sink = self.sink()
        invalid = [dict(record(), secret="PRIVATE"), dict(record(), route="/PRIVATE?token=PRIVATE"),
                   dict(record(), exception_category="PRIVATE"), dict(record(), duration_ms=float("nan")),
                   dict(record(), request_id="PRIVATE"), dict(record(), timestamp="PRIVATE"),
                   dict(record(), status=True), dict(record(), response_started="PRIVATE"),
                   dict(record(), event="PRIVATE"), "PRIVATE"]
        for value in invalid: sink.submit(value)
        sink.submit(record()); sink.close()
        status = sink.snapshot()
        self.assertEqual(status["invalid_records"], len(invalid))
        self.assertEqual(status["dropped"], len(invalid))
        self.assertEqual(status["written"], 1)
        self.assertNotIn("PRIVATE", (self.directory / ds._FileWriter.name).read_text())

    def test_settings_are_bounded_and_no_configurable_path(self):
        for kwargs in ({"capacity":0}, {"capacity":8193}, {"backups":11}, {"max_bytes":4095},
                       {"max_bytes":True}, {"flush_seconds":float("inf")}, {"flush_seconds":3}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError): ds.SinkSettings(**kwargs)
        with self.assertRaises(TypeError): ds.SinkSettings(path="../../PRIVATE")

    def test_directory_and_file_creation_errors_fail_closed(self):
        for method in ("mkdir", "open"):
            with self.subTest(method=method), patch.object(ds.os, method, side_effect=PermissionError("PRIVATE")):
                sink = self.sink(); wait_for(lambda: not sink.thread.is_alive())
                sink.submit(record())
                self.assertEqual(sink.snapshot()["writer_state"], "failed")
                self.assertEqual(sink.snapshot()["initialization_failures"], 1)
                self.assertEqual(sink.snapshot()["written"], 0)
                self.assertEqual(sink.snapshot()["dropped"], 1)

    def test_symlink_directory_or_file_and_hardlinks_are_rejected(self):
        target = self.root / "outside"; target.mkdir()
        (self.root / ".local").symlink_to(target, target_is_directory=True)
        sink = self.sink(); wait_for(lambda: not sink.thread.is_alive())
        self.assertEqual(sink.snapshot()["initialization_failures"], 1)
        (self.root / ".local").unlink()
        self.directory.mkdir(parents=True)
        external = self.root / "private"; external.write_text("DO NOT CHANGE")
        path = self.directory / ds._FileWriter.name
        path.symlink_to(external)
        sink = self.sink(); wait_for(lambda: not sink.thread.is_alive())
        self.assertEqual(sink.snapshot()["initialization_failures"], 1)
        path.unlink(); os.link(external, path)
        sink = self.sink(); wait_for(lambda: not sink.thread.is_alive())
        self.assertEqual(sink.snapshot()["initialization_failures"], 1)
        self.assertEqual(external.read_text(), "DO NOT CHANGE")

    def test_partial_trailing_line_disables_append(self):
        self.directory.mkdir(parents=True)
        path = self.directory / ds._FileWriter.name; path.write_bytes(b'{"unfinished":')
        sink = self.sink(); wait_for(lambda: not sink.thread.is_alive())
        self.assertEqual(sink.snapshot()["initialization_failures"], 1)
        self.assertEqual(path.read_bytes(), b'{"unfinished":')

    def test_second_writer_cannot_rotate_same_files(self):
        first = self.sink(); wait_for(lambda: first.snapshot()["writer_state"] == "running")
        second = self.sink(); wait_for(lambda: not second.thread.is_alive())
        self.assertEqual(second.snapshot()["initialization_failures"], 1)
        first.submit(record()); first.close()
        self.assertEqual(first.snapshot()["written"], 1)

    def test_disk_write_failure_drops_record_and_stops_writer_without_recursion(self):
        sink = self.sink(); wait_for(lambda: sink.snapshot()["writer_state"] == "running")
        with patch.object(ds.os, "write", side_effect=OSError("PRIVATE disk error")), \
             patch.object(obs.logger, "info") as log:
            sink.submit(record()); wait_for(lambda: not sink.thread.is_alive())
        sink.submit(record())
        status = sink.snapshot()
        self.assertEqual(status["written"], 0)
        self.assertEqual(status["dropped"], 2)
        self.assertEqual(status["sink_failures"], 1)
        log.assert_not_called()

    def test_rotation_failure_is_counted_not_written(self):
        sink = self.sink(max_bytes=4096)
        wait_for(lambda: sink.snapshot()["writer_state"] == "running")
        with patch.object(ds._FileWriter, "_rotate", side_effect=OSError("PRIVATE")):
            for _ in range(20): sink.submit(record())
            wait_for(lambda: not sink.thread.is_alive())
        status = sink.snapshot()
        self.assertGreater(status["written"], 0)
        self.assertEqual(status["sink_failures"], 1)
        self.assertEqual(status["written"] + status["dropped"], 20)

    def test_unexpected_consumer_exit_is_contained_and_future_records_drop(self):
        with patch.object(ds._FileWriter, "write", side_effect=SystemExit("PRIVATE")):
            sink = self.sink(); sink.submit(record()); wait_for(lambda: not sink.thread.is_alive())
        sink.submit(record())
        self.assertEqual(sink.snapshot()["writer_state"], "failed")
        self.assertEqual(sink.snapshot()["written"], 0)
        self.assertEqual(sink.snapshot()["dropped"], 2)

    def test_thread_start_failure_does_not_escape(self):
        with patch.object(ds.Thread, "start", side_effect=RuntimeError("PRIVATE")):
            sink = self.sink()
        sink.submit(record())
        self.assertEqual(sink.snapshot()["initialization_failures"], 1)
        self.assertEqual(sink.snapshot()["dropped"], 1)

    def test_full_queue_nonblocking_submission_and_bounded_shutdown(self):
        entered, release = Event(), Event()
        class Writer:
            def __init__(self, *args): pass
            def write(self, payload):
                entered.set(); release.wait(2)
            def close(self): pass
        with patch.object(ds, "_FileWriter", Writer):
            sink = self.sink(capacity=1, flush_seconds=0)
            sink.submit(record()); self.assertTrue(entered.wait(1))
            sink.submit(record())
            for _ in range(20): sink.submit(record())
            sink.close()
            status = sink.snapshot()
            self.assertEqual(status["queue_full"], 20)
            self.assertEqual(status["pending"], 1)
            self.assertEqual(status["inflight"], 1)
            self.assertGreaterEqual(status["shutdown_timeouts"], 1)
            self.assertEqual(status["written"], 0)
            release.set(); sink.thread.join(1)
        self.assertEqual(sink.snapshot()["written"], 2)
        self.assertEqual(sink.snapshot()["dropped"], 20)

    def test_concurrent_submission_accounting(self):
        sink = self.sink(capacity=1024)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: sink.submit(record()), range(200)))
        sink.close()
        status = sink.snapshot()
        self.assertEqual(status["written"], 200)
        self.assertEqual(status["dropped"], 0)
        self.assertEqual(status["pending"], 0)
        self.assertEqual(status["inflight"], 0)

    def test_filesystem_io_runs_only_in_writer_thread(self):
        caller = get_ident(); seen = []
        original_open, original_write = ds.os.open, ds.os.write
        def opened(*args, **kwargs):
            seen.append(get_ident()); return original_open(*args, **kwargs)
        def written(*args, **kwargs):
            seen.append(get_ident()); return original_write(*args, **kwargs)
        with patch.object(ds.os, "open", side_effect=opened), patch.object(ds.os, "write", side_effect=written):
            sink = self.sink(); sink.submit(record()); sink.close()
        self.assertTrue(seen)
        self.assertEqual(set(seen), {sink.thread.ident})
        self.assertNotIn(caller, seen)

    def test_api_lifespan_persists_events_without_duplicate_handlers_or_threads(self):
        handlers = list(obs.logger.handlers)
        async def run():
            a, b = create_app(), create_app()
            async with a.router.lifespan_context(a):
                first = ds._sink
                async with b.router.lifespan_context(b):
                    self.assertIs(ds._sink, first)
                    start, raw, error = await exchange(a)
                    self.assertEqual(start["status"], 200); self.assertIsNone(error)
                    self.assertEqual(json.loads(raw)["availability"], "no_match")
                    self.assertFalse(any(k == b"x-request-id" for k, _ in start["headers"]))
                self.assertTrue(first.accepting)
            self.assertFalse(first.thread.is_alive())
        with patch.dict("os.environ", {"MOOBY_DIAGNOSTIC_ENABLED":"1"}), \
             patch("mooby.api.app.fetch_disruptions", return_value=FeedPage(datetime.now(timezone.utc), [])):
            asyncio.run(run())
        rows = [json.loads(s) for s in (self.directory / ds._FileWriter.name).read_text().splitlines()]
        self.assertEqual(sum(r["event"] == "request_admitted" for r in rows), 1)
        self.assertEqual(sum(r["event"] == "request_completed" for r in rows), 1)
        self.assertEqual(obs.logger.handlers, handlers)

    def test_sink_initialization_failure_does_not_change_api_response(self):
        async def run():
            app = create_app()
            async with app.router.lifespan_context(app):
                wait_for(lambda: not ds._sink.thread.is_alive())
                start, raw, error = await exchange(app)
                self.assertEqual(start["status"], 200); self.assertIsNone(error)
                self.assertEqual(json.loads(raw)["availability"], "no_match")
                self.assertGreater(app.state.diagnostic_status()["dropped"], 0)
        with patch.dict("os.environ", {"MOOBY_DIAGNOSTIC_ENABLED":"1"}), \
             patch.object(ds, "_FileWriter", side_effect=PermissionError("PRIVATE")), \
             patch("mooby.api.app.fetch_disruptions", return_value=FeedPage(datetime.now(timezone.utc), [])):
            asyncio.run(run())

    def test_enabled_sink_preserves_http_outcomes_and_never_uses_raw_logger(self):
        async def run():
            app = create_app()
            async with app.router.lifespan_context(app):
                with patch("mooby.api.app.fetch_disruptions", return_value=FeedPage(datetime.now(timezone.utc), [])) as fetch:
                    for kwargs, expected in (({},200), ({"token":None},401),
                        ({"query":{}},422), ({"path":"/health/live"},200)):
                        start, _, error = await exchange(app, **kwargs)
                        self.assertEqual(start["status"], expected); self.assertIsNone(error)
                    fetch.side_effect = psycopg.OperationalError("PRIVATE database error")
                    start, raw, error = await exchange(app)
                    self.assertEqual(start["status"], 503); self.assertIsNone(error)
                    self.assertEqual(json.loads(raw)["availability"], "source_unavailable")
                with patch("mooby.api.app.check_database", return_value=True), \
                     patch("mooby.api.app.check_app_api_projection", return_value=True):
                    start, _, error = await exchange(app, path="/health/ready")
                    self.assertEqual(start["status"], 200); self.assertIsNone(error)
                with patch.dict("os.environ", {"APP_API_ENVIRONMENT":"development",
                    "APP_API_SYNTHETIC_FIXTURE_MODE":"development", "APP_API_SYNTHETIC_FIXTURE_TTL_MINUTES":"60"}), \
                     patch("mooby.api.app.synthetic_fixture_page", return_value=SimpleNamespace(
                         generated_at=datetime.now(timezone.utc),items=[])):
                    start, raw, error = await exchange(app, path="/api/v1/mock/disruptions")
                    self.assertEqual(start["status"], 200); self.assertIsNone(error)
                    self.assertEqual(json.loads(raw)["availability"], "no_match")
        with patch.dict("os.environ", {"MOOBY_DIAGNOSTIC_ENABLED":"1"}), \
             patch.object(obs.logger,"info",side_effect=AssertionError("raw handler must not run")) as logger:
            asyncio.run(run())
        logger.assert_not_called()
        text = (self.directory / ds._FileWriter.name).read_text()
        self.assertNotIn("PRIVATE", text)
        self.assertNotIn(TOKEN, text)

    def test_unflushed_writer_is_not_replaced_by_another_lifespan(self):
        entered, release = Event(), Event()
        class Writer:
            def __init__(self, *args): pass
            def write(self, payload): entered.set(); release.wait(2)
            def close(self): pass
        async def run():
            try:
                async with ds.diagnostic_lifespan():
                    first = ds._sink
                    first.submit(record()); self.assertTrue(entered.wait(1))
                self.assertTrue(first.thread.is_alive())
                async with ds.diagnostic_lifespan():
                    self.assertIs(ds._sink, first)
                    first.submit(record())
                    self.assertEqual(first.snapshot()["dropped"], 1)
            finally:
                release.set(); ds._sink.thread.join(1)
        with patch.dict("os.environ", {"MOOBY_DIAGNOSTIC_ENABLED":"1", "MOOBY_DIAGNOSTIC_FLUSH_SECONDS":"0"}), \
             patch.object(ds,"_FileWriter",Writer) as factory:
            asyncio.run(run())
        self.assertFalse(ds._sink.thread.is_alive())

    def test_group_writable_parent_directory_is_rejected(self):
        parent = self.root / ".local/services"; parent.mkdir(parents=True)
        parent.chmod(0o777)
        sink = self.sink(); wait_for(lambda: not sink.thread.is_alive())
        self.assertEqual(sink.snapshot()["initialization_failures"], 1)
        self.assertFalse(self.directory.exists())

    def test_full_sink_does_not_block_in_memory_api(self):
        entered, release = Event(), Event()
        class Writer:
            def __init__(self, *args): pass
            def write(self, payload): entered.set(); release.wait(2)
            def close(self): pass
        async def run():
            app = create_app()
            async with app.router.lifespan_context(app):
                ds._sink.submit(record()); self.assertTrue(entered.wait(1))
                ds._sink.submit(record())
                try:
                    start, raw, error = await asyncio.wait_for(exchange(app), timeout=1)
                    self.assertEqual(start["status"], 200); self.assertIsNone(error)
                    self.assertEqual(json.loads(raw)["availability"], "no_match")
                    self.assertGreater(ds._sink.snapshot()["queue_full"], 0)
                finally:
                    release.set()
        with patch.dict("os.environ", {"MOOBY_DIAGNOSTIC_ENABLED":"1", "MOOBY_DIAGNOSTIC_CAPACITY":"1"}), \
             patch.object(ds, "_FileWriter", Writer), \
             patch("mooby.api.app.fetch_disruptions", return_value=FeedPage(datetime.now(timezone.utc), [])):
            asyncio.run(run())

    def test_disabled_sink_never_creates_diagnostic_directory(self):
        async def run():
            app = create_app()
            async with app.router.lifespan_context(app):
                start, _, error = await exchange(app, path="/health/live")
                self.assertEqual(start["status"], 200); self.assertIsNone(error)
        with patch.object(ds.DiagnosticSink, "start") as start:
            asyncio.run(run())
        start.assert_not_called()
        self.assertFalse((self.root / ".local").exists())

    def test_lifespan_cleanup_also_runs_on_application_startup_failure(self):
        async def run():
            with self.assertRaises(RuntimeError):
                async with ds.diagnostic_lifespan():
                    ds.submit_record(record())
                    raise RuntimeError("PRIVATE startup failure")
            self.assertFalse(ds._sink.thread.is_alive())
            self.assertEqual(ds._users, 0)
            self.assertEqual(ds._sink.snapshot()["written"], 1)
        with patch.dict("os.environ", {"MOOBY_DIAGNOSTIC_ENABLED":"1"}):
            asyncio.run(run())

    def test_disabled_and_invalid_configuration_do_not_start_writer(self):
        async def run():
            async with ds.diagnostic_lifespan(): pass
        with patch.object(ds.Thread, "start") as start:
            asyncio.run(run())
            with patch.dict("os.environ", {"MOOBY_DIAGNOSTIC_ENABLED":"1", "MOOBY_DIAGNOSTIC_CAPACITY":"bad"}):
                asyncio.run(run())
            with patch.dict("os.environ", {"MOOBY_DIAGNOSTIC_ENABLED":"invalid"}):
                asyncio.run(run())
        start.assert_not_called()
        self.assertEqual(ds.diagnostic_status()["configuration_failures"], 2)

    def test_supervisor_opt_in_numeric_allowlist_and_safe_invalid_config(self):
        envfile = self.root / "config.env"
        envfile.write_text(f"APP_API_DATABASE_URL={URL}\nAPP_API_BEARER_TOKEN={TOKEN}\n"
                           "MOOBY_DIAGNOSTIC_ENABLED=1\nMOOBY_DIAGNOSTIC_PATH=/PRIVATE\n")
        config = {"expected_project_ref":REF}
        self.assertNotIn("MOOBY_DIAGNOSTIC_ENABLED", launchd_api.api_environment(config, envfile))
        env = launchd_api.api_environment({**config, "diagnostics":{"enabled":True}}, envfile)
        self.assertEqual(env["MOOBY_DIAGNOSTIC_ENABLED"], "1")
        self.assertEqual(env["MOOBY_DIAGNOSTIC_CAPACITY"], "1024")
        self.assertNotIn("MOOBY_DIAGNOSTIC_PATH", env)
        for diagnostics in ({"enabled":True,"path":"../../PRIVATE"}, {"enabled":True,"capacity":0}, "PRIVATE"):
            env = launchd_api.api_environment({**config,"diagnostics":diagnostics}, envfile)
            self.assertEqual(env["MOOBY_DIAGNOSTIC_ENABLED"], "invalid")
            self.assertEqual(env["APP_API_DATABASE_URL"], URL)
        path = self.root / "settings.json"
        path.write_text(json.dumps({**config,"ngrok_config_file":"unused","stable_domain":None,
                                    "diagnostics":{"enabled":True}}))
        self.assertTrue(launchd_api.read_settings(path)["diagnostics"]["enabled"])


if __name__ == "__main__":
    unittest.main()
