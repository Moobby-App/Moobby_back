"""Deterministic preparation and in-memory HTTP/driver compatibility tests."""

import ast
from contextlib import nullcontext
from dataclasses import asdict
from datetime import datetime, timezone
from functools import partial
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from uuid import UUID

import psycopg
from pydantic import ValidationError

from mooby.api import repository
from mooby.api import response_preparation as prep
from mooby.api.models import DisruptionFeed, Readiness
from test_request_observability import ObservabilityTestCase


NOW = datetime(2026, 11, 15, 1, 2, 3, tzinfo=timezone.utc)


def row(number=1, **updates):
    return {
        "public_event_id":str(UUID(int=number)), "event_kind":"station",
        "title":"Synthetic disruption", "summary":"Synthetic summary",
        "station_name":"Serpong", "station_id":"open-krl:SRP",
        "affected_station_ids":["open-krl:SRP"], "severity":None,
        "event_at":None, "event_time_certainty":None, "freshness":"fresh",
        "certainty_label":None, "source_display_name":"Synthetic source",
        "source_url":"https://example.invalid/post", "source_published_at":None,
        **updates,
    }


class ConnectionDouble:
    def __init__(self, rows=(), failure=None):
        self.rows, self.failure = list(rows), failure
        self.calls = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def transaction(self):
        return nullcontext()

    def execute(self, sql, parameters=()):
        self.calls.append((sql, parameters))
        if self.failure:
            raise self.failure
        return SimpleNamespace(fetchone=lambda:{"generated_at":NOW}, fetchall=lambda:self.rows)


class IsolatedPreparationCase(unittest.TestCase):
    def setUp(self):
        # Any accidental non-injected connection or process creation fails closed.
        for target in ("psycopg.connect", "multiprocessing.get_context", "socket.create_connection"):
            guard = patch(target, side_effect=AssertionError("external execution forbidden"))
            guard.start()
            self.addCleanup(guard.stop)


class FeedPreparationTests(IsolatedPreparationCase):
    def query(self, limit=20, offset=0):
        return prep.FeedQuery(("open-krl:SRP", "open-krl:THB"), limit, offset)

    def test_empty_result_exact_envelope_and_timestamp(self):
        result = prep.prepare_feed(self.query(7,3), repository.FeedPage(NOW, []))
        self.assertEqual(result.model_dump(mode="json"), {
            "availability":"no_match", "generated_at":"2026-11-15T01:02:03Z",
            "window_hours":24, "message":"Tidak ditemukan laporan gangguan yang cocok dalam 24 jam terakhir.",
            "items":[], "limit":7, "offset":3, "next_offset":None,
        })
        self.assertIs(result.generated_at, NOW)

    def test_single_result_exact_public_fields_and_nullable_values(self):
        result = prep.prepare_feed(self.query(), repository.FeedPage(NOW, [row()]))
        item = result.model_dump(mode="json")["items"][0]
        self.assertEqual(set(item), {"public_event_id", "title", "summary", "original_text",
            "station_or_segment", "severity", "event_time", "event_time_certainty", "freshness", "certainty", "source"})
        self.assertEqual(item["station_or_segment"], {
            "kind":"station", "line":None, "station":"Serpong", "station_id":"open-krl:SRP",
            "start_station":None, "start_station_id":None, "end_station":None,
            "end_station_id":None, "direction":None, "affected_station_ids":["open-krl:SRP"],
        })
        self.assertEqual(item["source"], {"display_name":"Synthetic source",
                                         "url":"https://example.invalid/post", "published_at":None})
        for name in ("original_text", "severity", "event_time", "event_time_certainty", "certainty"):
            self.assertIsNone(item[name])
        self.assertEqual(result.availability, "results")

    def test_multiple_rows_keep_repository_order_and_tie_breaking(self):
        rows = [row(9), row(2), row(1)]
        result = prep.prepare_feed(self.query(), repository.FeedPage(NOW, rows))
        self.assertEqual([item.public_event_id for item in result.items], [UUID(int=n) for n in (9,2,1)])
        self.assertEqual(result.next_offset, None)

    def test_pagination_boundaries_and_lookahead(self):
        for limit, offset, count, expected_next in ((1,0,0,None),(1,0,1,None),(1,1000,2,1001),
                                                   (50,0,50,None),(50,1000,51,1050)):
            with self.subTest(limit=limit,offset=offset,count=count):
                result = prep.prepare_feed(self.query(limit,offset),
                                           repository.FeedPage(NOW, [row(n+1) for n in range(count)]))
                self.assertEqual(len(result.items), min(limit,count))
                self.assertEqual((result.limit,result.offset,result.next_offset), (limit,offset,expected_next))

    def test_lookahead_row_is_not_mapped_or_validated(self):
        mapper = Mock(wraps=prep.prepare_item)
        result = prep.prepare_feed(self.query(1), repository.FeedPage(NOW, [row(), {}]), item_mapper=mapper)
        self.assertEqual(len(result.items),1)
        self.assertEqual(result.next_offset,1)
        mapper.assert_called_once()

    def test_load_delegates_once_preserving_station_sequence_and_limit(self):
        query = prep.FeedQuery(("open-krl:THB","open-krl:SRP","open-krl:THB"),2,7)
        page = repository.FeedPage(NOW, [])
        reader = Mock(return_value=page)
        self.assertIs(prep.load_feed(query, reader=reader), page)
        reader.assert_called_once_with(station_ids=["open-krl:THB","open-krl:SRP","open-krl:THB"],limit=2,offset=7)
        self.assertEqual(query.station_ids,("open-krl:THB","open-krl:SRP","open-krl:THB"))

    def test_real_repository_adapter_keeps_two_queries_and_limit_plus_one(self):
        query = self.query(2,7)
        connection = ConnectionDouble([row(1),row(2),row(3)])
        reader = partial(repository.fetch_disruptions, database_url="postgresql://synthetic.invalid/test",
                         connect_timeout_seconds=8, statement_timeout_ms=4500)
        with patch.object(repository,"_connect",return_value=connection) as connect:
            page = prep.load_feed(query,reader=reader)
        self.assertEqual(connection.calls, [
            ("SELECT transaction_timestamp() AS generated_at",()),
            (repository._SEARCH_SQL,(["open-krl:SRP","open-krl:THB"],3,7)),
        ])
        self.assertEqual(connect.call_args.kwargs["connect_timeout"],8)
        self.assertEqual(connect.call_args.kwargs["options"],"-c statement_timeout=4500")
        self.assertTrue(connection.closed)
        self.assertEqual(prep.prepare_feed(query,page).next_offset,9)

    def test_original_text_is_optional_and_unbounded_text_is_not_truncated(self):
        text = "  Synthetic text 🚉\n" * 2000
        item = prep.prepare_item(row(original_text=text))
        self.assertEqual(item.original_text,text)
        self.assertGreater(len(item.model_dump_json().encode()),16384)
        self.assertIsNone(prep.prepare_item(row()).original_text)

    def test_internal_row_extras_never_enter_prepared_output(self):
        value = row(original_text="approved synthetic display text", raw_text="INTERNAL_RAW",
                    evidence_quote="INTERNAL_QUOTE", author_handle="INTERNAL_AUTHOR",
                    source_snapshot_id="INTERNAL_SNAPSHOT", embedding=object(), queue_state=object())
        result = prep.prepare_feed(self.query(),repository.FeedPage(NOW,[value]))
        encoded = result.model_dump_json()
        for forbidden in ("INTERNAL_", "raw_text", "evidence_quote", "author_handle", "source_snapshot_id", "embedding", "queue_state"):
            self.assertNotIn(forbidden,encoded)
        self.assertEqual(result.items[0].original_text,"approved synthetic display text")

    def test_segment_fields_and_affected_ids_are_preserved_without_expansion(self):
        affected = ["open-krl:SRP","open-krl:PDJ","open-krl:THB"]
        item = prep.prepare_item(row(event_kind="segment",line_name="Synthetic line",
            start_station_name="Serpong",start_station_id="open-krl:SRP",
            end_station_name="Tanah Abang",end_station_id="open-krl:THB",direction="inbound",
            affected_station_ids=affected,severity="moderate",event_at=NOW,event_time_certainty="high"))
        self.assertEqual(item.station_or_segment.affected_station_ids,affected)
        self.assertEqual(item.station_or_segment.end_station_id,"open-krl:THB")
        self.assertEqual((item.event_time,item.severity,item.event_time_certainty),(NOW,"moderate","high"))

    def test_structural_mapping_and_pydantic_errors_propagate(self):
        for value, error in (({},KeyError),(row(event_kind="unresolved"),ValueError),
                             (row(title=None),ValidationError),(row(severity="invalid"),ValidationError),
                             (row(source_url="http://example.invalid/post"),ValidationError)):
            with self.subTest(error=error.__name__), self.assertRaises(error):
                prep.prepare_feed(self.query(),repository.FeedPage(NOW,[value]))

    def test_invalid_envelope_is_not_replaced_with_successful_empty_feed(self):
        for values in ([],[row()]):
            with self.assertRaises(ValidationError):
                prep.prepare_feed(self.query(),repository.FeedPage("invalid-timestamp",values))
        with self.assertRaises(ValidationError):
            prep.prepare_feed(self.query(0),repository.FeedPage(NOW,[]))

    def test_controlled_reader_errors_retain_identity_for_route_boundary(self):
        for error in (psycopg.OperationalError("synthetic"),psycopg.errors.QueryCanceled("synthetic"),
                      OSError("synthetic"),KeyError("synthetic")):
            with self.assertRaises(type(error)) as caught:
                prep.load_feed(self.query(),reader=Mock(side_effect=error))
            self.assertIs(caught.exception,error)

    def test_unexpected_errors_are_not_suppressed(self):
        error=RuntimeError("synthetic programming failure")
        with self.assertRaises(RuntimeError) as caught:
            prep.prepare_feed(self.query(),repository.FeedPage(NOW,[]),feed_builder=Mock(side_effect=error))
        self.assertIs(caught.exception,error)

    def test_data_contracts_are_immutable_and_serializable_without_credentials(self):
        query=self.query()
        with self.assertRaises(AttributeError):
            query.limit=9
        self.assertEqual(json.loads(json.dumps(asdict(query)))["station_ids"],["open-krl:SRP","open-krl:THB"])
        status=prep.ReadinessStatus(True,True)
        self.assertEqual(prep.prepare_readiness(status).model_dump(),{
            "api":"available","database":"available","app_api_projection":"available"})
        for values in (asdict(query),asdict(status)):
            self.assertNotIn("database_url",values)
            self.assertNotIn("bearer_token",values)
        output=prep.prepare_feed(query,repository.FeedPage(NOW,[row()]))
        self.assertEqual(DisruptionFeed.model_validate_json(output.model_dump_json()),output)

    def test_module_has_no_http_or_execution_dependencies(self):
        source=Path(prep.__file__).read_text()
        tree=ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node,(ast.Import,ast.ImportFrom)):
                modules = [node.module or ""] if isinstance(node,ast.ImportFrom) else [name.name for name in node.names]
                for forbidden in ("fastapi", "process_manager", "multiprocessing", "observability", "os"):
                    self.assertFalse(any(forbidden in module.split(".") for module in modules))


class ReadinessPreparationTests(IsolatedPreparationCase):
    def test_success_calls_probes_in_order_and_returns_existing_model(self):
        calls=[]
        observer=Mock()
        status=prep.probe_readiness(database_probe=lambda:calls.append("database") or True,
                                    projection_probe=lambda:calls.append("projection") or True,on_failure=observer)
        self.assertEqual(calls,["database","projection"])
        self.assertIsNone(status.failure_stage)
        self.assertEqual(prep.prepare_readiness(status),Readiness(database="available",app_api_projection="available"))
        observer.assert_not_called()

    def test_unavailable_database_short_circuits_projection_without_new_error_event(self):
        projection,observer=Mock(),Mock()
        status=prep.probe_readiness(database_probe=lambda:False,projection_probe=projection,on_failure=observer)
        self.assertEqual(status,prep.ReadinessStatus(False,False,"database_readiness"))
        self.assertEqual(prep.prepare_readiness(status).database,"unavailable")
        projection.assert_not_called();observer.assert_not_called()

    def test_unavailable_projection_retains_successful_database_status(self):
        status=prep.probe_readiness(database_probe=lambda:True,projection_probe=lambda:False)
        self.assertEqual(status.failure_stage,"projection_readiness")
        self.assertEqual(prep.prepare_readiness(status).model_dump(),{
            "api":"available","database":"available","app_api_projection":"unavailable"})

    def test_escaped_read_errors_have_exact_stage_and_original_exception(self):
        for error in (psycopg.OperationalError("synthetic"),psycopg.errors.QueryCanceled("synthetic"),
                      psycopg.errors.UndefinedColumn("synthetic"),OSError("synthetic"),
                      KeyError("synthetic"),ValueError("synthetic"),TypeError("synthetic")):
            for stage in ("database_readiness","projection_readiness"):
                observer=Mock();projection=Mock(return_value=True)
                database=Mock(side_effect=error) if stage=="database_readiness" else Mock(return_value=True)
                if stage=="projection_readiness":projection.side_effect=error
                status=prep.probe_readiness(database_probe=database,projection_probe=projection,on_failure=observer)
                observer.assert_called_once_with(stage,error)
                self.assertEqual(status.failure_stage,stage)
                self.assertEqual(status.database_available,stage=="projection_readiness")
                self.assertFalse(status.projection_available)
                if stage=="database_readiness":projection.assert_not_called()

    def test_unexpected_probe_errors_propagate_without_observer_reclassification(self):
        error=RuntimeError("synthetic bug");observer=Mock()
        with self.assertRaises(RuntimeError) as caught:
            prep.probe_readiness(database_probe=Mock(side_effect=error),projection_probe=Mock(),on_failure=observer)
        self.assertIs(caught.exception,error);observer.assert_not_called()

    def test_readiness_validation_error_is_controlled_without_retaining_exception_in_data(self):
        try:
            DisruptionFeed.model_validate({})
        except ValidationError as error:
            original=error
        observer=Mock()
        status=prep.probe_readiness(database_probe=Mock(side_effect=original),
                                    projection_probe=Mock(),on_failure=observer)
        observer.assert_called_once_with("database_readiness",original)
        self.assertEqual(asdict(status),{"database_available":False,"projection_available":False,
                                         "failure_stage":"database_readiness"})
        self.assertEqual(prep.prepare_readiness(status).database,"unavailable")

    def probes(self):
        settings=dict(database_url="postgresql://synthetic.invalid/test",connect_timeout_seconds=3,statement_timeout_ms=3000)
        return dict(database_probe=partial(repository.check_database,**settings),
                    projection_probe=partial(repository.check_app_api_projection,**settings))

    def test_actual_repository_probes_use_full_limit_zero_projection(self):
        clock,projection=ConnectionDouble(),ConnectionDouble()
        with patch.object(repository,"_connect",side_effect=[clock,projection]) as connect:
            status=prep.probe_readiness(**self.probes())
        self.assertEqual(prep.prepare_readiness(status).app_api_projection,"available")
        self.assertEqual(connect.call_count,2)
        self.assertEqual(clock.calls,[("SELECT transaction_timestamp()",())])
        self.assertEqual(projection.calls,[(repository._PROJECTION_SQL,([],))])
        self.assertTrue(repository._PROJECTION_SQL.strip().endswith("LIMIT 0"))
        self.assertTrue(clock.closed and projection.closed)

    def test_repository_connection_failure_is_unavailable_without_second_connection(self):
        with patch.object(repository,"_connect",side_effect=psycopg.OperationalError("synthetic")) as connect:
            status=prep.probe_readiness(**self.probes())
        self.assertEqual(status.failure_stage,"database_readiness")
        self.assertEqual(connect.call_count,1)

    def test_projection_timeout_or_missing_column_is_not_false_healthy_empty_result(self):
        for error in (psycopg.errors.QueryCanceled("synthetic"),psycopg.errors.UndefinedColumn("synthetic")):
            clock,projection=ConnectionDouble(),ConnectionDouble(failure=error)
            with patch.object(repository,"_connect",side_effect=[clock,projection]):
                status=prep.probe_readiness(**self.probes())
            self.assertEqual((status.database_available,status.projection_available),(True,False))
            self.assertTrue(clock.closed and projection.closed)


class PreparationHTTPCompatibilityTests(ObservabilityTestCase):
    def test_normal_response_matches_prepared_model_and_preserves_query_parameters(self):
        query=prep.FeedQuery(("open-krl:THB","open-krl:SRP","open-krl:THB"),1,4)
        page=repository.FeedPage(NOW,[row(2),row(1)])
        with patch("mooby.api.app.fetch_disruptions",return_value=page) as fetch:
            start,raw,error=self.call(query={"station_ids":list(query.station_ids),"limit":1,"offset":4})
        self.assertIsNone(error);self.assertEqual(start["status"],200)
        self.assertEqual(json.loads(raw),prep.prepare_feed(query,page).model_dump(mode="json"))
        self.assertEqual(fetch.call_args.kwargs["station_ids"],list(query.station_ids))
        self.assertEqual((fetch.call_args.kwargs["limit"],fetch.call_args.kwargs["offset"]),(1,4))
        self.terminal()

    def test_auth_and_validation_still_precede_repository_access(self):
        with patch("mooby.api.app.fetch_disruptions") as fetch:
            self.assertEqual(self.call(token=None)[0]["status"],401)
            self.events.clear()
            self.assertEqual(self.call(query={"station_ids":"invalid"})[0]["status"],422)
            fetch.assert_not_called()

    def test_controlled_feed_error_keeps_pagination_and_error_shape(self):
        with patch("mooby.api.app.fetch_disruptions",side_effect=psycopg.OperationalError("synthetic-private")):
            start,raw,error=self.call(query={"station_ids":"open-krl:SRP","limit":7,"offset":9})
        payload=json.loads(raw)
        self.assertIsNone(error);self.assertEqual(start["status"],503)
        self.assertEqual((payload["availability"],payload["items"],payload["limit"],payload["offset"]),
                         ("source_unavailable",[],7,9))
        self.assertNotIn("synthetic-private",raw.decode())
        self.assertEqual(self.terminal()["failure_stage"],"feed_read")

    def test_stage_wrappers_and_partial_readiness_failure_are_unchanged(self):
        with patch("mooby.api.app.fetch_disruptions",return_value=repository.FeedPage(NOW,[row()])):
            self.assertEqual(self.call()[0]["status"],200)
        stages=[event.get("stage") for event in self.events if event["event"]=="request_stage"]
        self.assertEqual(stages.count("feed_read"),1)
        self.assertEqual(stages.count("row_mapping"),1)
        self.assertEqual(stages.count("response_validation"),1)
        self.terminal();self.events.clear()
        with patch("mooby.api.app.check_database",return_value=True), \
             patch("mooby.api.app.check_app_api_projection",side_effect=OSError("synthetic-private")):
            start,raw,error=self.call(path="/health/ready")
        self.assertIsNone(error);self.assertEqual(start["status"],503)
        self.assertEqual(json.loads(raw),{"api":"available","database":"available","app_api_projection":"unavailable"})
        self.assertEqual(self.terminal()["failure_stage"],"projection_readiness")
