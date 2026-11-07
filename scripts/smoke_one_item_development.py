"""One historical snapshot, real local models, Development-only, always rollback.

Never start this module implicitly. Its CLI requires a separately supplied DSN
and explicit target identity. No collector updates, scheduler, or API calls.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
import math
import json
import os
from pathlib import Path
import sys
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row

from mooby.db.source_reader import (
    database_clock_now, load_snapshot, load_station_catalog, load_confirmed_ordered_paths)
from mooby.worker.classifier import ClassifierAdapter
from mooby.worker.embedding import _runtime as embedding_runtime, EventEmbedding
from mooby.worker.processor import WorkerSettings, process_and_finish_attempt
from mooby.worker.queue import enqueue_recent_snapshots, claim_batch


WORKER_SESSION_USER = 'mooby_nlp_worker_login_dev'


class SmokeFailure(RuntimeError):
    """Contains only a controlled report, never DB errors or source text."""
    def __init__(self, report):
        super().__init__('development smoke check failed')
        self.report = report


def database_error_report(stage, exc):
    """Allowlist driver diagnostics; never inspect message/detail/context/query."""
    report = {'status': 'failed', 'stage': stage,
              'sqlstate': getattr(exc, 'sqlstate', None)}
    diag = getattr(exc, 'diag', None)
    for source, target in [('schema_name', 'schema'), ('table_name', 'table'),
                           ('column_name', 'column')]:
        name = getattr(diag, source, None)
        if name:
            report[target] = name
    return report


class StagedCursor:
    def __init__(self, cursor, conn, stage):
        self.cursor, self.conn, self.stage = cursor, conn, stage

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def fetchone(self):
        return self._fetch('fetchone')

    def fetchall(self):
        return self._fetch('fetchall')

    def _fetch(self, method):
        try:
            return getattr(self.cursor, method)()
        except psycopg.Error as exc:
            self.conn.record(self.stage, exc)
            raise


class StagedConnection:
    """Harness-only instrumentation, including SQL inside worker helpers.

    Keep the FIRST driver failure even if a worker helper converts it to an
    outcome or a later aborted-transaction error obscures its diagnostics.
    """
    WRITE_STAGES = (
        ('with picked as', 'claim_smoke_queue'),
        ('update nlp.processing_queue', 'finish_queue_update'),
        ('insert into nlp.processing_queue', 'insert_smoke_queue'),
        ('insert into nlp.processing_runs', 'insert_processing_run'),
        ('insert into nlp.processing_notes', 'insert_processing_note'),
        ('insert into nlp.events ', 'insert_event'),
        ('insert into nlp.event_versions', 'insert_event_version'),
        ('insert into nlp.event_sources', 'insert_event_source'),
        ('insert into nlp.event_evidence', 'insert_event_evidence'),
        ('from collection.raw_snapshots', 'load_source_snapshot'),
        ('from reference.krl_stations', 'load_station_catalog'),
        ('from nlp.line_paths', 'load_confirmed_paths'),
        ('select clock_timestamp()', 'read_database_clock'),
    )

    def __init__(self, conn):
        self.conn = conn
        self.stage = 'outer_transaction'
        self.first_error = None
        self.writes = {}

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def record(self, stage, exc):
        if self.first_error is None:
            self.first_error = database_error_report(stage, exc)

    def execute(self, sql, params=()):
        normalized = ' '.join(sql.lower().split())
        stage = self.stage
        # Per-table counts keep their explicit preflight/post-rollback label.
        if 'as row_count' not in normalized:
            for token, label in self.WRITE_STAGES:
                if token in normalized:
                    stage = label
                    break
        try:
            cursor = self.conn.execute(sql, params)
        except psycopg.Error as exc:
            self.record(stage, exc)
            raise
        if stage.startswith('insert_'):
            write = {'rowcount': cursor.rowcount}
            if stage == 'insert_event_version':
                # Bind positions are the existing persist_draft_event contract.
                write.update(event_id=params[0], run_id=params[3],
                             input_version=params[-1], vector=params[-2],
                             publication_literals=(
                                 "'verified', 'published', transaction_timestamp(), transaction_timestamp()"
                                 in normalized))
            elif stage == 'insert_processing_run':
                write.update(run_id=params[0], result_status=params[4],
                             model_version=params[8], model_digest=params[-1])
            self.writes.setdefault(stage, []).append(write)
        return StagedCursor(cursor, self, stage)

    @contextmanager
    def transaction(self, **kwargs):
        try:
            with self.conn.transaction(**kwargs):
                yield
        except psycopg.Error as exc:
            self.record(self.stage, exc)
            raise

    def raise_recorded_error(self):
        if self.first_error is not None:
            raise SmokeFailure(dict(self.first_error))


def require(condition, stage):
    if not condition:
        raise SmokeFailure({'status': 'failed', 'stage': stage})


def validate_target(dsn, *, host, database, user):
    """No fallback to application/worker/Production DSNs or implicit PG defaults."""
    params = conninfo_to_dict(dsn)
    require(bool(host and database and user), 'explicit_development_target_required')
    require(not any(k in params for k in ('service', 'hostaddr')),
            'indirect_connection_target_not_allowed')
    require(',' not in host and '/' not in host, 'single_development_host_required')
    require(all(params.get(k) == value for k, value in
                [('host', host), ('dbname', database), ('user', user)]),
            'development_target_mismatch')


def validate_session_identity(conn, *, database, session_user):
    """Validate server identities independently of the pooler's DSN login.

    Both PostgreSQL roles must match: an unexpected SET ROLE must fail closed.
    """
    require(bool(database and session_user), 'explicit_database_session_identity_required')
    identity = conn.execute(
        'SELECT current_database() AS database, current_user AS current_role, '
        'session_user AS session_role').fetchone()
    require(identity is not None and identity.get('database') == database
            and identity.get('current_role') == session_user
            and identity.get('session_role') == session_user,
            'connected_development_identity_mismatch')


def rollback_only(conn, exercise, check_absent):
    """Own an outer transaction. Even successful savepoints cannot commit it.

    After it ends, use another rollback-only *read-only* transaction to check
    absence. A failed post-check never produces a success/rollback guarantee.
    """
    require(conn.autocommit and conn.info.transaction_status == TransactionStatus.IDLE,
            'idle_autocommit_connection_required')
    failure = None
    result = {}
    try:
        with conn.transaction(force_rollback=True):
            result = exercise()
    except Exception as exc:
        failure = exc
    # Exceptions propagate only after PostgreSQL has rolled back the outer scope.
    require(conn.info.transaction_status == TransactionStatus.IDLE,
            'outer_transaction_not_finished')
    try:
        with conn.transaction(force_rollback=True):
            if isinstance(conn, StagedConnection):
                conn.stage = 'post_rollback_read_only'
            conn.execute('SET TRANSACTION READ ONLY')
            remaining = check_absent()
            require(all(count == 0 for count in remaining.values()), 'rollback_residue_detected')
    except Exception as exc:
        post_stage = conn.stage if isinstance(conn, StagedConnection) else 'post_rollback_verification'
        post = dict(exc.report) if isinstance(exc, SmokeFailure) else database_error_report(post_stage, exc)
        primary = conn.first_error if isinstance(conn, StagedConnection) else None
        report = dict(primary or post)
        report['rollback_verified'] = False
        if primary and primary != post:
            report['rollback_check_error'] = post
        raise SmokeFailure(report) from None
    if failure is not None:
        primary = conn.first_error if isinstance(conn, StagedConnection) else None
        report = dict(primary) if primary else (
            dict(failure.report) if isinstance(failure, SmokeFailure) else
            database_error_report('processing_or_inspection', failure))
        report.update(rollback_verified=True, remaining_rows=remaining)
        raise SmokeFailure(report) from None
    return {**result, 'status': 'passed', 'rollback_verified': True, 'remaining_rows': remaining}


def row_counts(conn, pipeline, event_id):
    # Fixed table/column names only; every value remains a bound parameter.
    targets = (
        ('queue', 'nlp.processing_queue', 'pipeline_version', pipeline),
        ('runs', 'nlp.processing_runs', 'pipeline_version', pipeline),
        ('notes', 'nlp.processing_notes', 'pipeline_version', pipeline),
        ('events', 'nlp.events', 'event_id', event_id),
        ('versions', 'nlp.event_versions', 'pipeline_version', pipeline),
        ('sources', 'nlp.event_sources', 'pipeline_version', pipeline),
        ('evidence', 'nlp.event_evidence', 'event_id', event_id),
    )
    base = conn.stage if isinstance(conn, StagedConnection) else None
    counts = {}
    for key, table, column, value in targets:
        if base is not None:
            conn.stage = base + '.' + table
        row = conn.execute(f'SELECT count(*) AS row_count FROM {table} WHERE {column} = %s',
                           (value,)).fetchone()
        counts[key] = row['row_count']
    if base is not None:
        conn.stage = base
    return counts


def queue_count(conn, pipeline):
    return conn.execute(
        'SELECT count(*) AS row_count FROM nlp.processing_queue WHERE pipeline_version = %s',
        (pipeline,)).fetchone()['row_count']


def smoke_one(conn, snapshot_id, *, expected_database, expected_session_user, check_residue):
    """Worker-only transaction; external residue inspector runs AFTER rollback."""
    require(expected_session_user == WORKER_SESSION_USER, 'worker_login_required')
    require(callable(check_residue), 'post_rollback_inspector_required')
    conn = StagedConnection(conn)
    pipeline = 'development-rollback-smoke-' + str(uuid4())
    owner = 'rollback-smoke-' + str(uuid4())
    event_id = uuid5(NAMESPACE_URL, f'mooby-event-draft:{snapshot_id}:{pipeline}')

    def exercise():
        conn.stage = 'check_database_session_identity'
        validate_session_identity(conn, database=expected_database, session_user=expected_session_user)
        conn.stage = 'set_statement_timeout'
        conn.execute("SELECT set_config('statement_timeout', '30000', true)")
        conn.stage = 'set_lock_timeout'
        conn.execute("SELECT set_config('lock_timeout', '3000', true)")
        conn.stage = 'preflight_worker_queue_count'
        require(queue_count(conn, pipeline) == 0, 'smoke_namespace_not_empty')
        conn.stage = 'load_source_snapshot'
        original = load_snapshot(conn, snapshot_id)
        require(original is not None, 'snapshot_not_found')
        conn.stage = 'read_database_clock'
        now = database_clock_now(conn)
        require(original.captured_at is not None and original.captured_at <= now,
                'historical_capture_time_required')
        # Only the in-memory NLP input is time-simulated. Enqueue still reads the
        # immutable source captured_at and uses the normal dispatcher SQL.
        lookback = max(1, math.ceil((now - original.captured_at).total_seconds()) + 60)
        snapshot = replace(original, published_at=now - timedelta(minutes=1), captured_at=now)
        classifier = ClassifierAdapter.load()
        require(all(m['status'] == 'loaded' for m in classifier.artifact_manifest.values()),
                'real_classifier_unavailable')
        embedding_runtime()
        settings = WorkerSettings.from_env()
        require(settings.thresholds is not None and settings.publication_basis_error is None,
                'worker_configuration_unavailable')
        conn.stage = 'enqueue_smoke_snapshot'
        enqueued = enqueue_recent_snapshots(
            conn, pipeline_version=pipeline, lookback_seconds=lookback,
            batch_size=1, snapshot_id=str(snapshot_id))
        require(enqueued == [str(snapshot_id)], 'exact_snapshot_not_enqueued')
        require(queue_count(conn, pipeline) == 1, 'unexpected_namespace_queue_items')
        conn.stage = 'claim_smoke_queue'
        claimed = claim_batch(conn, pipeline_version=pipeline, worker_id=owner,
                              batch_size=1, lease_seconds=900)
        require(len(claimed) == 1 and str(claimed[0]['source_snapshot_id']) == str(snapshot_id)
                and claimed[0]['pipeline_version'] == pipeline
                and claimed[0]['lease_owner'] == owner, 'exact_smoke_item_not_claimed')
        conn.stage = 'process_and_finish_attempt'
        outcome, run_id = process_and_finish_attempt(
            conn, claimed[0], snapshot, worker_id=owner, classifier=classifier,
            settings=settings, max_attempts=1, transient_retry_after_seconds=30,
            station_catalog_loader=lambda: load_station_catalog(conn),
            path_catalog_loader=lambda as_of: load_confirmed_ordered_paths(conn, as_of=as_of))
        conn.raise_recorded_error()
        require(outcome.result_status == 'succeeded' and outcome.draft_event is not None,
                'snapshot_not_accepted_or_embedding_failed')
        require(outcome.draft_event.event_id == event_id, 'event_identity_contract_changed')
        # Worker has INSERT, not SELECT, on event/audit objects. Inspect successful
        # INSERT rowcounts and the real bound payload, never elevate/read them back.
        for stage in ('insert_processing_run', 'insert_event', 'insert_event_version',
                      'insert_event_source', 'insert_event_evidence'):
            writes = conn.writes.get(stage, [])
            require(len(writes) == 1 and writes[0]['rowcount'] == 1,
                    'worker_persistence_insert_not_confirmed')
        version = conn.writes['insert_event_version'][0]
        run = conn.writes['insert_processing_run'][0]
        provenance = outcome.result_metadata.get('embedding', {})
        require(version['event_id'] == event_id and version['run_id'] == run['run_id']
                and str(run['run_id']) == str(run_id) and run['result_status'] == 'succeeded'
                and version['publication_literals'], 'publication_insert_contract_failed')
        values = json.loads(version['vector'])
        embedding = EventEmbedding(tuple(values), run['model_version'], run['model_digest'],
                                   version['input_version'])
        embedding.validate()
        require(len(values) == 384 and version['input_version'] == provenance.get('input_version')
                and run['model_version'] == provenance.get('model_version')
                and run['model_digest'] == provenance.get('model_digest'),
                'embedding_insert_provenance_mismatch')
        conn.stage = 'inspect_completed_smoke_queue'
        completed = conn.execute(
            'SELECT source_snapshot_id, status FROM nlp.processing_queue WHERE pipeline_version = %s',
            (pipeline,)).fetchall()
        require(len(completed) == 1 and str(completed[0]['source_snapshot_id']) == str(snapshot_id)
                and completed[0]['status'] == 'succeeded', 'queue_completion_not_confirmed')
        return {'time_simulated_in_memory': True, 'worker_persistence_checked': True,
                'publication_insert_checked': True, 'embedding_dimensions': 384,
                'api_verification': 'separate_offline_tests_not_database_visibility'}

    def check_absent():
        conn.stage = 'post_rollback_worker_queue_count'
        require(queue_count(conn, pipeline) == 0, 'worker_queue_rollback_residue')
        # No event/API SELECT is issued by the worker. The inspector is opened
        # lazily by this callback, only after the outer transaction was rolled back.
        counts = check_residue(pipeline, event_id)
        require(set(counts) == {'queue', 'runs', 'notes', 'events', 'versions', 'sources', 'evidence'},
                'residue_inspection_incomplete')
        return counts

    return rollback_only(conn, exercise, check_absent)


def inspect_residue(dsn, *, host, database, user, session_user, pipeline, event_id):
    """New read-only inspection connection: committed state AFTER worker rollback."""
    validate_target(dsn, host=host, database=database, user=user)
    raw = psycopg.connect(dsn, autocommit=True, row_factory=dict_row,
                          connect_timeout=5, options='-c default_transaction_read_only=on')
    conn = StagedConnection(raw)
    try:
        require(conn.info.host == host, 'inspection_host_mismatch')
        with conn.transaction(force_rollback=True):
            conn.stage = 'post_rollback_inspector_read_only'
            conn.execute('SET TRANSACTION READ ONLY')
            conn.stage = 'post_rollback_inspector_identity'
            validate_session_identity(conn, database=database, session_user=session_user)
            conn.stage = 'post_rollback_row_counts'
            return row_counts(conn, pipeline, event_id)
    except Exception as exc:
        if conn.first_error:
            raise SmokeFailure(dict(conn.first_error)) from None
        if isinstance(exc, SmokeFailure):
            raise
        raise SmokeFailure(database_error_report(conn.stage, exc)) from None
    finally:
        raw.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-id', required=True, type=UUID)
    parser.add_argument('--expected-dev-host', required=True)
    parser.add_argument('--expected-dev-database', required=True)
    parser.add_argument('--expected-dev-user', required=True,
                        help='Exact DSN connection username, including any pooler suffix')
    parser.add_argument('--expected-dev-session-user', required=True,
                        help='Expected PostgreSQL role; both current_user and session_user must match')
    parser.add_argument('--expected-dev-inspection-user', required=True,
                        help='DSN username of existing read-only NLP residue inspector (post-rollback only)')
    parser.add_argument('--expected-dev-inspection-session-user', required=True,
                        help='Expected current_user/session_user on the read-only inspector')
    parser.add_argument('--confirm-development-rollback', required=True, action='store_true')
    args = parser.parse_args()
    conn = None
    try:
        dsn = os.environ.get('SMOKE_DEV_DATABASE_URL', '')
        require(bool(dsn), 'dedicated_smoke_dev_dsn_required')
        require(args.expected_dev_session_user == WORKER_SESSION_USER, 'worker_login_required')
        inspection_dsn = os.environ.get('SMOKE_DEV_INSPECTION_DATABASE_URL', '')
        require(bool(inspection_dsn), 'post_rollback_inspection_dsn_required')
        validate_target(dsn, host=args.expected_dev_host,
                        database=args.expected_dev_database, user=args.expected_dev_user)
        validate_target(inspection_dsn, host=args.expected_dev_host,
                        database=args.expected_dev_database, user=args.expected_dev_inspection_user)
        # Force local-only HF behavior before any model imports. Never emit env values.
        os.environ['HF_HUB_OFFLINE'] = '1'
        os.environ['TRANSFORMERS_OFFLINE'] = '1'
        conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row, connect_timeout=5)
        require(conn.info.host == args.expected_dev_host, 'connected_host_mismatch')
        report = smoke_one(conn, args.snapshot_id,
                           expected_database=args.expected_dev_database,
                           expected_session_user=args.expected_dev_session_user,
                           check_residue=lambda pipeline, event_id: inspect_residue(
                               inspection_dsn, host=args.expected_dev_host,
                               database=args.expected_dev_database,
                               user=args.expected_dev_inspection_user,
                               session_user=args.expected_dev_inspection_session_user,
                               pipeline=pipeline, event_id=event_id))
    except SmokeFailure as exc:
        print(json.dumps(exc.report))
        return 1
    except Exception as exc:
        print(json.dumps(database_error_report('connect_or_initialize_development', exc)))
        return 1
    finally:
        if conn is not None:
            conn.close()
    print(json.dumps(report))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
