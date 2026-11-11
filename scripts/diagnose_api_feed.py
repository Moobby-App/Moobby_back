"""Manual read-only API query diagnostic. Never prints data or exception text.

Not a service/worker. Every diagnostic transaction is read-only and rolled back.
Default settings come from local .env; --environment uses the caller's environment.
"""
import argparse
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
import json
import os
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))

import psycopg
from psycopg.rows import dict_row
from mooby.api.config import ApiSettings
from mooby.api.repository import _PROJECTION_SQL, _SEARCH_SQL
from mooby.api.app import _item

ROOT = Path(__file__).resolve().parents[1]
SETTING_KEYS = ('APP_API_DATABASE_URL', 'APP_API_BEARER_TOKEN',
                'APP_API_CONNECT_TIMEOUT_SECONDS', 'APP_API_STATEMENT_TIMEOUT_MS')


def report(stage, exc=None):
    # No message, diagnostics/context, query, parameters, rows, or config values.
    return {'stage': stage, 'exception_type': type(exc).__name__ if exc else None,
            'sqlstate': getattr(exc, 'sqlstate', None) if exc else None}


def settings_from_dotenv(path):
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        from dotenv import dotenv_values
        values = dotenv_values(path, interpolate=False)
    previous = {k: os.environ.get(k) for k in SETTING_KEYS}
    try:
        for k in SETTING_KEYS:
            if values.get(k) is not None:
                os.environ[k] = values[k]
            else:
                os.environ.pop(k, None)
        return ApiSettings.from_env()
    finally:
        for k, value in previous.items():
            if value is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = value


def probe(stage, operation, emit):
    try:
        value = operation()
    except Exception as exc:
        emit(report(stage, exc))
        return False, None
    emit(report(stage))
    return True, value


def query_probe(conn, stage, operation, emit):
    def run():
        # A failing SELECT must not poison the remaining diagnostic stages.
        with conn.transaction(force_rollback=True):
            return operation()
    return probe(stage, run, emit)


def diagnose(settings, station_ids, emit, *, limit=20, offset=0):
    all_ok = True
    for startup_options in (False, True):
        prefix = 'api_options' if startup_options else 'without_api_options'
        kwargs = dict(connect_timeout=settings.connect_timeout_seconds,
                      row_factory=dict_row, autocommit=True)
        if startup_options:
            kwargs['options'] = f'-c statement_timeout={settings.statement_timeout_ms}'
        ok, conn = probe(prefix+'.connect',
                         lambda: psycopg.connect(settings.database_url, **kwargs), emit)
        if not ok:
            all_ok = False
            continue
        stage = prefix+'.read_only_transaction'
        try:
            with conn.transaction(force_rollback=True):
                conn.execute('SET TRANSACTION READ ONLY')
                emit(report(stage))
                # Baseline differs only in *when* the same timeout is set. This
                # separates startup-parameter compatibility from SELECT failures.
                if not startup_options:
                    stage = prefix+'.set_timeout_after_connect'
                    conn.execute("SELECT set_config('statement_timeout', %s, true)",
                                 (str(settings.statement_timeout_ms),)).fetchone()
                    emit(report(stage))
                operations = [
                    ('database_readiness_clock', lambda: conn.execute('SELECT transaction_timestamp()').fetchone()),
                    ('feed_generated_at', lambda: conn.execute(
                        'SELECT transaction_timestamp() AS generated_at').fetchone()['generated_at']),
                    ('view_count', lambda: conn.execute(
                        'SELECT count(*) FROM app_api.published_disruptions').fetchone()),
                    ('projection_readiness', lambda: conn.execute(
                        _PROJECTION_SQL, ([],)).fetchall()),
                    ('feed_schema', lambda: conn.execute(
                        _PROJECTION_SQL, ([],)).fetchall()),
                ]
                for suffix, operation in operations:
                    ok, _ = query_probe(conn, prefix+'.'+suffix, operation, emit)
                    all_ok = all_ok and ok
                ok, rows = query_probe(conn, prefix+'.feed_query',
                    lambda: conn.execute(_SEARCH_SQL, (station_ids, limit+1, offset)).fetchall(), emit)
                all_ok = all_ok and ok
                if ok:
                    ok, _ = probe(prefix+'.feed_response_validation',
                                  lambda: [_item(dict(row)) for row in rows[:limit]], emit)
                    all_ok = all_ok and ok
                stage = prefix+'.read_only_transaction_end'
        except Exception as exc:
            emit(report(stage, exc))
            all_ok = False
        finally:
            try:
                conn.close()
            except Exception as exc:
                emit(report(prefix+'.close', exc))
                all_ok = False
    return all_ok


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--environment', action='store_true', help='Use already exported API settings instead of .env')
    parser.add_argument('--station-id', action='append', help='Public canonical station ID; repeat to match route')
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--offset', type=int, default=0)
    args = parser.parse_args()
    ids = args.station_id or ['open-krl:BOO']
    if (not 1 <= len(ids) <= 30 or any(not re.fullmatch(r'open-krl:[A-Z0-9]+', x) for x in ids)
        or not 1 <= args.limit <= 50 or not 0 <= args.offset <= 1000):
        parser.error('invalid public feed filter or pagination')
    emit = lambda value: print(json.dumps(value), flush=True)
    try:
        settings = ApiSettings.from_env() if args.environment else settings_from_dotenv(ROOT/'.env')
    except Exception as exc:
        emit(report('validate_api_settings', exc))
        return 1
    emit(report('validate_api_settings'))
    return 0 if diagnose(settings, ids, emit, limit=args.limit, offset=args.offset) else 1


if __name__ == '__main__':
    raise SystemExit(main())
