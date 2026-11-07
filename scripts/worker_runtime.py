"""Development CLI safeguards; no database or model operations."""
from contextlib import contextmanager
import fcntl
import json
from pathlib import Path
import re
from urllib.parse import parse_qsl, unquote, urlsplit

WORKER_ROLE = 'mooby_nlp_worker_login_dev'
EVENTS = {'worker_started', 'worker_stopped', 'worker_exited', 'worker_error',
          'configuration_invalid', 'configuration_valid', 'duplicate_instance', 'poll'}
COUNTS = {'enqueued_count', 'processed_count', 'succeeded_count', 'skipped_count',
          'rejected_count', 'failed_count', 'accepted_event_count'}


class WorkerConfigurationError(ValueError):
    pass


class AlreadyRunning(RuntimeError):
    pass


def validate_development_url(database_url, expected_ref):
    """Exact worker principal, explicit URL target, no libpq routing overrides."""
    try:
        if not re.fullmatch(r'[a-z0-9]{20}', expected_ref):
            raise ValueError()
        parsed = urlsplit(database_url)
        user = unquote(parsed.username or '')
        host = parsed.hostname or ''
        query = parse_qsl(parsed.query, keep_blank_values=True)
        keys = [k for k, _ in query]
        if (parsed.scheme not in {'postgres', 'postgresql'} or not parsed.path.strip('/')
            or parsed.fragment or len(keys) != len(set(keys))
            or any(k not in {'sslmode', 'connect_timeout'} for k in keys)):
            raise ValueError()
        direct = host == f'db.{expected_ref}.supabase.co' and user == WORKER_ROLE
        pooled = host.endswith('.pooler.supabase.com') and user == f'{WORKER_ROLE}.{expected_ref}'
        if not (direct or pooled):
            raise ValueError()
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError()
    except (ValueError, TypeError):
        raise WorkerConfigurationError('invalid Development worker target') from None


@contextmanager
def single_instance(path):
    """Shared by all invocations of this repo's existing worker CLI. Never unlink."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise AlreadyRunning() from None
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def safe_record(event, **fields):
    if event not in EVENTS:
        return None
    record = {'event': event}
    for key in COUNTS:
        value = fields.get(key)
        if type(value) is int and value >= 0:
            record[key] = value
    value = fields.get('exit_code')
    if type(value) is int:
        record['exit_code'] = value
    value = fields.get('sqlstate')
    if isinstance(value, str) and re.fullmatch(r'[0-9A-Z]{5}', value):
        record['sqlstate'] = value
    return record


def emit(event, **fields):
    print(json.dumps(safe_record(event, **fields)), flush=True)
