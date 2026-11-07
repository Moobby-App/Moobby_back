"""Opt-in launchd supervisor for run_worker.py; never implements processing."""
import argparse
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import signal
import subprocess
import sys

from worker_runtime import safe_record, validate_development_url

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / 'ops/launchd/development-worker.json'
ENV_KEYS = ('WORKER_DATABASE_URL', 'MOOBY_RELEVANCE_THRESHOLD', 'MOOBY_SEVERITY_THRESHOLD',
            'MOOBY_TRUSTED_PUBLISHED_AT_BASES', 'MOOBY_EMBEDDING_MODEL_DIR')


def load_configuration(config_path=CONFIG, env_path=ROOT/'.env'):
    config = json.loads(config_path.read_text())
    if set(config) != {'expected_project_ref', 'pipeline_version', 'worker_id'}:
        raise ValueError('invalid worker configuration')
    for key in ('pipeline_version', 'worker_id'):
        value = config[key]
        if (not isinstance(value, str) or value.startswith('REPLACE_')
            or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', value)):
            raise ValueError('invalid worker configuration')
    # Parse data rather than sourcing a shell file. No interpolation/commands,
    # inherited DB/admin credentials, or diagnostic output from the parser.
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        from dotenv import dotenv_values
        values = dotenv_values(env_path, interpolate=False)
    database_url = values.get('WORKER_DATABASE_URL')
    if not database_url:
        raise ValueError('WORKER_DATABASE_URL required in local .env')
    validate_development_url(database_url, config['expected_project_ref'])
    for key in ('MOOBY_RELEVANCE_THRESHOLD', 'MOOBY_SEVERITY_THRESHOLD',
                'MOOBY_TRUSTED_PUBLISHED_AT_BASES'):
        if not values.get(key):
            raise ValueError('worker configuration missing')
    env = {k: os.environ[k] for k in ('HOME', 'PATH', 'TMPDIR', 'LANG', 'LC_ALL') if k in os.environ}
    env.update({k: values[k] for k in ENV_KEYS if values.get(k) is not None})
    env.update(HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1', PYTHONUNBUFFERED='1',
               PYTHONDONTWRITEBYTECODE='1', TOKENIZERS_PARALLELISM='false')
    # Reuse the actual worker's configuration validator; do not duplicate or
    # modify its thresholds/publication-basis rules. This never loads a model.
    from mooby.worker.processor import WorkerSettings
    settings = WorkerSettings.from_env(env)
    if settings.thresholds is None or settings.publication_basis_error is not None:
        raise ValueError('invalid worker configuration')
    return config, env


def worker_command(config):
    return [str(ROOT/'.venv/bin/python'), '-B', '-u', str(ROOT/'scripts/run_worker.py'),
            '--expected-project-ref', config['expected_project_ref'],
            '--pipeline-version', config['pipeline_version'], '--worker-id', config['worker_id'],
            '--poll-seconds', '5', '--discovery-batch-size', '5', '--max-items', '1',
            '--lookback-seconds', '3600']


def filtered_record(line):
    try:
        value = json.loads(line)
        if not isinstance(value, dict):
            return None
        event = value.pop('event', None)
        return safe_record(event, **value)
    except (ValueError, TypeError):
        return None


def supervise(config, env, log):
    stopping = False
    with subprocess.Popen(worker_command(config), cwd=ROOT, env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                          text=True, encoding='utf-8', errors='replace', start_new_session=True) as child:
        def stop(signum, frame):
            nonlocal stopping
            stopping = True
            if child.poll() is None:
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        previous = {s: signal.signal(s, stop) for s in (signal.SIGTERM, signal.SIGINT)}
        try:
            while True:
                line = child.stdout.readline(8193)
                if not line:
                    break
                if len(line) > 8192 or not line.endswith('\n'):
                    while line and not line.endswith('\n'):
                        line = child.stdout.readline(8193)
                    continue
                record = filtered_record(line)
                if record is not None:
                    log(record)
            code = child.wait()
        finally:
            # If log/pipe handling fails, do not leave a hidden polling child
            # running while launchd starts a replacement supervisor.
            if child.poll() is None:
                stop(signal.SIGTERM, None)
            for s, handler in previous.items():
                signal.signal(s, handler)
        log(safe_record('worker_exited', exit_code=code))
        return 0 if stopping or code == 0 else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check-config', action='store_true', help='Validate local config only; no worker or DB')
    args = parser.parse_args()
    logdir = ROOT/'.local/worker'
    logdir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger('mooby-development-worker')
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = RotatingFileHandler(logdir/'worker.log', maxBytes=1_000_000, backupCount=3)
    handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    logger.addHandler(handler)
    log = lambda record: logger.info(json.dumps(record))
    try:
        try:
            config, env = load_configuration()
        except Exception:
            log(safe_record('configuration_invalid'))
            if args.check_config:
                print(json.dumps(safe_record('configuration_invalid')))
                return 1
            return 0  # Configuration errors require an explicit fix/kickstart, no restart loop.
        if args.check_config:
            print(json.dumps(safe_record('configuration_valid')))
            return 0
        try:
            return supervise(config, env, log)
        except Exception:
            log(safe_record('worker_error'))
            return 1
    finally:
        logger.removeHandler(handler)
        handler.close()


if __name__ == '__main__':
    raise SystemExit(main())
