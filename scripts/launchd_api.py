"""Opt-in localhost API / stable-domain ngrok supervisors. No database checks."""
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
from urllib.parse import parse_qsl, unquote, urlsplit

from worker_runtime import AlreadyRunning, single_instance

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT/'ops/launchd/development-api.json'
NGROK = Path('/opt/homebrew/bin/ngrok')
API_KEYS = ('APP_API_DATABASE_URL', 'APP_API_BEARER_TOKEN')


class ConfigurationBlocked(ValueError):
    def __init__(self, reason):
        self.reason = reason


def base_environment():
    return {k: os.environ[k] for k in ('HOME', 'PATH', 'TMPDIR', 'LANG', 'LC_ALL') if k in os.environ}


def read_settings(path=CONFIG):
    value = json.loads(path.read_text())
    if set(value) != {'expected_project_ref', 'ngrok_config_file', 'stable_domain'}:
        raise ConfigurationBlocked('invalid_local_configuration')
    return value


def api_environment(settings, env_file=ROOT/'.env'):
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        from dotenv import dotenv_values
        values = dotenv_values(env_file, interpolate=False)
    api = {key: (values.get(key) or '').strip() for key in API_KEYS}
    ref = settings['expected_project_ref']
    if not isinstance(ref, str) or not re.fullmatch(r'[a-z0-9]{20}', ref):
        raise ConfigurationBlocked('development_project_ref_required')
    url = urlsplit(api['APP_API_DATABASE_URL'])
    username, host = unquote(url.username or ''), url.hostname or ''
    direct = host == f'db.{ref}.supabase.co' and username == 'mooby_app_api_login_dev'
    pooled = host.endswith('.pooler.supabase.com') and username == f'mooby_app_api_login_dev.{ref}'
    query = parse_qsl(url.query, keep_blank_values=True)
    keys = [key for key, _ in query]
    if (url.scheme not in {'postgres', 'postgresql'} or not url.path.strip('/')
        or url.fragment or not (direct or pooled)
        or len(keys) != len(set(keys))
        or any(key not in {'sslmode', 'connect_timeout'} for key in keys)):
        raise ConfigurationBlocked('invalid_development_api_database_target')
    if len(api['APP_API_BEARER_TOKEN']) < 32:
        raise ConfigurationBlocked('invalid_api_bearer_configuration')
    env = base_environment()
    env.update(api)
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    return env


def domain_name(value):
    if not isinstance(value, str) or value.startswith('REPLACE_'):
        raise ConfigurationBlocked('stable_domain_not_configured')
    # Only a public hostname, no userinfo, credentials, paths, ports or query.
    value = value.lower()
    if not re.fullmatch(r'(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}', value):
        raise ConfigurationBlocked('invalid_stable_domain')
    return value


def configured_domains(data):
    """Only reuse endpoints explicitly forwarding to the existing API port."""
    values = []
    entries = list((data.get('tunnels') or {}).values()) + (data.get('endpoints') or [])
    for entry in entries:
        upstream = entry.get('addr') or (entry.get('upstream') or {}).get('url')
        if str(upstream) not in {'8000', 'localhost:8000', '127.0.0.1:8000',
                                 'http://localhost:8000', 'http://127.0.0.1:8000'}:
            continue
        value = entry.get('url') or entry.get('domain') or entry.get('hostname')
        if value:
            if '://' in value:
                parsed = urlsplit(value)
                if (parsed.scheme != 'https' or parsed.username or parsed.password
                    or parsed.port or parsed.path not in ('', '/') or parsed.query or parsed.fragment):
                    raise ConfigurationBlocked('invalid_stable_domain')
                value = parsed.hostname
            values.append(domain_name(value))
    return set(values)


def tunnel_configuration(settings):
    import yaml
    path = Path(settings['ngrok_config_file']).expanduser()
    if not path.is_absolute() or not path.is_file() or not NGROK.is_file():
        raise ConfigurationBlocked('existing_ngrok_configuration_required')
    data = yaml.safe_load(path.read_text()) or {}
    # The existing credential remains in this private file and is never copied
    # into the environment, arguments, tracked config, or a log record.
    if not ((data.get('agent') or {}).get('authtoken') or data.get('authtoken')):
        raise ConfigurationBlocked('existing_ngrok_authentication_required')
    existing = configured_domains(data)
    explicit = settings['stable_domain']
    if explicit is not None:
        domain = domain_name(explicit)  # Owner must already have configured/reserved it.
        if existing and domain not in existing:
            raise ConfigurationBlocked('stable_domain_configuration_mismatch')
    elif len(existing) == 1:
        domain = next(iter(existing))
    else:
        raise ConfigurationBlocked('stable_domain_not_configured')
    return path, domain


def service_command(service, settings):
    if service == 'api':
        return [str(ROOT/'.venv/bin/python'), '-B', '-u', '-m', 'uvicorn', 'mooby.api.app:app',
                '--host', '127.0.0.1', '--port', '8000', '--no-access-log', '--log-level', 'critical']
    path, domain = tunnel_configuration(settings)
    return [str(NGROK), 'http', 'http://127.0.0.1:8000', '--config', str(path),
            '--url', 'https://'+domain, '--inspect=false', '--log=false']


def supervise(command, env, service, log):
    stopping = False
    # Child streams are discarded, never parsed or copied to logs. Only fixed
    # supervisor events and numeric exit codes are retained.
    with subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL, start_new_session=True) as child:
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
            log({'event': 'service_started', 'service': service})
            code = child.wait()
        finally:
            if child.poll() is None:
                stop(signal.SIGTERM, None)
            for s, handler in previous.items():
                signal.signal(s, handler)
        log({'event': 'service_exited', 'service': service, 'exit_code': code})
        return 0 if stopping or code == 0 else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--service', choices=('api', 'tunnel'), required=True)
    parser.add_argument('--check-config', action='store_true', help='Local validation only; no child, HTTP or database')
    args = parser.parse_args()
    logdir = ROOT/'.local/services'
    logdir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger('mooby-development-'+args.service)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = RotatingFileHandler(logdir/(args.service+'.log'), maxBytes=1_000_000, backupCount=3)
    handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    logger.addHandler(handler)
    log = lambda record: logger.info(json.dumps(record))
    try:
        try:
            settings = read_settings()
            # Tunnel also checks API config first, but receives NONE of its credentials.
            api_env = api_environment(settings)
            command = service_command(args.service, settings)
            env = api_env if args.service == 'api' else base_environment()
        except Exception as exc:
            reason = exc.reason if isinstance(exc, ConfigurationBlocked) else 'invalid_local_configuration'
            record = {'event': 'configuration_blocked', 'service': args.service, 'reason': reason}
            log(record)
            if args.check_config:
                print(json.dumps(record))
                return 1
            return 0  # Fix manually, then kickstart; no configuration-error loop.
        if args.check_config:
            print(json.dumps({'event': 'configuration_valid', 'service': args.service}))
            return 0
        try:
            with single_instance(logdir/(args.service+'.lock')):
                return supervise(command, env, args.service, log)
        except AlreadyRunning:
            log({'event': 'duplicate_instance', 'service': args.service})
            return 0
        except Exception:
            log({'event': 'service_error', 'service': args.service})
            return 1
    finally:
        logger.removeHandler(handler)
        handler.close()


if __name__ == '__main__':
    raise SystemExit(main())
