"""Offline APNs readiness: no database access, JWT generation or push requests."""
import argparse
import json
from pathlib import Path
import stat
import sys
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))


def check(values):
    from mooby.worker.apns import APNsSettings
    required = ('MOOBY_APNS_TEAM_ID', 'MOOBY_APNS_KEY_ID', 'MOOBY_APNS_TOPIC',
                'MOOBY_APNS_ENVIRONMENT', 'MOOBY_APNS_KEY_PATH',
                'MOOBY_PUSH_API_DATABASE_URL', 'MOOBY_PUSH_SENDER_DATABASE_URL')
    missing = [name for name in required if not values.get(name)]
    report = {'missing_settings': missing, 'apple_config_valid': False,
              'private_key_valid': False, 'private_key_permissions_restricted': False,
              'dedicated_database_settings_present': False,
              'database_checked': False, 'device_registration_checked': False,
              'push_sent': False}
    if missing:
        return report
    try:
        config = APNsSettings.from_mapping(values, require_enabled=False)
        report['apple_config_valid'] = True
        report['private_key_permissions_restricted'] = (
            stat.S_IMODE(config.key_path.stat().st_mode) & 0o077 == 0)
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePrivateKey, SECP256R1
        key = load_pem_private_key(config.key_path.read_bytes(), password=None)
        report['private_key_valid'] = isinstance(key, EllipticCurvePrivateKey) and isinstance(key.curve, SECP256R1)
        forbidden = ('postgres', 'mooby_nlp_worker_login_dev', 'mooby_app_api_login_dev')
        urls = [urlsplit(values[name]) for name in
                ('MOOBY_PUSH_API_DATABASE_URL', 'MOOBY_PUSH_SENDER_DATABASE_URL')]
        report['dedicated_database_settings_present'] = all(
            u.scheme in {'postgres', 'postgresql'} and u.hostname and u.username
            and u.username.split('.')[0] not in forbidden for u in urls)
    except Exception:
        # Do not print key contents, filenames, secrets or exception messages.
        pass
    return report


def main():
    from dotenv import dotenv_values
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path,
                        default=Path(__file__).resolve().parents[1] / '.env')
    args = parser.parse_args()
    values = dotenv_values(args.env_file, interpolate=False)
    report = check(values)
    ready = all(report[name] for name in ('apple_config_valid', 'private_key_valid',
                'private_key_permissions_restricted', 'dedicated_database_settings_present'))
    print(json.dumps({'offline_configuration': 'PASS' if ready else 'INCOMPLETE', **report}))
    return 0 if ready else 1


if __name__ == '__main__':
    raise SystemExit(main())
