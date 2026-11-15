"""Explicit one-shot APNs sender. Disabled until all required settings are supplied."""
import json
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file',type=Path,help='Explicit private dotenv file; no shell expansion')
    args=parser.parse_args()
    if args.env_file is not None:
        from dotenv import dotenv_values
        values=dotenv_values(args.env_file,interpolate=False)
        keys=('MOOBY_APNS_ENABLED','MOOBY_APNS_ENVIRONMENT','MOOBY_APNS_TOPIC',
              'MOOBY_APNS_TEAM_ID','MOOBY_APNS_KEY_ID','MOOBY_APNS_KEY_PATH',
              'MOOBY_PUSH_SENDER_DATABASE_URL')
        # Explicit file is authoritative; inherited values cannot fill missing keys.
        for key in keys:
            os.environ.pop(key,None)
            if values.get(key) is not None:
                os.environ[key]=values[key]
    # Never use API/worker/admin DSNs as a fallback for the dedicated sender DSN.
    from mooby.worker.apns import APNsSettings, APNsClient
    try:
        settings=APNsSettings.from_env()
        dsn=os.environ.get('MOOBY_PUSH_SENDER_DATABASE_URL','')
        if not dsn.startswith(('postgresql://','postgres://')):
            raise ValueError('missing sender database')
    except ValueError:
        print(json.dumps({'status':'disabled_or_unconfigured'}))
        return 1
    import psycopg
    from psycopg.rows import dict_row
    from mooby.worker.push_sender import fan_out, dispatch_one, expire_deliveries
    connect=lambda:psycopg.connect(dsn,connect_timeout=3,row_factory=dict_row,
                                  options='-c statement_timeout=3000')
    client=None
    try:
        client=APNsClient(settings)
        with connect() as conn:
            expire_deliveries(conn,environment=settings.environment,topic=settings.topic)
            fan_out(conn,environment=settings.environment,topic=settings.topic)
        counts={}
        for _ in range(100):
            status=dispatch_one(connect,client)
            counts[status]=counts.get(status,0)+1
            if status=='idle':
                break
        print(json.dumps({'status':'completed','counts':counts}))
        return 0
    except Exception:
        # Never print token, DSN, HTTP request URL, JWT, private key or SQL values.
        print(json.dumps({'status':'sender_failed'}))
        return 1
    finally:
        if client is not None:
            client.close()


if __name__=='__main__':
    raise SystemExit(main())
