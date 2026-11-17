"""APNs preparation tests: in-memory HTTP/ASGI/SQL doubles, never sends a push."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
import tempfile
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import httpx
from fastapi.testclient import TestClient
from pydantic import ValidationError

from mooby.api.app import create_app
from mooby.api.push_devices import DeviceRegistration, secret_digest
from mooby.worker.apns import APNsSettings, APNsClient, SendResult, notification_payload
from mooby.worker.push_sender import fan_out, finish, dispatch_one
from test_request_observability import ENV, TOKEN


class DeviceApiTests(unittest.TestCase):
    def setUp(self):
        self.env=patch.dict('os.environ',{**ENV,'MOOBY_PUSH_REGISTRATION_ENABLED':'1',
            'MOOBY_PUSH_API_DATABASE_URL':'postgresql://NOT_USED',
            'MOOBY_APNS_ENVIRONMENT':'sandbox','MOOBY_APNS_TOPIC':'id.moobby.app'},clear=True)
        self.env.start(); self.addCleanup(self.env.stop)
        self.connect=patch('psycopg.connect',side_effect=AssertionError('DB forbidden'))
        self.network=self.connect.start(); self.addCleanup(self.connect.stop)
        self.client=TestClient(create_app()); self.addCleanup(self.client.close)
        self.installation=uuid4()
        self.url=f'/api/v1/push/devices/{self.installation}'
        self.headers={'Authorization':'Bearer '+TOKEN,'X-Installation-Secret':'x'*43}
        self.body={'device_token':'ab'*32,'station_ids':['open-krl:SRP'],
                   'severities':['moderate','severe']}

    def test_existing_bearer_auth_is_required_before_database(self):
        response=self.client.put(self.url,json=self.body)
        self.assertEqual(response.status_code,401)
        self.network.assert_not_called()

    def test_disabled_registration_does_not_access_database(self):
        with patch.dict('os.environ',{'MOOBY_PUSH_REGISTRATION_ENABLED':'0'}):
            response=self.client.put(self.url,headers=self.headers,json=self.body)
        self.assertEqual(response.status_code,503)
        self.network.assert_not_called()

    def test_invalid_input_or_missing_secret_rejected(self):
        for changes in ({'device_token':'not hex'},{'station_ids':['bad']},
                        {'severities':['extreme']}):
            self.assertEqual(self.client.put(self.url,headers=self.headers,
                json={**self.body,**changes}).status_code,422)
        headers={'Authorization':'Bearer '+TOKEN}
        self.assertEqual(self.client.put(self.url,headers=headers,json=self.body).status_code,422)
        self.network.assert_not_called()

    def mock_connection(self, row):
        conn=MagicMock()
        conn.__enter__.return_value=conn
        conn.execute.return_value.fetchone.return_value=row
        self.network.side_effect=None; self.network.return_value=conn
        return conn

    def test_registration_binds_secret_hash_and_server_environment(self):
        conn=self.mock_connection((self.installation,))
        response=self.client.put(self.url,headers=self.headers,json=self.body)
        self.assertEqual(response.status_code,200)
        sql,params=conn.execute.call_args.args
        self.assertIn('secret_digest=EXCLUDED.secret_digest',sql)
        self.assertEqual(params[1],hashlib.sha256(('x'*43).encode()).hexdigest())
        self.assertEqual(params[3:5],('sandbox','id.moobby.app'))
        self.assertNotIn(self.body['device_token'],response.text)

    def test_wrong_secret_cannot_replace_existing_registration(self):
        self.mock_connection(None)
        self.assertEqual(self.client.put(self.url,headers=self.headers,json=self.body).status_code,403)

    def test_disable_registration_requires_installation_secret(self):
        conn=self.mock_connection((self.installation,))
        self.assertEqual(self.client.delete(self.url,headers=self.headers).status_code,200)
        self.assertIn('installation_id=%s AND secret_digest=%s',conn.execute.call_args.args[0])

    def test_short_installation_secret_rejected_before_db(self):
        self.headers['X-Installation-Secret']='short'
        self.assertEqual(self.client.put(self.url,headers=self.headers,json=self.body).status_code,422)
        self.network.assert_not_called()


class APNsTransportTests(unittest.TestCase):
    def setUp(self):
        self.event=uuid4(); self.notification=uuid4()
        self.expiry=datetime.now(timezone.utc)+timedelta(minutes=30)
        self.payload=notification_payload(event_id=self.event,severity='light',
            title='Gangguan KRL',summary='Perjalanan tertahan',expires_at=self.expiry)
        self.requests=[]

    def client(self, status=200, body=None, headers=None):
        obj=APNsClient.__new__(APNsClient)
        obj.settings=APNsSettings('A'*10,'B'*10,'id.moobby.app','sandbox',Path('/unused.p8'))
        def respond(request):
            self.requests.append(request)
            return httpx.Response(status,json=body or {},headers=headers or {})
        obj.client=httpx.Client(transport=httpx.MockTransport(respond),http2=True)
        obj._authorization=lambda:'bearer SYNTHETIC'
        self.addCleanup(obj.close)
        return obj

    def send(self,client,expiry=None):
        return client.send(device_token='ab'*32,event_id=self.event,notification_id=self.notification,
                           expires_at=expiry or self.expiry,payload=self.payload)

    def test_payload_and_expiry_headers_use_event_identity(self):
        self.assertEqual(self.send(self.client()).status,'accepted')
        request=self.requests[0]
        self.assertEqual(request.url.host,'api.sandbox.push.apple.com')
        self.assertEqual(request.headers['apns-expiration'],str(int(self.expiry.timestamp())))
        self.assertEqual(request.headers['apns-collapse-id'],str(self.event))
        self.assertEqual(request.headers['apns-push-type'],'alert')
        self.assertEqual(request.headers['apns-id'],str(self.notification))
        self.assertNotIn('original_text',json.loads(request.content))

    def test_expired_push_is_never_sent(self):
        self.assertEqual(self.send(self.client(),datetime.now(timezone.utc)-timedelta(seconds=1)).status,'expired')
        self.assertEqual(self.requests,[])

    def test_retryable_and_terminal_apple_responses(self):
        self.assertEqual(self.send(self.client(429,headers={'retry-after':'60'})),SendResult('retry',60))
        self.assertEqual(self.send(self.client(503)).status,'retry')
        self.assertEqual(self.send(self.client(403,{'reason':'InvalidProviderToken'})).status,'rejected')
        self.assertEqual(self.send(self.client(400,{'reason':'BadDeviceToken'})).status,'invalid_device')
        self.assertEqual(self.send(self.client(410,{'reason':'Unregistered','timestamp':100000})),
                         SendResult('invalid_device',invalid_since=100))

    def test_transport_failure_is_sanitized_retry(self):
        client=self.client()
        client.client.post=MagicMock(side_effect=httpx.ConnectError('PRIVATE_TOKEN'))
        result=self.send(client)
        self.assertEqual(result,SendResult('retry',30))
        self.assertNotIn('PRIVATE',repr(result))

    def test_payload_bounds_and_no_missing_timezone(self):
        payload=notification_payload(event_id=self.event,severity='severe',title='🚨'*1000,
                                     summary='🚨'*10000,expires_at=self.expiry)
        self.assertLessEqual(len(payload),4096)
        with self.assertRaises(ValueError):
            notification_payload(event_id=self.event,severity='light',title='x',summary='x',
                                 expires_at=datetime(2026,1,1))

    def test_sender_disabled_without_configuration(self):
        with patch.dict('os.environ',{},clear=True):
            with self.assertRaises(ValueError): APNsSettings.from_env()


class OutboxTests(unittest.TestCase):
    def test_dispatch_network_happens_after_claim_transaction_closes(self):
        client=MagicMock()
        client.settings.environment='sandbox'; client.settings.topic='id.moobby.app'
        row={'device_token':'ab'*32,'event_id':uuid4(),'notification_id':uuid4(),
             'expires_at':datetime.now(timezone.utc)+timedelta(minutes=30),'payload':{'aps':{}}}
        conn=MagicMock(); conn.__enter__.return_value=conn
        closed=[]
        conn.__exit__.side_effect=lambda *args:closed.append(True)
        client.send.side_effect=lambda **kwargs: (self.assertEqual(len(closed),1) or SendResult('accepted'))
        with patch('mooby.worker.push_sender.claim',return_value=row), \
             patch('mooby.worker.push_sender.finish') as finish_call:
            self.assertEqual(dispatch_one(lambda:conn,client),'accepted')
        finish_call.assert_called_once()
        self.assertEqual(len(closed),2)

    def test_fanout_uses_active_projection_and_idempotent_event_device_identity(self):
        event=uuid4(); installation=uuid4()
        row={'event_id':event,'title':'KRL','summary':'Gangguan','severity':'light',
             'affected_station_ids':['open-krl:SRP'],'created_at':datetime.now(timezone.utc),
             'expires_at':datetime.now(timezone.utc)+timedelta(minutes=30)}
        conn=MagicMock()
        conn.execute.side_effect=[SimpleNamespace(fetchall=lambda:[row]),
            SimpleNamespace(fetchall=lambda:[{'installation_id':installation}]),None,None]
        self.assertEqual(fan_out(conn,environment='sandbox',topic='id.moobby.app'),1)
        self.assertIn('FOR UPDATE OF o SKIP LOCKED',conn.execute.call_args_list[0].args[0])
        self.assertIn('station_ids && %s::text[]',conn.execute.call_args_list[1].args[0])
        self.assertIn('ON CONFLICT(event_id,installation_id) DO NOTHING',conn.execute.call_args_list[2].args[0])

    def test_stale_claim_cannot_disable_new_device_token(self):
        conn=MagicMock(); conn.execute.return_value.fetchone.return_value=None
        row={'notification_id':uuid4(),'claim_token':uuid4(),'attempts':1}
        finish(conn,row,SendResult('invalid_device'))
        self.assertEqual(conn.execute.call_count,1)
        self.assertIn('claim_token=%s',conn.execute.call_args.args[0])

    def test_publication_trigger_is_atomic_and_has_no_backfill(self):
        sql=(Path(__file__).resolve().parents[1]/'migrations/019_apns_notification_outbox.sql').read_text()
        self.assertIn('SECURITY DEFINER SET search_path=pg_catalog',sql)
        self.assertIn('ON CONFLICT(event_id) DO NOTHING',sql)
        self.assertIn('AFTER INSERT OR UPDATE OF verification_status,publication_status',sql)
        self.assertNotIn('INSERT INTO notifications.outbox(event_id,initial_event_version)\nSELECT',sql)


class OfflinePushReadinessTests(unittest.TestCase):
    def test_missing_settings_never_contact_database_or_apns(self):
        from scripts.check_push_setup import check
        with patch('psycopg.connect', side_effect=AssertionError('DB forbidden')), \
             patch.object(APNsClient, 'send', side_effect=AssertionError('push forbidden')):
            report = check({})
        self.assertIn('MOOBY_APNS_TEAM_ID', report['missing_settings'])
        self.assertFalse(report['push_sent'])

    def test_private_p256_key_and_dedicated_dsn_preflight_without_sending(self):
        from scripts.check_push_setup import check
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'test.p8'
            path.write_bytes(ec.generate_private_key(ec.SECP256R1()).private_bytes(
                Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()))
            path.chmod(0o600)
            values = {'MOOBY_APNS_TEAM_ID':'A'*10, 'MOOBY_APNS_KEY_ID':'B'*10,
                'MOOBY_APNS_TOPIC':'id.test.app', 'MOOBY_APNS_ENVIRONMENT':'sandbox',
                'MOOBY_APNS_KEY_PATH':str(path),
                'MOOBY_PUSH_API_DATABASE_URL':'postgresql://push_api@localhost/db',
                'MOOBY_PUSH_SENDER_DATABASE_URL':'postgresql://push_sender@localhost/db'}
            report = check(values)
            self.assertTrue(report['private_key_valid'])
            self.assertTrue(report['private_key_permissions_restricted'])
            self.assertTrue(report['dedicated_database_settings_present'])
            self.assertFalse(report['push_sent'])
            values['MOOBY_PUSH_API_DATABASE_URL'] = 'postgresql://postgres@localhost/db'
            self.assertFalse(check(values)['dedicated_database_settings_present'])
