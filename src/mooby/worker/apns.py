"""APNs transport and payload preparation. Importing this module never sends.

Only the explicit sender command creates a client. Credentials stay server-side.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re
import time
from uuid import UUID


@dataclass(frozen=True)
class APNsSettings:
    team_id: str
    key_id: str
    topic: str
    environment: str
    key_path: Path = field(repr=False)

    @classmethod
    def from_env(cls):
        if os.environ.get('MOOBY_APNS_ENABLED') != '1':
            raise ValueError('APNs sender disabled')
        team = os.environ.get('MOOBY_APNS_TEAM_ID','')
        key = os.environ.get('MOOBY_APNS_KEY_ID','')
        topic = os.environ.get('MOOBY_APNS_TOPIC','')
        env = os.environ.get('MOOBY_APNS_ENVIRONMENT','')
        path = Path(os.environ.get('MOOBY_APNS_KEY_PATH','')).expanduser()
        if (not re.fullmatch(r'[A-Z0-9]{10}',team) or not re.fullmatch(r'[A-Z0-9]{10}',key)
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.-]{1,254}',topic)
                or env not in {'sandbox','production'} or not path.is_absolute() or not path.is_file()):
            raise ValueError('APNs sender not configured')
        return cls(team,key,topic,env,path)

    @property
    def host(self):
        return 'https://api.sandbox.push.apple.com' if self.environment=='sandbox' else 'https://api.push.apple.com'


def notification_payload(*, event_id: UUID, severity: str, title: str,
                         summary: str, expires_at: datetime) -> bytes:
    if severity not in {'light','moderate','severe'} or expires_at.tzinfo is None:
        raise ValueError('invalid push event')
    payload = {
        'aps': {'alert':{'title':title[:100], 'body':summary[:300]},
                'sound':'default', 'thread-id':'moobby-disruptions'},
        'schema_version':1, 'type':'disruption_published', 'event_id':str(event_id),
        'severity':severity, 'expires_at':expires_at.astimezone(timezone.utc).isoformat(),
    }
    encoded = json.dumps(payload,ensure_ascii=False,separators=(',',':')).encode()
    if len(encoded)>4096:
        raise ValueError('APNs payload too large')
    return encoded


@dataclass(frozen=True)
class SendResult:
    status: str  # accepted, retry, invalid_device, rejected, expired
    retry_after_seconds: int = 0
    invalid_since: float | None = None


class APNsClient:
    def __init__(self, settings: APNsSettings):
        import httpx
        # HTTPX INFO logging includes request paths, which contain device tokens.
        logging.getLogger('httpx').setLevel(logging.WARNING)
        logging.getLogger('httpcore').setLevel(logging.WARNING)
        self.settings = settings
        self.client = httpx.Client(http2=True,timeout=httpx.Timeout(10,connect=3),
                                   limits=httpx.Limits(max_connections=4),trust_env=False)
        self._token = None
        self._issued_at = 0

    def _authorization(self):
        now = int(time.time())
        if self._token is None or not 0 <= now-self._issued_at < 3000:
            import jwt
            self._token = jwt.encode({'iss':self.settings.team_id,'iat':now},
                                     self.settings.key_path.read_bytes(),algorithm='ES256',
                                     headers={'kid':self.settings.key_id})
            self._issued_at = now
        return 'bearer ' + self._token

    def send(self, *, device_token: str, event_id: UUID, notification_id: UUID,
             expires_at: datetime, payload: bytes) -> SendResult:
        if expires_at.tzinfo is None:
            raise ValueError('timezone required')
        if expires_at.timestamp() <= time.time():
            return SendResult('expired')
        if (not re.fullmatch(r'[0-9a-fA-F]+',device_token) or len(device_token)%2
                or not 2<=len(device_token)<=1024 or len(payload)>4096):
            raise ValueError('invalid APNs request')
        import httpx
        try:
            response = self.client.post(
                self.settings.host+'/3/device/'+device_token,
                content=payload, headers={
                    'authorization':self._authorization(), 'apns-topic':self.settings.topic,
                    'apns-push-type':'alert', 'apns-priority':'10',
                    'apns-expiration':str(int(expires_at.timestamp())),
                    'apns-collapse-id':str(event_id), 'apns-id':str(notification_id),
                    'content-type':'application/json',
                })
        except httpx.TransportError:
            return SendResult('retry',30)
        if response.status_code==200:
            return SendResult('accepted')
        try:
            body=response.json()
            reason=body.get('reason','')
        except (ValueError,AttributeError):
            body={}; reason=''
        if response.status_code==410 and reason=='Unregistered':
            timestamp=body.get('timestamp')
            return SendResult('invalid_device', invalid_since=timestamp/1000
                              if isinstance(timestamp,(int,float)) else None)
        if response.status_code==400 and reason in {'BadDeviceToken','DeviceTokenNotForTopic'}:
            return SendResult('invalid_device')
        if response.status_code==429 or response.status_code>=500:
            try:
                delay=max(1,min(3600,int(response.headers.get('retry-after','30'))))
            except ValueError:
                delay=30
            return SendResult('retry',delay)
        return SendResult('rejected')

    def close(self):
        self.client.close()
