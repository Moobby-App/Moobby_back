"""Opt-in installation registration, separate from the read-only feed database."""
from dataclasses import dataclass, field
import hashlib
import os
import re
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator
import psycopg


class DeviceRegistration(BaseModel):
    model_config = ConfigDict(extra='forbid')
    device_token: str = Field(min_length=2, max_length=1024, repr=False)
    station_ids: list[str] = Field(min_length=1, max_length=30)
    severities: list[Literal['light', 'moderate', 'severe']] = Field(min_length=1, max_length=3)

    @field_validator('device_token')
    @classmethod
    def validate_token(cls, value):
        # APNs tokens are opaque, variable-length bytes represented as hex.
        if len(value) % 2 or re.fullmatch(r'[0-9a-fA-F]+', value) is None:
            raise ValueError('invalid APNs token encoding')
        return value.lower()

    @field_validator('station_ids')
    @classmethod
    def validate_stations(cls, values):
        if any(re.fullmatch(r'open-krl:[A-Z0-9]+', v) is None for v in values):
            raise ValueError('invalid station ID')
        return sorted(set(values))

    @field_validator('severities')
    @classmethod
    def deduplicate_severities(cls, values):
        return sorted(set(values))


@dataclass(frozen=True)
class PushRegistrationSettings:
    database_url: str = field(repr=False)
    environment: str
    topic: str

    @classmethod
    def from_env(cls):
        if os.environ.get('MOOBY_PUSH_REGISTRATION_ENABLED') != '1':
            raise ValueError('push registration disabled')
        dsn = os.environ.get('MOOBY_PUSH_API_DATABASE_URL', '').strip()
        environment = os.environ.get('MOOBY_APNS_ENVIRONMENT', '')
        topic = os.environ.get('MOOBY_APNS_TOPIC', '').strip()
        if (not dsn.startswith(('postgresql://', 'postgres://'))
                or environment not in {'sandbox', 'production'}
                or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.-]{1,254}', topic) is None):
            raise ValueError('push registration not configured')
        return cls(dsn, environment, topic)


def secret_digest(secret: str) -> str:
    if not 43 <= len(secret) <= 256 or re.fullmatch(r'[A-Za-z0-9_-]+', secret) is None:
        raise HTTPException(422, detail={'code':'invalid_installation_secret'})
    return hashlib.sha256(secret.encode()).hexdigest()


def device_router(require_auth):
    router = APIRouter(prefix='/api/v1/push/devices', dependencies=[Depends(require_auth)])

    def settings():
        try:
            return PushRegistrationSettings.from_env()
        except ValueError:
            raise HTTPException(503, detail={'code':'push_registration_unavailable'}) from None

    @router.put('/{installation_id}')
    def register(installation_id: UUID, body: DeviceRegistration,
                 installation_secret: Annotated[str, Header(alias='X-Installation-Secret')]):
        config = settings()
        digest = secret_digest(installation_secret)
        try:
            with psycopg.connect(config.database_url, connect_timeout=3,
                                 options='-c statement_timeout=3000') as conn:
                row = conn.execute('''
                    INSERT INTO notifications.devices
                        (installation_id, secret_digest, device_token, environment, topic,
                         station_ids, severities)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (installation_id) DO UPDATE SET
                        device_token=EXCLUDED.device_token, environment=EXCLUDED.environment,
                        topic=EXCLUDED.topic, station_ids=EXCLUDED.station_ids,
                        severities=EXCLUDED.severities, disabled_at=NULL,
                        updated_at=transaction_timestamp()
                    WHERE notifications.devices.secret_digest=EXCLUDED.secret_digest
                    RETURNING installation_id
                ''', (installation_id,digest,body.device_token,config.environment,
                      config.topic,body.station_ids,body.severities)).fetchone()
        except psycopg.errors.UniqueViolation:
            raise HTTPException(409, detail={'code':'device_registration_conflict'}) from None
        except psycopg.Error:
            raise HTTPException(503, detail={'code':'push_registration_unavailable'}) from None
        if row is None:
            raise HTTPException(403, detail={'code':'installation_not_authorized'})
        return {'installation_id':str(installation_id), 'status':'registered'}

    @router.delete('/{installation_id}')
    def unregister(installation_id: UUID,
                   installation_secret: Annotated[str, Header(alias='X-Installation-Secret')]):
        config = settings()
        digest = secret_digest(installation_secret)
        try:
            with psycopg.connect(config.database_url, connect_timeout=3,
                                 options='-c statement_timeout=3000') as conn:
                row = conn.execute('''
                    UPDATE notifications.devices SET disabled_at=transaction_timestamp(),
                        updated_at=transaction_timestamp()
                    WHERE installation_id=%s AND secret_digest=%s RETURNING installation_id
                ''', (installation_id,digest)).fetchone()
        except psycopg.Error:
            raise HTTPException(503, detail={'code':'push_registration_unavailable'}) from None
        if row is None:
            raise HTTPException(403, detail={'code':'installation_not_authorized'})
        return {'installation_id':str(installation_id), 'status':'disabled'}

    return router
