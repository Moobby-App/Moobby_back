"""Explicit, bounded notification dispatch; no automatic scheduler or service."""
from datetime import datetime, timezone
import json
from uuid import UUID, uuid4, uuid5

from mooby.api.publication_policy import TTL_SQL
from mooby.worker.apns import notification_payload


def expire_deliveries(conn, *, environment: str, topic: str):
    """Bounded cleanup of expired or exhausted abandoned leases."""
    conn.execute('''
        WITH obsolete AS (
            SELECT notification_id FROM notifications.deliveries
            WHERE environment=%s AND topic=%s AND status IN ('pending','sending')
              AND (lease_until IS NULL OR lease_until<=transaction_timestamp())
              AND (expires_at<=transaction_timestamp() OR attempts>=5)
            ORDER BY next_attempt_at LIMIT 100 FOR UPDATE SKIP LOCKED
        )
        UPDATE notifications.deliveries q SET status=CASE
            WHEN q.expires_at<=transaction_timestamp() THEN 'expired' ELSE 'rejected' END,
            claim_token=NULL,lease_until=NULL
        FROM obsolete o WHERE o.notification_id=q.notification_id
    ''', (environment,topic))


def fan_out(conn, *, environment: str, topic: str, batch_size: int = 25) -> int:
    """Caller transaction locks outbox rows and commits delivery inserts atomically.

    Only installations already registered when the event was published qualify.
    An outbox row is fan-out once per APNs environment/topic; see deployment doc.
    """
    if not 1 <= batch_size <= 100:
        raise ValueError('invalid batch size')
    # One configured environment/topic per deployment. Keep pending outbox rows
    # when no eligible source projection is visible yet (source insert/retry).
    rows = conn.execute(f'''
        SELECT o.event_id, v.title, v.summary, v.severity, v.affected_station_ids,
               v.freshness_anchor_at + ({TTL_SQL}) AS expires_at, o.created_at
        FROM notifications.outbox o JOIN app_api.published_disruptions v
            ON v.public_event_id=o.event_id
        WHERE o.fanned_out_at IS NULL AND v.severity IN ('light','moderate','severe')
          AND v.freshness_anchor_at <= transaction_timestamp()
          AND v.freshness_anchor_at + ({TTL_SQL}) > transaction_timestamp()
        ORDER BY o.created_at,o.event_id LIMIT %s FOR UPDATE OF o SKIP LOCKED
    ''', (batch_size,)).fetchall()
    for row in rows:
        payload = notification_payload(event_id=row['event_id'],severity=row['severity'],
            title=row['title'],summary=row['summary'],expires_at=row['expires_at'])
        devices = conn.execute('''
            SELECT installation_id FROM notifications.devices
            WHERE disabled_at IS NULL AND environment=%s AND topic=%s
              AND created_at<=%s AND station_ids && %s::text[]
              AND %s=ANY(severities)
        ''', (environment,topic,row['created_at'],row['affected_station_ids'],row['severity'])).fetchall()
        for device in devices:
            notification_id=uuid5(row['event_id'],str(device['installation_id']))
            conn.execute('''
                INSERT INTO notifications.deliveries
                    (notification_id,event_id,installation_id,environment,topic,expires_at,payload)
                VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb)
                ON CONFLICT(event_id,installation_id) DO NOTHING
            ''', (notification_id,row['event_id'],device['installation_id'],environment,
                  topic,row['expires_at'],payload.decode()))
        conn.execute('UPDATE notifications.outbox SET fanned_out_at=transaction_timestamp() WHERE event_id=%s',
                     (row['event_id'],))
    return len(rows)


def claim(conn, *, environment: str, topic: str):
    """Acquire one delivery, release DB transaction before the APNs network call."""
    token=uuid4()
    return conn.execute(f'''
        WITH candidate AS (
            SELECT q.notification_id FROM notifications.deliveries q
            JOIN notifications.devices d ON d.installation_id=q.installation_id
            WHERE q.environment=%s AND q.topic=%s
              AND q.status IN ('pending','sending') AND q.attempts<5
              AND q.next_attempt_at<=transaction_timestamp()
              AND (q.lease_until IS NULL OR q.lease_until<=transaction_timestamp())
              AND q.expires_at>transaction_timestamp() AND d.disabled_at IS NULL
              AND d.environment=q.environment AND d.topic=q.topic
              AND (q.payload->>'severity')=ANY(d.severities)
              AND EXISTS (SELECT 1 FROM app_api.published_disruptions v
                  WHERE v.public_event_id=q.event_id AND v.affected_station_ids && d.station_ids
                    AND v.freshness_anchor_at + ({TTL_SQL}) > transaction_timestamp())
            ORDER BY q.next_attempt_at,q.notification_id LIMIT 1 FOR UPDATE OF q SKIP LOCKED
        )
        UPDATE notifications.deliveries q SET status='sending', attempts=attempts+1,
            lease_until=transaction_timestamp()+interval '60 seconds', claim_token=%s
        FROM candidate c, notifications.devices d
        WHERE q.notification_id=c.notification_id AND d.installation_id=q.installation_id
        RETURNING q.*,d.device_token,d.updated_at AS device_updated_at
    ''', (environment,topic,token)).fetchone()


def finish(conn, delivery, result):
    status=result.status
    delay=result.retry_after_seconds
    if status=='retry':
        status='pending' if delivery['attempts']<5 else 'rejected'
        delay=max(delay,min(300,2**delivery['attempts']))
    row=conn.execute('''
        UPDATE notifications.deliveries SET status=%s,
            next_attempt_at=transaction_timestamp()+(%s * interval '1 second'),
            lease_until=NULL,claim_token=NULL,
            accepted_at=CASE WHEN %s='accepted' THEN transaction_timestamp() ELSE accepted_at END
        WHERE notification_id=%s AND claim_token=%s AND status='sending'
        RETURNING notification_id
    ''', (status,delay,status,delivery['notification_id'],delivery['claim_token'])).fetchone()
    if row and status=='invalid_device':
        # Do not revoke a token refreshed after this attempt or Apple's invalidation.
        invalid_before = (datetime.fromtimestamp(result.invalid_since,timezone.utc)
                          if result.invalid_since is not None else delivery['device_updated_at'])
        conn.execute('''UPDATE notifications.devices SET disabled_at=transaction_timestamp()
            WHERE installation_id=%s AND device_token=%s AND updated_at<=%s
              AND updated_at<=%s''', (delivery['installation_id'],delivery['device_token'],
                                    delivery['device_updated_at'],invalid_before))


def dispatch_one(connect, client):
    """Network is outside all DB transactions; `accepted` means APNs acceptance."""
    with connect() as conn:
        delivery=claim(conn,environment=client.settings.environment,topic=client.settings.topic)
    if delivery is None:
        return 'idle'
    payload=delivery['payload']
    encoded=json.dumps(payload,ensure_ascii=False,separators=(',',':')).encode()
    result=client.send(device_token=delivery['device_token'],event_id=delivery['event_id'],
        notification_id=delivery['notification_id'],expires_at=delivery['expires_at'],payload=encoded)
    with connect() as conn:
        finish(conn,delivery,result)
    return result.status
