import os
from uuid import UUID

import psycopg
from mooby.db.source_reader import load_snapshot

snapshot_id = UUID("6fffa2e4-e6ea-4f43-a350-e9cd5176d2ef")
database_url = os.environ["DATABASE_URL"]

with psycopg.connect(database_url) as conn:
    snapshot = load_snapshot(conn, snapshot_id)

if snapshot is None:
    raise SystemExit("Snapshot not found")

print({
    "snapshot_id": str(snapshot.snapshot_id),
    "platform": snapshot.platform,
    "text_kind": snapshot.text_kind,
    "text_completeness": snapshot.text_completeness,
    "published_at": snapshot.published_at.isoformat() if snapshot.published_at else None,
    "raw_text_chars": len(snapshot.raw_text),
})