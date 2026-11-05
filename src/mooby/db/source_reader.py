from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from psycopg import Connection


@dataclass(frozen=True)
class SourceSnapshot:
    snapshot_id: UUID
    raw_content_id: UUID
    capture_hash: str
    raw_text: str
    text_kind: str
    text_completeness: str
    published_at: datetime | None
    published_at_basis: str
    captured_at: datetime
    platform: str
    canonical_url: str


_LOAD_SNAPSHOT_SQL = """
SELECT
    s.id,
    s.raw_content_id,
    s.capture_hash,
    s.raw_text,
    s.text_kind,
    s.text_completeness,
    s.published_at,
    s.published_at_basis,
    s.captured_at,
    c.platform,
    c.canonical_url
FROM collection.raw_snapshots AS s
JOIN collection.raw_contents AS c
    ON c.id = s.raw_content_id
WHERE s.id = %s
"""


def database_clock_now(conn: Connection) -> datetime:
    """Return PostgreSQL's clock for eligibility decisions."""
    row = conn.execute("SELECT clock_timestamp()").fetchone()
    if row is None or not isinstance(row[0], datetime):
        raise RuntimeError("Database clock returned an invalid timestamp")
    return row[0]


def load_snapshot(
    conn: Connection,
    snapshot_id: UUID,
) -> SourceSnapshot | None:
    row = conn.execute(_LOAD_SNAPSHOT_SQL, (snapshot_id,)).fetchone()

    if row is None:
        return None

    return SourceSnapshot(*row)
