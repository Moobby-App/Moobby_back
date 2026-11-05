from dataclasses import dataclass
from datetime import datetime
from collections.abc import Mapping
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


@dataclass(frozen=True)
class StationReference:
    station_id: str
    canonical_name: str
    aliases: tuple[str, ...]


class StationCatalogUnavailable(RuntimeError):
    """The read-only station catalog could not be loaded or validated."""


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

_LOAD_STATION_CATALOG_SQL = """
SELECT station_id, canonical_name, aliases
FROM reference.krl_stations
ORDER BY station_id
"""


def database_clock_now(conn: Connection) -> datetime:
    """Return PostgreSQL's clock for eligibility decisions."""
    row = conn.execute("SELECT clock_timestamp()").fetchone()
    if row is None:
        raise RuntimeError("Database clock returned an invalid timestamp")
    value = row["clock_timestamp"] if isinstance(row, Mapping) else row[0]
    if not isinstance(value, datetime):
        raise RuntimeError("Database clock returned an invalid timestamp")
    return value


def load_station_catalog(conn: Connection) -> list[StationReference]:
    """Read only the canonical names and aliases needed for text matching."""
    try:
        # Isolate a permission/query failure so the caller can continue without
        # station resolution and still finish the processing attempt safely.
        with conn.transaction():
            cursor = conn.execute(_LOAD_STATION_CATALOG_SQL)
            rows = cursor.fetchall()
            columns = [column.name for column in cursor.description]
    except Exception as exc:
        raise StationCatalogUnavailable(type(exc).__name__) from exc

    catalog: list[StationReference] = []
    for row in rows:
        values = dict(row) if isinstance(row, Mapping) else dict(zip(columns, row, strict=True))
        station_id = values.get("station_id")
        canonical_name = values.get("canonical_name")
        aliases = values.get("aliases")
        if not isinstance(station_id, str) or not station_id.strip():
            raise StationCatalogUnavailable("invalid station_id")
        if not isinstance(canonical_name, str) or not canonical_name.strip():
            raise StationCatalogUnavailable("invalid canonical_name")
        if aliases is None:
            alias_values: tuple[str, ...] = ()
        elif isinstance(aliases, (list, tuple)) and all(
            isinstance(alias, str) for alias in aliases
        ):
            alias_values = tuple(aliases)
        else:
            raise StationCatalogUnavailable("invalid aliases")
        catalog.append(StationReference(station_id, canonical_name, alias_values))
    return catalog


def load_snapshot(
    conn: Connection,
    snapshot_id: UUID,
) -> SourceSnapshot | None:
    row = conn.execute(_LOAD_SNAPSHOT_SQL, (snapshot_id,)).fetchone()

    if row is None:
        return None

    if isinstance(row, Mapping):
        return SourceSnapshot(
            snapshot_id=row["id"],
            raw_content_id=row["raw_content_id"],
            capture_hash=row["capture_hash"],
            raw_text=row["raw_text"],
            text_kind=row["text_kind"],
            text_completeness=row["text_completeness"],
            published_at=row["published_at"],
            published_at_basis=row["published_at_basis"],
            captured_at=row["captured_at"],
            platform=row["platform"],
            canonical_url=row["canonical_url"],
        )
    return SourceSnapshot(*row)
