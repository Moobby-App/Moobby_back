import csv
import os
from collections import defaultdict
from datetime import date
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
REF = ROOT / "evaluation" / "reference"
FEED_SHA256 = "b7070274011d1be1da0919e1210f8728df454d0d5964276d79320df26e40ab17"
FEED_NAME = "Open-KRL pipeline v2"
PATH_VERSION = 1


def read_csv(name: str) -> list[dict[str, str]]:
    with (REF / name).open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def parse_date(value: str) -> date | None:
    value = value.strip()
    return date.fromisoformat(value) if value else None


paths = read_csv("path_candidates.csv")
stations = read_csv("path_candidate_stations.csv")
patterns = read_csv("route_pattern_review.csv")

if not paths or not stations:
    raise SystemExit("Path candidate CSV files must not be empty.")

path_by_id = {row["path_id"]: row for row in paths}
if len(path_by_id) != len(paths):
    raise SystemExit("Duplicate path_id in path_candidates.csv.")

pattern_ids = {row["pattern_id"] for row in patterns}
groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)

for row in stations:
    groups[(row["path_id"], row["direction"])].append(row)

if {row["path_id"] for row in stations} != set(path_by_id):
    raise SystemExit("Path IDs in the two path CSV files do not match.")

for key, rows in groups.items():
    seqs = sorted(int(row["seq"]) for row in rows)
    if seqs != list(range(1, len(rows) + 1)):
        raise SystemExit(f"Non-contiguous sequence numbers for {key}.")
    if key[0] not in path_by_id:
        raise SystemExit(f"Unknown path ID in station rows: {key[0]}")

for row in paths:
    source_ids = [item.strip() for item in row["source_pattern_ids"].split(";")]
    if not source_ids or any(item not in pattern_ids for item in source_ids):
        raise SystemExit(f"Unknown source pattern for {row['path_id']}.")
    if row["review_status"] not in {"pending", "confirmed", "rejected", "superseded"}:
        raise SystemExit(f"Invalid review_status for {row['path_id']}.")

for row in stations:
    if row["source_pattern_id"] not in pattern_ids:
        raise SystemExit(f"Unknown source pattern: {row['source_pattern_id']}.")

station_ids = sorted({row["station_id"] for row in stations})
db_url = os.environ.get("DATABASE_URL")
if not db_url:
    raise SystemExit("DATABASE_URL is not set.")

with psycopg.connect(db_url) as conn:
    with conn.transaction():
        ref_rows = conn.execute(
            """
            SELECT station_id, source_station_code, line_memberships, source_sha256
            FROM reference.krl_stations
            WHERE station_id = ANY (%s)
            """,
            (station_ids,),
        ).fetchall()
        reference = {row[0]: row[1:] for row in ref_rows}

        if set(reference) != set(station_ids):
            missing = sorted(set(station_ids) - set(reference))
            raise SystemExit(f"Unknown station_id values: {missing}")

        for row in stations:
            code, _, master_hash = reference[row["station_id"]]
            if code != row["source_station_code"]:
                raise SystemExit(
                    f"Crosswalk mismatch for {row['station_id']}: "
                    f"CSV={row['source_station_code']} DB={code}"
                )
            if master_hash != row["source_sha256"]:
                raise SystemExit(f"Station-master hash mismatch for {code}.")

        conn.execute("SET LOCAL ROLE mooby_nlp_owner_dev")

        line_rows = conn.execute(
            """
            SELECT line_id, reference_membership_name
            FROM nlp.transit_lines
            """
        ).fetchall()
        lines = {line_id: membership_name for line_id, membership_name in line_rows}

        for row in stations:
            code, memberships, _ = reference[row["station_id"]]
            line_id = path_by_id[row["path_id"]]["line_id"]
            if line_id not in lines:
                raise SystemExit(f"Unknown line_id: {line_id}")
            if lines[line_id] not in memberships:
                raise SystemExit(
                    f"{code} is not listed under line {lines[line_id]}."
                )

        for path in paths:
            path_id = path["path_id"]
            line_id = path["line_id"]
            path_station_rows = [
                row for row in stations if row["path_id"] == path_id
            ]
            master_hashes = {
                row["source_sha256"] for row in path_station_rows
            }
            if len(master_hashes) != 1:
                raise SystemExit(
                    f"Expected one station-master hash for {path_id}."
                )

            source_pattern_ids = [
                item.strip() for item in path["source_pattern_ids"].split(";")
            ]
            expected_path = (
                line_id,
                path["review_status"],
                FEED_NAME,
                FEED_SHA256,
                next(iter(master_hashes)),
                source_pattern_ids,
                None,
                None,
                path["evidence_note"],
            )

            conn.execute(
                """
                INSERT INTO nlp.line_paths (
                    path_id, path_version, line_id, review_status,
                    source_feed, source_feed_sha256, station_master_sha256,
                    source_pattern_ids, effective_since, effective_until,
                    evidence_note
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (path_id, path_version) DO NOTHING
                """,
                (path_id, PATH_VERSION, *expected_path),
            )

            stored_path = conn.execute(
                """
                SELECT line_id, review_status, source_feed,
                       source_feed_sha256, station_master_sha256,
                       source_pattern_ids, effective_since, effective_until,
                       evidence_note
                FROM nlp.line_paths
                WHERE path_id = %s AND path_version = %s
                """,
                (path_id, PATH_VERSION),
            ).fetchone()

            if stored_path is None or tuple(stored_path) != expected_path:
                raise SystemExit(
                    f"Existing path version differs from CSV: {path_id} v{PATH_VERSION}. "
                    "Do not overwrite it; review and create a new version."
                )

        for row in stations:
            conn.execute(
                """
                INSERT INTO nlp.line_path_stations (
                    path_id, path_version, direction, seq, station_id,
                    source_pattern_id, source_sha256,
                    effective_since, effective_until
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (path_id, path_version, direction, seq) DO NOTHING
                """,
                (
                    row["path_id"],
                    PATH_VERSION,
                    row["direction"],
                    int(row["seq"]),
                    row["station_id"],
                    row["source_pattern_id"],
                    row["source_sha256"],
                    parse_date(row["effective_since"]),
                    parse_date(row["effective_until"]),
                ),
            )

        path_ids = list(path_by_id)
        stored_stations = conn.execute(
            """
            SELECT path_id, direction, seq, station_id, source_pattern_id,
                   source_sha256, effective_since, effective_until
            FROM nlp.line_path_stations
            WHERE path_version = %s AND path_id = ANY (%s)
            """,
            (PATH_VERSION, path_ids),
        ).fetchall()

        expected_stations = {
            (
                row["path_id"],
                row["direction"],
                int(row["seq"]),
                row["station_id"],
                row["source_pattern_id"],
                row["source_sha256"],
                parse_date(row["effective_since"]),
                parse_date(row["effective_until"]),
            )
            for row in stations
        }

        if set(stored_stations) != expected_stations:
            raise SystemExit(
                "Stored station rows do not exactly match the CSV; "
                "the transaction will roll back."
            )

print(f"Validated and loaded {len(paths)} paths and {len(stations)} ordered station rows.")
