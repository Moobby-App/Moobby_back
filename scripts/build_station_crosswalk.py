import csv
import json
import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


stops_path = Path("../open-krl-pipeline/data/build/gtfs-v2/stops.txt")
output_path = Path("evaluation/reference/gtfs_stop_crosswalk.csv")

if not stops_path.exists():
    raise SystemExit(f"GTFS stops.txt tidak ditemukan: {stops_path}")

with psycopg.connect(
    os.environ["DATABASE_URL"],
    row_factory=dict_row,
) as conn:
    references = conn.execute(
        """
        SELECT station_id, source_station_code, canonical_name, line_memberships
        FROM reference.krl_stations
        """
    ).fetchall()

reference_by_code = {
    row["source_station_code"]: row
    for row in references
}

output_path.parent.mkdir(parents=True, exist_ok=True)
matched = 0
unmatched = 0

with stops_path.open(newline="", encoding="utf-8") as source, output_path.open(
    "w", newline="", encoding="utf-8"
) as destination:
    reader = csv.DictReader(source)
    fields = [
        "stop_id",
        "stop_name",
        "source_station_code",
        "station_id",
        "canonical_name",
        "line_memberships",
        "mapping_status",
    ]
    writer = csv.DictWriter(destination, fieldnames=fields)
    writer.writeheader()

    for stop in reader:
        ref = reference_by_code.get(stop["stop_id"])

        if ref:
            matched += 1
        else:
            unmatched += 1

        writer.writerow({
            "stop_id": stop["stop_id"],
            "stop_name": stop["stop_name"],
            "source_station_code": ref["source_station_code"] if ref else "",
            "station_id": ref["station_id"] if ref else "",
            "canonical_name": ref["canonical_name"] if ref else "",
            "line_memberships": json.dumps(ref["line_memberships"]) if ref else "",
            "mapping_status": "mapped_exact" if ref else "review_unmapped",
        })

print(f"GTFS stops: {matched + unmatched}")
print(f"Cocok persis: {matched}")
print(f"Perlu ditinjau: {unmatched}")
print(f"File: {output_path}")