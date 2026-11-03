import csv
import json
import os
from collections import defaultdict
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


gtfs_dir = Path("../open-krl-pipeline/data/build/gtfs-v2")
stops_path = gtfs_dir / "stops.txt"
trips_path = gtfs_dir / "trips.txt"
stop_times_path = gtfs_dir / "stop_times.txt"
output_path = Path("evaluation/reference/gtfs_stop_crosswalk.csv")

for path in (stops_path, trips_path, stop_times_path):
    if not path.exists():
        raise SystemExit(f"File GTFS tidak ditemukan: {path}")

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

with trips_path.open(newline="", encoding="utf-8") as source:
    route_by_trip = {
        row["trip_id"]: row["route_id"]
        for row in csv.DictReader(source)
    }

routes_by_stop = defaultdict(set)
with stop_times_path.open(newline="", encoding="utf-8") as source:
    for row in csv.DictReader(source):
        route_id = route_by_trip.get(row["trip_id"])
        if route_id:
            routes_by_stop[row["stop_id"]].add(route_id)

output_path.parent.mkdir(parents=True, exist_ok=True)

counts = {
    "mapped_exact": 0,
    "unmapped": 0,
    "out_of_scope": 0,
    "catalog_only": 0,
    "in_scope_unmapped": 0,
}

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
        "scope_status",
        "scope_reason",
        "used_route_ids",
    ]
    writer = csv.DictWriter(destination, fieldnames=fields)
    writer.writeheader()

    for stop in reader:
        stop_id = stop["stop_id"]
        ref = reference_by_code.get(stop_id)
        used_routes = sorted(routes_by_stop.get(stop_id, set()))

        if ref:
            mapping_status = "mapped_exact"
            counts["mapped_exact"] += 1
        else:
            mapping_status = "unmapped"
            counts["unmapped"] += 1

        if not used_routes:
            scope_status = "catalog_only"
            scope_reason = "listed_in_stops_txt_but_unused_in_stop_times"
            counts["catalog_only"] += 1
        elif set(used_routes) == {"MERAK"}:
            scope_status = "out_of_scope"
            scope_reason = "used_only_by_merak_route"
            counts["out_of_scope"] += 1
        else:
            scope_status = "in_scope"
            scope_reason = "used_by_in_scope_route"
            if not ref:
                counts["in_scope_unmapped"] += 1

        writer.writerow({
            "stop_id": stop_id,
            "stop_name": stop["stop_name"],
            "source_station_code": ref["source_station_code"] if ref else "",
            "station_id": ref["station_id"] if ref else "",
            "canonical_name": ref["canonical_name"] if ref else "",
            "line_memberships": json.dumps(ref["line_memberships"]) if ref else "",
            "mapping_status": mapping_status,
            "scope_status": scope_status,
            "scope_reason": scope_reason,
            "used_route_ids": json.dumps(used_routes),
        })

print(f"Mapped exact: {counts['mapped_exact']}")
print(f"Unmapped total: {counts['unmapped']}")
print(f"Out of scope: {counts['out_of_scope']}")
print(f"Catalog only: {counts['catalog_only']}")
print(f"In-scope unmapped needing review: {counts['in_scope_unmapped']}")
print(f"File: {output_path}")