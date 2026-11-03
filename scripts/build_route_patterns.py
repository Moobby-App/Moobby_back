import csv
from collections import defaultdict
from pathlib import Path

gtfs = Path("../open-krl-pipeline/data/build/gtfs-v2")
output = Path("evaluation/reference/route_stop_patterns.csv")
included_routes = {
    "BOGOR", "CIKARANG", "RANGKASBITUNG", "TANGERANG", "TANJUNGPRIUK"
}

with (gtfs / "stops.txt").open(newline="", encoding="utf-8") as f:
    stop_names = {row["stop_id"]: row["stop_name"] for row in csv.DictReader(f)}

with (gtfs / "trips.txt").open(newline="", encoding="utf-8") as f:
    trips = {
        row["trip_id"]: row
        for row in csv.DictReader(f)
        if row["route_id"] in included_routes
    }

stops_by_trip = defaultdict(list)
with (gtfs / "stop_times.txt").open(newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        if row["trip_id"] in trips:
            stops_by_trip[row["trip_id"]].append(
                (int(row["stop_sequence"]), row["stop_id"])
            )

patterns = defaultdict(int)
for trip_id, stops in stops_by_trip.items():
    trip = trips[trip_id]
    ordered_ids = tuple(stop_id for _, stop_id in sorted(stops))
    key = (trip["route_id"], trip["trip_headsign"], ordered_ids)
    patterns[key] += 1

output.parent.mkdir(parents=True, exist_ok=True)
with output.open("w", newline="", encoding="utf-8") as f:
    fields = [
        "route_id", "trip_headsign", "trip_count", "stop_count",
        "first_stop_id", "first_stop_name", "last_stop_id", "last_stop_name",
        "stop_ids", "stop_names",
    ]
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()

    for (route, headsign, ids), trip_count in sorted(patterns.items()):
        writer.writerow({
            "route_id": route,
            "trip_headsign": headsign,
            "trip_count": trip_count,
            "stop_count": len(ids),
            "first_stop_id": ids[0] if ids else "",
            "first_stop_name": stop_names.get(ids[0], "") if ids else "",
            "last_stop_id": ids[-1] if ids else "",
            "last_stop_name": stop_names.get(ids[-1], "") if ids else "",
            "stop_ids": " > ".join(ids),
            "stop_names": " > ".join(stop_names.get(i, i) for i in ids),
        })

print(f"Pola unik: {len(patterns)}")
print(f"File: {output}")