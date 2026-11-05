"""Run synthetic CSV text through the backend processor without persistence.

This is a local simulation only. It assigns synthetic timestamps and source
metadata in memory so the normal intake and NLP stages can be exercised. It
does not connect to PostgreSQL, enqueue work, write events, or expose results
through app_api.
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from typing import Any
from uuid import NAMESPACE_URL, uuid5

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from mooby.db.source_reader import SourceSnapshot, StationReference
from mooby.worker.classifier import ClassifierAdapter
from mooby.worker.processor import WorkerSettings, process_snapshot


DEFAULT_INPUT = PROJECT_ROOT / "evaluation/private/synthetic_training_v1.csv"
DEFAULT_REPORT = (
    PROJECT_ROOT
    / "evaluation/private/Scrape_Manual/synthetic_batch_replay_report.json"
)
DEFAULT_STATIONS = PROJECT_ROOT / "evaluation/reference/station_alias_review.csv"


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--pipeline-version", default="synthetic-local-replay-v1")
    parser.add_argument(
        "--simulation-time",
        help="ISO-8601 timestamp used only in memory; defaults to current UTC.",
    )
    parser.add_argument(
        "--confirm-simulation-only",
        action="store_true",
        help="Required acknowledgement that this does not write to the database.",
    )
    args = parser.parse_args()
    if not args.confirm_simulation_only:
        parser.error("pass --confirm-simulation-only; no database writes are performed")
    if not args.pipeline_version.strip():
        parser.error("pipeline-version must not be empty")
    return args


def _parse_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit("--simulation-time must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SystemExit("--simulation-time must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def _json_array(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return ()
    if not isinstance(decoded, list):
        return ()
    return tuple(item for item in decoded if isinstance(item, str) and item.strip())


def _load_station_catalog(path: Path) -> list[StationReference]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        required = {"station_id", "canonical_name", "curated_aliases"}
        if not required.issubset(reader.fieldnames or ()):
            raise SystemExit("station alias reference CSV is missing required columns")
        return [
            StationReference(
                station_id=row["station_id"],
                canonical_name=row["canonical_name"],
                aliases=_json_array(row.get("curated_aliases")),
            )
            for row in reader
            if row.get("station_id") and row.get("canonical_name")
        ]


def _row_text(row: dict[str, str]) -> str:
    value = row.get("raw_text")
    return value if isinstance(value, str) else ""


def _snapshot(
    row: dict[str, str],
    row_number: int,
    raw_text: str,
    timestamp: datetime,
) -> SourceSnapshot:
    source_key = (row.get("record_id") or "").strip() or f"row-{row_number}"
    snapshot_id = uuid5(
        NAMESPACE_URL, f"mooby-synthetic-snapshot:{source_key}:{row_number}"
    )
    raw_content_id = uuid5(
        NAMESPACE_URL, f"mooby-synthetic-content:{source_key}:{row_number}"
    )

    return SourceSnapshot(
        snapshot_id=snapshot_id,
        raw_content_id=raw_content_id,
        capture_hash=hashlib.sha256(raw_text.encode("utf-8")).hexdigest(),
        raw_text=raw_text,
        text_kind="post_text",
        text_completeness="synthetic_fixture",
        published_at=timestamp,
        published_at_basis="page_timestamp",
        captured_at=timestamp,
        platform="x",
        canonical_url=f"https://example.invalid/synthetic/{snapshot_id}",
    )


def main() -> None:
    args = _arguments()
    if not args.input.is_file():
        raise SystemExit(f"input CSV not found: {args.input}")
    try:
        settings = WorkerSettings.from_env()
        classifier = ClassifierAdapter.load()
    except Exception as exc:
        raise SystemExit(
            f"could not load processor settings or models ({type(exc).__name__})"
        ) from None

    timestamp = _parse_timestamp(args.simulation_time)
    station_catalog = _load_station_catalog(DEFAULT_STATIONS)
    outcomes: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    relevance: Counter[str] = Counter()
    severity: Counter[str] = Counter()
    excluded_blank = 0
    row_count = 0
    with args.input.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if "raw_text" not in (reader.fieldnames or ()):
            raise SystemExit("input CSV must contain a raw_text column")
        for row_number, row in enumerate(reader, start=2):
            row_count += 1
            raw_text = _row_text(row)
            if not raw_text.strip():
                excluded_blank += 1
                continue
            snapshot = _snapshot(row, row_number, raw_text, timestamp)
            outcome = process_snapshot(
                snapshot,
                pipeline_version=args.pipeline_version,
                database_now=timestamp,
                classifier=classifier,
                settings=settings,
                station_catalog=station_catalog,
            )
            outcomes[outcome.result_status] += 1
            if outcome.reason_code:
                reasons[outcome.reason_code] += 1
            metadata: dict[str, Any] = outcome.result_metadata
            relevance_prediction = metadata.get("relevance_prediction") or {}
            severity_prediction = metadata.get("severity_prediction") or {}
            if relevance_prediction.get("label"):
                relevance[relevance_prediction["label"]] += 1
            if severity_prediction.get("label"):
                severity[severity_prediction["label"]] += 1

    report = {
        "mode": "simulation_only",
        "pipeline_version": args.pipeline_version,
        "input_file": str(args.input),
        "input_rows": row_count,
        "processed_nonblank_rows": row_count - excluded_blank,
        "excluded_blank_text_rows": excluded_blank,
        "synthetic_timestamp_used_for_all_rows": timestamp.isoformat(),
        "source_metadata_synthetic": True,
        "database_connection": False,
        "queue_or_event_writes": False,
        "app_api_exposure": False,
        "outcomes": dict(sorted(outcomes.items())),
        "reason_codes": dict(sorted(reasons.items())),
        "relevance_predictions": dict(sorted(relevance.items())),
        "severity_predictions": dict(sorted(severity.items())),
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Report saved: {args.report}")


if __name__ == "__main__":
    main()
