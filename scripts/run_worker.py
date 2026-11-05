"""Development worker loop: discover recent snapshots, enqueue, and process."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import time
from urllib.parse import urlparse
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from mooby.worker.processor import run_available
from mooby.worker.queue import enqueue_recent_snapshots


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-project-ref", required=True)
    parser.add_argument("--pipeline-version", required=True)
    parser.add_argument("--worker-id", default="mooby-dev-worker")
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--discovery-batch-size", type=int, default=5)
    parser.add_argument("--max-items", type=int, default=5)
    parser.add_argument("--lookback-seconds", type=int, default=3600)
    parser.add_argument("--snapshot-id", help="Restrict a one-shot run to this UUID.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Enqueue/process at most one item, then exit; requires a unique pipeline version.",
    )
    args = parser.parse_args()
    if not args.pipeline_version.strip() or not args.worker_id.strip():
        parser.error("pipeline-version and worker-id must not be empty")
    if args.poll_seconds <= 0:
        parser.error("poll-seconds must be positive")
    if not 1 <= args.discovery_batch_size <= 100:
        parser.error("discovery-batch-size must be between 1 and 100")
    if not 1 <= args.max_items <= 5:
        parser.error("max-items must be between 1 and 5")
    if args.lookback_seconds < 1:
        parser.error("lookback-seconds must be positive")
    if args.snapshot_id:
        try:
            args.snapshot_id = str(UUID(args.snapshot_id))
        except ValueError:
            parser.error("snapshot-id must be a UUID")
    if args.once:
        args.discovery_batch_size = 1
        args.max_items = 1
    elif args.snapshot_id:
        parser.error("snapshot-id may only be used with --once")
    return args


def main() -> None:
    args = _arguments()
    database_url = os.environ.get("WORKER_DATABASE_URL")
    if not database_url:
        raise SystemExit("WORKER_DATABASE_URL is not configured.")

    parsed = urlparse(database_url)
    username = parsed.username or ""
    expected_ref = args.expected_project_ref
    if not (
        parsed.hostname == f"db.{expected_ref}.supabase.co"
        or username.endswith(f".{expected_ref}")
    ):
        raise SystemExit("Database target does not match --expected-project-ref.")

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        if args.once:
            existing = conn.execute(
                "SELECT count(*) AS row_count FROM nlp.processing_queue WHERE pipeline_version = %s",
                (args.pipeline_version,),
            ).fetchone()
            if existing is None or existing["row_count"] != 0:
                raise SystemExit(
                    "One-shot pipeline version already has queue rows; choose a new unique version."
                )

        while True:
            enqueued_ids = enqueue_recent_snapshots(
                conn,
                pipeline_version=args.pipeline_version,
                lookback_seconds=args.lookback_seconds,
                batch_size=args.discovery_batch_size,
                snapshot_id=args.snapshot_id,
            )
            results = run_available(
                conn,
                pipeline_version=args.pipeline_version,
                worker_id=args.worker_id,
                max_attempts=1 if args.once else 3,
                lease_seconds=120,
                transient_retry_after_seconds=30,
                max_items=args.max_items,
            )

            if enqueued_ids:
                print(f"enqueued_count: {len(enqueued_ids)}")
            for result in results:
                outcome = result.outcome
                print(
                    {
                        "result_status": outcome.result_status if outcome else None,
                        "reason_code": outcome.reason_code if outcome else None,
                        "run_id": result.run_id,
                        "event_draft_status": (
                            outcome.result_metadata.get("event_draft_persistence", {}).get("status")
                            if outcome
                            else None
                        ),
                    }
                )

            if args.once:
                if not results:
                    print("No eligible queue item found for this one-shot run.")
                return
            if not results:
                time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
