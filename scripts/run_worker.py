"""Development worker loop: discover recent snapshots, enqueue, and process."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import time
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from worker_runtime import (AlreadyRunning, emit,
                            single_instance, validate_development_url)
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

    validate_development_url(database_url, args.expected_project_ref)
    with single_instance(PROJECT_ROOT / '.local/worker/instance.lock'):
        emit('worker_started')
        _poll(args, database_url)


def _poll(args, database_url):
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

            outcomes = [r.outcome for r in results if r.outcome is not None]
            emit('poll', enqueued_count=len(enqueued_ids), processed_count=len(results),
                 succeeded_count=sum(o.result_status == 'succeeded' for o in outcomes),
                 skipped_count=sum(o.result_status == 'skipped' for o in outcomes),
                 rejected_count=sum(o.result_status == 'policy_rejection' for o in outcomes),
                 failed_count=sum(o.result_status in {'error', 'format_failure'} for o in outcomes),
                 accepted_event_count=sum(o.result_status == 'succeeded' and o.draft_event is not None
                                          for o in outcomes))

            if args.once:
                return
            if not results:
                time.sleep(args.poll_seconds)


def entrypoint():
    try:
        main()
    except AlreadyRunning:
        emit('duplicate_instance')
        return 0  # Do not cause launchd to restart a redundant instance forever.
    except KeyboardInterrupt:
        emit('worker_stopped')
        return 0
    except SystemExit as exc:
        if exc.code in (None, 0):
            return 0
        emit('worker_error')
        return 1
    except Exception as exc:
        emit('worker_error', sqlstate=getattr(exc, 'sqlstate', None))
        return 1
    emit('worker_stopped')
    return 0


if __name__ == "__main__":
    raise SystemExit(entrypoint())
