from typing import Any
from uuid import uuid4

from psycopg import Connection
from psycopg.types.json import Jsonb

CLAIM_BATCH_SQL = """
WITH picked AS (
    SELECT source_snapshot_id, pipeline_version
    FROM nlp.processing_queue
    WHERE pipeline_version = %(pipeline_version)s
      AND (
          (status = 'queued' AND available_at <= clock_timestamp())
          OR
          (status = 'claimed' AND lease_until <= clock_timestamp())
      )
    ORDER BY available_at, created_at, source_snapshot_id
    FOR UPDATE SKIP LOCKED
    LIMIT %(batch_size)s
)
UPDATE nlp.processing_queue AS q
SET status = 'claimed',
    attempt_count = q.attempt_count + 1,
    claimed_at = clock_timestamp(),
    lease_until = clock_timestamp()
        + (%(lease_seconds)s * interval '1 second'),
    lease_owner = %(worker_id)s,
    updated_at = clock_timestamp()
FROM picked
WHERE q.source_snapshot_id = picked.source_snapshot_id
  AND q.pipeline_version = picked.pipeline_version
RETURNING
    q.source_snapshot_id,
    q.pipeline_version,
    q.attempt_count,
    q.claimed_at,
    q.lease_until,
    q.lease_owner;
"""


def claim_batch(
    conn: Connection[Any],
    *,
    pipeline_version: str,
    worker_id: str,
    batch_size: int = 1,
    lease_seconds: int = 60,
) -> list[dict[str, Any]]:
    if not pipeline_version.strip():
        raise ValueError("pipeline_version must not be empty")
    if not worker_id.strip():
        raise ValueError("worker_id must not be empty")
    if batch_size < 1 or lease_seconds < 1:
        raise ValueError("batch_size and lease_seconds must be positive")

    with conn.transaction():
        rows = conn.execute(
            CLAIM_BATCH_SQL,
            {
                "pipeline_version": pipeline_version,
                "worker_id": worker_id,
                "batch_size": batch_size,
                "lease_seconds": lease_seconds,
            },
        ).fetchall()

    return [dict(row) for row in rows]


def finish_attempt(
    conn: Connection[Any],
    item: dict[str, Any],
    *,
    worker_id: str,
    result_status: str,
    code_version: str,
    rules_version: str,
    source_digest: str,
    max_attempts: int,
    retry_after_seconds: int | None = None,
    classifier_model_version: str | None = None,
    classifier_model_digest: str | None = None,
    embedding_model_version: str | None = None,
    embedding_model_digest: str | None = None,
    result_metadata: dict[str, Any] | None = None,
    reason_code: str | None = None,
    note_detail: dict[str, Any] | None = None,
) -> str:
    queue_status_by_result = {
        "succeeded": "succeeded",
        "skipped": "skipped",
        "policy_rejection": "rejected",
        "format_failure": "failed",
        "error": "failed",
    }
    note_type_by_result = {
        "skipped": "skip",
        "policy_rejection": "policy_rejection",
        "format_failure": "format_failure",
        "error": "error",
    }

    if result_status not in queue_status_by_result:
        raise ValueError(f"Unsupported result_status: {result_status}")

    attempt_no = item["attempt_count"]
    if attempt_no < 1:
        raise ValueError("Claimed item must have attempt_count >= 1")
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")

    retry = (
        result_status == "error"
        and retry_after_seconds is not None
        and attempt_no < max_attempts
    )
    if retry and retry_after_seconds <= 0:
        raise ValueError("retry_after_seconds must be positive")

    queue_status = "queued" if retry else queue_status_by_result[result_status]
    note_type = note_type_by_result.get(result_status)

    if note_type is not None and not (reason_code or "").strip():
        raise ValueError(f"{result_status} requires a reason_code")

    run_id = uuid4()
    source_snapshot_id = item["source_snapshot_id"]
    pipeline_version = item["pipeline_version"]

    with conn.transaction():
        claimed = conn.execute(
            """
            UPDATE nlp.processing_queue
            SET status = %s,
                available_at = CASE
                    WHEN %s THEN clock_timestamp()
                        + (%s * interval '1 second')
                    ELSE available_at
                END,
                claimed_at = NULL,
                lease_until = NULL,
                lease_owner = NULL,
                updated_at = clock_timestamp()
            WHERE source_snapshot_id = %s
              AND pipeline_version = %s
              AND status = 'claimed'
              AND lease_owner = %s
              AND lease_until > clock_timestamp()
            RETURNING source_snapshot_id
            """,
            (
                queue_status,
                retry,
                retry_after_seconds or 0,
                source_snapshot_id,
                pipeline_version,
                worker_id,
            ),
        ).fetchone()

        if claimed is None:
            raise RuntimeError("Lease expired or this worker no longer owns the item")

        conn.execute(
            """
            INSERT INTO nlp.processing_runs (
                run_id,
                source_snapshot_id,
                pipeline_version,
                attempt_no,
                result_status,
                code_version,
                rules_version,
                classifier_model_version,
                embedding_model_version,
                source_digest,
                started_at,
                finished_at,
                result_metadata,
                classifier_model_digest,
                embedding_model_digest
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                clock_timestamp(), %s, %s, %s
            )
            """,
            (
                run_id,
                source_snapshot_id,
                pipeline_version,
                attempt_no,
                result_status,
                code_version,
                rules_version,
                classifier_model_version,
                embedding_model_version,
                source_digest,
                item["claimed_at"],
                Jsonb(result_metadata or {}),
                classifier_model_digest,
                embedding_model_digest,
            ),
        )

        if note_type is not None:
            conn.execute(
                """
                INSERT INTO nlp.processing_notes (
                    note_id,
                    run_id,
                    source_snapshot_id,
                    pipeline_version,
                    note_type,
                    reason_code,
                    detail
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    uuid4(),
                    run_id,
                    source_snapshot_id,
                    pipeline_version,
                    note_type,
                    reason_code,
                    Jsonb(note_detail or {}),
                ),
            )

    return str(run_id)
