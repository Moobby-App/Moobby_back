from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import math
import os
import re
import unicodedata
from typing import Any, Callable, Literal, Mapping, Sequence
from uuid import UUID

from psycopg import Connection

from mooby.db.source_reader import (
    SourceSnapshot,
    StationCatalogUnavailable,
    StationReference,
    database_clock_now,
    load_snapshot,
    load_station_catalog,
)
from mooby.worker.classifier import ClassifierAdapter, ModelArtifactError, ModelPrediction
from mooby.worker.event_fields import (
    EVENT_TIME_RULES_VERSION,
    EVENT_TIMEZONE,
    extract_event_time_candidate,
)
from mooby.worker.queue import claim_batch, finish_attempt

ResultStatus = Literal[
    "succeeded",
    "skipped",
    "policy_rejection",
    "format_failure",
    "error",
]

KEYWORD_RULES_VERSION = "krl-keyword-prefilter-v1"
NORMALIZATION_VERSION = "nfkc-casefold-whitespace-v1"
PROCESSOR_CODE_VERSION = "classification-worker-slice-v3"
INTAKE_WINDOW = timedelta(hours=1)
KEYWORDS = (
    "gangguan",
    "terganggu",
    "perjalanan terganggu",
    "terlambat",
    "keterlambatan",
    "delay",
    "tertahan",
    "antre",
    "rekayasa pola operasi",
    "listrik",
    "sinyal",
    "persinyalan",
)
TRUSTED_PUBLICATION_BASES_ENV = "MOOBY_TRUSTED_PUBLISHED_AT_BASES"


class ThresholdConfigurationError(ValueError):
    def __init__(self, reason_code: str, variable: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.variable = variable


@dataclass(frozen=True)
class ClassificationThresholds:
    relevance: float
    severity: float

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not 0.0 <= value <= 1.0
            for value in (self.relevance, self.severity)
        ):
            raise ThresholdConfigurationError("threshold_invalid", "thresholds")

    @classmethod
    def from_env(
        cls, environ: Mapping[str, str] | None = None
    ) -> ClassificationThresholds:
        """Read required MOOBY_RELEVANCE_THRESHOLD and MOOBY_SEVERITY_THRESHOLD."""
        values = os.environ if environ is None else environ
        parsed: dict[str, float] = {}
        for key, variable in (
            ("relevance", "MOOBY_RELEVANCE_THRESHOLD"),
            ("severity", "MOOBY_SEVERITY_THRESHOLD"),
        ):
            raw = values.get(variable)
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                raise ThresholdConfigurationError("threshold_unconfigured", variable)
            if not isinstance(raw, str):
                raise ThresholdConfigurationError("threshold_invalid", variable)
            try:
                value = float(raw)
            except (TypeError, ValueError) as exc:
                raise ThresholdConfigurationError("threshold_invalid", variable) from exc
            if not 0.0 <= value <= 1.0:
                raise ThresholdConfigurationError("threshold_invalid", variable)
            parsed[key] = value
        return cls(relevance=parsed["relevance"], severity=parsed["severity"])


@dataclass(frozen=True)
class WorkerSettings:
    """Load once at worker startup; retain invalid config for auditable skips."""

    thresholds: ClassificationThresholds | None
    threshold_error: ThresholdConfigurationError | None
    trusted_publication_bases: frozenset[str] | None
    publication_basis_error: str | None

    @classmethod
    def from_env(
        cls, environ: Mapping[str, str] | None = None
    ) -> WorkerSettings:
        try:
            thresholds = ClassificationThresholds.from_env(environ)
            threshold_error = None
        except ThresholdConfigurationError as exc:
            thresholds = None
            threshold_error = exc

        try:
            trusted_bases = _trusted_publication_bases(environ)
            basis_error = None
        except ValueError:
            trusted_bases = None
            raw = (os.environ if environ is None else environ).get(
                TRUSTED_PUBLICATION_BASES_ENV
            )
            basis_error = (
                "publication_timestamp_basis_unconfigured"
                if raw is None or raw == ""
                else "publication_timestamp_basis_config_invalid"
            )

        return cls(
            thresholds=thresholds,
            threshold_error=threshold_error,
            trusted_publication_bases=trusted_bases,
            publication_basis_error=basis_error,
        )


@dataclass(frozen=True)
class StationResolution:
    status: Literal["unavailable", "no_match", "resolved", "ambiguous"]
    station_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProcessingOutcome:
    result_status: ResultStatus
    result_metadata: dict[str, Any] = field(default_factory=dict)
    reason_code: str | None = None
    note_detail: dict[str, Any] = field(default_factory=dict)
    retry_after_seconds: int | None = None


def normalize_text(raw_text: str) -> str:
    """Create a matching/model copy; never mutate or replace source text."""
    return " ".join(unicodedata.normalize("NFKC", raw_text).casefold().split())


def _trusted_publication_bases(
    environ: Mapping[str, str] | None,
) -> frozenset[str] | None:
    values = os.environ if environ is None else environ
    raw = values.get(TRUSTED_PUBLICATION_BASES_ENV)
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError("invalid trusted publication basis configuration")
    if not raw.strip():
        return None
    bases = frozenset(value.strip().casefold() for value in raw.split(",") if value.strip())
    if not bases or "unknown" in bases:
        raise ValueError("invalid trusted publication basis configuration")
    return bases


def _reason(
    status: ResultStatus,
    code: str,
    stage: str,
    metadata: dict[str, Any],
    **detail: Any,
) -> ProcessingOutcome:
    metadata["outcome"] = status
    metadata["reason_code"] = code
    return ProcessingOutcome(
        result_status=status,
        result_metadata=metadata,
        reason_code=code,
        note_detail={"stage": stage, **detail},
    )


def resolve_station_mentions(
    normalized_text: str,
    station_catalog: Sequence[StationReference] | None,
) -> StationResolution:
    if station_catalog is None:
        return StationResolution("unavailable")

    found: set[str] = set()
    aliases: dict[str, set[str]] = {}
    for station in station_catalog:
        for name in (station.canonical_name, *station.aliases):
            normalized_name = normalize_text(name)
            if not normalized_name:
                continue
            aliases.setdefault(normalized_name, set()).add(station.station_id)

    for alias, station_ids in aliases.items():
        if re.search(rf"(?<!\w){re.escape(alias)}(?!\w)", normalized_text):
            found.update(station_ids)
            if len(station_ids) > 1:
                return StationResolution("ambiguous", tuple(sorted(found)))

    if not found:
        return StationResolution("no_match")
    return StationResolution("resolved", tuple(sorted(found)))


def _prediction_metadata(prediction: ModelPrediction) -> dict[str, Any]:
    return {
        "label": prediction.label,
        "probabilities": prediction.probabilities,
        "confidence": prediction.confidence,
        "model_version": prediction.model_version,
        "artifact_sha256": prediction.artifact_sha256,
    }


def process_snapshot(
    snapshot: SourceSnapshot,
    *,
    database_now: datetime,
    classifier: ClassifierAdapter,
    settings: WorkerSettings,
    station_catalog: Sequence[StationReference] | None = None,
    station_catalog_loader: Callable[[], Sequence[StationReference]] | None = None,
) -> ProcessingOutcome:
    """Run the classification-only pipeline after a trusted source read.

    Thresholds and accepted timestamp provenance values come from environment
    configuration; neither has an implicit default.
    """
    metadata: dict[str, Any] = {
        "code_version": PROCESSOR_CODE_VERSION,
        "keyword_rules_version": KEYWORD_RULES_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "model_artifacts": classifier.artifact_manifest,
    }

    published_at = snapshot.published_at
    if published_at is None:
        return _reason("skipped", "publication_timestamp_missing", "time_gate", metadata)
    if (
        not isinstance(published_at, datetime)
        or published_at.tzinfo is None
        or published_at.utcoffset() is None
    ):
        return _reason("skipped", "publication_timestamp_invalid", "time_gate", metadata)
    if (
        not isinstance(database_now, datetime)
        or database_now.tzinfo is None
        or database_now.utcoffset() is None
    ):
        return _reason("error", "transient_infrastructure_error", "database_clock", metadata)

    published_utc = published_at.astimezone(timezone.utc)
    now_utc = database_now.astimezone(timezone.utc)
    metadata["published_at"] = published_utc.isoformat()
    metadata["database_clock_at"] = now_utc.isoformat()
    if published_utc > now_utc:
        return _reason("skipped", "publication_timestamp_future", "time_gate", metadata)

    basis = (snapshot.published_at_basis or "").strip().casefold()
    metadata["published_at_basis"] = basis or None
    if basis == "unknown" or not basis:
        return _reason("skipped", "publication_timestamp_basis_unknown", "time_gate", metadata)
    if settings.publication_basis_error == "publication_timestamp_basis_config_invalid":
        return _reason(
            "skipped",
            "publication_timestamp_basis_config_invalid",
            "time_gate",
            metadata,
        )
    if settings.publication_basis_error is not None or settings.trusted_publication_bases is None:
        return _reason(
            "skipped",
            "publication_timestamp_basis_unconfigured",
            "time_gate",
            metadata,
        )
    metadata["trusted_publication_bases"] = sorted(settings.trusted_publication_bases)
    if basis not in settings.trusted_publication_bases:
        return _reason("skipped", "publication_timestamp_basis_invalid", "time_gate", metadata)

    if published_utc < now_utc - INTAKE_WINDOW:
        return _reason("skipped", "publication_timestamp_outside_window", "time_gate", metadata)

    if not isinstance(snapshot.raw_text, str) or not snapshot.raw_text.strip():
        return _reason("format_failure", "source_text_missing", "source_validation", metadata)

    normalized = normalize_text(snapshot.raw_text)
    matched_keywords = [
        keyword for keyword in KEYWORDS if contains_keyword(normalized, keyword)
    ]
    if not matched_keywords:
        metadata["keyword_matches"] = []
        return _reason(
            "skipped",
            "keyword_prefilter_no_match",
            "keyword_prefilter",
            metadata,
        )
    metadata["keyword_matches"] = matched_keywords

    station_catalog_status = "injected" if station_catalog is not None else "unavailable"
    if station_catalog_loader is not None:
        try:
            station_catalog = station_catalog_loader()
            station_catalog_status = "loaded"
        except StationCatalogUnavailable as exc:
            station_catalog = None
            station_catalog_status = f"unavailable:{type(exc).__name__}"
    station_resolution = resolve_station_mentions(normalized, station_catalog)
    metadata["station_resolution"] = {
        "status": station_resolution.status,
        "catalog_status": station_catalog_status,
        "station_ids": list(station_resolution.station_ids),
        "vector_lookup": "unavailable_no_index_provider",
    }
    if station_resolution.status in {"ambiguous", "no_match"}:
        return _reason(
            "skipped",
            "station_unresolved",
            "station_resolution",
            metadata,
        )

    if settings.threshold_error is not None:
        metadata["threshold_variable"] = settings.threshold_error.variable
        return _reason(
            "skipped",
            settings.threshold_error.reason_code,
            "threshold_configuration",
            metadata,
        )
    thresholds = settings.thresholds
    if thresholds is None:
        return _reason(
            "skipped",
            "threshold_unconfigured",
            "threshold_configuration",
            metadata,
        )

    metadata["thresholds"] = {
        "relevance": thresholds.relevance,
        "severity": thresholds.severity,
    }
    try:
        relevance = classifier.predict_relevance(normalized)
        metadata["relevance_prediction"] = _prediction_metadata(relevance)
    except ModelArtifactError as exc:
        metadata["model_error"] = str(exc)
        return _reason(
            "error",
            "transient_model_error",
            "relevance_classification",
            metadata,
        )

    disruption_probability = relevance.probabilities.get("disruption")
    if disruption_probability is None:
        return _reason("error", "transient_model_error", "relevance_classification", metadata)
    if relevance.confidence < thresholds.relevance:
        return _reason(
            "skipped",
            "relevance_below_threshold",
            "relevance_classification",
            metadata,
        )
    if relevance.label != "disruption":
        return _reason(
            "skipped",
            "non_disruption",
            "relevance_classification",
            metadata,
        )
    try:
        severity = classifier.predict_severity(normalized)
        metadata["severity_prediction"] = _prediction_metadata(severity)
    except ModelArtifactError as exc:
        metadata["model_error"] = str(exc)
        return _reason(
            "error",
            "transient_model_error",
            "severity_classification",
            metadata,
        )

    if severity.confidence < thresholds.severity:
        return _reason(
            "skipped",
            "severity_below_threshold",
            "severity_classification",
            metadata,
        )

    metadata["outcome"] = "succeeded"
    metadata["accepted_labels"] = {
        "relevance": relevance.label,
        "severity": severity.label,
    }
    event_time = extract_event_time_candidate(snapshot.raw_text, published_at)
    metadata["event_time_candidate"] = {
        "status": event_time.status,
        "event_at": event_time.event_at.isoformat() if event_time.event_at else None,
        "method": event_time.method,
        "certainty": event_time.certainty,
        "timezone": EVENT_TIMEZONE.key,
        "rules_version": EVENT_TIME_RULES_VERSION,
    }
    return ProcessingOutcome("succeeded", result_metadata=metadata)


def contains_keyword(normalized_text: str, keyword: str) -> bool:
    """Use token boundaries for terms while retaining multiword phrases."""
    return re.search(
        rf"(?<!\w){re.escape(keyword)}(?!\w)", normalized_text
    ) is not None


def process_and_finish_attempt(
    conn: Connection[Any],
    item: dict[str, Any],
    snapshot: SourceSnapshot,
    *,
    worker_id: str,
    classifier: ClassifierAdapter,
    settings: WorkerSettings,
    max_attempts: int,
    transient_retry_after_seconds: int,
    station_catalog: Sequence[StationReference] | None = None,
    station_catalog_loader: Callable[[], Sequence[StationReference]] | None = None,
) -> tuple[ProcessingOutcome, str]:
    """Process one loaded snapshot and persist outcome through finish_attempt."""
    _assert_snapshot_identity(item, snapshot)
    if transient_retry_after_seconds < 1:
        raise ValueError("transient_retry_after_seconds must be positive")
    try:
        with conn.transaction():
            now = database_clock_now(conn)
    except Exception as exc:
        outcome = ProcessingOutcome(
            result_status="error",
            result_metadata={
                "code_version": PROCESSOR_CODE_VERSION,
                "keyword_rules_version": KEYWORD_RULES_VERSION,
                "model_artifacts": classifier.artifact_manifest,
                "error_type": type(exc).__name__,
            },
            reason_code="transient_infrastructure_error",
            note_detail={"stage": "database_clock"},
            retry_after_seconds=transient_retry_after_seconds,
        )
    else:
        try:
            outcome = process_snapshot(
                snapshot,
                database_now=now,
                classifier=classifier,
                settings=settings,
                station_catalog=station_catalog,
                station_catalog_loader=station_catalog_loader,
            )
        except Exception as exc:
            outcome = ProcessingOutcome(
                result_status="error",
                result_metadata={
                    "code_version": PROCESSOR_CODE_VERSION,
                    "keyword_rules_version": KEYWORD_RULES_VERSION,
                    "model_artifacts": classifier.artifact_manifest,
                    "error_type": type(exc).__name__,
                },
                reason_code="transient_infrastructure_error",
                note_detail={"stage": "processing"},
                retry_after_seconds=transient_retry_after_seconds,
            )

    if outcome.result_status == "error" and outcome.retry_after_seconds is None:
        outcome = ProcessingOutcome(
            result_status=outcome.result_status,
            result_metadata=outcome.result_metadata,
            reason_code=outcome.reason_code,
            note_detail=outcome.note_detail,
            retry_after_seconds=transient_retry_after_seconds,
        )

    source_digest = hashlib.sha256(snapshot.raw_text.encode("utf-8")).hexdigest()
    manifest = classifier.artifact_manifest
    version = manifest.get("relevance", {}).get("model_version")
    classifier_version = version.split(":", 1)[0] if isinstance(version, str) else None
    run_id = finish_attempt(
        conn,
        item,
        worker_id=worker_id,
        result_status=outcome.result_status,
        code_version=PROCESSOR_CODE_VERSION,
        rules_version=KEYWORD_RULES_VERSION,
        source_digest=source_digest,
        max_attempts=max_attempts,
        retry_after_seconds=outcome.retry_after_seconds,
        classifier_model_version=classifier_version,
        # The two classifiers have different artifact digests; both exact
        # digests are stored under result_metadata.model_artifacts.
        classifier_model_digest=None,
        result_metadata=outcome.result_metadata,
        reason_code=outcome.reason_code,
        note_detail=outcome.note_detail,
    )
    return outcome, run_id


class SnapshotIdentityMismatch(RuntimeError):
    """Raised without processing or finishing a claim for a different snapshot."""


class SnapshotUnavailable(RuntimeError):
    """Raised when the claimed snapshot ID cannot be read from the source."""


def _assert_snapshot_identity(
    item: Mapping[str, Any], snapshot: SourceSnapshot
) -> None:
    try:
        claimed_id = UUID(str(item["source_snapshot_id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise SnapshotIdentityMismatch(
            "Claimed item has no valid source snapshot identity; item was not finished."
        ) from exc
    try:
        loaded_id = UUID(str(snapshot.snapshot_id))
    except (TypeError, ValueError) as exc:
        raise SnapshotIdentityMismatch(
            "Loaded source snapshot has no valid identity; item was not finished."
        ) from exc
    if claimed_id != loaded_id:
        raise SnapshotIdentityMismatch(
            "Claimed and loaded snapshot IDs differ; item was not processed or finished."
        )


@lru_cache(maxsize=1)
def _process_runtime() -> tuple[ClassifierAdapter, WorkerSettings]:
    """Cache model loading and environment validation once for this process."""
    return ClassifierAdapter.load(), WorkerSettings.from_env()


@dataclass(frozen=True)
class RunOnceResult:
    claimed: bool
    outcome: ProcessingOutcome | None = None
    run_id: str | None = None


def run_once(
    conn: Connection[Any],
    *,
    pipeline_version: str,
    worker_id: str,
    max_attempts: int,
    lease_seconds: int,
    transient_retry_after_seconds: int,
) -> RunOnceResult:
    """Claim and process at most one item; never publishes an event."""
    if not pipeline_version.strip() or not worker_id.strip():
        raise ValueError("pipeline_version and worker_id must not be empty")
    if max_attempts < 1 or lease_seconds < 1 or transient_retry_after_seconds < 1:
        raise ValueError("attempt, lease, and retry settings must be positive")

    claimed_items = claim_batch(
        conn,
        pipeline_version=pipeline_version,
        worker_id=worker_id,
        batch_size=1,
        lease_seconds=lease_seconds,
    )
    if not claimed_items:
        return RunOnceResult(claimed=False)

    item = claimed_items[0]
    try:
        claimed_id = UUID(str(item["source_snapshot_id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise SnapshotIdentityMismatch(
            "Claimed item has no valid source snapshot identity; item was not finished."
        ) from exc

    with conn.transaction():
        snapshot = load_snapshot(conn, claimed_id)
    if snapshot is None:
        raise SnapshotUnavailable(
            "Claimed source snapshot was not found; item was not finished."
        )
    _assert_snapshot_identity(item, snapshot)

    classifier, settings = _process_runtime()
    outcome, run_id = process_and_finish_attempt(
        conn,
        item,
        snapshot,
        worker_id=worker_id,
        classifier=classifier,
        settings=settings,
        max_attempts=max_attempts,
        transient_retry_after_seconds=transient_retry_after_seconds,
        station_catalog_loader=lambda: load_station_catalog(conn),
    )
    return RunOnceResult(claimed=True, outcome=outcome, run_id=run_id)
