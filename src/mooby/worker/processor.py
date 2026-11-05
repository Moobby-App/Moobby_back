from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import math
import os
import re
import unicodedata
from typing import Any, Callable, Literal, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg import Connection

from mooby.db.source_reader import (
    SourceSnapshot,
    OrderedPathReference,
    StationCatalogUnavailable,
    StationReference,
    database_clock_now,
    load_snapshot,
    load_confirmed_ordered_paths,
    load_station_catalog,
)
from mooby.worker.classifier import ClassifierAdapter, ModelArtifactError, ModelPrediction
from mooby.worker.event_persistence import DraftEvent
from mooby.worker.event_fields import (
    EVIDENCE_QUOTE_RULES_VERSION,
    EVENT_TIME_RULES_VERSION,
    EVENT_TIMEZONE,
    extract_event_time_candidate,
    select_exact_evidence_quote,
    validate_exact_evidence_quote,
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
LOCATION_RULES_VERSION = "ordered-confirmed-path-mentions-v1"
DRAFT_EVENT_RULES_VERSION = "single-station-or-confirmed-segment-draft-v1"
NORMALIZATION_VERSION = "nfkc-casefold-whitespace-v1"
PROCESSOR_CODE_VERSION = "classification-worker-slice-v5"
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
    draft_event: DraftEvent | None = None


@dataclass(frozen=True)
class StructuredLocation:
    status: Literal["station_resolved", "segment_resolved", "unresolved", "ambiguous"]
    station_ids: tuple[str, ...]
    line_id: str | None = None
    path_id: str | None = None
    path_version: int | None = None
    direction: str | None = None
    start_station_id: str | None = None
    end_station_id: str | None = None


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


def _ordered_station_ids_in_text(
    normalized_text: str,
    station_catalog: Sequence[StationReference],
) -> tuple[str, ...] | None:
    """Return unique station IDs in text order; ambiguous/overlapping aliases fail closed."""
    aliases: dict[str, set[str]] = {}
    for station in station_catalog:
        for name in (station.canonical_name, *station.aliases):
            normalized_name = normalize_text(name)
            if normalized_name:
                aliases.setdefault(normalized_name, set()).add(station.station_id)

    mentions: list[tuple[int, int, str]] = []
    for alias, station_ids in aliases.items():
        matches = list(
            re.finditer(rf"(?<!\w){re.escape(alias)}(?!\w)", normalized_text)
        )
        if matches and len(station_ids) != 1:
            return None
        if matches:
            station_id = next(iter(station_ids))
            mentions.extend((match.start(), match.end(), station_id) for match in matches)

    mentions.sort(key=lambda value: (value[0], -(value[1] - value[0])))
    ordered: list[str] = []
    previous_end = -1
    previous_start = -1
    previous_station: str | None = None
    for start, end, station_id in mentions:
        if start < previous_end:
            if start == previous_start and station_id == previous_station:
                continue
            return None
        if not ordered or ordered[-1] != station_id:
            if station_id not in ordered:
                ordered.append(station_id)
        previous_start, previous_end, previous_station = start, end, station_id
    return tuple(ordered)


def _resolve_structured_location(
    normalized_text: str,
    station_resolution: StationResolution,
    station_catalog: Sequence[StationReference] | None,
    path_catalog: Sequence[OrderedPathReference] | None,
) -> StructuredLocation:
    if station_resolution.status != "resolved" or station_catalog is None:
        return StructuredLocation("unresolved", station_resolution.station_ids)

    ordered_ids = _ordered_station_ids_in_text(normalized_text, station_catalog)
    if ordered_ids is None or set(ordered_ids) != set(station_resolution.station_ids):
        return StructuredLocation("ambiguous", station_resolution.station_ids)
    if len(ordered_ids) == 1:
        return StructuredLocation("station_resolved", ordered_ids)
    if path_catalog is None:
        return StructuredLocation("unresolved", ordered_ids)

    matching_paths: list[OrderedPathReference] = []
    for path in path_catalog:
        cursor = -1
        for station_id in ordered_ids:
            try:
                cursor = path.station_ids.index(station_id, cursor + 1)
            except ValueError:
                break
        else:
            matching_paths.append(path)

    if len(matching_paths) != 1:
        status: Literal["unresolved", "ambiguous"] = (
            "ambiguous" if matching_paths else "unresolved"
        )
        return StructuredLocation(status, ordered_ids)

    path = matching_paths[0]
    return StructuredLocation(
        status="segment_resolved",
        station_ids=ordered_ids,
        line_id=path.line_id,
        path_id=path.path_id,
        path_version=path.path_version,
        direction=path.direction,
        start_station_id=ordered_ids[0],
        end_station_id=ordered_ids[-1],
    )


def _build_draft_event(
    snapshot: SourceSnapshot,
    *,
    pipeline_version: str | None,
    location: StructuredLocation,
    station_catalog: Sequence[StationReference] | None,
    event_time: Any,
    quote: Any,
) -> tuple[DraftEvent | None, str | None]:
    if not pipeline_version or not pipeline_version.strip():
        return None, "pipeline_version_unavailable"
    if location.status not in {"station_resolved", "segment_resolved"}:
        return None, "location_unresolved"
    if quote.status != "validated" or not quote.quote:
        return None, "evidence_quote_unresolved"
    if station_catalog is None:
        return None, "station_catalog_unavailable"
    if (
        not isinstance(snapshot.capture_hash, str)
        or re.fullmatch(r"[0-9a-f]{64}", snapshot.capture_hash) is None
    ):
        return None, "source_digest_invalid"
    if snapshot.platform not in {"x", "threads", "rss", "web"}:
        return None, "source_platform_unsupported"
    if not isinstance(snapshot.canonical_url, str) or not snapshot.canonical_url.startswith("https://"):
        return None, "source_url_invalid"
    if (
        not isinstance(snapshot.published_at, datetime)
        or snapshot.published_at.tzinfo is None
        or snapshot.published_at.utcoffset() is None
        or not isinstance(snapshot.captured_at, datetime)
        or snapshot.captured_at.tzinfo is None
        or snapshot.captured_at.utcoffset() is None
        or snapshot.published_at_basis not in {"dom_datetime", "feed_timestamp", "page_timestamp"}
    ):
        return None, "source_provenance_incomplete"

    stations = {station.station_id: station for station in station_catalog}
    if location.status == "station_resolved":
        station_id = location.station_ids[0]
        station = stations.get(station_id)
        if station is None:
            return None, "station_reference_missing"
        title = f"Gangguan layanan KRL di {station.canonical_name}"
        summary = f"Laporan gangguan layanan KRL menyebut Stasiun {station.canonical_name}."
        event_kind: Literal["station", "segment"] = "station"
        start_station_id = end_station_id = None
        line_id = direction = path_id = None
        path_version = None
        event_station_id = station_id
    else:
        start_station = stations.get(location.start_station_id or "")
        end_station = stations.get(location.end_station_id or "")
        if start_station is None or end_station is None:
            return None, "station_reference_missing"
        title = (
            "Gangguan layanan KRL antara "
            f"{start_station.canonical_name} dan {end_station.canonical_name}"
        )
        summary = (
            "Laporan gangguan layanan KRL pada lintas "
            f"{start_station.canonical_name}–{end_station.canonical_name}."
        )
        event_kind = "segment"
        event_station_id = None
        start_station_id = location.start_station_id
        end_station_id = location.end_station_id
        line_id = location.line_id
        direction = location.direction
        path_id = location.path_id
        path_version = location.path_version

    try:
        validate_exact_evidence_quote(snapshot.raw_text, quote.quote)
    except ValueError:
        return None, "evidence_quote_unresolved"
    quote_start = snapshot.raw_text.find(quote.quote)
    if quote_start < 0:
        return None, "evidence_quote_unresolved"

    return DraftEvent(
        event_id=uuid5(
            NAMESPACE_URL,
            f"mooby-event-draft:{snapshot.snapshot_id}:{pipeline_version}",
        ),
        source_snapshot_id=snapshot.snapshot_id,
        raw_content_id=snapshot.raw_content_id,
        pipeline_version=pipeline_version,
        event_kind=event_kind,
        station_id=event_station_id,
        line_id=line_id,
        start_station_id=start_station_id,
        end_station_id=end_station_id,
        direction=direction,
        path_id=path_id,
        path_version=path_version,
        event_at=event_time.event_at,
        event_time_raw_phrase=(
            event_time.raw_phrase if event_time.status == "parsed_candidate" else None
        ),
        event_time_method=event_time.method,
        event_time_certainty=event_time.certainty,
        title=title,
        summary=summary,
        source_platform=snapshot.platform,
        canonical_url=snapshot.canonical_url,
        source_digest=snapshot.capture_hash,
        source_published_at=snapshot.published_at,
        source_published_at_basis=snapshot.published_at_basis,
        captured_at=snapshot.captured_at,
        raw_text=snapshot.raw_text,
        evidence_quote=quote.quote,
        quote_start_char=quote_start,
        validator_version=EVIDENCE_QUOTE_RULES_VERSION,
    ), None


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
    pipeline_version: str | None = None,
    database_now: datetime,
    classifier: ClassifierAdapter,
    settings: WorkerSettings,
    station_catalog: Sequence[StationReference] | None = None,
    station_catalog_loader: Callable[[], Sequence[StationReference]] | None = None,
    path_catalog_loader: Callable[[date], Sequence[OrderedPathReference]] | None = None,
) -> ProcessingOutcome:
    """Classify and prepare a safe, non-published draft after trusted source read.

    Thresholds and accepted timestamp provenance values come from environment
    configuration; neither has an implicit default.
    """
    metadata: dict[str, Any] = {
        "code_version": PROCESSOR_CODE_VERSION,
        "keyword_rules_version": KEYWORD_RULES_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "location_rules_version": LOCATION_RULES_VERSION,
        "draft_event_rules_version": DRAFT_EVENT_RULES_VERSION,
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

    path_catalog: Sequence[OrderedPathReference] | None = None
    if station_resolution.status == "resolved" and len(station_resolution.station_ids) > 1:
        if path_catalog_loader is not None:
            path_catalog = path_catalog_loader(now_utc.date())
    location = _resolve_structured_location(
        normalized,
        station_resolution,
        station_catalog,
        path_catalog,
    )
    metadata["structured_location_candidate"] = {
        "status": location.status,
        "station_ids": list(location.station_ids),
        "line_id": location.line_id,
        "path_id": location.path_id,
        "path_version": location.path_version,
        "direction": location.direction,
        "start_station_id": location.start_station_id,
        "end_station_id": location.end_station_id,
    }

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
    required_station_ids: tuple[str, ...] | None = None
    if location.status == "station_resolved" and location.station_ids:
        required_station_ids = (location.station_ids[0],)
    elif (
        location.status == "segment_resolved"
        and location.start_station_id is not None
        and location.end_station_id is not None
    ):
        required_station_ids = (location.start_station_id, location.end_station_id)
    quote_candidate = select_exact_evidence_quote(
        snapshot.raw_text,
        resolved_station_ids=station_resolution.station_ids,
        station_catalog=tuple(station_catalog or ()),
        matched_keywords=tuple(matched_keywords),
        required_station_ids=required_station_ids,
    )
    metadata["event_time_candidate"] = {
        "status": event_time.status,
        "event_at": event_time.event_at.isoformat() if event_time.event_at else None,
        "method": event_time.method,
        "certainty": event_time.certainty,
        "timezone": EVENT_TIMEZONE.key,
        "rules_version": EVENT_TIME_RULES_VERSION,
    }
    metadata["evidence_quote_validation"] = {
        "status": quote_candidate.status,
        "sha256": quote_candidate.sha256,
        "character_count": quote_candidate.character_count,
        "rules_version": EVIDENCE_QUOTE_RULES_VERSION,
    }
    draft_event, draft_blocker = _build_draft_event(
        snapshot,
        pipeline_version=pipeline_version,
        location=location,
        station_catalog=station_catalog,
        event_time=event_time,
        quote=quote_candidate,
    )
    metadata["event_draft_persistence"] = {
        "status": "ready_for_atomic_insert" if draft_event is not None else "not_created",
        "event_id": str(draft_event.event_id) if draft_event is not None else None,
        "reason": draft_blocker,
        "category": None,
        "severity_storage": "processing_run_metadata_only",
        "verification_status": "unverified" if draft_event is not None else None,
        "publication_status": "not_eligible" if draft_event is not None else None,
    }
    blockers = []
    if location.status not in {"station_resolved", "segment_resolved"}:
        blockers.append("station_or_confirmed_path_unresolved")
    if quote_candidate.status != "validated":
        blockers.append("evidence_quote_unresolved")
    if draft_event is None and draft_blocker not in {
        None,
        "location_unresolved",
        "evidence_quote_unresolved",
    }:
        blockers.append(draft_blocker)
    metadata["event_stage_readiness"] = {
        "status": "draft_ready" if draft_event is not None else "not_ready",
        "blockers": blockers,
        "notices": (
            []
            if event_time.status == "parsed_candidate"
            else ["event_time_unknown_or_ambiguous"]
        ),
    }
    return ProcessingOutcome(
        "succeeded",
        result_metadata=metadata,
        draft_event=draft_event,
    )


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
    path_catalog_loader: Callable[[date], Sequence[OrderedPathReference]] | None = None,
) -> tuple[ProcessingOutcome, str]:
    """Process one snapshot and atomically persist its run and optional event draft."""
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
                pipeline_version=item["pipeline_version"],
                database_now=now,
                classifier=classifier,
                settings=settings,
                station_catalog=station_catalog,
                station_catalog_loader=station_catalog_loader,
                path_catalog_loader=path_catalog_loader,
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
        draft_event=outcome.draft_event,
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
    """Claim at most one item; persist eligible drafts internally without publishing."""
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
        path_catalog_loader=lambda as_of: load_confirmed_ordered_paths(
            conn, as_of=as_of
        ),
    )
    return RunOnceResult(claimed=True, outcome=outcome, run_id=run_id)
