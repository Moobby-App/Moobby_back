"""Conservative, deterministic helpers for event-field preparation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
import re
import unicodedata
from typing import Any, Literal, Sequence
from zoneinfo import ZoneInfo


EVENT_TIME_RULES_VERSION = "event-time-explicit-relative-v1"
EVIDENCE_QUOTE_RULES_VERSION = "exact-sentence-station-keyword-v1"
EVENT_TIMEZONE = ZoneInfo("Asia/Jakarta")

_ISO_DATETIME = re.compile(
    r"(?<!\d)"
    r"(?P<date>\d{4}-\d{2}-\d{2})[ T]"
    r"(?P<time>\d{2}:\d{2}(?::\d{2})?)"
    r"(?:\s*(?P<zone>Z|UTC|WIB|[+-]\d{2}:\d{2}))?"
    r"(?!\d)",
    re.IGNORECASE,
)
_LOCAL_DATETIME = re.compile(
    r"(?<!\d)"
    r"(?P<date>\d{1,2}[/-]\d{1,2}[/-]\d{4})\s+"
    r"(?:(?:pukul|jam)\s*)?"
    r"(?P<time>\d{1,2}[.:]\d{2}(?::\d{2})?)"
    r"(?:\s*(?P<zone>WIB|UTC|Z|[+-]\d{2}:\d{2}))?"
    r"(?!\d)",
    re.IGNORECASE,
)
_RELATIVE_DURATION = re.compile(
    r"(?<!\w)(?P<amount>\d{1,3})\s*"
    r"(?P<unit>detik|menit|jam|hari)\s+"
    r"(?:yang\s+)?lalu(?!\w)",
    re.IGNORECASE,
)
_VAGUE_RELATIVE_TIME = re.compile(
    r"(?<!\w)(?:barusan|baru\s+saja|tadi(?:\s+(?:pagi|siang|sore|malam))?|"
    r"kemarin(?:\s+(?:pagi|siang|sore|malam))?)(?!\w)",
    re.IGNORECASE,
)
_APPROXIMATE_RELATIVE_PREFIX = re.compile(
    r"(?:sekitar|kira-kira|kurang\s+lebih|lebih\s+dari|hampir)\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class EventTimeCandidate:
    """A parsed candidate; this alone does not establish event truth."""

    event_at: datetime | None
    raw_phrase: str | None
    method: Literal["explicit", "relative_phrase", "unknown"]
    certainty: Literal["high", "medium", "unknown"]
    status: Literal["parsed_candidate", "unknown", "ambiguous"]


@dataclass(frozen=True)
class EvidenceQuoteCandidate:
    """An in-memory exact quote candidate; quote text is not run metadata."""

    quote: str | None
    status: Literal["validated", "unresolved", "ambiguous"]
    sha256: str | None
    character_count: int | None


def extract_event_time_candidate(
    raw_text: str,
    published_at: datetime | None,
) -> EventTimeCandidate:
    """Parse only unambiguous full datetimes or numeric relative durations.

    Relative durations are anchored to trusted source publication time in
    Asia/Jakarta. Vague phrases and time-only mentions remain unresolved.
    """
    if not isinstance(raw_text, str) or not raw_text.strip():
        return _unknown()

    candidates: list[EventTimeCandidate] = []
    for match in _ISO_DATETIME.finditer(raw_text):
        parsed = _parse_explicit_datetime(
            match.group("date"), match.group("time"), match.group("zone")
        )
        if parsed is not None:
            candidates.append(
                EventTimeCandidate(
                    parsed, match.group(0), "explicit", "medium", "parsed_candidate"
                )
            )

    for match in _LOCAL_DATETIME.finditer(raw_text):
        parsed = _parse_explicit_datetime(
            match.group("date"), match.group("time"), match.group("zone"),
            local_date_format=True,
        )
        if parsed is not None:
            candidates.append(
                EventTimeCandidate(
                    parsed, match.group(0), "explicit", "medium", "parsed_candidate"
                )
            )

    relative_matches = list(_RELATIVE_DURATION.finditer(raw_text))
    vague_matches = list(_VAGUE_RELATIVE_TIME.finditer(raw_text))
    if vague_matches and relative_matches:
        return EventTimeCandidate(None, None, "unknown", "unknown", "ambiguous")
    if vague_matches and not relative_matches:
        if candidates:
            return EventTimeCandidate(None, None, "unknown", "unknown", "ambiguous")
        return EventTimeCandidate(
            None,
            vague_matches[0].group(0) if len(vague_matches) == 1 else None,
            "unknown",
            "unknown",
            "unknown" if len(vague_matches) == 1 else "ambiguous",
        )
    if relative_matches:
        if len(relative_matches) == 1 and _aware(published_at):
            match = relative_matches[0]
            prefix = raw_text[max(0, match.start() - 32) : match.start()]
            if _APPROXIMATE_RELATIVE_PREFIX.search(prefix):
                return EventTimeCandidate(
                    None, match.group(0), "unknown", "unknown", "unknown"
                )
            amount = int(match.group("amount"))
            unit = match.group("unit").casefold()
            delta = {
                "detik": timedelta(seconds=amount),
                "menit": timedelta(minutes=amount),
                "jam": timedelta(hours=amount),
                "hari": timedelta(days=amount),
            }[unit]
            event_at = published_at.astimezone(EVENT_TIMEZONE) - delta
            candidates.append(
                EventTimeCandidate(
                    event_at,
                    match.group(0),
                    "relative_phrase",
                    "medium",
                    "parsed_candidate",
                )
            )
        else:
            return EventTimeCandidate(
                None,
                relative_matches[0].group(0) if len(relative_matches) == 1 else None,
                "unknown",
                "unknown",
                "ambiguous" if len(relative_matches) > 1 else "unknown",
            )

    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        return EventTimeCandidate(None, None, "unknown", "unknown", "ambiguous")

    return _unknown()


def validate_exact_evidence_quote(raw_text: str, quote: str) -> None:
    """Raise ValueError unless quote is a non-empty exact source substring."""
    if not isinstance(raw_text, str) or not raw_text:
        raise ValueError("source text is missing")
    if not isinstance(quote, str) or not quote:
        raise ValueError("evidence quote is missing")
    if quote not in raw_text:
        raise ValueError("evidence quote is not an exact source substring")


def select_exact_evidence_quote(
    raw_text: str,
    *,
    resolved_station_ids: tuple[str, ...],
    station_catalog: Sequence[Any],
    matched_keywords: Sequence[str],
    required_station_ids: Sequence[str] | None = None,
) -> EvidenceQuoteCandidate:
    """Select one original sentence containing a resolved station and keyword.

    Matching uses normalized sentence copies, while the returned quote is the
    unchanged source slice. No quote is selected when zero or multiple source
    sentences satisfy the rule.
    """
    if not isinstance(raw_text, str) or not raw_text.strip():
        return EvidenceQuoteCandidate(None, "unresolved", None, None)
    if not resolved_station_ids or not matched_keywords:
        return EvidenceQuoteCandidate(None, "unresolved", None, None)

    allowed_station_ids = set(
        resolved_station_ids if required_station_ids is None else required_station_ids
    )
    station_names: set[str] = set()
    names_by_station: dict[str, set[str]] = {}
    for station in station_catalog:
        station_id = getattr(station, "station_id", None)
        if station_id not in allowed_station_ids:
            continue
        for name in (
            getattr(station, "canonical_name", None),
            *getattr(station, "aliases", ()),
        ):
            if isinstance(name, str) and name.strip():
                normalized_name = _normalize_for_matching(name)
                station_names.add(normalized_name)
                names_by_station.setdefault(station_id, set()).add(normalized_name)

    required_name_groups = (
        [names_by_station.get(station_id, set()) for station_id in required_station_ids]
        if required_station_ids is not None
        else None
    )
    if required_name_groups is not None and any(not group for group in required_name_groups):
        return EvidenceQuoteCandidate(None, "unresolved", None, None)

    keyword_values = tuple(_normalize_for_matching(value) for value in matched_keywords)
    candidates: list[str] = []
    start = 0
    for separator in re.finditer(r"(?<!Univ)[.!?…\n]+", raw_text, re.IGNORECASE):
        end = separator.end()
        _append_matching_sentence(
            raw_text, start, end, station_names, keyword_values, candidates,
            required_name_groups,
        )
        start = end
    _append_matching_sentence(
        raw_text, start, len(raw_text), station_names, keyword_values, candidates,
        required_name_groups,
    )

    if not candidates:
        return EvidenceQuoteCandidate(None, "unresolved", None, None)
    unique_candidates = list(dict.fromkeys(candidates))
    if len(unique_candidates) != 1:
        return EvidenceQuoteCandidate(None, "ambiguous", None, None)

    quote = unique_candidates[0]
    try:
        validate_exact_evidence_quote(raw_text, quote)
    except ValueError:
        return EvidenceQuoteCandidate(None, "unresolved", None, None)
    return EvidenceQuoteCandidate(
        quote=quote,
        status="validated",
        sha256=hashlib.sha256(quote.encode("utf-8")).hexdigest(),
        character_count=len(quote),
    )


def _append_matching_sentence(
    raw_text: str,
    start: int,
    end: int,
    station_names: set[str],
    matched_keywords: tuple[str, ...],
    candidates: list[str],
    required_name_groups: Sequence[set[str]] | None = None,
) -> None:
    source_sentence = raw_text[start:end]
    left_trimmed = len(source_sentence) - len(source_sentence.lstrip())
    right_trimmed = len(source_sentence.rstrip())
    if right_trimmed <= left_trimmed:
        return
    quote_start = start + left_trimmed
    quote_end = start + right_trimmed
    quote = raw_text[quote_start:quote_end]
    normalized = _normalize_for_matching(quote)

    if required_name_groups is None:
        station_found = any(
            re.search(rf"(?<!\w){re.escape(name)}(?!\w)", normalized)
            for name in station_names
        )
    else:
        station_found = all(
            any(
                re.search(rf"(?<!\w){re.escape(name)}(?!\w)", normalized)
                for name in names
            )
            for names in required_name_groups
        )
    keyword_found = any(
        _contains_keyword(normalized, keyword) for keyword in matched_keywords
    )
    if station_found and keyword_found:
        candidates.append(quote)


def _parse_explicit_datetime(
    date_text: str,
    time_text: str,
    zone_text: str | None,
    *,
    local_date_format: bool = False,
) -> datetime | None:
    try:
        if local_date_format:
            date_format = "%d/%m/%Y" if "/" in date_text else "%d-%m-%Y"
            date_value = datetime.strptime(date_text, date_format).date()
            normalized_time = time_text.replace(".", ":")
            time_format = "%H:%M:%S" if normalized_time.count(":") == 2 else "%H:%M"
            time_value = datetime.strptime(normalized_time, time_format).time()
        else:
            date_value = datetime.strptime(date_text, "%Y-%m-%d").date()
            time_format = "%H:%M:%S" if time_text.count(":") == 2 else "%H:%M"
            time_value = datetime.strptime(time_text, time_format).time()
    except ValueError:
        return None

    zone = (zone_text or "").upper()
    naive = datetime.combine(date_value, time_value)
    if zone in {"", "WIB"}:
        aware = naive.replace(tzinfo=EVENT_TIMEZONE)
    elif zone in {"Z", "UTC"}:
        aware = naive.replace(tzinfo=ZoneInfo("UTC"))
    else:
        try:
            aware = datetime.fromisoformat(f"{naive.isoformat()}{zone}")
        except ValueError:
            return None
    return aware.astimezone(EVENT_TIMEZONE)


def _aware(value: datetime | None) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() is not None
    )


def _normalize_for_matching(text: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", text).casefold().split())


def _contains_keyword(normalized_text: str, keyword: str) -> bool:
    return (
        re.search(rf"(?<!\w){re.escape(keyword)}(?!\w)", normalized_text)
        is not None
    )


def _unknown() -> EventTimeCandidate:
    return EventTimeCandidate(None, None, "unknown", "unknown", "unknown")
