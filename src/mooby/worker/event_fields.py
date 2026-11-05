"""Conservative, deterministic helpers for event-field preparation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import re
from typing import Literal
from zoneinfo import ZoneInfo


EVENT_TIME_RULES_VERSION = "event-time-explicit-relative-v1"
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


def _unknown() -> EventTimeCandidate:
    return EventTimeCandidate(None, None, "unknown", "unknown", "unknown")
