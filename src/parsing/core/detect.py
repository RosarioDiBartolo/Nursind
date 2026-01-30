from __future__ import annotations

import os
import re
from typing import Any, Dict, Literal

from ..timbrature_shared.day_values import extract_day_values
from ..timbrature_shared.utils import normalize_text, parse_day_header
from .errors import DetectionError

CARTELLINO_DAY_RE = re.compile(
    r"^(?P<day>0[1-9]|[12][0-9]|3[01])\s+(?P<dow>LU|MA|ME|GI|VE|SA|DO)\b"
)
EVENT_RE = re.compile(r"[EU]\s*\d{2}:\d{2}", re.IGNORECASE)

CARTELLINO_HINTS = (
    "cartellino mensile",
    "ore lavorate",
    "ore dovute programmate",
    "ore dovute contrattuali",
    "db/cr netto",
)
TIMBRATURE_HINTS = (
    "elenco timbrature",
    "totali mensili nel mese di",
    "matricola",
    "utente ",
)

DocFamily = Literal["cartellino", "timbrature"]
TimbratureVariant = Literal["timbrature_elenco", "timbrature_compact"]


def _is_strict_detection() -> bool:
    return os.getenv("PARSER_DETECT_STRICT", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _count_day_lines(lines: list[str]) -> tuple[int, int]:
    cartellino_hits = 0
    timbrature_hits = 0
    for line in lines:
        raw = line.strip()
        if not raw:
            continue
        if CARTELLINO_DAY_RE.match(raw):
            cartellino_hits += 1
        if parse_day_header(raw) and EVENT_RE.search(raw):
            timbrature_hits += 1
    return cartellino_hits, timbrature_hits


def detect_document_family(text: str, strict: bool | None = None) -> DocFamily:
    if strict is None:
        strict = True
    norm = normalize_text(text)
    lines = text.splitlines()

    cart_hint_hits = sum(1 for hint in CARTELLINO_HINTS if hint in norm)
    timb_hint_hits = sum(1 for hint in TIMBRATURE_HINTS if hint in norm)
    cart_day_hits, timb_day_hits = _count_day_lines(lines)

    score_cart = cart_hint_hits * 5 + cart_day_hits
    score_timb = timb_hint_hits * 5 + timb_day_hits

    if score_cart == 0 and score_timb == 0:
        raise DetectionError(
            "Unable to detect parser type: no cartellino or timbrature markers found."
        )
    if strict and abs(score_cart - score_timb) <= 2:
        raise DetectionError(
            "Ambiguous parser type: cartellino score="
            f"{score_cart}, timbrature score={score_timb}."
        )
    return "cartellino" if score_cart >= score_timb else "timbrature"


def _timbrature_compact_signals(text: str) -> tuple[int, int, int]:
    day_lines = 0
    event_lines = 0
    compact_hits = 0
    for line in text.splitlines():
        extracted = extract_day_values(line)
        if not extracted:
            continue
        _day, _dow, values, has_event = extracted
        day_lines += 1
        if not has_event:
            continue
        event_lines += 1
        if len(values) < 2:
            continue
        large = [value for value in values if value >= 1.0]
        if len(large) == 1:
            compact_hits += 1
    return day_lines, event_lines, compact_hits


def detect_timbrature_variant(text: str, strict: bool | None = None) -> TimbratureVariant:
    if strict is None:
        strict = _is_strict_detection()
    day_lines, event_lines, compact_hits = _timbrature_compact_signals(text)
    if compact_hits >= 3:
        return "timbrature_compact"
    if event_lines == 0:
        if not strict:
            return "timbrature_compact"
    if compact_hits == 0 and event_lines >= 5:
        return "timbrature_elenco"
    if not strict:
        return "timbrature_compact"
    raise DetectionError(
        "Ambiguous timbrature variant: "
        f"day_lines={day_lines}, event_lines={event_lines}, "
        f"compact_signals={compact_hits}."
    )


def analyze_detection(text: str) -> Dict[str, Any]:
    norm = normalize_text(text)
    lines = text.splitlines()

    cart_hint_hits = sum(1 for hint in CARTELLINO_HINTS if hint in norm)
    timb_hint_hits = sum(1 for hint in TIMBRATURE_HINTS if hint in norm)
    cart_day_hits, timb_day_hits = _count_day_lines(lines)

    score_cart = cart_hint_hits * 5 + cart_day_hits
    score_timb = timb_hint_hits * 5 + timb_day_hits

    if score_cart == 0 and score_timb == 0:
        detected_family: str | None = "none"
    elif abs(score_cart - score_timb) <= 2:
        detected_family = "ambiguous"
    else:
        detected_family = "cartellino" if score_cart > score_timb else "timbrature"

    day_lines, event_lines, compact_hits = _timbrature_compact_signals(text)

    return {
        "text_length": len(text),
        "line_count": len(lines),
        "cart_hint_hits": cart_hint_hits,
        "timb_hint_hits": timb_hint_hits,
        "cart_day_hits": cart_day_hits,
        "timb_day_hits": timb_day_hits,
        "score_cart": score_cart,
        "score_timb": score_timb,
        "detected_family": detected_family,
        "timbrature_signals": {
            "day_lines": day_lines,
            "event_lines": event_lines,
            "compact_hits": compact_hits,
        },
    }


__all__ = [
    "DocFamily",
    "TimbratureVariant",
    "CARTELLINO_HINTS",
    "TIMBRATURE_HINTS",
    "CARTELLINO_DAY_RE",
    "EVENT_RE",
    "detect_document_family",
    "detect_timbrature_variant",
    "analyze_detection",
]
