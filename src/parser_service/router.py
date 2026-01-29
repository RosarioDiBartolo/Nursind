from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, Literal

from cartellino_parser.parser import parse_text as parse_cartellino_text
from parser_shared.extract import extract_text, extract_text_vertical
from parser_shared.models import CartellinoParseError, ParserDetectionError, ParsedCartellino
from timbrature_elenco_compact_parser.parser import parse_text as parse_compact_text
from timbrature_elenco_parser.parser import parse_text as parse_elenco_text
from timbrature_shared.day_values import extract_day_values
from timbrature_shared.utils import normalize_text, parse_day_header

LOGGER = logging.getLogger(__name__)

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


def detect_document_family(text: str) -> DocFamily:
    norm = normalize_text(text)
    lines = text.splitlines()

    cart_hint_hits = sum(1 for hint in CARTELLINO_HINTS if hint in norm)
    timb_hint_hits = sum(1 for hint in TIMBRATURE_HINTS if hint in norm)
    cart_day_hits, timb_day_hits = _count_day_lines(lines)

    score_cart = cart_hint_hits * 5 + cart_day_hits
    score_timb = timb_hint_hits * 5 + timb_day_hits

    if score_cart == 0 and score_timb == 0:
        raise ParserDetectionError(
            "Unable to detect parser type: no cartellino or timbrature markers found."
        )
    if abs(score_cart - score_timb) <= 2:
        raise ParserDetectionError(
            "Ambiguous parser type: cartellino score="
            f"{score_cart}, timbrature score={score_timb}."
        )
    return "cartellino" if score_cart > score_timb else "timbrature"


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
            LOGGER.warning(
                "Ambiguous timbrature variant (no events). "
                "Defaulting to timbrature_compact: "
                "day_lines=%s, event_lines=%s, compact_signals=%s.",
                day_lines,
                event_lines,
                compact_hits,
            )
            return "timbrature_compact"
    if compact_hits == 0 and event_lines >= 5:
        return "timbrature_elenco"
    if not strict:
        LOGGER.warning(
            "Ambiguous timbrature variant. Defaulting to timbrature_compact: "
            "day_lines=%s, event_lines=%s, compact_signals=%s.",
            day_lines,
            event_lines,
            compact_hits,
        )
        return "timbrature_compact"
    raise ParserDetectionError(
        "Ambiguous timbrature variant: "
        f"day_lines={day_lines}, event_lines={event_lines}, "
        f"compact_signals={compact_hits}."
    )


def parse_text(text: str, source: object | None = None) -> ParsedCartellino:
    family = detect_document_family(text)
    if family == "cartellino":
        return parse_cartellino_text(text, source)

    variant = detect_timbrature_variant(text)
    if variant == "timbrature_compact":
        return parse_compact_text(text, source)
    return parse_elenco_text(text, source)


def parse_pdf(source) -> ParsedCartellino:
    text = extract_text(source)
    detect_info = analyze_detection(text)
    if detect_info["score_cart"] == 0 and detect_info["score_timb"] == 0:
        
        text = extract_text_vertical(source)
    try:
        return parse_text(text, source)
    except ParserDetectionError as exc:
        message = str(exc).lower()
        if "no cartellino or timbrature markers found" not in message:
            raise
        LOGGER.info(
            "No detection markers found after parse for %s; trying vertical reconstruction",
            source,
        )
        text = extract_text_vertical(source)
        return parse_text(text, source)
    except CartellinoParseError as exc:
        if "No day lines found" not in str(exc):
            raise
        LOGGER.info(
            "No day lines found after parse for %s; trying vertical reconstruction",
            source,
        )
        text = extract_text_vertical(source)
        return parse_text(text, source)

 
