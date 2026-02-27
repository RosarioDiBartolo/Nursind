from __future__ import annotations

import re

from src.raw_text_parsing import DAY_PREFIX_RE, QTA_RE, normalize_text

from .base import ParseValues

NUMBER_RE = re.compile(r"^[+-]?\d+(?:[.,]\d+)?$")
HHMM_RE = re.compile(r"^[+-]?\d{1,3}:\d{2}$")
DAY_PREFIX_OCR_RE = re.compile(r"^\s*\d{4}\s*[a-z\.]+")


def _format_token_sign(token: str) -> str:
    clean = token.strip().strip("|,;!")
    if not clean:
        return ""
    if clean.endswith("-") and not clean.startswith("-"):
        clean = f"-{clean[:-1]}"
    if clean.endswith("+") and not clean.startswith("+"):
        clean = f"+{clean[:-1]}"
    return clean


def parse_hhmm(token: str) -> float | None:
    clean = _format_token_sign(token)
    sign = -1.0 if clean.startswith("-") else 1.0
    clean = clean.lstrip("+-")
    if not HHMM_RE.fullmatch(clean):
        return None
    hour_s, minute_s = clean.split(":")
    hour = int(hour_s)
    minute = int(minute_s)
    if not (0 <= minute <= 59):
        return None
    return sign * (hour + minute / 60.0)


def parse_decimal(token: str) -> float | None:
    clean = _format_token_sign(token)
    if not NUMBER_RE.fullmatch(clean):
        return None
    normalized = clean.replace(",", ".")
    value = float(normalized)
    if "." not in normalized:
        return value

    sign = -1.0 if value < 0 else 1.0
    abs_value = abs(value)
    hours = int(abs_value)
    minutes = int(round((abs_value - hours) * 100))
    if 0 <= minutes <= 59:
        return sign * (hours + minutes / 60.0)
    return value


def parse_numeric_token(
    token: str,
    *,
    allow_hhmm: bool,
    max_abs: float | None = None,
) -> float | None:
    parsed = parse_hhmm(token) if allow_hhmm else None
    if parsed is None:
        parsed = parse_decimal(token)
    if parsed is None:
        return None
    if max_abs is not None and abs(parsed) > max_abs:
        return None
    return parsed


def extract_leading_values(
    value_text: str,
    *,
    allow_hhmm: bool,
    max_abs: float | None = None,
) -> list[float]:
    values: list[float] = []
    for token in value_text.split():
        parsed = parse_numeric_token(token, allow_hhmm=allow_hhmm, max_abs=max_abs)
        if parsed is not None:
            values.append(parsed)
    return values


def extract_trailing_values(
    value_text: str,
    *,
    allow_hhmm: bool,
    max_abs: float | None = None,
) -> list[float]:
    tokens = value_text.split()
    out_rev: list[float] = []
    collecting = False
    for raw_token in reversed(tokens):
        parsed = parse_numeric_token(raw_token, allow_hhmm=allow_hhmm, max_abs=max_abs)
        if parsed is None:
            if collecting:
                break
            continue
        collecting = True
        out_rev.append(parsed)
    out_rev.reverse()
    return out_rev


def extract_all_values(
    value_text: str,
    *,
    allow_hhmm: bool,
    max_abs: float | None = None,
) -> list[float]:
    values: list[float] = []
    for token in value_text.split():
        parsed = parse_numeric_token(token, allow_hhmm=allow_hhmm, max_abs=max_abs)
        if parsed is not None:
            values.append(parsed)
    return values


def strip_day_prefix(value_text: str) -> str:
    trimmed = DAY_PREFIX_RE.sub("", value_text, count=1)
    if trimmed != value_text:
        return trimmed
    return DAY_PREFIX_OCR_RE.sub("", value_text, count=1)


def strip_day_prefix_and_qta(value_text: str) -> str:
    rest = strip_day_prefix(value_text)
    return QTA_RE.sub("", rest)


def split_bang_segments(value_text: str) -> list[str]:
    return [segment.strip() for segment in value_text.split("!") if segment.strip()]


def finalize_presence_values(
    contratt: float | None,
    lavorato: float | None,
    *,
    has_event: bool,
    any_event: bool,
) -> ParseValues:
    mo_f = 0.0 if contratt is None else contratt
    mo_t = 0.0 if lavorato is None else lavorato
    mo_lav = mo_t
    if any_event and not has_event:
        mo_lav = 0.0
    return ParseValues(mo_f=mo_f, mo_t=mo_t, mo_lav=mo_lav)


def assign_timbrature(values: list[float], *, has_event: bool, any_event: bool) -> ParseValues:
    contratt: float | None = None
    lavorato: float | None = None

    if values:
        if not has_event:
            contratt = values[0]
            lavorato = values[1] if len(values) >= 2 else values[0]
        elif len(values) >= 2:
            contratt = values[0]
            lavorato = values[1]
        else:
            lavorato = values[0]
    return finalize_presence_values(
        contratt,
        lavorato,
        has_event=has_event,
        any_event=any_event,
    )


def assign_situazione(values: list[float], *, has_event: bool, any_event: bool) -> ParseValues:
    contratt: float | None = None
    lavorato: float | None = None
    if len(values) >= 3:
        contratt = values[1] if len(values) >= 4 else values[0]
        lavorato = values[2]
    elif len(values) == 2:
        contratt, lavorato = values[0], values[1]
    elif len(values) == 1:
        contratt, lavorato = values[0], values[0]
    return finalize_presence_values(
        contratt,
        lavorato,
        has_event=has_event,
        any_event=any_event,
    )


def assign_cartellino(values: list[float]) -> ParseValues:
    if len(values) >= 3:
        mo_f, mo_t, mo_lav = values[-3], values[-2], values[-1]
    elif len(values) == 2:
        mo_f, mo_t, mo_lav = values[0], values[1], values[1]
    elif len(values) == 1:
        mo_f, mo_t, mo_lav = values[0], values[0], values[0]
    else:
        mo_f, mo_t, mo_lav = None, None, None
    return ParseValues(mo_f=mo_f, mo_t=mo_t, mo_lav=mo_lav)


def normalized_raw(raw: str) -> str:
    return normalize_text(raw)
