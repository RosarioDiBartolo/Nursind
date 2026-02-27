from __future__ import annotations

"""Shared parsing helpers for raw extracted payroll/timbrature text."""

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

DOW_PREFIXES: tuple[tuple[str, str], ...] = (
    ("luned", "LU"),
    ("lun", "LU"),
    ("lu", "LU"),
    ("marted", "MA"),
    ("mar", "MA"),
    ("ma", "MA"),
    ("mercoled", "ME"),
    ("merc", "ME"),
    ("mer", "ME"),
    ("me", "ME"),
    ("gioved", "GI"),
    ("giov", "GI"),
    ("gio", "GI"),
    ("gi", "GI"),
    ("venerd", "VE"),
    ("ven", "VE"),
    ("ve", "VE"),
    ("sabato", "SA"),
    ("sab", "SA"),
    ("sa", "SA"),
    ("domenica", "DO"),
    ("dom", "DO"),
    ("do", "DO"),
)
VALID_DOW = {"LU", "MA", "ME", "GI", "VE", "SA", "DO"}

MONTH_WORDS = {
    "gennaio": 1,
    "genn": 1,
    "febbraio": 2,
    "febb": 2,
    "marzo": 3,
    "mar": 3,
    "aprile": 4,
    "apr": 4,
    "maggio": 5,
    "mag": 5,
    "giugno": 6,
    "giu": 6,
    "luglio": 7,
    "lug": 7,
    "agosto": 8,
    "ago": 8,
    "settembre": 9,
    "sett": 9,
    "settembr": 9,
    "settembe": 9,
    "settebre": 9,
    "ottobre": 10,
    "ott": 10,
    "novembre": 11,
    "nov": 11,
    "dicembre": 12,
    "dic": 12,
}

DAY_HEADER_RE = re.compile(
    r"^\s*(?P<day>0?[1-9]|[12]\d|3[01])\s*(?P<dow>[a-z\.]{2,}?)(?=\s|e\s|u\s|e\d|u\d|$)"
)
DAY_PREFIX_RE = re.compile(r"^\s*\d{1,2}\s*[a-z\.]+")
QTA_RE = re.compile(r"qta\s*:?\s*\d{1,3}[.,:]\d{2}", re.IGNORECASE)

EVENT_CANDIDATE_RE = re.compile(
    r"""
    (?:
        \b[eu]\s*(?:\(|-)?\s*[0-2]?\d:[0-5]\d\s*(?:\)|-)?[a-z]?\b
        |
        \d{1,2}:\d{2}\s*[eu]\s*(?:\(|-)?\s*[0-2]?\d:[0-5]\d
    )
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)


@dataclass(frozen=True)
class EventMatch:
    kind: str
    time_str: str
    start: int
    end: int
    pattern: str


EVENT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "paren",
        re.compile(r"(?<!\w)(?P<kind>[EU])\s*\(\s*(?P<time>[0-2]?\d:[0-5]\d)\s*\)"),
    ),
    (
        "dash",
        re.compile(r"(?<!\w)(?P<kind>[EU])\s*-\s*(?P<time>[0-2]?\d:[0-5]\d)\s*-"),
    ),
    (
        "plain",
        re.compile(r"(?<!\w)(?P<kind>[EU])\s+(?P<time>[0-2]?\d:[0-5]\d)(?:[A-DF-TV-Z])?"),
    ),
    (
        "compact",
        re.compile(r"(?<!\w)(?P<kind>[EU])(?P<time>[0-2]?\d:[0-5]\d)(?:[A-DF-TV-Z])?"),
    ),
    (
        "glued_trailing",
        re.compile(
            r"(?:(?<=\d:[0-5]\d)|(?<=\d\d:[0-5]\d))(?P<kind>[EU])\s*(?P<time>[0-2]?\d:[0-5]\d)"
        ),
    ),
    (
        "generic",
        re.compile(
            r"(?<!\w)(?P<kind>[EU])\s*(?:\(|-)?\s*(?P<time>[0-2]?\d:[0-5]\d)\s*(?:\)|-)?(?:[A-DF-TV-Z])?"
        ),
    ),
]

HEADER_MESI_PATTERN = re.compile(
    r"m+e+s+e+\s+d+i+\s*:?\s*(?P<month>[a-z]{3,40})\s+(?P<year>\d{2,10})\b",
    flags=re.IGNORECASE,
)
HEADER_DATE_PATTERN = re.compile(
    r"\b(?P<day>\d{1,2})[/-](?P<month>\d{1,2})[/-](?P<year>\d{2,4})\b"
)


def strip_accents(value: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", value) if not unicodedata.combining(ch)
    )


def normalize_text(value: str) -> str:
    value = strip_accents(value)
    value = value.lower()
    return " ".join(value.split())


def alpha_token(value: str) -> str:
    return re.sub(r"[^a-z]", "", normalize_text(value))


def to_dow(value: str) -> str | None:
    token = alpha_token(value)
    if not token:
        return None
    for candidate in (token, _collapse_ocr_doubled_token(token)):
        for prefix, code in DOW_PREFIXES:
            if candidate.startswith(prefix):
                return code
    return None


def _normalize_ocr_day_prefix(line: str) -> str:
    match = re.match(r"^\s*(?P<digits>\d{4})(?P<tail>.*)$", line)
    if not match:
        return line
    digits = match.group("digits")
    if not (digits[0] == digits[1] and digits[2] == digits[3]):
        return line
    collapsed = f"{digits[0]}{digits[2]}"
    try:
        day = int(collapsed)
    except Exception:
        return line
    if not (1 <= day <= 31):
        return line
    return f"{collapsed}{match.group('tail')}"


def parse_day_header(line: str) -> tuple[int, str] | None:
    norm = _normalize_ocr_day_prefix(normalize_text(line))
    match = DAY_HEADER_RE.match(norm)
    if not match:
        return None
    day = int(match.group("day"))
    dow = to_dow(match.group("dow"))
    if dow not in VALID_DOW:
        return None
    return day, dow


def month_from_word(value: str) -> int | None:
    token = alpha_token(value)
    if not token:
        return None
    if token in MONTH_WORDS:
        return MONTH_WORDS[token]
    for key, month in MONTH_WORDS.items():
        if token.startswith(key) or key.startswith(token):
            return month
    return None


def coerce_year(value: str | int) -> int | None:
    try:
        year = int(value)
    except Exception:
        return None
    if year < 100:
        year = 2000 + year
    if 1900 <= year <= 2100:
        return year
    return None


def _collapse_ocr_doubled_token(token: str) -> str:
    if len(token) < 4 or len(token) % 2 != 0:
        return token
    pair_count = len(token) // 2
    doubled_pairs = 0
    for idx in range(0, len(token), 2):
        if token[idx] == token[idx + 1]:
            doubled_pairs += 1
    if pair_count == 0:
        return token
    if (doubled_pairs / pair_count) < 0.75:
        return token
    return token[::2]


def _month_from_header_token(value: str) -> int | None:
    month = month_from_word(value)
    if month is not None:
        return month
    token = alpha_token(value)
    if not token:
        return None
    collapsed = _collapse_ocr_doubled_token(token)
    if collapsed == token:
        return None
    return month_from_word(collapsed)


def _coerce_year_from_header_token(value: str) -> int | None:
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None
    year = coerce_year(digits)
    if year is not None:
        return year
    collapsed = _collapse_ocr_doubled_token(digits)
    if collapsed == digits:
        return None
    return coerce_year(collapsed)


def infer_year_month_from_header(text: str) -> tuple[int | None, int | None]:
    for raw_line in text.splitlines()[:16]:
        norm_line = normalize_text(raw_line)
        match = HEADER_MESI_PATTERN.search(norm_line)
        if not match:
            continue
        month = _month_from_header_token(match.group("month"))
        year = _coerce_year_from_header_token(match.group("year"))
        if month is not None and year is not None:
            return year, month
    return None, None


def infer_year_month_from_header_date(text: str) -> tuple[int | None, int | None]:
    for raw_line in text.splitlines()[:8]:
        norm_line = normalize_text(raw_line)
        match = HEADER_DATE_PATTERN.search(norm_line)
        if not match:
            continue
        month = int(match.group("month"))
        year = coerce_year(match.group("year"))
        if year is not None and 1 <= month <= 12:
            return year, month
    return None, None


def detect_doc_format(text: str) -> str:
    norm = normalize_text(text)
    if "riepilogo presenze/assenze" in norm:
        return "cartellino_classic"
    if "elenco timbrature" in norm:
        return "timbrature_web"
    if "situazione mensile presenze" in norm:
        return "situazione_mensile"

    short_dow = 0
    long_dow = 0
    for line in text.splitlines():
        norm_line = normalize_text(line)
        match = DAY_HEADER_RE.match(norm_line)
        if not match:
            continue
        token = alpha_token(match.group("dow"))
        if token in {"lu", "ma", "me", "gi", "ve", "sa", "do"}:
            short_dow += 1
        else:
            long_dow += 1
    if short_dow > 0 or long_dow > 0:
        return "cartellino_classic" if short_dow >= long_dow else "timbrature_web"
    return "unknown"


def infer_year_month_from_text(text: str) -> tuple[int | None, int | None]:
    norm = normalize_text(text)

    strong_patterns = (
        r"riepilogo presenze/assenze\s*-\s*(?P<month>[a-z]+)\s+(?P<year>\d{4})",
        r"elenco timbrature\s*-\s*(?P<month>[a-z]+)\s+(?P<year>\d{4})",
        r"totali mensili nel mese di[^\n\r]{0,60}(?P<month>[a-z]+)\s+(?P<year>\d{4})",
    )
    for pattern in strong_patterns:
        match = re.search(pattern, norm, flags=re.IGNORECASE)
        if not match:
            continue
        month = month_from_word(match.group("month"))
        year = coerce_year(match.group("year"))
        if month is not None and year is not None:
            return year, month

    generic_patterns = (
        r"\b(?P<year>\d{4})\s+(?P<month>[a-z]{3,15})\b",
        r"\b(?P<month>[a-z]{3,15})\s+(?P<year>\d{4})\b",
    )
    for pattern in generic_patterns:
        for match in re.finditer(pattern, norm, flags=re.IGNORECASE):
            month = month_from_word(match.group("month"))
            year = coerce_year(match.group("year"))
            if month is not None and year is not None:
                return year, month
    return None, None


def infer_year_month_from_filename(path: Path | str) -> tuple[int | None, int | None]:
    source = path if isinstance(path, Path) else Path(path)
    stem = normalize_text(source.stem)

    patterns = (
        r"(?P<year>\d{4})[-_/ ](?P<month>\d{1,2})",
        r"(?P<month>\d{1,2})[-_/ ](?P<year>\d{2,4})",
        r"(?P<month>[a-z]{3,15})[-_/ ](?P<year>\d{4})",
        r"(?P<year>\d{4})[-_/ ](?P<month>[a-z]{3,15})",
        r"(?P<month>[a-z]{3,15})(?P<year>\d{2,4})",
        r"(?P<month>\d{1,2})(?P<year>\d{4})",
    )
    for pattern in patterns:
        match = re.search(pattern, stem, flags=re.IGNORECASE)
        if not match:
            continue
        year = coerce_year(match.group("year"))
        month_group = match.group("month")
        month: int | None
        if month_group.isdigit():
            month = int(month_group)
        else:
            month = month_from_word(month_group)
        if year is not None and month is not None and 1 <= month <= 12:
            return year, month
    return None, None


def resolve_year_month(text: str, path: Path) -> tuple[int | None, int | None]:
    year, month = infer_year_month_from_text(text)
    if year is not None and month is not None:
        return year, month
    year, month = infer_year_month_from_header(text)
    if year is not None and month is not None:
        return year, month
    year, month = infer_year_month_from_filename(path)
    if year is not None and month is not None:
        return year, month
    return infer_year_month_from_header_date(text)


def line_has_event(raw: str) -> bool:
    return bool(EVENT_CANDIDATE_RE.search(normalize_text(raw)))


def _overlaps(existing: list[tuple[int, int]], start: int, end: int) -> bool:
    for s, e in existing:
        if not (end <= s or start >= e):
            return True
    return False


def extract_events(raw: str) -> list[EventMatch]:
    spans: list[tuple[int, int]] = []
    events: list[EventMatch] = []
    for pattern_name, regex in EVENT_PATTERNS:
        for match in regex.finditer(raw):
            start, end = match.span()
            if _overlaps(spans, start, end):
                continue
            kind = str(match.group("kind")).upper()
            time_str = str(match.group("time"))
            events.append(
                EventMatch(
                    kind=kind,
                    time_str=time_str,
                    start=start,
                    end=end,
                    pattern=pattern_name,
                )
            )
            spans.append((start, end))
    events.sort(key=lambda item: item.start)
    return events
