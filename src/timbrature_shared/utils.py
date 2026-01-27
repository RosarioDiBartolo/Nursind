from __future__ import annotations

import re
import unicodedata
from typing import Optional

from parser_shared.numbers import MONTHS_IT as CART_MONTHS

DOW_MAP = {
    "lun": "LU",
    "mar": "MA",
    "mer": "ME",
    "gio": "GI",
    "giov": "GI",
    "ven": "VE",
    "sab": "SA",
    "dom": "DO",
}
MONTHS_IT = {key.lower(): value for key, value in CART_MONTHS.items()}

DAY_LINE_RE = re.compile(
    r"^(?P<day>\d{1,2})\s*(?P<dow>[a-z\.]+?)(?=\s|e\s|u\s|e\d|u\d|$)"
)


def _strip_accents(value: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", value) if not unicodedata.combining(ch)
    )


def normalize_text(value: str) -> str:
    value = _strip_accents(value)
    value = value.lower().strip()
    return " ".join(value.split())


def normalize_dow(value: str) -> Optional[str]:
    value = normalize_text(value)
    if not value:
        return None
    for prefix, code in DOW_MAP.items():
        if value.startswith(prefix):
            return code
    return value.upper()


def parse_day_header(line: str) -> tuple[int, str] | None:
    norm = normalize_text(line)
    match = DAY_LINE_RE.match(norm)
    if not match:
        return None
    day = int(match.group("day"))
    dow = normalize_dow(match.group("dow")) or match.group("dow").upper()
    return day, dow


def parse_month_year(text: str) -> tuple[Optional[int], Optional[int], Optional[str]]:
    norm = normalize_text(text)
    header_match = re.search(
        r"elenco timbrature\s*-\s*(?P<month>[a-z]+)\s+(?P<year>\d{4})",
        norm,
    )
    match = header_match
    if not match and "totali mensili nel mese di" in norm:
        idx = norm.find("totali mensili nel mese di")
        window = norm[idx : idx + 300]
        match = re.search(r"(?P<month>[a-z]+)\s+(?P<year>\d{4})", window)

    if not match:
        return None, None, None

    month_name = match.group("month")
    month = MONTHS_IT.get(month_name)
    year = int(match.group("year"))
    return month, year, month_name


def parse_employee(text: str) -> tuple[Optional[str], Optional[str]]:
    name = None
    emp_id = None
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        norm = normalize_text(raw)
        if norm.startswith("utente "):
            name = raw.split(" ", 1)[1].strip()
        if "matricola" in norm:
            match = re.search(r"matricola\s*n[^\d]*(?P<id>\d+)", norm)
            if match:
                emp_id = match.group("id")
    return name, emp_id
