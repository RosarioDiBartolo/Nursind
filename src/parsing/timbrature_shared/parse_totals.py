from __future__ import annotations

import re
from typing import Dict

from ..parser_shared.numbers import hhmm_to_decimal, parse_number

TOTALS_HEADER_RE = re.compile(r"totali mensili nel mese di", re.IGNORECASE)
TOTALS_VALUE_RE = re.compile(r"\b\d{1,3}[,.]\d{2}\b")


def _extract_totals_window(text: str) -> str | None:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for i, line in enumerate(lines):
        if TOTALS_HEADER_RE.search(line):
            return " ".join(lines[i : i + 3])
    return None


def parse_totals(text: str) -> Dict[str, float]:
    totals: Dict[str, float] = {}
    window = _extract_totals_window(text)
    if not window:
        return totals

    numbers = TOTALS_VALUE_RE.findall(window)
    values = [hhmm_to_decimal(parse_number(value)) for value in numbers]

    if values:
        totals["ore_dovute_programmate"] = values[0]
    if len(values) > 1:
        totals["ore_lavorate"] = values[1]
    if len(values) > 2:
        totals["flex"] = values[2]
    if len(values) > 3:
        totals["vestizione"] = values[3]
    if len(values) > 4:
        totals["da_autor"] = values[4]

    return totals
