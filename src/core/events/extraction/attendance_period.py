"""Recover a damaged attendance title only when the printed weekdays agree."""
from __future__ import annotations

import re
from datetime import date
from collections.abc import Mapping, Sequence

from core.events.extraction.monthly_header import monthly_title_year_month
from core.events.extraction.models import ParsedRow
from core.parsing import MONTH_WORDS, calendar_dow, normalize_text, parse_day_header


def _title(text: str) -> str:
    lines = []
    for line in text.splitlines()[:12]:
        if parse_day_header(line):
            break
        # A timestamp on the same line can follow the actual title.
        line = re.split(r"data e ora stampa|codice tipo stampa|stampato|elaborazione", line,
                        maxsplit=1, flags=re.I)[0]
        line = re.sub(r"(?<=[a-zA-Z])(?=20\d)", " ", line)
        lines.append(re.sub(r"\b(20\d)\s+(\d)\b", r"\1\2", line))
    return "\n".join(lines)


def recover_attendance_period(
    texts: Mapping[int, str], page_no: int, rows: Sequence[ParsedRow]
) -> tuple[int, int, str] | None:
    title = _title(texts.get(page_no, ""))
    year, month = monthly_title_year_month(title)
    source = "ocr_title_calendar"
    if not year:
        # An isolated month is usable only between two dated titles exactly
        # two months apart. A new page can start a completely different year.
        months = {MONTH_WORDS[w] for w in re.findall(r"[a-z]+", normalize_text(title))
                  if w in MONTH_WORDS and len(w) >= 4}
        if len(months) != 1 or re.search(r"\d{4}|(?:19|20)\d", title):
            return None
        neighbors = []
        for number in (page_no - 1, page_no + 1):
            y, m = monthly_title_year_month(_title(texts.get(number, "")))
            if not y or not m:
                return None
            neighbors.append(y * 12 + m - 1)
        if abs(neighbors[0] - neighbors[1]) != 2:
            return None
        year, index = divmod(sum(neighbors) // 2, 12)
        month = index + 1
        if month not in months:
            return None
        source = "neighbor_titles_calendar"
    if month is None or len({row.day for row in rows}) < 3:
        return None
    try:
        if any(calendar_dow(date(year, month, row.day)) != row.dow for row in rows):
            return None
    except ValueError:
        return None
    return year, month, source
