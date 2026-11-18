"""Conservative recovery of damaged Situazione mensile title tokens."""
from __future__ import annotations

import re
from datetime import date
from difflib import SequenceMatcher

from core.parsing import MONTH_WORDS, normalize_text


def monthly_title_year_month(text: str) -> tuple[int | None, int | None]:
    # Never use footer print dates, totals, or daily schedule codes as the title.
    header = re.split(r"mese\s+chiuso|timbrature|giorno", text, maxsplit=1, flags=re.I)[0]
    tokens = re.findall(r"[a-z0-9\ufffd]+", normalize_text(header))
    months = {word: month for word, month in MONTH_WORDS.items() if len(word) >= 3}
    candidates: set[tuple[int, int]] = set()
    for index, token in enumerate(tokens):
        if len(token) != 4:
            continue
        digits = token.translate(str.maketrans("zZoOiIlL", "22001111"))
        if not digits.isdigit() or not digits.startswith(("19", "20")):
            continue
        year = int(digits)
        if not 1900 <= year <= date.today().year:
            continue
        for neighbor in tokens[max(0, index - 1):index] + tokens[index + 1:index + 2]:
            scores = [(SequenceMatcher(None, neighbor, word).ratio(), month) for word, month in months.items()]
            best = max(score for score, _ in scores)
            matches = {month for score, month in scores if score == best and score >= 0.8}
            if len(matches) == 1:
                candidates.add((year, matches.pop()))
    return next(iter(candidates)) if len(candidates) == 1 else (None, None)
