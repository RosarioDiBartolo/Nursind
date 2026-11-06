"""Header decoding for the WORKFLOW monthly attendance PDF format."""

import re

from core.parsing import MONTH_WORDS

TITLE_RE = re.compile(r"WORKFLOW[G\s]*CARTELLINO[G\s]*MENSILE", re.IGNORECASE)
MONTH_NAMES = [
    name
    for name in MONTH_WORDS
    if len(name) >= 5 and name not in {"settembr", "settembe", "settebre"}
]


def decode_workflow_header(text: str) -> str:
    # This PDF font maps printable ASCII glyphs into U+E000..U+E05E.
    # Decode only for format/header recognition; source text stays untouched.
    return "".join(chr(ord(ch) - 0xDFDF) if "\ue000" <= ch <= "\ue05e" else ch for ch in text)


def workflow_year_month(text: str) -> tuple[int | None, int | None]:
    for line in text.splitlines()[:8]:
        decoded = decode_workflow_header(line)
        title = TITLE_RE.search(decoded)
        if not title:
            continue
        tail = decoded[title.end() :]
        for month in MONTH_NAMES:
            pattern = r"[G\s]*".join(re.escape(ch) for ch in month)
            match = re.search(pattern + r"[G\s]*(?P<year>20\d{2}|19\d{2})\b", tail, re.IGNORECASE)
            if match:
                return int(match["year"]), MONTH_WORDS[month]
    return None, None


def is_workflow_monthly(text: str) -> bool:
    return bool(TITLE_RE.search(decode_workflow_header(text)))
