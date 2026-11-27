"""Recover the ASCII subset of the embedded ASL Bologna attendance font."""

from __future__ import annotations

import re

from .parsers.common import get_layout_pages

_CID = re.compile(r"\(cid:(\d{1,2})\)")
_ASL_BOLOGNA = "".join(f"(cid:{ord(c) - 29})" for c in "ASL Bologna")


def _decode(match: re.Match[str]) -> str:
    value = int(match[1]) + 29
    return chr(value) if 32 <= value <= 126 else match[0]


def decode_bologna_layout(document: dict) -> dict:
    pages = get_layout_pages(document)
    decoded = []
    changed = False
    for page in pages:
        lines = page.get("lines") or []
        if any(_ASL_BOLOGNA in str(line.get("text", "")) for line in lines):
            changed = True
            page = {
                **page,
                **{
                    key: [{**item, "text": _CID.sub(_decode, item["text"])} for item in page[key]]
                    for key in ("lines", "words")
                },
            }
        decoded.append(page)
    if not changed:
        return document
    text = "\n".join(line["text"] for page in decoded for line in page["lines"])
    return {
        **document,
        "document": {**document["document"], "full_text": text},
        "layout": {**document["layout"], "pages": decoded},
    }
