from __future__ import annotations

import re

from core.events.extraction.models import ParsedEvent
from core.events.extraction.workflow_header import is_workflow_monthly

from .base import BaseFormatParser
from .common import document_text, get_layout_pages, iter_layout_lines, to_row_result


class WorkflowMonthlyParser(BaseFormatParser):
    """Six fixed punch columns in the WORKFLOW monthly attendance template.

    Column positions are relative to the page width (A4 template). Missing punches
    retain their entry/exit slot; durations and justification columns are excluded.
    Source text and word coordinates remain unmodified.
    """

    parser_id = "workflow_monthly"
    legacy_doc_format = "workflow_monthly"
    priority = 15
    _DAY = re.compile(r"^\s*(?P<day>0?[1-9]|[12]\d|3[01])\s+(?P<dow>[LMGVSD])(?=\s|$)")
    _TIME = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$|^24:00$")
    # Known template: x0=59.61,81.61,103.61,124.61,146.61,168.61 on width=595.
    _CENTERS = tuple(
        (value + 7.7) / 595 for value in (59.61, 81.61, 103.61, 124.61, 146.61, 168.61)
    )

    def score_document(self, document: dict) -> int:
        return 180 if is_workflow_monthly(document_text(document)) else -200

    def parse_document(self, document: dict):
        widths = {int(p["page_no"]): float(p["width"]) for p in get_layout_pages(document)}
        rows = []
        for line in iter_layout_lines(document):
            header = self._DAY.match(line.text)
            if not header:
                continue
            day = int(header["day"])
            # M is ambiguous (Tuesday/Wednesday); output weekday is computed from date.
            dow = {"L": "LU", "M": "MA", "G": "GI", "V": "VE", "S": "SA", "D": "DO"}[header["dow"]]
            width = widths.get(line.page_no or 0)
            events = []
            occupied = set()
            entry_times: dict[int, str] = {}
            for index, word in enumerate(line.words):
                token = str(word.get("text", ""))
                if not width or not self._TIME.fullmatch(token):
                    continue
                position = (float(word["x0"]) + float(word["x1"])) / (2 * width)
                slot = min(range(6), key=lambda i: abs(position - self._CENTERS[i]))
                if abs(position - self._CENTERS[slot]) > 0.017 or slot in occupied:
                    continue
                occupied.add(slot)
                kind = "E" if slot % 2 == 0 else "U"
                day_offset = 0
                if kind == "E":
                    entry_times[slot // 2] = token
                elif slot // 2 in entry_times and token < entry_times[slot // 2]:
                    day_offset = 1
                events.append(
                    ParsedEvent(
                        line=line,
                        day=day,
                        dow=dow,
                        event_kind=kind,
                        event_time_hhmm=token,
                        event_raw=token,
                        event_pattern=f"{self.parser_id}:column_{slot + 1}",
                        source_origin="layout_column",
                        day_offset=day_offset,
                        source_slot=f"{'ent' if kind == 'E' else 'usc'}_{slot // 2 + 1}",
                        source_word_start=index,
                        source_word_end=index,
                        source_bbox_x0=float(word["x0"]),
                        source_bbox_y0=float(word["y0"]),
                        source_bbox_x1=float(word["x1"]),
                        source_bbox_y1=float(word["y1"]),
                    )
                )
            rows.append(to_row_result(day, dow, line, events))
        return tuple(rows)
