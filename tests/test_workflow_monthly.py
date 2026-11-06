"""Behavioral cases for the WORKFLOW monthly PDF and macOS sidecar filtering."""

from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pytest

from core.drive.archive_utils import list_pdf_members
from core.drive.scan.scan_service import file_excluded, folder_excluded
from core.events.extraction.parsers import resolve_parser
from core.events.extraction.page_analysis import parse_document_pages
from core.events.extraction.source_context import resolve_source_context
from core.events.extraction.workflow_header import workflow_year_month


def document(header="WORKFLOWGCARTELLINOGMENSILEGGGSGGEGGGTGGTEMBRE 2020", times=None):
    times = times if times is not None else [(0, "06:55"), (1, "14:03"), (2, "14:04"), (3, "20:06")]
    words = []
    for token, x in (
        [("3", 31.58), ("G", 44.61)]
        + [(t, 59.61 + slot * 22) for slot, t in times]
        + [("001", 199.87), ("7.00", 224.31), ("12:30", 374.0)]
    ):
        words.append(dict(text=token, x0=x, y0=100.0, x1=x + 15.4, y1=106.0, word_index=len(words)))
    row = " ".join(w["text"] for w in words)
    return {
        "document": {"full_text": header + "\n" + row, "page_count": 1},
        "layout": {
            "pages": [
                {
                    "page_no": 1,
                    "width": 595.0,
                    "height": 842.0,
                    "words": words,
                    "lines": [
                        {"line_id": "p1_l1", "text": header, "word_indices": []},
                        {"line_id": "p1_l2", "text": row, "word_indices": list(range(len(words)))},
                    ],
                }
            ]
        },
    }


def test_workflow_routes_and_uses_columns_excluding_totals_and_justifications():
    d = document()
    parser = resolve_parser(d)
    assert parser.parser_id == "workflow_monthly"
    rows = parser.parse_document(d)
    assert [(e.event_kind, e.event_time_hhmm, e.source_slot) for e in rows[0].events] == [
        ("E", "06:55", "ent_1"),
        ("U", "14:03", "usc_1"),
        ("E", "14:04", "ent_2"),
        ("U", "20:06", "usc_2"),
    ]
    event = rows[0].events[0]
    assert event.line.line_id == "p1_l2"
    assert event.source_bbox_x0 == 59.61
    assert event.event_raw == "06:55"


def test_missing_entry_keeps_exit_slot_and_night_punches_keep_order():
    parser = resolve_parser(document())
    rows = parser.parse_document(document(times=[(1, "07:35")]))
    assert [(e.event_kind, e.source_slot) for e in rows[0].events] == [("U", "usc_1")]
    rows = parser.parse_document(document(times=[(0, "19:32"), (1, "07:35")]))
    assert [(e.event_kind, e.event_time_hhmm) for e in rows[0].events] == [
        ("E", "19:32"),
        ("U", "07:35"),
    ]


@pytest.mark.parametrize(
    "month,number",
    [
        (m, i + 1)
        for i, m in enumerate(
            [
                "GENNAIO",
                "FEBBRAIO",
                "MARZO",
                "APRILE",
                "MAGGIO",
                "GIUGNO",
                "LUGLIO",
                "AGOSTO",
                "SETTEMBRE",
                "OTTOBRE",
                "NOVEMBRE",
                "DICEMBRE",
            ]
        )
    ],
)
def test_month_headers_decode_padding_and_preserve_g_in_month_names(month, number):
    header = "WORKFLOWGCARTELLINOGMENSILEGGG" + "GG".join(month) + " 2023"
    assert workflow_year_month(header) == (2023, number)
    encoded = (
        "".join(
            chr(ord(c) + 0xDFDF) if c.isalpha() or c == "." else c
            for c in "WORKFLOW CARTELLINO MENSILE"
        )
        + " "
        + month
        + " 2024"
    )
    assert workflow_year_month(encoded) == (2024, number)


def test_monthly_events_use_title_date_and_calendar_weekday_not_hire_date():
    d = document("WORKFLOWGCARTELLINOGMENSILEGGGSGGEGGGTGGTEMBRE 2020\nData assunzione 01/12/2002")
    context = resolve_source_context(
        {"doc_json": "docs/test.json", "employee": "Test"}, d, input_dir=None
    )
    result, events, pages, missing, _ = parse_document_pages(
        document=d,
        source_context=context,
        source_path=Path("3.pdf"),
        max_unmatched_examples_per_file=3,
    )
    assert result["events_extracted"] == 4
    assert not missing
    assert events[0]["event_ts"] == "2020-09-03 06:55:00"
    assert events[0]["dow"] == "GI"
    assert pages[0]["page_kind"] == "events_table"
    assert pages[0]["year_month_source"] == "workflow_header"


def test_macos_sidecars_filtered_even_without_user_exclusions():
    assert file_excluded("._3.pdf", []) == "macos_metadata"
    assert folder_excluded("__MACOSX", []) == "macos_metadata"
    assert file_excluded("3.pdf", []) is None
    stream = BytesIO()
    with ZipFile(stream, "w") as archive:
        for name in ["3.pdf", "nested/._3.pdf", "__MACOSX/3.pdf", "nested/4.pdf"]:
            archive.writestr(name, b"test")
    assert list_pdf_members(stream.getvalue()) == ["3.pdf", "nested/4.pdf"]


def test_overnight_exit_rolls_to_next_day_before_pairing():
    d = document(times=[(0, "19:32"), (1, "07:35")])
    context = resolve_source_context(
        {"doc_json": "docs/test.json", "employee": "Test"}, d, input_dir=None
    )
    _, events, _, _, _ = parse_document_pages(
        document=d,
        source_context=context,
        source_path=Path("3.pdf"),
        max_unmatched_examples_per_file=3,
    )
    assert [e["event_ts"] for e in events] == ["2020-09-03 19:32:00", "2020-09-04 07:35:00"]


def test_undated_workflow_does_not_use_hire_date_as_month():
    from core.events.extraction.page_analysis import resolve_page_year_month

    assert resolve_page_year_month(
        page_text="WORKFLOW CARTELLINO MENSILE\nData assunzione 01/12/2002",
        source_path=Path("12.pdf"),
    ) == (None, None, "workflow_missing_header")
