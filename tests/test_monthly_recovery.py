from pathlib import Path

import pandas as pd

from core.events.extraction.monthly_header import monthly_title_year_month
from core.events.extraction.page_analysis import parse_document_pages
from core.events.extraction.service import process_many_text_rows
from core.parsing import extract_events
from core.shifts.pairing.service import process_many_employee_events


def test_title_recovers_only_supported_tokens_and_never_footer():
    assert monthly_title_year_month("SITUAZIONE MENSILE PRESENZE\nzozi\naprile\nMese chiuso") == (2021, 4)
    assert monthly_title_year_month("2017\nn\ufffdvembre;\nMese chiuso") == (2017, 11)
    assert monthly_title_year_month("2099 maggio\nMese chiuso\n27/05/2022") == (None, None)
    assert monthly_title_year_month("Mese chiuso\nTotale maggio 2022") == (None, None)


def test_ocr_time_repair_preserves_original_spans():
    raw = "4 ven E 13:32 U 2l:21 T01P 7:00"
    events = extract_events(raw, repair_ocr=True)
    assert [(e.kind, e.time_str) for e in events] == [("E", "13:32"), ("U", "21:21")]
    assert raw[events[1].start:events[1].end] == "U 2l:21"
    assert len(extract_events(raw)) == 1
    assert extract_events("U 29:72", repair_ocr=True) == []


def test_weekday_conflict_cannot_create_a_false_short_shift():
    text = (
        "SITUAZIONE MENSILE PRESENZE\n2021 giugno\nMese chiuso\n"
        "4 ven E 13:32 U 2l:21\n4,, mar E 06:27 U 14:01\n"
    )
    result, events, pages, _, _ = parse_document_pages(
        document={"document": {"full_text": text}},
        source_context={"source_doc_json": "synthetic.json"},
        source_path=Path("synthetic"), max_unmatched_examples_per_file=10,
    )
    assert [(e["event_kind"], e["event_time_hhmm"]) for e in events] == [("E", "13:32"), ("U", "21:21")]
    assert pages[0]["decision"] == "error_day_weekday_mismatch"
    assert result["pages_error"] == 1


def test_pairing_keeps_sources_and_reports_rejected_cross_row(tmp_path):
    path = tmp_path / "events.csv"
    rows = []
    for kind, time, line in [("E", "06:00", "a"), ("U", "14:00", "a"), ("E", "15:00", "b"), ("U", "16:00", "c")]:
        rows.append({
            "event_kind": kind, "event_ts": f"2021-06-04 {time}:00",
            "source_employee": "Synthetic", "source_file_id": "pdf-1",
            "source_file_name": "attendance.pdf", "source_doc_json": "doc.json",
            "source_page_no": 1, "source_line_id": line,
            "source_event_ref": f"{line}-{kind}", "parser_id": "situazione_mensile",
        })
    pd.DataFrame(rows).to_csv(path, index=False)
    report = process_many_employee_events(
        [{"employee": "Synthetic", "files": [{"events_csv": str(path), "file_name": "events"}]}],
        output_dir=str(tmp_path / "pairs"),
    )
    assert report["status"] == "warning"
    assert report["stats"]["rows_unmatched_after_close"] == 2
    pairs = pd.read_csv(report["items"][0]["output_csv"])
    assert len(pairs) == 1
    assert pairs.loc[0, "file_id"] == "pdf-1"
    assert pairs.loc[0, "file_name"] == "attendance.pdf"
    assert pairs.loc[0, "entry_event_ref"] == "a-E"
    assert pairs.loc[0, "exit_event_ref"] == "a-U"


def test_extraction_report_exposes_missing_title(tmp_path):
    from tests.extract_events_manifest_fixtures import build_manifest_row

    row = build_manifest_row(
        tmp_path, employee="Synthetic", employee_id="s", file_id="f", file_name="attendance",
        full_text="SITUAZIONE MENSILE PRESENZE\nMese chiuso\n4 ven E 13:32 U 21:21",
        source_text_ref="synthetic.txt",
    )
    report = process_many_text_rows([row], input_dir=str(tmp_path), output_dir=str(tmp_path / "events"))
    assert report["status"] == "warning"
    assert report["issues"][0]["code"] == "error_missing_year_month"
    assert report["stats"]["pages_relevant"] == 1
    assert report["stats"]["coverage_ratio_relevant_pages"] == 0
