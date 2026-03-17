from pathlib import Path

import pandas as pd
import pytest

from cartellino_parser.extract_events_from_documents.service import (
    extract_events_from_documents_dir,
    process_many_text_rows,
    process_one_text_row,
)
from tests.extract_events_manifest_fixtures import build_manifest_row, write_manifest_csv


def test_process_one_text_row_writes_run_level_outputs(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "events"
    row = build_manifest_row(
        input_dir,
        employee="Mario Rossi",
        employee_id="emp-1",
        file_id="pdf-001",
        file_name="gennaio23.pdf",
        full_text=(
            "Riepilogo presenze/assenze - gennaio 2023\n"
            "01 lu E 08:00 U 14:00 6 6 6\n"
        ),
        source_text_ref="Mario Rossi/gennaio23.txt",
    )

    result = process_one_text_row(
        row,
        output_dir=str(output_dir),
        out_name="events.csv",
        pages_name="pages.csv",
        input_dir=str(input_dir),
    )

    assert result["status"] == "ok"
    assert result["error"] is None
    assert int(result["rows_considered"]) == 1
    assert int(result["rows_with_events"]) == 1
    assert int(result["events_extracted"]) == 2

    out_events_csv = Path(str(result["output_events_csv"]))
    out_pages_csv = Path(str(result["output_pages_csv"]))
    assert out_events_csv.exists()
    assert out_pages_csv.exists()

    events_df = pd.read_csv(out_events_csv)
    assert list(events_df["event_kind"]) == ["E", "U"]
    assert list(events_df["event_time_hhmm"]) == ["08:00", "14:00"]
    assert int(events_df.loc[0, "source_line_no"]) == 2
    assert str(events_df.loc[0, "source_file_link"]) == "https://drive.google.com/file/d/pdf-001/view"
    assert "line_no=2" in str(events_df.loc[0, "source_event_ref"])

    pages_df = pd.read_csv(out_pages_csv)
    assert int(len(pages_df)) >= 1
    assert str(pages_df.loc[0, "decision"]) == "parsed"
    assert str(pages_df.loc[0, "source_drive_path"]) == "/Root/Mario Rossi/gennaio23.pdf"


def test_process_many_text_rows_tracks_missing_page_year_month(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "events"

    good_row = build_manifest_row(
        input_dir,
        employee="Mario Rossi",
        employee_id="emp-1",
        file_id="pdf-002",
        file_name="febbraio23.pdf",
        full_text=(
            "Riepilogo presenze/assenze - febbraio 2023\n"
            "01 lu E 08:00 U 14:00 6 6 6\n"
        ),
        source_text_ref="Mario Rossi/febbraio23.txt",
    )

    no_events_row = build_manifest_row(
        input_dir,
        employee="Mario Rossi",
        employee_id="emp-1",
        file_id="pdf-003",
        file_name="marzo23.pdf",
        full_text=(
            "Riepilogo presenze/assenze - marzo 2023\n"
            "01 lu nessuna timbratura 6 6 6\n"
        ),
        source_text_ref="Mario Rossi/marzo23.txt",
    )

    missing_year_month_row = build_manifest_row(
        input_dir,
        employee="Mario Rossi",
        employee_id="emp-1",
        file_id="pdf-004",
        file_name="report.pdf",
        full_text="01 lu E 08:00 U 14:00 6 6 6\n",
        source_text_ref="Mario Rossi/report.txt",
    )

    report = process_many_text_rows(
        [missing_year_month_row, no_events_row, good_row],
        output_dir=str(output_dir),
        out_name="events.csv",
        pages_name="pages.csv",
        input_dir=str(input_dir),
    )

    stats = report["stats"]
    assert int(stats["files_total"]) == 3
    assert int(stats["files_processed"]) == 3
    assert int(stats["files_error"]) == 0
    assert int(stats["files_with_events"]) == 1
    assert int(stats["files_without_events"]) == 2
    assert int(stats["events_dropped_missing_year_month"]) >= 2
    assert len(report["issues"]) == 0
    assert int(report["row_totals"]["pages_missing_year_month"]) >= 1
    assert len(report["items"]) == 3


def test_extract_events_from_documents_dir_reads_employee_manifest_csv(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "events"
    report_json = tmp_path / "extract_events.report.json"
    row = build_manifest_row(
        input_dir,
        employee="Mario Rossi",
        employee_id="emp-1",
        file_id="pdf-123",
        file_name="aprile23.pdf",
        full_text=(
            "Riepilogo presenze/assenze - aprile 2023\n"
            "01 lu E 08:00 U 14:00 6 6 6\n"
        ),
        source_text_ref="Mario Rossi/aprile23.txt",
    )

    write_manifest_csv(
        input_dir / "Mario Rossi.csv",
        [row],
    )

    report = extract_events_from_documents_dir(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        report_json=str(report_json),
    )

    assert report["inputs"]["input_mode"] == "employee_manifest_csv"
    assert int(report["stats"]["files_total"]) == 1

    out_events_csv = output_dir / "events.csv"
    out_pages_csv = output_dir / "pages.csv"
    assert out_events_csv.exists()
    assert out_pages_csv.exists()

    events_df = pd.read_csv(out_events_csv)
    assert list(events_df["event_kind"]) == ["E", "U"]
    source_doc_json = str(events_df.loc[0, "source_doc_json"]).replace("\\", "/")
    assert not Path(source_doc_json).is_absolute()
    assert "/docs/" in f"/{source_doc_json}"
    assert str(events_df.loc[0, "source_file_name"]) == "aprile23.pdf"
    assert str(events_df.loc[0, "source_file_link"]) == "https://drive.google.com/file/d/pdf-123/view"


def test_process_one_text_row_requires_doc_json(tmp_path: Path) -> None:
    result = process_one_text_row(
        {
            "source_text_ref": "Mario Rossi/missing.txt",
            "file_name": "missing.pdf",
        },
        output_dir=str(tmp_path / "events"),
        input_dir=str(tmp_path / "input"),
    )

    assert result["status"] == "error"
    assert result["error_code"] == "missing_document_extraction_doc"


def test_extract_events_from_documents_dir_requires_employee_manifests(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="NO_DOCUMENT_EXTRACTION_MANIFESTS"):
        extract_events_from_documents_dir(
            input_dir=str(tmp_path / "input"),
            output_dir=str(tmp_path / "events"),
            report_json=str(tmp_path / "events.report.json"),
        )


