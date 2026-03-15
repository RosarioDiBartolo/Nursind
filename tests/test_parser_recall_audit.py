import csv
import json
from pathlib import Path

from src.parser_recall_audit.service import audit_parser_recall_root, build_parser_recall_report


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_doc(path: Path, pages: dict[int, list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "document": {
            "page_count": len(pages),
            "full_text": "\n".join(line for lines in pages.values() for line in lines),
        },
        "layout": {
            "pages": [
                {
                    "page_no": page_no,
                    "lines": [{"text": line} for line in lines],
                }
                for page_no, lines in sorted(pages.items())
            ]
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_output_root(tmp_path: Path) -> Path:
    root_dir = tmp_path / "output"

    pipeline_a = root_dir / "PIPE_A"
    _write_csv(
        pipeline_a / "documents" / "Mario Rossi.csv",
        [
            {
                "employee": "Mario Rossi",
                "employee_id": "emp-1",
                "file_id": "file-a",
                "file_link": "https://drive.google.com/file/d/file-a/view",
                "file_name": "good-a.pdf",
                "drive_path": "/Root/Mario Rossi/good-a.pdf",
                "doc_json": "docs/doc-a.json",
            },
            {
                "employee": "Mario Rossi",
                "employee_id": "emp-1",
                "file_id": "file-b",
                "file_link": "https://drive.google.com/file/d/file-b/view",
                "file_name": "absence-b.pdf",
                "drive_path": "/Root/Mario Rossi/absence-b.pdf",
                "doc_json": "docs/doc-b.json",
            },
        ],
    )
    _write_doc(
        pipeline_a / "documents" / "docs" / "doc-a.json",
        {
            1: ["Header", "01 LU 06:50 13:10", "02 MA 07:00 13:20"],
            2: ["Header", "01 LU 06:50 13:10"],
        },
    )
    _write_doc(
        pipeline_a / "documents" / "docs" / "doc-b.json",
        {
            1: [
                "Header",
                "01 LU FERIE 6:00 6:00",
                "02 MA FERIE 6:00 6:00",
                "03 ME FACOLT 6:00 6:00",
                "04 GI FACOLT 6:00 6:00",
                "05 VE INTERDIZIONE 6:00 6:00",
                "06 SA INTERDIZIONE 6:00 6:00",
            ]
        },
    )
    _write_csv(
        pipeline_a / "events" / "pages.csv",
        [
            {
                "page_ref": "docs/doc-a.json#p1",
                "source_doc_json": "docs/doc-a.json",
                "source_file_id": "file-a",
                "source_file_name": "good-a.pdf",
                "source_employee": "Mario Rossi",
                "source_drive_path": "/Root/Mario Rossi/good-a.pdf",
                "source_file_link": "https://drive.google.com/file/d/file-a/view",
                "page_no": 1,
                "page_kind": "events_table",
                "decision": "parsed",
                "decision_reason": "parsed",
                "parser_id": "cartellino_ocr",
                "page_year": 2024,
                "page_month": 1,
                "year_month_source": "header",
                "relevant_for_coverage": "true",
                "rows_considered": 31,
                "rows_with_events": 25,
                "rows_without_events": 6,
                "events_extracted": 40,
                "events_dropped_missing_year_month": 0,
                "coverage_ratio_page": 0.806452,
                "header_preview": "good-a",
                "parse_error": "",
            },
            {
                "page_ref": "docs/doc-a.json#p2",
                "source_doc_json": "docs/doc-a.json",
                "source_file_id": "file-a",
                "source_file_name": "good-a.pdf",
                "source_employee": "Mario Rossi",
                "source_drive_path": "/Root/Mario Rossi/good-a.pdf",
                "source_file_link": "https://drive.google.com/file/d/file-a/view",
                "page_no": 2,
                "page_kind": "events_table",
                "decision": "parsed",
                "decision_reason": "parsed",
                "parser_id": "cartellino_ocr",
                "page_year": 2024,
                "page_month": 2,
                "year_month_source": "header",
                "relevant_for_coverage": "true",
                "rows_considered": 2,
                "rows_with_events": 0,
                "rows_without_events": 2,
                "events_extracted": 0,
                "events_dropped_missing_year_month": 0,
                "coverage_ratio_page": 0.0,
                "header_preview": "tiny-a",
                "parse_error": "",
            },
            {
                "page_ref": "docs/doc-b.json#p1",
                "source_doc_json": "docs/doc-b.json",
                "source_file_id": "file-b",
                "source_file_name": "absence-b.pdf",
                "source_employee": "Mario Rossi",
                "source_drive_path": "/Root/Mario Rossi/absence-b.pdf",
                "source_file_link": "https://drive.google.com/file/d/file-b/view",
                "page_no": 1,
                "page_kind": "events_table",
                "decision": "parsed",
                "decision_reason": "parsed",
                "parser_id": "cartellino_unico",
                "page_year": 2024,
                "page_month": 3,
                "year_month_source": "header",
                "relevant_for_coverage": "true",
                "rows_considered": 30,
                "rows_with_events": 0,
                "rows_without_events": 30,
                "events_extracted": 0,
                "events_dropped_missing_year_month": 0,
                "coverage_ratio_page": 0.0,
                "header_preview": "absence-b",
                "parse_error": "",
            },
        ],
    )

    pipeline_b = root_dir / "PIPE_B"
    _write_csv(
        pipeline_b / "documents" / "Giulia Bianchi.csv",
        [
            {
                "employee": "Giulia Bianchi",
                "employee_id": "emp-2",
                "file_id": "file-c",
                "file_link": "https://drive.google.com/file/d/file-c/view",
                "file_name": "low-c.pdf",
                "drive_path": "/Root/Giulia Bianchi/low-c.pdf",
                "doc_json": "docs/doc-c.json",
            },
            {
                "employee": "Giulia Bianchi",
                "employee_id": "emp-2",
                "file_id": "file-d",
                "file_link": "https://drive.google.com/file/d/file-d/view",
                "file_name": "zero-d.pdf",
                "drive_path": "/Root/Giulia Bianchi/zero-d.pdf",
                "doc_json": "docs/doc-d.json",
            },
        ],
    )
    _write_doc(
        pipeline_b / "documents" / "docs" / "doc-c.json",
        {
            1: ["Header", "01 LU 06:50 13:10", "02 MA 07:00 13:20"],
            2: [
                "Header",
                "01 LU 06:50 13:10 13:40 20:05",
                "02 MA 06:55 13:15 13:45 20:00",
            ],
            3: ["Header", "01 LU 06:45 13:05", "02 MA 07:05 13:25"],
        },
    )
    _write_doc(
        pipeline_b / "documents" / "docs" / "doc-d.json",
        {
            1: [
                "Header",
                "01 LU 06:50 13:10 13:40 20:05",
                "02 MA 06:55 13:15 13:45 20:00",
                "03 ME 06:45 13:05",
            ]
        },
    )
    _write_csv(
        pipeline_b / "events" / "pages.csv",
        [
            {
                "page_ref": "docs/doc-c.json#p1",
                "source_doc_json": "docs/doc-c.json",
                "source_file_id": "file-c",
                "source_file_name": "low-c.pdf",
                "source_employee": "Giulia Bianchi",
                "source_drive_path": "/Root/Giulia Bianchi/low-c.pdf",
                "source_file_link": "https://drive.google.com/file/d/file-c/view",
                "page_no": 1,
                "page_kind": "events_table",
                "decision": "parsed",
                "decision_reason": "parsed",
                "parser_id": "cartellino_ocr",
                "page_year": 2024,
                "page_month": 4,
                "year_month_source": "header",
                "relevant_for_coverage": "true",
                "rows_considered": 30,
                "rows_with_events": 24,
                "rows_without_events": 6,
                "events_extracted": 38,
                "events_dropped_missing_year_month": 0,
                "coverage_ratio_page": 0.8,
                "header_preview": "good-c-1",
                "parse_error": "",
            },
            {
                "page_ref": "docs/doc-c.json#p2",
                "source_doc_json": "docs/doc-c.json",
                "source_file_id": "file-c",
                "source_file_name": "low-c.pdf",
                "source_employee": "Giulia Bianchi",
                "source_drive_path": "/Root/Giulia Bianchi/low-c.pdf",
                "source_file_link": "https://drive.google.com/file/d/file-c/view",
                "page_no": 2,
                "page_kind": "events_table",
                "decision": "parsed",
                "decision_reason": "parsed",
                "parser_id": "cartellino_ocr",
                "page_year": 2024,
                "page_month": 5,
                "year_month_source": "header",
                "relevant_for_coverage": "true",
                "rows_considered": 30,
                "rows_with_events": 2,
                "rows_without_events": 28,
                "events_extracted": 4,
                "events_dropped_missing_year_month": 0,
                "coverage_ratio_page": 0.066667,
                "header_preview": "low-c-2",
                "parse_error": "",
            },
            {
                "page_ref": "docs/doc-c.json#p3",
                "source_doc_json": "docs/doc-c.json",
                "source_file_id": "file-c",
                "source_file_name": "low-c.pdf",
                "source_employee": "Giulia Bianchi",
                "source_drive_path": "/Root/Giulia Bianchi/low-c.pdf",
                "source_file_link": "https://drive.google.com/file/d/file-c/view",
                "page_no": 3,
                "page_kind": "events_table",
                "decision": "parsed",
                "decision_reason": "parsed",
                "parser_id": "cartellino_ocr",
                "page_year": 2024,
                "page_month": 6,
                "year_month_source": "header",
                "relevant_for_coverage": "true",
                "rows_considered": 30,
                "rows_with_events": 23,
                "rows_without_events": 7,
                "events_extracted": 36,
                "events_dropped_missing_year_month": 0,
                "coverage_ratio_page": 0.766667,
                "header_preview": "good-c-3",
                "parse_error": "",
            },
            {
                "page_ref": "docs/doc-d.json#p1",
                "source_doc_json": "docs/doc-d.json",
                "source_file_id": "file-d",
                "source_file_name": "zero-d.pdf",
                "source_employee": "Giulia Bianchi",
                "source_drive_path": "",
                "source_file_link": "",
                "page_no": 1,
                "page_kind": "events_table",
                "decision": "parsed",
                "decision_reason": "parsed",
                "parser_id": "cartellino_unico",
                "page_year": 2024,
                "page_month": 7,
                "year_month_source": "header",
                "relevant_for_coverage": "true",
                "rows_considered": 31,
                "rows_with_events": 0,
                "rows_without_events": 31,
                "events_extracted": 0,
                "events_dropped_missing_year_month": 0,
                "coverage_ratio_page": 0.0,
                "header_preview": "zero-d",
                "parse_error": "",
            },
            {
                "page_ref": "docs/doc-e.json#p1",
                "source_doc_json": "docs/doc-e.json",
                "source_file_id": "file-e",
                "source_file_name": "missing-year-month.pdf",
                "source_employee": "Giulia Bianchi",
                "source_drive_path": "/Root/Giulia Bianchi/missing-year-month.pdf",
                "source_file_link": "https://drive.google.com/file/d/file-e/view",
                "page_no": 1,
                "page_kind": "events_table",
                "decision": "error_missing_year_month",
                "decision_reason": "missing_page_year_month",
                "parser_id": "cartellino_ocr",
                "page_year": "",
                "page_month": "",
                "year_month_source": "none",
                "relevant_for_coverage": "false",
                "rows_considered": 25,
                "rows_with_events": 0,
                "rows_without_events": 25,
                "events_extracted": 0,
                "events_dropped_missing_year_month": 8,
                "coverage_ratio_page": "",
                "header_preview": "missing-year-month",
                "parse_error": "",
            },
        ],
    )

    return root_dir


def test_audit_parser_recall_root_returns_ranked_suspicious_pages(tmp_path: Path) -> None:
    root_dir = _build_output_root(tmp_path)

    report = audit_parser_recall_root(
        root_dir,
        max_tiny_rows=3,
        min_large_rows=10,
        low_coverage_threshold=0.25,
    )

    assert isinstance(report["stats"], dict)
    assert isinstance(report["suspicious_rows"], list)
    assert isinstance(report["artifacts"], dict)

    stats = report["stats"]
    assert stats["pipelines_total"] == 2
    assert stats["pages_total"] == 8
    assert stats["suspicious_pages_total"] == 5
    assert stats["tiny_page_total"] == 1
    assert stats["zero_event_page_total"] == 2
    assert stats["low_coverage_page_total"] == 1
    assert stats["missing_year_month_total"] == 1

    rows_by_name = {
        (row["source_file_name"], row["issue_bucket"]): row for row in report["suspicious_rows"]
    }

    tiny_row = rows_by_name[("good-a.pdf", "tiny_page")]
    assert tiny_row["source_file_link"] == "https://drive.google.com/file/d/file-a/view"
    assert tiny_row["source_drive_path"] == "/Root/Mario Rossi/good-a.pdf"
    assert tiny_row["page_text_found"] is True

    legit_zero_row = rows_by_name[("absence-b.pdf", "zero_event_page")]
    assert legit_zero_row["likely_legitimate_no_events"] is True
    assert legit_zero_row["absence_keyword_hits"] >= 5
    assert "facolt" in legit_zero_row["absence_keywords_found"]

    low_cov_row = rows_by_name[("low-c.pdf", "low_coverage_page")]
    assert low_cov_row["neighbor_avg_coverage"] == 0.783334
    assert low_cov_row["suspicion_score"] > legit_zero_row["suspicion_score"]

    suspicious_zero_row = rows_by_name[("zero-d.pdf", "zero_event_page")]
    assert suspicious_zero_row["likely_legitimate_no_events"] is False
    assert suspicious_zero_row["absence_keyword_hits"] == 0
    assert suspicious_zero_row["source_file_link"] == "https://drive.google.com/file/d/file-d/view"
    assert suspicious_zero_row["source_drive_path"] == "/Root/Giulia Bianchi/zero-d.pdf"

    missing_year_row = rows_by_name[("missing-year-month.pdf", "missing_year_month")]
    assert missing_year_row["events_dropped_missing_year_month"] == 8
    assert missing_year_row["suspicion_score"] == 100


def test_build_parser_recall_report_writes_csv_and_json(tmp_path: Path) -> None:
    root_dir = _build_output_root(tmp_path)

    report = build_parser_recall_report(
        root_dir=str(root_dir),
        max_tiny_rows=3,
        min_large_rows=10,
        low_coverage_threshold=0.25,
    )

    suspicious_csv_path = Path(report["outputs"]["suspicious_csv"])
    report_json_path = Path(report["outputs"]["report_json"])

    assert suspicious_csv_path.exists()
    assert report_json_path.exists()

    with suspicious_csv_path.open("r", encoding="utf-8", newline="") as handle:
        suspicious_rows = list(csv.DictReader(handle))
    report_json_payload = json.loads(report_json_path.read_text(encoding="utf-8"))

    assert len(suspicious_rows) == 5
    assert "source_file_link" in suspicious_rows[0]
    assert "issue_bucket" in suspicious_rows[0]
    assert report_json_payload["outputs"]["suspicious_csv"] == str(suspicious_csv_path.resolve())
    assert report_json_payload["row_totals"]["items"] == 5
    assert report_json_payload["row_totals"]["issues"] == 0
    assert "items" not in report_json_payload
