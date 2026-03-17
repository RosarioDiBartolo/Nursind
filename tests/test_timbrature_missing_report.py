import csv
import json
from pathlib import Path

from cartellino_parser.drive_service.text_extraction_csv import TEXT_EXTRACTION_COLUMNS
from cartellino_parser.timbrature_missing_report.service import (
    audit_missing_timbrature_pipeline,
    build_missing_timbrature_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, str]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _build_pipeline_dir(tmp_path: Path) -> Path:
    pipeline_dir = tmp_path / "pipeline"
    _write_json(
        pipeline_dir / "scan" / "scan_directory.report.json",
        {
            "employees_found": [
                {
                    "employee": "Mario Rossi",
                    "employee_id": "emp-1",
                    "status": "ok",
                    "included_files": 2,
                },
                {
                    "employee": "Giulia Bianchi",
                    "employee_id": "emp-2",
                    "status": "ok",
                    "included_files": 0,
                },
            ],
            "employees_without_included_files": [
                {
                    "employee": "Giulia Bianchi",
                    "employee_id": "emp-2",
                    "filtered_files": 0,
                    "filtered_folders": 0,
                }
            ],
        },
    )
    _write_csv(
        pipeline_dir / "documents" / "Mario Rossi.csv",
        [
            {
                "employee": "Mario Rossi",
                "employee_id": "emp-1",
                "file_id": "file-1",
                "file_name": "Gennaio 2014.pdf",
                "drive_path": "Mario Rossi/Gennaio 2014.pdf",
                "doc_json": "",
            }
        ],
        TEXT_EXTRACTION_COLUMNS,
    )
    _write_json(
        pipeline_dir / "documents" / "excluded_documents.index.json",
        {
            "root_id": None,
            "generated_at": "2026-03-13T09:00:00Z",
            "employee_count": 1,
            "total_files": 1,
            "files": {
                "file-ocr-missing": {
                    "employee": "Mario Rossi",
                    "employee_id": "emp-1",
                    "local": False,
                    "file_id": "file-ocr-missing",
                    "file_name": "Febbraio 2014.pdf",
                    "drive_path": "Mario Rossi/Febbraio 2014.pdf",
                    "reason": "missing_text_layer",
                    "type": "file",
                }
            },
        },
    )
    _write_csv(
        pipeline_dir / "events" / "pages.csv",
        [
            {
                "source_file_id": "file-1",
                "source_doc_json": "",
                "source_file_name": "Gennaio 2014.pdf",
                "source_employee": "Mario Rossi",
                "page_no": "1",
                "page_year": "2014",
                "page_month": "1",
                "relevant_for_coverage": "true",
                "decision_reason": "parsed",
                "events_extracted": "2",
                "events_dropped_missing_year_month": "0",
            }
        ],
        [
            "source_file_id",
            "source_doc_json",
            "source_file_name",
            "source_employee",
            "page_no",
            "page_year",
            "page_month",
            "relevant_for_coverage",
            "decision_reason",
            "events_extracted",
            "events_dropped_missing_year_month",
        ],
    )
    _write_csv(
        pipeline_dir / "events" / "events.cleaned.csv",
        [
            {"event_ts": "2014-01-10 08:00:00", "source_employee": "Mario Rossi"},
            {"event_ts": "2014-03-05 14:00:00", "source_employee": "Mario Rossi"},
            {"event_ts": "2027-01-01 09:00:00", "source_employee": "Mario Rossi"},
        ],
        ["event_ts", "source_employee"],
    )
    _write_csv(
        pipeline_dir / "shifts" / "Mario Rossi.pairs.csv",
        [
            {
                "year": "2014",
                "month": "1",
                "day": "10",
                "dow": "ven",
                "pair_index": "1",
                "entry_ts": "2014-01-10 08:00:00",
                "exit_ts": "2014-01-10 14:00:00",
            }
        ],
        ["year", "month", "day", "dow", "pair_index", "entry_ts", "exit_ts"],
    )
    return pipeline_dir


def test_audit_missing_timbrature_uses_relevant_pages_for_fixed_coverage_window(
    tmp_path: Path,
) -> None:
    pipeline_dir = _build_pipeline_dir(tmp_path)

    report = audit_missing_timbrature_pipeline(pipeline_dir)
    rows = {row["employee"]: row for row in report["summary_rows"]}

    assert sorted(rows) == ["Giulia Bianchi", "Mario Rossi"]

    mario = rows["Mario Rossi"]
    assert mario["coverage_month_range"] == "2014-01..2025-12"
    assert int(mario["coverage_months_count"]) == 1
    assert int(mario["missing_coverage_months_count"]) == 143
    assert int(mario["finding_count"]) == 1
    assert int(mario["coverage_gap_count"]) == 143

    giulia = rows["Giulia Bianchi"]
    assert giulia["coverage_month_range"] == "2014-01..2025-12"
    assert int(giulia["coverage_months_count"]) == 0
    assert int(giulia["missing_coverage_months_count"]) == 144
    assert giulia["scan_without_included_files"] is True
    assert int(giulia["finding_count"]) == 1
    assert int(giulia["coverage_gap_count"]) == 144

    coverage_rows = report["coverage_rows"]
    missing_coverage_rows = [
        row for row in coverage_rows if row["gap_type"] == "missing_coverage_month"
    ]
    assert len(missing_coverage_rows) == 287
    assert any(
        row["employee"] == "Mario Rossi" and row["year_month"] == "2014-02"
        for row in missing_coverage_rows
    )
    assert any(
        row["employee"] == "Giulia Bianchi" and row["year_month"] == "2014-01"
        for row in missing_coverage_rows
    )

    finding_types = {row["finding_type"] for row in report["finding_rows"]}
    assert finding_types == {"missing_text_layer", "scan_without_included_files"}

    assert int(report["stats"]["employees_total"]) == 2
    assert int(report["stats"]["employees_with_findings"]) == 2
    assert int(report["stats"]["employees_with_coverage_gaps"]) == 2
    assert int(report["stats"]["employees_with_any_gaps"]) == 2
    assert int(report["stats"]["findings_total"]) == 2
    assert int(report["stats"]["coverage_gaps_total"]) == 287
    assert report["stats"]["coverage_month_range"] == "2014-01..2025-12"
    assert int(report["stats"]["coverage_months_total"]) == 144
    assert int(report["stats"]["employees_missing_coverage_months"]) == 2
    assert int(report["stats"]["missing_coverage_months_total"]) == 287


def test_build_missing_timbrature_report_writes_new_artifacts(
    tmp_path: Path,
) -> None:
    pipeline_dir = _build_pipeline_dir(tmp_path)

    report = build_missing_timbrature_report(pipeline_dir=str(pipeline_dir))
    summary_csv_path = Path(report["outputs"]["summary_csv"])
    findings_csv_path = Path(report["outputs"]["findings_csv"])
    coverage_csv_path = Path(report["outputs"]["coverage_csv"])
    report_json_path = Path(report["outputs"]["report_json"])

    with summary_csv_path.open("r", encoding="utf-8", newline="") as handle:
        summary_rows = list(csv.DictReader(handle))
    with findings_csv_path.open("r", encoding="utf-8", newline="") as handle:
        finding_rows = list(csv.DictReader(handle))
    with coverage_csv_path.open("r", encoding="utf-8", newline="") as handle:
        coverage_rows = list(csv.DictReader(handle))
    report_json_payload = json.loads(report_json_path.read_text(encoding="utf-8"))

    assert summary_csv_path.exists()
    assert findings_csv_path.exists()
    assert coverage_csv_path.exists()
    assert report_json_path.exists()

    assert summary_rows
    assert "coverage_month_range" in summary_rows[0]
    assert "coverage_months_count" in summary_rows[0]
    assert "missing_coverage_months_count" in summary_rows[0]
    assert "expected_month_range" not in summary_rows[0]
    assert "pair_rows" not in summary_rows[0]
    assert "pair_status" not in summary_rows[0]

    assert finding_rows
    assert "finding_type" in finding_rows[0]
    assert "pair_status" not in finding_rows[0]
    assert {row["finding_type"] for row in finding_rows} == {
        "missing_text_layer",
        "scan_without_included_files",
    }

    assert coverage_rows
    assert "gap_type" in coverage_rows[0]
    assert "upstream_causes" not in coverage_rows[0]
    assert {row["gap_type"] for row in coverage_rows} == {"missing_coverage_month"}

    assert report_json_payload["outputs"]["summary_csv"] == str(summary_csv_path.resolve())
    assert report_json_payload["outputs"]["findings_csv"] == str(findings_csv_path.resolve())
    assert report_json_payload["outputs"]["coverage_csv"] == str(coverage_csv_path.resolve())
    assert report_json_payload["row_totals"]["items"] == 2
    assert report_json_payload["row_totals"]["issues"] == 2
    assert report_json_payload["row_totals"]["coverage_rows"] == 287
    assert report_json_payload["stats"]["finding_counts_by_type"]["missing_text_layer"] == 1
    assert report_json_payload["stats"]["coverage_counts_by_type"]["missing_coverage_month"] == 287
    assert "items" not in report_json_payload
    assert "coverage_rows" not in report_json_payload


def test_audit_missing_timbrature_counts_valid_month_pages_without_events_as_coverage(
    tmp_path: Path,
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    _write_json(
        pipeline_dir / "scan" / "scan_directory.report.json",
        {
            "employees_found": [
                {
                    "employee": "Mario Rossi",
                    "employee_id": "emp-1",
                    "status": "ok",
                    "included_files": 1,
                }
            ],
            "employees_without_included_files": [],
        },
    )
    _write_csv(
        pipeline_dir / "documents" / "Mario Rossi.csv",
        [
            {
                "employee": "Mario Rossi",
                "employee_id": "emp-1",
                "file_id": "file-1",
                "file_name": "bundle.pdf",
                "drive_path": "Mario Rossi/bundle.pdf",
                "doc_json": "",
            }
        ],
        TEXT_EXTRACTION_COLUMNS,
    )
    _write_csv(
        pipeline_dir / "events" / "pages.csv",
        [
            {
                "source_file_id": "file-1",
                "source_doc_json": "",
                "source_file_name": "bundle.pdf",
                "source_employee": "Mario Rossi",
                "page_no": "1",
                "page_year": "2016",
                "page_month": "12",
                "relevant_for_coverage": "true",
                "decision_reason": "parsed",
                "events_extracted": "0",
                "events_dropped_missing_year_month": "0",
            }
        ],
        [
            "source_file_id",
            "source_doc_json",
            "source_file_name",
            "source_employee",
            "page_no",
            "page_year",
            "page_month",
            "relevant_for_coverage",
            "decision_reason",
            "events_extracted",
            "events_dropped_missing_year_month",
        ],
    )
    _write_csv(
        pipeline_dir / "events" / "events.cleaned.csv",
        [{"event_ts": "2014-01-10 08:00:00", "source_employee": "Mario Rossi"}],
        ["event_ts", "source_employee"],
    )
    _write_csv(
        pipeline_dir / "shifts" / "Mario Rossi.pairs.csv",
        [
            {
                "year": "2014",
                "month": "1",
                "day": "10",
                "dow": "ven",
                "pair_index": "1",
                "entry_ts": "2014-01-10 08:00:00",
                "exit_ts": "2014-01-10 14:00:00",
            }
        ],
        ["year", "month", "day", "dow", "pair_index", "entry_ts", "exit_ts"],
    )

    report = audit_missing_timbrature_pipeline(pipeline_dir)
    mario = report["summary_rows"][0]

    assert mario["coverage_months_count"] == 1
    assert mario["missing_coverage_months_count"] == 143
    assert not any(
        row["gap_type"] == "missing_coverage_month" and row["year_month"] == "2016-12"
        for row in report["coverage_rows"]
    )


def test_audit_missing_timbrature_reads_pairing_stage_report_items(tmp_path: Path) -> None:
    pipeline_dir = _build_pipeline_dir(tmp_path)
    missing_output = pipeline_dir / "shifts" / "missing-output.csv"
    _write_json(
        pipeline_dir / "shifts" / "pair_employee_events.report.json",
        {
            "stage": "pair_employee_events",
            "status": "ok",
            "items": [
                {
                    "status": "error",
                    "source_employee": "Mario Rossi",
                    "employee_id": "emp-1",
                    "error_code": "processing_error",
                    "error": "simulated failure",
                    "output_csv": str(missing_output),
                }
            ],
        },
    )

    report = audit_missing_timbrature_pipeline(pipeline_dir)
    mario_findings = [
        row for row in report["finding_rows"] if row["employee"] == "Mario Rossi"
    ]
    finding_types = {row["finding_type"] for row in mario_findings}

    assert "pairing_failed" in finding_types
    assert "pair_output_missing" in finding_types
    assert any("simulated failure" in str(row["detail"]) for row in mario_findings)


def test_audit_missing_timbrature_rejects_legacy_layout(tmp_path: Path) -> None:
    legacy_pipeline_dir = tmp_path / "legacy-pipeline"
    (legacy_pipeline_dir / "text_extracted").mkdir(parents=True)
    (legacy_pipeline_dir / "employee_shifts_from_raw").mkdir(parents=True)

    try:
        audit_missing_timbrature_pipeline(legacy_pipeline_dir)
    except ValueError as exc:
        assert "Legacy pipeline layout is no longer supported" in str(exc)
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("Expected legacy-only pipeline layouts to be rejected")

