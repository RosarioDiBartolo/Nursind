import csv
import json
from pathlib import Path

from src.timbrature_missing_report import (
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
                    "file_name": "Gennaio 2014.pdf",
                    "drive_path": "Mario Rossi/Gennaio 2014.pdf",
                    "reason": "missing_text_layer",
                    "type": "file",
                }
            },
        },
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
    return pipeline_dir


def test_audit_missing_timbrature_includes_scan_employees_and_required_months(
    tmp_path: Path,
) -> None:
    pipeline_dir = _build_pipeline_dir(tmp_path)

    report = audit_missing_timbrature_pipeline(pipeline_dir)
    rows = {row["employee"]: row for row in report["employee_summary_rows"]}

    assert sorted(rows) == ["Giulia Bianchi", "Mario Rossi"]

    mario = rows["Mario Rossi"]
    assert mario["required_month_range"] == "2014-01..2026-12"
    assert int(mario["found_event_months_count"]) == 2
    assert int(mario["missing_required_months_count"]) == 154
    assert "2014-01" in mario["found_event_months"]
    assert "2014-03" in mario["found_event_months"]
    assert "2027-01" not in mario["found_event_months"]
    assert "2014-02" in mario["missing_required_months"]
    assert "2014-01" not in mario["missing_required_months"]

    giulia = rows["Giulia Bianchi"]
    assert giulia["scan_without_included_files"] is True
    assert int(giulia["found_event_months_count"]) == 0
    assert int(giulia["missing_required_months_count"]) == 156

    assert int(report["stats"]["employees_total"]) == 2
    assert int(report["stats"]["required_months_total"]) == 156
    assert int(report["stats"]["employees_missing_required_months"]) == 2
    assert int(report["stats"]["missing_required_months_total"]) == 310


def test_build_missing_timbrature_report_writes_required_month_columns(tmp_path: Path) -> None:
    pipeline_dir = _build_pipeline_dir(tmp_path)

    report = build_missing_timbrature_report(pipeline_dir=str(pipeline_dir))
    employee_csv_path = Path(report["outputs"]["employee_summary_csv"])
    non_ocr_dir = Path(report["outputs"]["non_ocr_files_dir"])
    missing_months_dir = Path(report["outputs"]["missing_months_dir"])

    with employee_csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    with (non_ocr_dir / "Mario Rossi.csv").open("r", encoding="utf-8", newline="") as handle:
        mario_non_ocr_rows = list(csv.DictReader(handle))
    with (non_ocr_dir / "Giulia Bianchi.csv").open("r", encoding="utf-8", newline="") as handle:
        giulia_non_ocr_rows = list(csv.DictReader(handle))
    with (missing_months_dir / "Mario Rossi.csv").open("r", encoding="utf-8", newline="") as handle:
        mario_missing_month_rows = list(csv.DictReader(handle))
    with (missing_months_dir / "Giulia Bianchi.csv").open("r", encoding="utf-8", newline="") as handle:
        giulia_missing_month_rows = list(csv.DictReader(handle))
    report_json_path = Path(report["outputs"]["report_json"])
    report_json_payload = json.loads(report_json_path.read_text(encoding="utf-8"))

    assert employee_csv_path.exists()
    assert non_ocr_dir.exists()
    assert missing_months_dir.exists()
    assert rows
    assert "required_month_range" in rows[0]
    assert "found_event_months" in rows[0]
    assert "missing_required_months" in rows[0]
    assert len(mario_non_ocr_rows) == 1
    assert mario_non_ocr_rows[0]["file_name"] == "Gennaio 2014.pdf"
    assert mario_non_ocr_rows[0]["file_link"].endswith("/file-ocr-missing/view")
    assert mario_non_ocr_rows[0]["month_name"] == "gennaio"
    assert "year_month" not in mario_non_ocr_rows[0]
    assert giulia_non_ocr_rows == []
    assert len(mario_missing_month_rows) == 154
    assert mario_missing_month_rows[0]["year"] == "2014"
    assert mario_missing_month_rows[0]["month"] == "2"
    assert mario_missing_month_rows[0]["month_name"] == "febbraio"
    assert "year_month" not in mario_missing_month_rows[0]
    assert "required_month_range" not in mario_missing_month_rows[0]
    assert len(giulia_missing_month_rows) == 156
    assert report_json_payload["outputs"]["non_ocr_files_dir"] == str(non_ocr_dir.resolve())
    assert report_json_payload["outputs"]["missing_months_dir"] == str(
        missing_months_dir.resolve()
    )
