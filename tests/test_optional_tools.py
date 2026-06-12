from __future__ import annotations

import pandas as pd

from core.tools.afternoon_export.service import export_afternoon_long_from_dir
from core.tools.missing_report.options import TimbratureMissingReportOptions
from core.tools.parser_recall.options import ParserRecallAuditOptions


def test_parser_recall_uses_explicit_output_contract(tmp_path) -> None:
    recall = ParserRecallAuditOptions(
        root_dir=str(tmp_path),
        report_json=str(tmp_path / "recall.json"),
        suspicious_csv=str(tmp_path / "suspicious.csv"),
    )

    assert recall.low_coverage_threshold == 0.25
    assert recall.report_json.endswith("recall.json")


def test_missing_report_uses_canonical_summary_name(tmp_path) -> None:
    missing = TimbratureMissingReportOptions(pipeline_dir=str(tmp_path))

    assert missing.summary_csv.endswith(".summary.csv")


def test_afternoon_export_requires_enrichment_and_writes_employee_artifacts(
    tmp_path, write_csv
) -> None:
    enriched_dir = tmp_path / "enrichment"
    pairs_dir = tmp_path / "shifts"
    output_dir = tmp_path / "afternoon"
    report_json = output_dir / "report.json"

    missing_report = export_afternoon_long_from_dir(
        enriched_dir=str(enriched_dir),
        pairs_dir=str(pairs_dir),
        output_dir=str(output_dir),
        report_json=str(report_json),
    )

    assert missing_report["status"] == "error"
    assert missing_report["issues"][0]["code"] == "missing_enriched_inputs"

    write_csv(
        enriched_dir / "Mario Rossi.enriched.csv",
        [
            {
                "employee": "Mario Rossi",
                "entry_ts": "2025-05-10 13:40:00",
                "exit_ts": "2025-05-10 21:40:00",
                "duration_hours": 8.0,
                "is_holiday": True,
                "is_afternoon": True,
                "is_long": True,
                "turno": "Pomeriggio",
            }
        ],
    )
    write_csv(
        pairs_dir / "Mario Rossi.pairs.csv",
        [
            {
                "entry_ts": "2025-05-10 13:40:00",
                "exit_ts": "2025-05-10 21:40:00",
                "duration_hhmm": "08:00",
            }
        ],
    )

    report = export_afternoon_long_from_dir(
        enriched_dir=str(enriched_dir),
        pairs_dir=str(pairs_dir),
        output_dir=str(output_dir),
        report_json=str(report_json),
    )

    employee_dir = output_dir / "Mario Rossi"
    assert report["status"] == "ok"
    assert report["stats"]["files_processed"] == 1
    assert (employee_dir / "Mario Rossi.pomeriggi.csv").exists()
    assert (employee_dir / "Mario Rossi.csv").exists()
    assert (employee_dir / "Mario Rossi.pdf").read_bytes().startswith(b"%PDF")
    assert len(pd.read_csv(employee_dir / "Mario Rossi.pomeriggi.csv")) == 1
