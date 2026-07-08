from __future__ import annotations

import pandas as pd
import pytest

from core.csv_validation import MissingColumnsError
from core.tools.afternoon_export.service import export_afternoon_long_from_dir
from core.tools.missing_report.options import TimbratureMissingReportOptions
from core.tools.parser_recall.options import ParserRecallAuditOptions
from core.tools.turni_custom_counts.options import TurniCustomCountsOptions
from core.tools.turni_custom_counts.service import build_turni_custom_counts_from_dir


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


def test_turni_custom_counts_counts_requested_categories(tmp_path, write_csv) -> None:
    enriched_dir = tmp_path / "enrichment"
    output_dir = tmp_path / "custom_counts"
    write_csv(
        enriched_dir / "Mario Rossi.enriched.csv",
        [
            {
                "employee": "Mario Rossi",
                "entry_ts": "2025-05-05 14:00:00",
                "exit_ts": "2025-05-05 21:00:00",
                "is_holiday": True,
                "is_afternoon": True,
                "is_night": False,
                "is_long": True,
                "year": 2025,
            },
            {
                "employee": "Mario Rossi",
                "entry_ts": "2025-05-06 20:00:00",
                "exit_ts": "2025-05-07 04:00:00",
                "is_holiday": True,
                "is_afternoon": False,
                "is_night": True,
                "is_long": True,
                "year": 2025,
            },
            {
                "employee": "Mario Rossi",
                "entry_ts": "2025-05-10 08:00:00",
                "exit_ts": "2025-05-10 14:00:00",
                "is_holiday": False,
                "is_afternoon": False,
                "is_night": False,
                "is_long": True,
                "year": 2025,
            },
            {
                "employee": "Mario Rossi",
                "entry_ts": "2025-05-10 08:00:00",
                "exit_ts": "2025-05-10 14:00:00",
                "is_holiday": True,
                "is_afternoon": False,
                "is_night": False,
                "is_long": True,
                "year": 2025,
            },
            {
                "employee": "Mario Rossi",
                "entry_ts": "2025-05-12 08:00:00",
                "exit_ts": "2025-05-12 14:00:00",
                "is_holiday": False,
                "is_afternoon": False,
                "is_night": False,
                "is_long": True,
                "year": 2025,
            },
            {
                "employee": "Mario Rossi",
                "entry_ts": "2025-05-13 14:00:00",
                "exit_ts": "2025-05-13 15:30:00",
                "is_holiday": False,
                "is_afternoon": True,
                "is_night": False,
                "is_long": False,
                "year": 2025,
            },
        ],
    )

    report = build_turni_custom_counts_from_dir(
        enriched_dir=str(enriched_dir),
        output_dir=str(output_dir),
        year_start=2025,
        year_end=2025,
    )

    rows = pd.read_csv(output_dir / "turni_custom_counts.csv")
    workbook_rows = pd.read_excel(output_dir / "turni_custom_counts.xlsx")
    counts = dict(zip(rows["turno"], rows["2025"]))
    assert report["status"] == "ok"
    assert report["outputs"]["summary_xlsx"].endswith("turni_custom_counts.xlsx")
    assert list(workbook_rows.columns) == list(rows.columns)
    assert counts == {"P": 1, "N": 1, "M": 2, "MF": 1}


def test_turni_custom_counts_requires_enriched_columns(tmp_path, write_csv) -> None:
    enriched_dir = tmp_path / "enrichment"
    output_dir = tmp_path / "custom_counts"
    write_csv(
        enriched_dir / "Mario Rossi.enriched.csv",
        [
            {
                "entry_ts": "2025-05-05 14:00:00",
                "is_holiday": False,
                "is_afternoon": True,
                "year": 2025,
            }
        ],
    )

    with pytest.raises(MissingColumnsError, match="is_night"):
        build_turni_custom_counts_from_dir(
            enriched_dir=str(enriched_dir),
            output_dir=str(output_dir),
        )


def test_afternoon_export_requires_enriched_columns(tmp_path, write_csv) -> None:
    enriched_dir = tmp_path / "enrichment"
    pairs_dir = tmp_path / "shifts"
    output_dir = tmp_path / "afternoon"
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

    with pytest.raises(MissingColumnsError, match="is_long"):
        export_afternoon_long_from_dir(
            enriched_dir=str(enriched_dir),
            pairs_dir=str(pairs_dir),
            output_dir=str(output_dir),
        )


def test_turni_custom_counts_defaults_to_discovered_years(tmp_path, write_csv) -> None:
    enriched_dir = tmp_path / "enrichment"
    output_dir = tmp_path / "custom_counts"
    write_csv(
        enriched_dir / "Mario Rossi.enriched.csv",
        [
            {
                "entry_ts": "2025-05-05 14:00:00",
                "exit_ts": "2025-05-05 21:00:00",
                "is_holiday": False,
                "is_afternoon": True,
                "is_night": False,
                "is_long": True,
                "year": 2025,
            },
            {
                "entry_ts": "2026-05-05 14:00:00",
                "exit_ts": "2026-05-05 21:00:00",
                "is_holiday": False,
                "is_afternoon": True,
                "is_night": False,
                "is_long": True,
                "year": 2026,
            },
        ],
    )
    options = TurniCustomCountsOptions(
        enriched_dir=str(enriched_dir),
        output_dir=str(output_dir),
    )

    report = build_turni_custom_counts_from_dir(
        enriched_dir=options.enriched_dir,
        output_dir=options.output_dir,
        year_start=options.year_start,
        year_end=options.year_end,
    )
    rows = pd.read_csv(output_dir / "turni_custom_counts.csv")
    workbook_rows = pd.read_excel(output_dir / "turni_custom_counts.xlsx")

    assert report["years"] == [2025, 2026]
    assert {"2025", "2026"}.issubset(rows.columns)
    assert {"2025", "2026"}.issubset(workbook_rows.columns)
