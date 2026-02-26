import csv
from pathlib import Path

from src.turni_employee_summary import process_many_enriched_files, process_one_enriched_file
from tests.step_contract import assert_process_many_contract, assert_process_one_contract


def _write_enriched_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = ["year", "turno_bucket", "entry_ts", "duration_hours", "is_holiday", "is_afternoon", "is_night"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_turni_summary_process_one_contract(tmp_path: Path) -> None:
    enriched_dir = tmp_path / "enriched"
    enriched_csv = enriched_dir / "Mario Rossi.enriched.csv"
    _write_enriched_csv(
        enriched_csv,
        [
            {
                "year": "2023",
                "turno_bucket": "N",
                "entry_ts": "2023-01-01 20:00:00",
                "duration_hours": "8",
                "is_holiday": "false",
                "is_afternoon": "false",
                "is_night": "true",
            },
            {
                "year": "2023",
                "turno_bucket": "M",
                "entry_ts": "2023-01-02 08:00:00",
                "duration_hours": "8",
                "is_holiday": "false",
                "is_afternoon": "false",
                "is_night": "false",
            },
        ],
    )

    result = process_one_enriched_file(
        enriched_csv,
        year_start=2023,
        year_end=2023,
    )
    assert_process_one_contract(result, source_key="source_enriched_csv")
    assert result["status"] == "ok"
    assert int(result["rows_total"]) == 2
    assert int(result["rows_classified"]) == 2
    assert len(result["summary_rows"]) == 5


def test_turni_summary_process_many_contract(tmp_path: Path) -> None:
    enriched_dir = tmp_path / "enriched"
    good_csv = enriched_dir / "Mario Rossi.enriched.csv"
    _write_enriched_csv(
        good_csv,
        [
            {
                "year": "2024",
                "turno_bucket": "P",
                "entry_ts": "2024-02-01 14:00:00",
                "duration_hours": "8",
                "is_holiday": "false",
                "is_afternoon": "true",
                "is_night": "false",
            }
        ],
    )
    missing_csv = enriched_dir / "Giulia Bianchi.enriched.csv"

    report = process_many_enriched_files(
        [good_csv, missing_csv],
        enriched_dir=str(enriched_dir),
        year_start=2024,
        year_end=2024,
    )

    assert_process_many_contract(report)
    assert int(report["stats"]["files_total"]) == 2
    assert int(report["stats"]["files_processed"]) == 1
    assert int(report["stats"]["files_error"]) == 1
