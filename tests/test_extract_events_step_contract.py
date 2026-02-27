import csv
from pathlib import Path

from src.extract_events_from_days_raw import process_many_days_files, process_one_days_file
from tests.step_contract import assert_process_many_contract, assert_process_one_contract


def _write_days_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = ["year", "month", "day", "dow", "mo_f", "mo_t", "mo_lav", "raw"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_extract_events_process_one_contract(tmp_path: Path) -> None:
    input_base = tmp_path / "days"
    output_dir = tmp_path / "events"
    days_csv = input_base / "Mario Rossi" / "sample.days.csv"
    _write_days_csv(
        days_csv,
        [
            {
                "year": "2023",
                "month": "8",
                "day": "1",
                "dow": "LU",
                "mo_f": "6",
                "mo_t": "6",
                "mo_lav": "6",
                "raw": "01 lu E 08:00 U 14:00 6 6 6",
            }
        ],
    )

    result = process_one_days_file(
        days_csv,
        output_dir=str(output_dir),
        out_name="events_from_days_raw.csv",
        input_base=input_base,
    )
    assert_process_one_contract(result, source_key="source_days_csv")
    assert result["status"] == "ok"
    assert int(result["events_extracted"]) == 2
    for key in (
        "rows_with_hint_events",
        "rows_fallback_regex",
        "events_from_hints",
        "events_from_regex",
        "rows_with_invalid_hints",
    ):
        assert key in result
    assert Path(str(result["output_events_csv"])).exists()


def test_extract_events_process_many_contract(tmp_path: Path) -> None:
    input_base = tmp_path / "days"
    output_dir = tmp_path / "events"
    good_days = input_base / "Mario Rossi" / "good.days.csv"
    _write_days_csv(
        good_days,
        [
            {
                "year": "2023",
                "month": "9",
                "day": "1",
                "dow": "LU",
                "mo_f": "6",
                "mo_t": "6",
                "mo_lav": "6",
                "raw": "01 lu E 08:00 U 14:00 6 6 6",
            }
        ],
    )

    missing_days = input_base / "Mario Rossi" / "missing.days.csv"
    report = process_many_days_files(
        [good_days, missing_days],
        output_dir=str(output_dir),
        out_name="events_from_days_raw.csv",
        input_base=input_base,
        input_dir=str(input_base),
    )

    assert_process_many_contract(report)
    assert report["stats"]["files_total"] == 2
    assert report["stats"]["files_processed"] == 1
    assert report["stats"]["files_error"] == 1
    for key in (
        "rows_with_hint_events",
        "rows_fallback_regex",
        "events_from_hints",
        "events_from_regex",
        "rows_with_invalid_hints",
    ):
        assert key in report["stats"]
