import csv
from pathlib import Path

import pandas as pd

from src.filter_midnight_events_from_days_raw import (
    process_many_events_files,
    process_one_events_file,
)
from tests.step_contract import assert_process_many_contract, assert_process_one_contract


def _write_events_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "year",
        "month",
        "day",
        "dow",
        "event_index",
        "event_kind",
        "event_time_hhmm",
        "event_ts",
        "event_raw",
        "event_pattern",
        "raw",
        "source_row_index",
        "source_days_csv",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_filter_midnight_process_one_contract(tmp_path: Path) -> None:
    input_base = tmp_path / "events"
    output_dir = tmp_path / "cleaned"
    events_csv = input_base / "Mario Rossi" / "sample.events_from_days_raw.csv"
    _write_events_csv(
        events_csv,
        [
            {
                "year": "2023",
                "month": "1",
                "day": "1",
                "dow": "LU",
                "event_index": "0",
                "event_kind": "E",
                "event_time_hhmm": "00:00",
                "event_ts": "2023-01-01 00:00:00",
                "event_raw": "E 00:00",
                "event_pattern": "default",
                "raw": "01 lu E 00:00 U 07:00",
                "source_row_index": "0",
                "source_days_csv": "input.days.csv",
            },
            {
                "year": "2023",
                "month": "1",
                "day": "1",
                "dow": "LU",
                "event_index": "1",
                "event_kind": "U",
                "event_time_hhmm": "07:00",
                "event_ts": "2023-01-01 07:00:00",
                "event_raw": "U 07:00",
                "event_pattern": "default",
                "raw": "01 lu E 00:00 U 07:00",
                "source_row_index": "0",
                "source_days_csv": "input.days.csv",
            },
        ],
    )

    result = process_one_events_file(
        events_csv,
        output_dir=str(output_dir),
        out_name="events_from_days_raw.cleaned.csv",
        input_base=input_base,
    )
    assert_process_one_contract(result, source_key="source_events_csv")
    assert result["status"] == "ok"
    assert int(result["rows_in"]) == 2
    assert int(result["rows_removed_midnight"]) == 1
    assert int(result["rows_out"]) == 1
    output_csv = Path(str(result["output_events_csv"]))
    assert output_csv.exists()
    cleaned_df = pd.read_csv(output_csv)
    assert int(len(cleaned_df)) == 1


def test_filter_midnight_process_many_contract(tmp_path: Path) -> None:
    input_base = tmp_path / "events"
    output_dir = tmp_path / "cleaned"
    good_events = input_base / "Mario Rossi" / "good.events_from_days_raw.csv"
    _write_events_csv(
        good_events,
        [
            {
                "year": "2023",
                "month": "2",
                "day": "1",
                "dow": "MA",
                "event_index": "0",
                "event_kind": "E",
                "event_time_hhmm": "08:00",
                "event_ts": "2023-02-01 08:00:00",
                "event_raw": "E 08:00",
                "event_pattern": "default",
                "raw": "01 ma E 08:00 U 14:00",
                "source_row_index": "0",
                "source_days_csv": "input.days.csv",
            }
        ],
    )
    missing_events = input_base / "Mario Rossi" / "missing.events_from_days_raw.csv"

    report = process_many_events_files(
        [good_events, missing_events],
        output_dir=str(output_dir),
        out_name="events_from_days_raw.cleaned.csv",
        input_base=input_base,
        input_dir=str(input_base),
        events_name="*.events_from_days_raw.csv",
    )

    assert_process_many_contract(report)
    assert report["stats"]["files_total"] == 2
    assert report["stats"]["files_processed"] == 1
    assert report["stats"]["files_error"] == 1
