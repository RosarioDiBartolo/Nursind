import csv
from pathlib import Path

import pandas as pd
import pytest

from src.pair_employee_events.options import parse_options
from src.pair_employee_events.service import process_many_employee_events, process_one_employee_events
from tests.step_contract import assert_process_many_contract, assert_process_one_contract


def _write_events_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "event_id",
        "event_kind",
        "event_ts",
        "event_raw",
        "source_line_no",
        "event_index",
        "source_employee",
        "source_file_id",
        "source_file_name",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_pair_employee_process_one_contract(tmp_path: Path) -> None:
    input_base = tmp_path / "events"
    output_dir = tmp_path / "pairs"
    events_csv = input_base / "events.cleaned.csv"
    _write_events_csv(
        events_csv,
        [
            {
                "event_id": "doc#p1:l1:i0",
                "event_kind": "E",
                "event_ts": "2023-01-01 08:00:00",
                "event_raw": "E 08:00",
                "source_line_no": "1",
                "event_index": "0",
                "source_employee": "Mario Rossi",
                "source_file_id": "file-1",
                "source_file_name": "sample",
            },
            {
                "event_id": "doc#p1:l1:i1",
                "event_kind": "U",
                "event_ts": "2023-01-01 14:00:00",
                "event_raw": "U 14:00",
                "source_line_no": "1",
                "event_index": "1",
                "source_employee": "Mario Rossi",
                "source_file_id": "file-1",
                "source_file_name": "sample",
            },
        ],
    )

    employee = {
        "employee": "Mario Rossi",
        "employee_id": "emp-1",
        "files": [
            {
                "events_csv": str(events_csv),
                "file_id": None,
                "file_name": None,
            }
        ],
    }

    result = process_one_employee_events(
        employee,
        output_dir=str(output_dir),
        max_gap_hours=16.0,
    )
    assert_process_one_contract(result, source_key="source_employee")
    assert result["status"] == "ok"
    assert int(result["pairs_out"]) == 1
    output_csv = Path(str(result["output_csv"]))
    assert output_csv.exists()
    out_df = pd.read_csv(output_csv)
    assert int(len(out_df)) == 1


def test_pair_employee_process_many_contract(tmp_path: Path) -> None:
    input_base = tmp_path / "events"
    output_dir = tmp_path / "pairs"
    good_csv = input_base / "events.cleaned.csv"
    _write_events_csv(
        good_csv,
        [
            {
                "event_id": "doc#p1:l1:i0",
                "event_kind": "E",
                "event_ts": "2023-01-02 08:00:00",
                "event_raw": "E 08:00",
                "source_line_no": "1",
                "event_index": "0",
                "source_employee": "Mario Rossi",
                "source_file_id": "file-1",
                "source_file_name": "good",
            },
            {
                "event_id": "doc#p1:l1:i1",
                "event_kind": "U",
                "event_ts": "2023-01-02 14:00:00",
                "event_raw": "U 14:00",
                "source_line_no": "1",
                "event_index": "1",
                "source_employee": "Mario Rossi",
                "source_file_id": "file-1",
                "source_file_name": "good",
            },
        ],
    )

    employees = [
        {
            "employee": "Mario Rossi",
            "employee_id": "emp-1",
            "files": [
                {
                    "events_csv": str(good_csv),
                    "file_id": None,
                    "file_name": None,
                }
            ],
        },
        {
            "employee": "Giulia Bianchi",
            "employee_id": "emp-2",
            "files": [
                {
                    "events_csv": str(good_csv),
                    "file_id": None,
                    "file_name": None,
                }
            ],
        },
    ]

    report = process_many_employee_events(
        employees,
        output_dir=str(output_dir),
        max_gap_hours=16.0,
        input_mode="folder",
        input_dir=str(input_base),
        events_name="events.cleaned.csv",
    )

    assert_process_many_contract(report)
    assert int(report["stats"]["files_total"]) == 2
    assert int(report["stats"]["files_processed"]) == 2
    assert int(report["stats"]["files_error"]) == 0
    assert int(report["stats"]["employees_with_pairs"]) == 1


def test_pair_employee_parser_rejects_removed_index_flag() -> None:
    with pytest.raises(SystemExit):
        parse_options(["--index", "scan/included.index.json"])

