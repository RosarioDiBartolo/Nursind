import csv
from pathlib import Path

import pandas as pd

from src.turni_enrichment.service import process_many_pairs_files, process_one_pairs_file
from tests.step_contract import assert_process_many_contract, assert_process_one_contract


def _write_pairs_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = ["entry_ts", "exit_ts", "turno", "file_id", "file_name", "source_csv"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_turni_enrichment_process_one_contract(tmp_path: Path) -> None:
    input_base = tmp_path / "pairs"
    output_dir = tmp_path / "enriched"
    pair_csv = input_base / "Mario Rossi.pairs.csv"
    _write_pairs_csv(
        pair_csv,
        [
            {
                "entry_ts": "2023-01-01 08:00:00",
                "exit_ts": "2023-01-01 14:00:00",
                "turno": "Mattina",
                "file_id": "file-1",
                "file_name": "source-1",
                "source_csv": "source-1",
            }
        ],
    )

    result = process_one_pairs_file(
        pair_csv,
        output_dir=str(output_dir),
        min_hours=6.0,
        include_holidays=False,
    )
    assert_process_one_contract(result, source_key="source_pairs_csv")
    assert result["status"] == "ok"
    assert int(result["rows_total"]) == 1
    assert int(result["rows_enriched"]) == 1
    output_csv = Path(str(result["output_enriched_csv"]))
    assert output_csv.exists()
    out_df = pd.read_csv(output_csv)
    assert int(len(out_df)) == 1


def test_turni_enrichment_process_many_contract(tmp_path: Path) -> None:
    input_base = tmp_path / "pairs"
    output_dir = tmp_path / "enriched"
    good_csv = input_base / "Mario Rossi.pairs.csv"
    _write_pairs_csv(
        good_csv,
        [
            {
                "entry_ts": "2023-01-02 14:00:00",
                "exit_ts": "2023-01-02 20:00:00",
                "turno": "Pomeriggio",
                "file_id": "file-2",
                "file_name": "source-2",
                "source_csv": "source-2",
            }
        ],
    )
    missing_csv = input_base / "Giulia Bianchi.pairs.csv"

    report = process_many_pairs_files(
        [good_csv, missing_csv],
        output_dir=str(output_dir),
        min_hours=6.0,
        include_holidays=False,
        input_dir=str(input_base),
    )

    assert_process_many_contract(report)
    assert int(report["stats"]["files_total"]) == 2
    assert int(report["stats"]["files_processed"]) == 1
    assert int(report["stats"]["files_error"]) == 1
