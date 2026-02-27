import csv
from pathlib import Path

from src.extract_days_from_text_raw import (
    process_many_text_files,
    process_one_text_file,
)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_process_one_text_file_writes_days_csv(tmp_path: Path) -> None:
    input_base = tmp_path / "input"
    out_dir = tmp_path / "out"
    source_txt = input_base / "Mario Rossi" / "gennaio23.txt"
    _write_text(
        source_txt,
        (
            "Riepilogo presenze/assenze - gennaio 2023\n"
            "01 lu E 08:00 U 14:00 6 6 6\n"
        ),
    )

    result = process_one_text_file(
        source_txt,
        out_dir=str(out_dir),
        out_name="days.csv",
        input_base=input_base,
    )

    assert result["status"] == "ok"
    assert result["error"] is None
    assert result["rows_parsed"] == 1
    assert result["rows_with_event"] == 1

    out_csv = Path(str(result["output_days_csv"]))
    assert out_csv.exists()
    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["year"] == "2023"
    assert rows[0]["month"] == "1"
    assert rows[0]["day"] == "1"
    assert rows[0]["dow"] == "LU"
    assert rows[0]["parser_id"] == "cartellino_classic"
    assert rows[0]["hint_count"] == "2"
    assert rows[0]["hint_overflow"] == "0"
    assert rows[0]["hint_1_kind"] == "E"
    assert rows[0]["hint_1_time_hhmm"] == "08:00"
    assert rows[0]["hint_2_kind"] == "U"
    assert rows[0]["hint_2_time_hhmm"] == "14:00"


def test_process_many_text_files_handles_small_batch(tmp_path: Path) -> None:
    input_base = tmp_path / "input"
    out_dir = tmp_path / "out"

    good_txt = input_base / "Mario Rossi" / "febbraio23.txt"
    _write_text(
        good_txt,
        (
            "Riepilogo presenze/assenze - febbraio 2023\n"
            "01 lu E 08:00 U 14:00 6 6 6\n"
        ),
    )

    no_days_txt = input_base / "Mario Rossi" / "marzo23.txt"
    _write_text(
        no_days_txt,
        (
            "Riepilogo presenze/assenze - marzo 2023\n"
            "nessuna riga giorno valida\n"
        ),
    )

    missing_year_month_txt = input_base / "Mario Rossi" / "report.txt"
    _write_text(
        missing_year_month_txt,
        "01 lu E 08:00 U 14:00 6 6 6\n",
    )

    report = process_many_text_files(
        [missing_year_month_txt, no_days_txt, good_txt],
        out_dir=str(out_dir),
        out_name="days.csv",
        input_base=input_base,
        max_no_days_files=10,
        max_no_days_lines=3,
        input_dir=str(input_base),
    )

    stats = report["stats"]
    assert stats["files_total"] == 3
    assert stats["files_processed"] == 2
    assert stats["files_error"] == 1
    assert stats["files_missing_year_month"] == 1
    assert stats["files_with_days"] == 1
    assert stats["files_without_days"] == 1
    assert len(report["errors"]) == 1
    assert len(report["files_missing_year_month"]) == 1
    assert len(report["files_without_days"]) == 1
    assert len(report["items"]) == 3
