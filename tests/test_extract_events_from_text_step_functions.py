from pathlib import Path

import pandas as pd

from src.extract_events_from_text_raw import process_many_text_files, process_one_text_file


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_process_one_text_file_writes_events_csv(tmp_path: Path) -> None:
    input_base = tmp_path / "input"
    output_dir = tmp_path / "events"
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
        output_dir=str(output_dir),
        out_name="events_from_text_raw.csv",
        input_base=input_base,
    )

    assert result["status"] == "ok"
    assert result["error"] is None
    assert int(result["rows_considered"]) == 1
    assert int(result["rows_with_events"]) == 1
    assert int(result["events_extracted"]) == 2

    out_csv = Path(str(result["output_events_csv"]))
    assert out_csv.exists()
    df = pd.read_csv(out_csv)
    assert list(df["event_kind"]) == ["E", "U"]
    assert list(df["event_time_hhmm"]) == ["08:00", "14:00"]
    assert df.loc[0, "source_line_no"] == 2
    assert str(df.loc[0, "source_event_ref"]).endswith(":2")


def test_process_many_text_files_handles_small_batch(tmp_path: Path) -> None:
    input_base = tmp_path / "input"
    output_dir = tmp_path / "events"

    good_txt = input_base / "Mario Rossi" / "febbraio23.txt"
    _write_text(
        good_txt,
        (
            "Riepilogo presenze/assenze - febbraio 2023\n"
            "01 lu E 08:00 U 14:00 6 6 6\n"
        ),
    )

    no_events_txt = input_base / "Mario Rossi" / "marzo23.txt"
    _write_text(
        no_events_txt,
        (
            "Riepilogo presenze/assenze - marzo 2023\n"
            "01 lu nessuna timbratura 6 6 6\n"
        ),
    )

    missing_year_month_txt = input_base / "Mario Rossi" / "report.txt"
    _write_text(
        missing_year_month_txt,
        "01 lu E 08:00 U 14:00 6 6 6\n",
    )

    report = process_many_text_files(
        [missing_year_month_txt, no_events_txt, good_txt],
        output_dir=str(output_dir),
        out_name="events_from_text_raw.csv",
        input_base=input_base,
        input_dir=str(input_base),
    )

    stats = report["stats"]
    assert stats["files_total"] == 3
    assert stats["files_processed"] == 2
    assert stats["files_error"] == 1
    assert stats["files_missing_year_month"] == 1
    assert stats["files_with_events"] == 1
    assert stats["files_without_events"] == 1
    assert len(report["errors"]) == 1
    assert len(report["files_missing_year_month"]) == 1
    assert len(report["items"]) == 3
