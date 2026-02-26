from pathlib import Path

from src.extract_days_from_text_raw import process_many_text_files, process_one_text_file
from tests.step_contract import assert_process_many_contract, assert_process_one_contract


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_extract_days_process_one_contract(tmp_path: Path) -> None:
    input_base = tmp_path / "input"
    out_dir = tmp_path / "out"
    source_txt = input_base / "Mario Rossi" / "aprile23.txt"
    _write_text(
        source_txt,
        (
            "Riepilogo presenze/assenze - aprile 2023\n"
            "01 lu E 08:00 U 14:00 6 6 6\n"
        ),
    )

    result = process_one_text_file(
        source_txt,
        out_dir=str(out_dir),
        input_base=input_base,
    )
    assert_process_one_contract(result, source_key="source_txt")


def test_extract_days_process_many_contract(tmp_path: Path) -> None:
    input_base = tmp_path / "input"
    out_dir = tmp_path / "out"

    good_txt = input_base / "Mario Rossi" / "maggio23.txt"
    _write_text(
        good_txt,
        (
            "Riepilogo presenze/assenze - maggio 2023\n"
            "01 lu E 08:00 U 14:00 6 6 6\n"
        ),
    )

    missing_year_month_txt = input_base / "Mario Rossi" / "documento.txt"
    _write_text(
        missing_year_month_txt,
        "01 lu E 08:00 U 14:00 6 6 6\n",
    )

    report = process_many_text_files(
        [good_txt, missing_year_month_txt],
        out_dir=str(out_dir),
        input_base=input_base,
        input_dir=str(input_base),
    )

    assert_process_many_contract(report)
