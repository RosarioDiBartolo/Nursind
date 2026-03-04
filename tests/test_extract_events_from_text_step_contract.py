from pathlib import Path

from src.extract_events_from_documents import process_many_text_rows, process_one_text_row
from tests.extract_events_manifest_fixtures import build_manifest_row
from tests.step_contract import assert_process_many_contract, assert_process_one_contract


def test_extract_events_from_text_process_one_contract(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "events"
    row = build_manifest_row(
        input_dir,
        employee="Mario Rossi",
        employee_id="emp-1",
        file_id="pdf-401",
        file_name="aprile23.pdf",
        full_text=(
            "Riepilogo presenze/assenze - aprile 2023\n"
            "01 lu E 08:00 U 14:00 6 6 6\n"
        ),
        source_text_ref="Mario Rossi/aprile23.txt",
    )

    result = process_one_text_row(
        row,
        output_dir=str(output_dir),
        input_dir=str(input_dir),
    )
    assert_process_one_contract(result, source_key="source_doc_json")
    assert Path(str(result["output_events_csv"])).exists()
    assert Path(str(result["output_pages_csv"])).exists()


def test_extract_events_from_text_process_many_contract(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "events"

    good_row = build_manifest_row(
        input_dir,
        employee="Mario Rossi",
        employee_id="emp-1",
        file_id="pdf-402",
        file_name="maggio23.pdf",
        full_text=(
            "Riepilogo presenze/assenze - maggio 2023\n"
            "01 lu E 08:00 U 14:00 6 6 6\n"
        ),
        source_text_ref="Mario Rossi/maggio23.txt",
    )

    missing_year_month_row = build_manifest_row(
        input_dir,
        employee="Mario Rossi",
        employee_id="emp-1",
        file_id="pdf-403",
        file_name="documento.pdf",
        full_text="01 lu E 08:00 U 14:00 6 6 6\n",
        source_text_ref="Mario Rossi/documento.txt",
    )

    report = process_many_text_rows(
        [good_row, missing_year_month_row],
        output_dir=str(output_dir),
        input_dir=str(input_dir),
    )

    assert_process_many_contract(report)

