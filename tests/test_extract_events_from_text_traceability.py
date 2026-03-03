from pathlib import Path

import pandas as pd

from src.extract_events_from_text_raw import process_one_text_row
from tests.extract_events_manifest_fixtures import build_manifest_row


def test_traceability_columns_include_exact_spans(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "events"
    row = build_manifest_row(
        input_dir,
        employee="Mario Rossi",
        employee_id="emp-1",
        file_id="pdf-201",
        file_name="giugno23.pdf",
        full_text="Header line\n01 lu E 08:00 U 14:00 6 6 6\n",
        source_text_ref="Mario Rossi/giugno23.txt",
    )

    result = process_one_text_row(
        row,
        output_dir=str(output_dir),
        out_name="events_from_text_raw.csv",
        input_dir=str(input_dir),
    )

    assert result["status"] == "ok"
    df = pd.read_csv(Path(str(result["output_events_csv"])))
    assert df.loc[0, "source_line_no"] == 2
    assert str(df.loc[0, "source_doc_json"]).startswith("docs/")
    assert str(df.loc[0, "source_file_name"]) == "giugno23.pdf"
    assert df.loc[0, "source_line_start_char"] == len("Header line\n")
    assert df.loc[0, "source_match_col_start"] == 7
    assert df.loc[0, "source_match_col_end"] == 14
    assert df.loc[0, "source_match_start_char"] == len("Header line\n") + 6
    assert df.loc[0, "source_match_end_char"] == len("Header line\n") + 13
    assert str(df.loc[0, "source_event_ref"]).endswith("#line_no=2:col=7")
