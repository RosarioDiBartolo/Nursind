from pathlib import Path

import pandas as pd

from src.extract_events_from_text_raw import process_one_text_file


def test_traceability_columns_include_exact_spans(tmp_path: Path) -> None:
    input_base = tmp_path / "input"
    output_dir = tmp_path / "events"
    source_txt = input_base / "Mario Rossi" / "giugno23.txt"
    source_txt.parent.mkdir(parents=True, exist_ok=True)
    source_txt.write_text(
        "Header line\n01 lu E 08:00 U 14:00 6 6 6\n",
        encoding="utf-8",
    )

    result = process_one_text_file(
        source_txt,
        output_dir=str(output_dir),
        out_name="events_from_text_raw.csv",
        input_base=input_base,
    )

    assert result["status"] == "ok"
    df = pd.read_csv(Path(str(result["output_events_csv"])))
    assert df.loc[0, "source_line_no"] == 2
    assert df.loc[0, "source_line_start_char"] == len("Header line\n")
    assert df.loc[0, "source_match_col_start"] == 7
    assert df.loc[0, "source_match_col_end"] == 14
    assert df.loc[0, "source_match_start_char"] == len("Header line\n") + 6
    assert df.loc[0, "source_match_end_char"] == len("Header line\n") + 13
    assert str(df.loc[0, "source_event_ref"]).endswith(":2:7")
