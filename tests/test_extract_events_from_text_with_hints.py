from pathlib import Path

import pandas as pd

from src.extract_events_from_text_raw import process_one_text_file


def test_hint_first_extraction_keeps_line_level_provenance(tmp_path: Path) -> None:
    input_base = tmp_path / "input"
    output_dir = tmp_path / "events"
    source_txt = input_base / "Mario Rossi" / "ottobre23.txt"
    source_txt.parent.mkdir(parents=True, exist_ok=True)
    source_txt.write_text(
        (
            "R R I I L L E E V V A A Z Z I I O O N N E E\n"
            "02 Lu *76 6,54 13,11 ! 6,15 6,00! 15 ! !\n"
        ),
        encoding="utf-8",
    )

    result = process_one_text_file(
        source_txt,
        output_dir=str(output_dir),
        out_name="events_from_text_raw.csv",
        input_base=input_base,
    )

    assert result["status"] == "ok"
    assert int(result["rows_with_hint_events"]) == 1
    assert int(result["rows_fallback_regex"]) == 0
    assert int(result["events_from_hints"]) == 2
    assert int(result["events_from_regex"]) == 0

    df = pd.read_csv(Path(str(result["output_events_csv"])))
    assert list(df["event_kind"]) == ["E", "U"]
    assert list(df["event_time_hhmm"]) == ["06:54", "13:11"]
    assert pd.isna(df.loc[0, "source_match_start_char"])
    assert pd.isna(df.loc[0, "source_match_col_start"])
    assert str(df.loc[0, "source_event_ref"]).endswith(":2")
