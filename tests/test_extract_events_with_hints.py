from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.extract_events_from_days_raw import process_one_days_file


def _write_days_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def test_extract_events_uses_hints_before_regex(tmp_path: Path) -> None:
    input_base = tmp_path / "days"
    output_dir = tmp_path / "events"
    days_csv = input_base / "Mario Rossi" / "tipo4.days.csv"
    _write_days_csv(
        days_csv,
        [
            {
                "year": "2023",
                "month": "10",
                "day": "2",
                "dow": "LU",
                "mo_f": "6",
                "mo_t": "6.25",
                "mo_lav": "6.25",
                "raw": "02 Lu *76 6,54 13,11 ! 6,15 6,00! 15 ! !",
                "parser_id": "cartellino_ocr",
                "hint_count": "2",
                "hint_overflow": "0",
                "hint_1_kind": "E",
                "hint_1_time_hhmm": "06:54",
                "hint_1_source": "segment_1_pos_1",
                "hint_1_confidence": "0.9",
                "hint_2_kind": "U",
                "hint_2_time_hhmm": "13:11",
                "hint_2_source": "segment_1_pos_2",
                "hint_2_confidence": "0.9",
            }
        ],
    )

    result = process_one_days_file(
        days_csv,
        output_dir=str(output_dir),
        out_name="events_from_days_raw.csv",
        input_base=input_base,
    )

    assert result["status"] == "ok"
    assert int(result["rows_with_hint_events"]) == 1
    assert int(result["rows_fallback_regex"]) == 0
    assert int(result["events_from_hints"]) == 2
    assert int(result["events_from_regex"]) == 0
    assert int(result["events_extracted"]) == 2

    out_df = pd.read_csv(Path(str(result["output_events_csv"])))
    assert list(out_df["event_kind"]) == ["E", "U"]
    assert list(out_df["event_time_hhmm"]) == ["06:54", "13:11"]
    assert out_df.loc[0, "event_pattern"] == "hint:cartellino_ocr:segment_1_pos_1"
    assert out_df.loc[1, "event_pattern"] == "hint:cartellino_ocr:segment_1_pos_2"
    assert out_df.loc[0, "event_raw"] == "E 06:54"
    assert out_df.loc[1, "event_raw"] == "U 13:11"


def test_extract_events_falls_back_to_regex_on_invalid_hints(tmp_path: Path) -> None:
    input_base = tmp_path / "days"
    output_dir = tmp_path / "events"
    days_csv = input_base / "Mario Rossi" / "invalid_hints.days.csv"
    _write_days_csv(
        days_csv,
        [
            {
                "year": "2023",
                "month": "1",
                "day": "1",
                "dow": "LU",
                "mo_f": "6",
                "mo_t": "6",
                "mo_lav": "6",
                "raw": "01 lu E 08:00 U 14:00 6 6 6",
                "parser_id": "cartellino_ocr",
                "hint_count": "1",
                "hint_overflow": "0",
                "hint_1_kind": "X",
                "hint_1_time_hhmm": "99:99",
                "hint_1_source": "broken",
                "hint_1_confidence": "0.1",
            }
        ],
    )

    result = process_one_days_file(
        days_csv,
        output_dir=str(output_dir),
        out_name="events_from_days_raw.csv",
        input_base=input_base,
    )

    assert result["status"] == "ok"
    assert int(result["rows_with_invalid_hints"]) == 1
    assert int(result["rows_with_hint_events"]) == 0
    assert int(result["rows_fallback_regex"]) == 1
    assert int(result["events_from_hints"]) == 0
    assert int(result["events_from_regex"]) == 2

    out_df = pd.read_csv(Path(str(result["output_events_csv"])))
    assert list(out_df["event_kind"]) == ["E", "U"]
    assert all(not str(pattern).startswith("hint:") for pattern in out_df["event_pattern"])


def test_extract_events_keeps_legacy_regex_path_without_hints(tmp_path: Path) -> None:
    input_base = tmp_path / "days"
    output_dir = tmp_path / "events"
    days_csv = input_base / "Mario Rossi" / "legacy.days.csv"
    _write_days_csv(
        days_csv,
        [
            {
                "year": "2023",
                "month": "1",
                "day": "2",
                "dow": "MA",
                "mo_f": "6",
                "mo_t": "6",
                "mo_lav": "6",
                "raw": "02 ma E 07:30 U 13:30 6 6 6",
            }
        ],
    )

    result = process_one_days_file(
        days_csv,
        output_dir=str(output_dir),
        out_name="events_from_days_raw.csv",
        input_base=input_base,
    )

    assert result["status"] == "ok"
    assert int(result["rows_with_hint_events"]) == 0
    assert int(result["rows_fallback_regex"]) == 1
    assert int(result["events_from_hints"]) == 0
    assert int(result["events_from_regex"]) == 2
    assert int(result["events_extracted"]) == 2
