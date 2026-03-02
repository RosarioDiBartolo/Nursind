from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.extract_events_from_text_raw.parsers import resolve_parser
from src.extract_events_from_text_raw.service import _parse_rows_for_file

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "days_parsers"


def _assert_hour_sanity(value: float | None) -> None:
    if value is None:
        return
    assert abs(float(value)) <= 24.0


@pytest.mark.parametrize(
    "expected_path",
    sorted(FIXTURES_ROOT.glob("*/*.expected.json")),
)
def test_parser_contracts_from_fixtures(expected_path: Path) -> None:
    payload = json.loads(expected_path.read_text(encoding="utf-8"))
    text_path = expected_path.with_name(
        expected_path.name.replace(".expected.json", ".txt")
    )
    text = text_path.read_text(encoding="utf-8")
    parser = resolve_parser(text)
    assert parser.parser_id == str(payload["parser_id"])

    rows, stats = _parse_rows_for_file(text, parser=parser)
    expected_rows = payload["rows"]
    assert int(stats["rows_parsed"]) == len(expected_rows)
    assert len(rows) == len(expected_rows)

    for expected_row in expected_rows:
        expected_day = int(expected_row["day"])
        expected_dow = str(expected_row["dow"])
        row = next(
            item for item in rows if item.day == expected_day and item.dow == expected_dow
        )
        assert row.mo_f == pytest.approx(float(expected_row["mo_f"]), abs=1e-4)
        assert row.mo_t == pytest.approx(float(expected_row["mo_t"]), abs=1e-4)
        assert row.mo_lav == pytest.approx(float(expected_row["mo_lav"]), abs=1e-4)
        _assert_hour_sanity(row.mo_f)
        _assert_hour_sanity(row.mo_t)
        _assert_hour_sanity(row.mo_lav)

        expected_hints = expected_row.get("event_hints")
        if expected_hints is None:
            continue

        assert len(row.event_hints) == len(expected_hints)
        for index, expected_hint in enumerate(expected_hints):
            hint = row.event_hints[index]
            assert hint.kind == str(expected_hint["kind"])
            assert hint.time_hhmm == str(expected_hint["time_hhmm"])
            if "source" in expected_hint:
                assert hint.source == str(expected_hint["source"])
            assert 0.0 <= float(hint.confidence) <= 1.0
