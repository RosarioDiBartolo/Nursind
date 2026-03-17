from __future__ import annotations

import json
from pathlib import Path

import pytest

from cartellino_parser.extract_events_from_documents.parsers import resolve_parser

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "days_parsers"


def _document(text: str) -> dict[str, object]:
    return {
        "document": {
            "full_text": text,
        },
        "layout": {
            "pages": [],
        },
    }


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
    parser = resolve_parser(_document(text))
    assert parser.parser_id == str(payload["parser_id"])

    rows = parser.parse_document(_document(text))
    expected_rows = payload["rows"]
    assert len(rows) == len(expected_rows)

    for expected_row in expected_rows:
        expected_day = int(expected_row["day"])
        expected_dow = str(expected_row["dow"])
        row = next(
            item for item in rows if item.day == expected_day and item.dow == expected_dow
        )
        expected_events = expected_row.get("event_hints")
        if expected_events is None:
            continue
        assert len(row.events) == len(expected_events)
        for index, expected_event in enumerate(expected_events):
            event = row.events[index]
            assert event.event_kind == str(expected_event["kind"])
            assert event.event_time_hhmm == str(expected_event["time_hhmm"])
            assert isinstance(event.event_pattern, str) and event.event_pattern


