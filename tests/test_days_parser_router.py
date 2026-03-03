from __future__ import annotations

import json
from pathlib import Path

from src.extract_events_from_text_raw.parsers import load_parsers, resolve_parser

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


def test_load_parsers_discovers_expected_ids() -> None:
    parser_ids = {parser.parser_id for parser in load_parsers()}
    assert {
        "timbrature_web",
        "cartellino_classic",
        "situazione_mensile",
        "cartellino_unico",
        "cartellino_ocr",
    }.issubset(parser_ids)


def test_resolve_parser_uses_deterministic_fallback() -> None:
    parser = resolve_parser(_document("documento senza indicatori forti"))
    assert parser.parser_id == "timbrature_web"


def test_resolve_parser_matches_fixture_expectations() -> None:
    for expected_path in sorted(FIXTURES_ROOT.glob("*/*.expected.json")):
        payload = json.loads(expected_path.read_text(encoding="utf-8"))
        text_path = expected_path.with_name(
            expected_path.name.replace(".expected.json", ".txt")
        )
        text = text_path.read_text(encoding="utf-8")
        parser = resolve_parser(_document(text))
        assert parser.parser_id == str(payload["parser_id"])
