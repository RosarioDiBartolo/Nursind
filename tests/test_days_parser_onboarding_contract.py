from __future__ import annotations

from pathlib import Path

from src.extract_events_from_documents.parsers import load_parsers

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "days_parsers"


def test_every_discovered_parser_has_required_fixture_bundle() -> None:
    for parser in load_parsers():
        fixture_dir = FIXTURES_ROOT / parser.parser_id
        assert fixture_dir.exists(), f"Missing fixtures folder for parser '{parser.parser_id}'"
        assert any(fixture_dir.glob("*.txt")), (
            f"Missing .txt fixture for parser '{parser.parser_id}'"
        )
        assert any(fixture_dir.glob("*.expected.json")), (
            f"Missing .expected.json fixture for parser '{parser.parser_id}'"
        )


def test_discovered_parser_contract_shape() -> None:
    for parser in load_parsers():
        assert isinstance(parser.parser_id, str) and parser.parser_id
        assert isinstance(parser.legacy_doc_format, str) and parser.legacy_doc_format
        assert isinstance(parser.priority, int)
        assert callable(parser.score_document)
        assert callable(parser.parse_document)

