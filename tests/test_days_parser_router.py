from __future__ import annotations

import json
from pathlib import Path

from cartellino_parser.extract_events_from_documents.parsers import load_parsers, resolve_parser

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


def test_resolve_parser_and_parse_document_for_rilevazione_compact_rows() -> None:
    text = (
        "Azienda Ospedaliero Universitaria ''Policlinico - Vittorio Emanuele''\n"
        "RILEVAZIONE DEL MESE DI OTTOBRE 2021\n"
        "ve 01 E0650 U1409 07.09 01.09 01.09 2CARD\n"
        "lu 04 E1258 U2008 07.08 01.08 01.08 2CARD\n"
        "do*10 GG:RS 2CARD\n"
    )

    parser = resolve_parser(_document(text))

    assert parser.parser_id == "cartellino_classic"
    rows = parser.parse_document(_document(text))
    assert len(rows) == 3
    assert [(event.event_kind, event.event_time_hhmm) for event in rows[0].events] == [
        ("E", "06:50"),
        ("U", "14:09"),
    ]


