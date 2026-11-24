"""Contract tests driven by the same synthetic documents shown in the guide."""

import pytest

from core.events.extraction.parsers import load_parsers, resolve_parser
from core.tools.parser_catalog.cases import FIXTURES, inspect_case, load_case


def test_each_discovered_parser_has_documents_and_a_guide():
    assert {path.name for path in FIXTURES.iterdir() if path.is_dir()} == {
        parser.parser_id for parser in load_parsers()
    }
    for directory in FIXTURES.iterdir():
        if directory.is_dir():
            assert list(directory.glob("*.case.json")), directory
            assert (directory / "guide.json").is_file()


@pytest.mark.parametrize("path", sorted(FIXTURES.glob("*/*.txt")),
                         ids=lambda path: f"{path.parent.name}/{path.name}")
def test_legacy_document_routes_to_its_parser(path):
    document = {"document": {"full_text": path.read_text(encoding="utf-8")}}
    assert resolve_parser(document).parser_id == path.parent.name


@pytest.mark.parametrize("path", sorted(FIXTURES.glob("*/*.case.json")),
                         ids=lambda path: f"{path.parent.name}/{path.name}")
def test_parser_document(path):
    case = load_case(path)
    seen = inspect_case(case)
    expected = case.expected
    assert seen["parser"].parser_id == case.parser_id
    assert [row.day for row in seen["rows"]] == expected.get("days", [])
    assert [f"{event['event_kind']} {event['event_ts'][:16]}"
            for event in seen["events"]] == expected["events"]
    assert seen["identity"]["employee"] == expected.get("employee")
    assert seen["identity"]["employee_identifier"] == expected.get("employee_identifier")
    identity_default = "recognized" if expected.get("employee") else "missing"
    assert seen["identity"]["identity_status"] == expected.get("identity_status", identity_default)
    if "slots" in expected:
        assert [event["source_slot"] for event in seen["events"]] == expected["slots"]
    if "decision" in expected:
        assert seen["page"]["decision"] == expected["decision"]
    if "period_source" in expected:
        assert seen["page"]["year_month_source"] == expected["period_source"]
    if "candidates" in expected:
        assert sum(len(row.events) for row in seen["rows"]) == expected["candidates"]
