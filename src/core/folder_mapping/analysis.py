"""Per-page adapters around the primary event parser, plus identity evidence."""
from __future__ import annotations

from pathlib import Path

from core.events.extraction.identity import employee_identity
from core.events.extraction.page_analysis import parse_document_pages, resolve_page_texts
from core.events.extraction.parsers.common import get_layout_pages


def analyze_document(document: dict, context: dict) -> tuple[list[dict], list[dict]]:
    texts = resolve_page_texts(document)
    layout = {int(page["page_no"]): page for page in get_layout_pages(document)}
    count = int(document.get("document", {}).get("page_count") or 1)
    numbers = sorted(set(range(1, count + 1)) | texts.keys() | layout.keys())
    identities = {number: employee_identity(texts.get(number, "")) for number in numbers}
    names = {value["employee"].casefold(): value["employee"] for value in identities.values() if value["employee"]}
    identifiers = {value["employee_identifier"] for value in identities.values() if value.get("employee_identifier")}
    unique_name = next(iter(names.values())) if len(names) == 1 and len(identifiers) <= 1 and not any(
        value["identity_status"] == "ambiguous" for value in identities.values()
    ) else None
    pages, events = [], []
    for number in numbers:
        identity = dict(identities[number])
        if identity["identity_status"] == "missing" and unique_name:
            identity.update(employee=unique_name, identity_status="document_unique")
        if unique_name and identity["employee"] and len(identifiers) == 1:
            identity["employee_identifier"] = next(iter(identifiers))
        page_context = {**context, "source_employee": identity["employee"]}
        single = {
            **document,
            "document": {"page_count": 0, "full_text": texts.get(number, "")},
            "layout": {"pages": [layout[number]] if number in layout else []},
        }
        try:
            _, page_events, parsed_pages, _, _ = parse_document_pages(
                document=single, source_context=page_context,
                # Mapping must derive dates from content rather than an arbitrary filename.
                source_path=Path("document.pdf"), max_unmatched_examples_per_file=5,
            )
            parsed = next((page for page in parsed_pages if int(page["page_no"]) == number), None)
            # Text-only fixtures use the primary parser's default page number 1.
            if parsed is None and not layout and len(numbers) == 1:
                parsed = parsed_pages[0]
            if parsed is None:
                parsed = {"decision": "skipped_non_event", "decision_reason": "empty_page", "events_extracted": 0}
            recognized = parsed.get("decision") == "parsed" and int(parsed.get("events_extracted") or 0) > 0
            pages.append({**parsed, **identity, "page_no": number, "is_timbrature": recognized,
                          "reason": "recognized" if recognized else parsed.get("decision_reason"),
                          "error": None, "file_id": context["source_file_id"]})
            events.extend(page_events)
        except Exception as exc:
            pages.append({**identity, "page_no": number, "is_timbrature": False,
                          "reason": "processing_error", "error": f"{type(exc).__name__}: {exc}",
                          "file_id": context["source_file_id"]})
    return pages, events
