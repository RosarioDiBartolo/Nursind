"""Build a complete folder map from inventory and canonical extraction artifacts."""
from __future__ import annotations

import json
import logging

from core.drive.index.map_index import MapIndex
from core.drive.text_extraction_csv import load_text_extraction_rows
from core.events.extraction.source_context import load_manifest_document, resolve_source_context
from core.events.extraction.writers import EVENT_COLUMNS, PAGE_COLUMNS, write_rows_csv
from core.reporting import write_json_report

from .analysis import analyze_document

logger = logging.getLogger(__name__)


def map_extracted_folder(config) -> dict:
    paths = config.paths
    inventory = json.loads((paths.scan_dir / "inventory.json").read_text(encoding="utf-8"))
    if inventory["root_id"] != config.drive_root_id:
        raise ValueError("Inventory belongs to another Drive folder; run scan again")
    manifests = load_text_extraction_rows(paths.documents_dir)
    excluded = MapIndex.load_index(str(paths.documents_excluded_index)).files
    total = len(inventory["files"])
    logger.info("Content mapping started: files=%s extracted documents=%s", total, len(manifests))
    files, all_pages, all_events = [], [], []
    for position, item in enumerate(inventory["files"], start=1):
        file_id = item["file_id"]
        label = item.get("relative_path") or item.get("file_name") or file_id
        logger.info("Mapping file %s/%s: %s [%s]", position, total, label, file_id)
        row = manifests.get(file_id)
        pages: list[dict] = []
        events: list[dict] = []
        error = None
        if not item["is_pdf"]:
            reason = "not_pdf"
        elif row is None:
            reason = "extraction_failed" if file_id in excluded else "not_extracted"
            error = excluded[file_id].reason if file_id in excluded else None
        else:
            try:
                payload = load_manifest_document(row, input_dir=str(paths.documents_dir))
                if payload is None:
                    raise ValueError("Canonical document payload missing")
                context = resolve_source_context(row, payload, input_dir=str(paths.documents_dir))
                pages, events = analyze_document(payload, context)
                failed = any(page.get("error") for page in pages)
                if failed:
                    # File failures must never become positive attendance classifications.
                    for page in pages:
                        page["is_timbrature"] = False
                    events = []
                    reason = "processing_error"
                    error = "; ".join(page["error"] for page in pages if page.get("error"))
                else:
                    reason = "recognized" if any(page["is_timbrature"] for page in pages) else "no_recognized_events"
            except Exception as exc:
                reason = "processing_error"
                error = f"{type(exc).__name__}: {exc}"
        employees = sorted({page["employee"] for page in pages if page.get("employee")}, key=str.casefold)
        files.append({**item, "is_timbrature": reason == "recognized", "reason": reason,
                      "error": error, "employees": employees, "pages": pages})
        all_pages.extend(pages)
        all_events.extend(events)
        log = logger.warning if reason in {"processing_error", "extraction_failed", "not_extracted"} else logger.info
        log("Mapped file %s/%s: %s | result=%s pages=%s events=%s identities=%s unresolved pages=%s",
            position, total, label, reason, len(pages), len(events), len(employees),
            sum(page.get("identity_status") in {"missing", "ambiguous"} for page in pages))
        # Detailed exception messages and identity evidence stay in mapping.json.
        for page in pages:
            logger.debug("Page mapped: file=%s page=%s | timbrature=%s identity=%s reason=%s",
                         file_id, page.get("page_no"), page["is_timbrature"],
                         page.get("identity_status"), page.get("reason"))
    report = {
        "schema_version": 1, "root_id": config.drive_root_id, "name": config.name,
        "stats": {"files": len(files), "pdfs": sum(item["is_pdf"] for item in files),
                  "timbrature_files": sum(item["is_timbrature"] for item in files),
                  "pages": len(all_pages), "events": len(all_events)},
        "files": files,
    }
    write_json_report(paths.pipeline_root / "mapping.json", report)
    write_rows_csv(rows=all_events, out_csv=paths.events_csv, columns=EVENT_COLUMNS)
    columns = PAGE_COLUMNS + ["file_id", "employee", "identity_status", "is_timbrature", "reason", "error"]
    write_rows_csv(rows=all_pages, out_csv=paths.pages_csv, columns=columns)
    logger.info("Mapping saved: files=%s timbrature=%s pages=%s events=%s | %s",
                len(files), report["stats"]["timbrature_files"], len(all_pages), len(all_events),
                paths.pipeline_root / "mapping.json")
    logger.info("Mapping CSV outputs: events=%s | pages=%s", paths.events_csv, paths.pages_csv)
    return report
