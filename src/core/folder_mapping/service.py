"""Build a complete folder map from inventory and canonical extraction artifacts."""
from __future__ import annotations

from collections import Counter
import json
import logging

from core.drive.index.map_index import MapIndex
from core.drive.text_extraction_csv import build_google_drive_file_link, load_text_extraction_rows
from core.events.extraction.source_context import load_manifest_document, resolve_source_context
from core.events.extraction.writers import EVENT_COLUMNS, PAGE_COLUMNS, write_rows_csv
from core.reporting import write_json_report

from .analysis import analyze_document

FILE_COLUMNS = ["file_id", "file_url", "file_name", "relative_path", "is_timbrature", "employee", "identity_status", "reason", "error"]

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
        if item.get("reason") or not item["is_pdf"]:
            reason = item.get("reason") or "not_pdf"
            error = item.get("error")
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
        attendance = [page for page in pages if page["is_timbrature"]]
        names = {page["employee"].casefold(): page["employee"] for page in attendance if page.get("employee")}
        status = "not_applicable"
        if attendance:
            identifiers = {page["employee_identifier"] for page in attendance if page.get("employee_identifier")}
            if len(names) > 1 or len(identifiers) > 1:
                status = "multiple"
            elif any(not page.get("employee") for page in attendance):
                status = "unresolved"
            else:
                status = "unique"
        files.append({**item, "file_url": build_google_drive_file_link(file_id=file_id), "is_timbrature": reason == "recognized", "reason": reason,
                      "identity_status": status, "employee": next(iter(names.values())) if status == "unique" else None,
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
        "schema_version": 2, "root_id": config.drive_root_id, "name": config.name,
        "stats": {"files": len(files), "pdfs": sum(item["is_pdf"] for item in files),
                  "timbrature_files": sum(item["is_timbrature"] for item in files),
                  "pages": len(all_pages), "events": len(all_events)},
        "files": files,
    }
    report["stats"]["unassigned_events"] = sum(not row.get("source_employee") for row in all_events)
    coverage = Counter((page.get("parser_id") or "unknown", page.get("identity_status") or "missing")
                       for page in all_pages if page.get("is_timbrature"))
    write_rows_csv(rows=[{"parser_id": parser, "identity_status": status, "pages": count}
                         for (parser, status), count in sorted(coverage.items())],
                   columns=["parser_id", "identity_status", "pages"], out_csv=paths.events_dir / "identity_coverage.csv")
    write_rows_csv(rows=files, out_csv=paths.events_dir / "files.csv", columns=FILE_COLUMNS)
    relations = [{"file_id": item["file_id"], "employee": name} for item in files
                 for name in sorted({p["employee"] for p in item["pages"] if p["is_timbrature"] and p.get("employee")})]
    write_rows_csv(rows=relations, out_csv=paths.events_dir / "file_employees.csv", columns=["file_id", "employee"])
    write_json_report(paths.pipeline_root / "mapping.json", report)
    write_rows_csv(rows=all_events, out_csv=paths.events_csv, columns=EVENT_COLUMNS)
    columns = PAGE_COLUMNS + ["employee_identifier", "file_id", "employee", "identity_status", "is_timbrature", "reason", "error"]
    write_rows_csv(rows=all_pages, out_csv=paths.pages_csv, columns=columns)
    logger.info("Mapping saved: files=%s timbrature=%s pages=%s events=%s | %s",
                len(files), report["stats"]["timbrature_files"], len(all_pages), len(all_events),
                paths.pipeline_root / "mapping.json")
    logger.info("Mapping CSV outputs: events=%s | pages=%s", paths.events_csv, paths.pages_csv)
    return report
