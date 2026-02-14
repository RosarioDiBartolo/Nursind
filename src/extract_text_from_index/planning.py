from __future__ import annotations

import hashlib
import os

from src.drive_service.archive_utils import parse_archive_member_id
from src.drive_service.index_runtime import doc_attr
from src.drive_service.names import safe_name


def build_initial_stats(source_total: int) -> dict:
    return {
        "source_total": source_total,
        "skipped_missing_file_id": 0,
        "skipped_folder_items": 0,
        "skipped_excluded": 0,
        "skipped_included": 0,
        "queued": 0,
        "processed": 0,
        "succeeded": 0,
        "failed": 0,
        "download_failed": 0,
        "extract_failed": 0,
        "used_vertical": 0,
    }


def collect_docs(
    source_files: dict,
    included_map: dict,
    excluded_map: dict,
    *,
    skip_included: bool,
    skip_excluded: bool,
    limit: int,
    stats: dict,
) -> list[dict]:
    docs: list[dict] = []
    for doc in source_files.values():
        file_id = doc_attr(doc, "file_id")
        if not file_id:
            stats["skipped_missing_file_id"] += 1
            continue
        if doc_attr(doc, "type") == "folder":
            stats["skipped_folder_items"] += 1
            continue
        if skip_excluded and file_id in excluded_map:
            stats["skipped_excluded"] += 1
            continue
        if skip_included and file_id in included_map:
            stats["skipped_included"] += 1
            continue

        docs.append(
            _build_doc_payload(
                employee=doc_attr(doc, "employee") or "unknown",
                employee_id=doc_attr(doc, "employee_id"),
                file_id=file_id,
                file_name=doc_attr(doc, "file_name") or file_id,
                drive_path=doc_attr(doc, "drive_path"),
            )
        )

    if limit > 0:
        docs = docs[:limit]
    _assign_output_stems(docs)
    stats["queued"] = len(docs)
    return docs


def _assign_output_stems(docs: list[dict]) -> None:
    stem_counts: dict[tuple[str, str], int] = {}
    for doc in docs:
        employee_key = safe_name(doc["employee"] or "unknown")
        base_name = safe_name(doc.get("file_name") or doc["file_id"] or "unknown.pdf")
        stem, _ = os.path.splitext(base_name)
        key = (employee_key, stem)
        stem_counts[key] = stem_counts.get(key, 0) + 1

    for doc in docs:
        employee_key = safe_name(doc["employee"] or "unknown")
        base_name = safe_name(doc.get("file_name") or doc["file_id"] or "unknown.pdf")
        stem, _ = os.path.splitext(base_name)
        key = (employee_key, stem)
        if stem_counts.get(key, 0) > 1:
            doc["out_stem"] = f"{stem}__{_doc_suffix(doc)}"
        else:
            doc["out_stem"] = stem


def _build_doc_payload(*, employee: str, employee_id: str | None, file_id: str, file_name: str, drive_path: str | None) -> dict:
    payload = {
        "employee": employee,
        "employee_id": employee_id,
        "file_id": file_id,
        "file_name": file_name,
        "drive_path": drive_path,
        "source_kind": "drive_pdf",
    }
    parsed = parse_archive_member_id(file_id)
    if parsed:
        archive_file_id, archive_member_path = parsed
        payload["source_kind"] = "zip_member"
        payload["archive_file_id"] = archive_file_id
        payload["archive_member_path"] = archive_member_path
    return payload


def _doc_suffix(doc: dict) -> str:
    source_kind = doc.get("source_kind")
    if source_kind == "zip_member":
        archive_file_id = str(doc.get("archive_file_id") or "")
        archive_member_path = str(doc.get("archive_member_path") or "")
        base = f"{archive_file_id}|{archive_member_path}"
        return hashlib.sha1(base.encode("utf-8")).hexdigest()[:10]
    file_id = str(doc.get("file_id") or "")
    if file_id:
        return safe_name(file_id)[:12]
    return "unknown"
