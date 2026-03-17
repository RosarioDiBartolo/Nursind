from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from cartellino_parser.drive_service.text_extraction_csv import load_text_extraction_doc


def first_non_empty_string(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str):
            clean = value.strip()
            if clean:
                return clean
    return None


def build_source_file_ref(
    *,
    employee: str | None,
    file_name: str | None,
    file_id: str | None,
    doc_json: str,
) -> str:
    base_name = file_name or file_id or Path(doc_json).name
    if employee:
        return str(Path(employee) / base_name)
    return base_name


def normalize_source_doc_json(
    *,
    doc_json: str,
    input_dir: str | None,
) -> str:
    normalized = str(doc_json or "").strip()
    if not normalized:
        return "unknown-doc.json"

    cwd = Path.cwd().resolve()
    doc_path = Path(normalized)
    if doc_path.is_absolute():
        try:
            return doc_path.resolve().relative_to(cwd).as_posix()
        except Exception:
            try:
                return Path(os.path.relpath(doc_path.resolve(), cwd)).as_posix()
            except Exception:
                return doc_path.name or "unknown-doc.json"

    if input_dir:
        try:
            resolved = (Path(input_dir) / doc_path).resolve()
            try:
                return resolved.relative_to(cwd).as_posix()
            except Exception:
                return Path(os.path.relpath(resolved, cwd)).as_posix()
        except Exception:
            return doc_path.as_posix()

    return doc_path.as_posix()


def resolve_source_context(
    row: dict[str, Any],
    payload: dict[str, Any],
    *,
    input_dir: str | None,
) -> dict[str, Any]:
    source = payload.get("source")
    source_map = source if isinstance(source, dict) else {}
    source_doc_json = normalize_source_doc_json(
        doc_json=str(row.get("doc_json") or ""),
        input_dir=input_dir,
    )
    source_file_id = first_non_empty_string(row.get("file_id"), source_map.get("file_id"))
    source_file_name = first_non_empty_string(
        source_map.get("file_name"),
        row.get("file_name"),
    )
    source_employee = first_non_empty_string(
        row.get("employee"),
        source_map.get("employee"),
    )
    source_drive_path = first_non_empty_string(
        source_map.get("drive_path"),
        row.get("drive_path"),
    )
    source_file_link = first_non_empty_string(
        source_map.get("file_link"),
        row.get("file_link"),
    )
    return {
        "source_doc_json": source_doc_json,
        "source_file_id": source_file_id,
        "source_file_name": source_file_name,
        "source_employee": source_employee,
        "source_drive_path": source_drive_path,
        "source_file_link": source_file_link,
        "source_file_ref": build_source_file_ref(
            employee=source_employee,
            file_name=source_file_name,
            file_id=source_file_id,
            doc_json=source_doc_json,
        ),
    }


def resolve_source_path(
    row: dict[str, Any],
    payload: dict[str, Any],
    *,
    fallback: str,
) -> Path:
    source = payload.get("source")
    if isinstance(source, dict):
        file_name = source.get("file_name")
        if isinstance(file_name, str) and file_name.strip():
            return Path(file_name.strip())
    row_file_name = row.get("file_name")
    if isinstance(row_file_name, str) and row_file_name.strip():
        return Path(row_file_name.strip())
    return Path(fallback)


def load_manifest_document(
    row: dict[str, Any],
    *,
    input_dir: str | None,
) -> dict[str, Any] | None:
    doc_json = str(row.get("doc_json") or "").strip()
    if not doc_json:
        return None
    if input_dir:
        payload = load_text_extraction_doc(input_dir, doc_json)
        if isinstance(payload, dict):
            return payload
    doc_path = Path(doc_json)
    if doc_path.is_absolute() and doc_path.parent.name == "docs" and doc_path.exists():
        try:
            return json.loads(doc_path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


__all__ = [
    "load_manifest_document",
    "resolve_source_context",
    "resolve_source_path",
]

