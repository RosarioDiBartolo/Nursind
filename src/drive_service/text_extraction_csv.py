from __future__ import annotations

import csv
import hashlib
from collections import defaultdict
from pathlib import Path
from typing import Iterable

from .fs_utils import ensure_parent_dir
from .io_json import load_json, write_json
from .names import safe_name

TEXT_EXTRACTION_CSV_SUFFIX = ".csv"
TEXT_EXTRACTION_CSV_GLOB = f"*{TEXT_EXTRACTION_CSV_SUFFIX}"
LEGACY_TEXT_EXTRACTION_CSV_NAME = "text_extracted.csv"
TEXT_EXTRACTION_DOCS_DIR = "docs"
TEXT_EXTRACTION_DOC_SCHEMA_VERSION = "text_layout_v1"
TEXT_EXTRACTION_COLUMNS = [
    "employee",
    "employee_id",
    "file_id",
    "google_drive_file_id",
    "file_link",
    "file_name",
    "drive_path",
    "source_kind",
    "archive_file_id",
    "archive_member_path",
    "source_text_ref",
    "doc_json",
    "has_text_layer",
    "selected_mode",
    "tried_vertical",
    "normal_quality",
    "vertical_quality",
]


def build_google_drive_file_id(
    *,
    file_id: str | None,
    source_kind: str | None = None,
    archive_file_id: str | None = None,
) -> str | None:
    if source_kind == "local_pdf":
        return None
    if source_kind == "zip_member" and archive_file_id:
        return str(archive_file_id)
    if file_id:
        return str(file_id)
    return None


def build_google_drive_file_link(
    *,
    file_id: str | None,
    source_kind: str | None = None,
    archive_file_id: str | None = None,
) -> str | None:
    google_drive_file_id = build_google_drive_file_id(
        file_id=file_id,
        source_kind=source_kind,
        archive_file_id=archive_file_id,
    )
    if not google_drive_file_id:
        return None
    return f"https://drive.google.com/file/d/{google_drive_file_id}/view"


def build_employee_csv_rel_path(employee: str | None) -> str:
    safe_employee = safe_name(employee or "unknown")
    return Path(f"{safe_employee}{TEXT_EXTRACTION_CSV_SUFFIX}").as_posix()


def build_source_text_ref(employee: str | None, out_stem: str | None) -> str:
    safe_employee = safe_name(employee or "unknown")
    safe_stem = safe_name(out_stem or "unknown")
    return Path(safe_employee, f"{safe_stem}.txt").as_posix()


def build_doc_json_rel_path(file_id: str | None) -> str | None:
    normalized_file_id = str(file_id or "").strip()
    if not normalized_file_id:
        return None
    digest = hashlib.sha1(normalized_file_id.encode("utf-8")).hexdigest()[:12]
    stem = safe_name(normalized_file_id)[:80]
    return Path(TEXT_EXTRACTION_DOCS_DIR, f"{stem}__{digest}.json").as_posix()


def build_text_extraction_row(
    *,
    employee: str | None,
    employee_id: str | None,
    file_id: str | None,
    file_name: str | None,
    drive_path: str | None,
    source_kind: str | None,
    archive_file_id: str | None,
    archive_member_path: str | None,
    source_text_ref: str | None,
    doc_json: str | None,
    has_text_layer: bool | str | None,
    selected_mode: str | None,
    tried_vertical: bool | str | None,
    normal_quality: float | str | None,
    vertical_quality: float | str | None,
) -> dict[str, object]:
    return {
        "employee": employee or "unknown",
        "employee_id": employee_id,
        "file_id": file_id,
        "google_drive_file_id": build_google_drive_file_id(
            file_id=file_id,
            source_kind=source_kind,
            archive_file_id=archive_file_id,
        ),
        "file_link": build_google_drive_file_link(
            file_id=file_id,
            source_kind=source_kind,
            archive_file_id=archive_file_id,
        ),
        "file_name": file_name,
        "drive_path": drive_path,
        "source_kind": source_kind,
        "archive_file_id": archive_file_id,
        "archive_member_path": archive_member_path,
        "source_text_ref": source_text_ref,
        "doc_json": doc_json,
        "has_text_layer": has_text_layer,
        "selected_mode": selected_mode,
        "tried_vertical": tried_vertical,
        "normal_quality": normal_quality,
        "vertical_quality": vertical_quality,
    }


def find_text_extraction_csvs(base_dir: str | Path) -> list[Path]:
    base_path = Path(base_dir)
    if not base_path.exists():
        return []
    csv_paths = [path for path in base_path.glob(TEXT_EXTRACTION_CSV_GLOB) if path.is_file()]
    legacy_paths = [
        path for path in base_path.rglob(LEGACY_TEXT_EXTRACTION_CSV_NAME) if path.is_file()
    ]
    return sorted({*csv_paths, *legacy_paths})


def read_text_extraction_rows(
    csv_paths: Iterable[str | Path],
    *,
    hydrate_text: bool = True,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for csv_path in sorted(Path(path) for path in csv_paths):
        base_dir = _base_dir_for_manifest_csv(csv_path)
        with open(csv_path, "r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for raw_row in reader:
                row = {column: raw_row.get(column, "") for column in TEXT_EXTRACTION_COLUMNS}
                extracted_text = str(raw_row.get("extracted_text") or "")
                if hydrate_text and not extracted_text:
                    extracted_text = _load_full_text_from_doc(base_dir, row)
                if extracted_text:
                    row["extracted_text"] = extracted_text
                rows.append(row)
    return rows


def load_text_extraction_rows(
    base_dir: str | Path,
    *,
    hydrate_text: bool = False,
) -> dict[str, dict[str, str]]:
    rows_by_file_id: dict[str, dict[str, str]] = {}
    for row in read_text_extraction_rows(
        find_text_extraction_csvs(base_dir),
        hydrate_text=hydrate_text,
    ):
        file_id = str(row.get("file_id") or "").strip()
        if not file_id:
            continue
        if not _row_has_backing_payload(base_dir, row):
            continue
        rows_by_file_id[file_id] = row
    return rows_by_file_id


def write_text_extraction_rows(
    base_dir: str | Path,
    rows_by_file_id: dict[str, dict[str, object]],
) -> list[str]:
    base_path = Path(base_dir)
    existing_paths = {path.resolve() for path in find_text_extraction_csvs(base_path)}
    grouped_rows: dict[str, list[dict[str, object]]] = defaultdict(list)

    for row in rows_by_file_id.values():
        employee = str(row.get("employee") or "unknown")
        grouped_rows[employee].append(_normalize_row(row))

    written_paths: list[str] = []
    written_resolved: set[Path] = set()
    for employee in sorted(grouped_rows):
        rel_path = build_employee_csv_rel_path(employee)
        csv_path = (base_path / rel_path).resolve()
        ensure_parent_dir(str(csv_path))
        with open(csv_path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=TEXT_EXTRACTION_COLUMNS)
            writer.writeheader()
            for row in sorted(
                grouped_rows[employee],
                key=lambda item: (
                    str(item.get("source_text_ref") or ""),
                    str(item.get("file_name") or ""),
                    str(item.get("file_id") or ""),
                ),
            ):
                writer.writerow(row)
        written_paths.append(str(csv_path))
        written_resolved.add(csv_path)

    for stale_path in existing_paths - written_resolved:
        stale_path.unlink(missing_ok=True)

    return written_paths


def write_text_extraction_doc(
    base_dir: str | Path,
    file_id: str | None,
    payload: dict[str, object],
) -> str:
    rel_path = build_doc_json_rel_path(file_id)
    if rel_path is None:
        raise ValueError("Cannot write text extraction doc without file_id")
    out_path = Path(base_dir) / rel_path
    ensure_parent_dir(str(out_path))
    write_json(str(out_path), payload)
    return rel_path


def load_text_extraction_doc(
    base_dir: str | Path,
    rel_path: str | None,
) -> dict | None:
    resolved = resolve_doc_json_path(base_dir, rel_path)
    if resolved is None or not resolved.exists():
        return None
    data = load_json(str(resolved))
    if not isinstance(data, dict):
        return None
    return data


def resolve_doc_json_path(base_dir: str | Path, rel_path: str | None) -> Path | None:
    normalized_rel = str(rel_path or "").strip()
    if not normalized_rel:
        return None
    raw_path = Path(normalized_rel)
    if raw_path.is_absolute():
        return None

    base_path = Path(base_dir).resolve()
    docs_root = (base_path / TEXT_EXTRACTION_DOCS_DIR).resolve()
    candidate = (base_path / raw_path).resolve()
    try:
        candidate.relative_to(docs_root)
    except ValueError:
        return None
    return candidate


def prune_stale_text_extraction_docs(
    base_dir: str | Path,
    rows_by_file_id: dict[str, dict[str, object]],
) -> None:
    docs_dir = Path(base_dir) / TEXT_EXTRACTION_DOCS_DIR
    if not docs_dir.exists():
        return
    expected_paths = {
        resolve_doc_json_path(base_dir, str(row.get("doc_json") or "")).resolve()
        for row in rows_by_file_id.values()
        if row.get("doc_json")
    }
    for existing in docs_dir.glob("*.json"):
        if existing.resolve() not in expected_paths:
            existing.unlink(missing_ok=True)


def _normalize_row(row: dict[str, object]) -> dict[str, object]:
    return {column: row.get(column) for column in TEXT_EXTRACTION_COLUMNS}


def _base_dir_for_manifest_csv(csv_path: Path) -> Path:
    if csv_path.name == LEGACY_TEXT_EXTRACTION_CSV_NAME:
        return csv_path.parent.parent
    return csv_path.parent


def _load_full_text_from_doc(base_dir: Path, row: dict[str, str]) -> str:
    payload = load_text_extraction_doc(base_dir, row.get("doc_json"))
    if payload is None:
        return ""
    document = payload.get("document")
    if not isinstance(document, dict):
        return ""
    full_text = document.get("full_text")
    if isinstance(full_text, str):
        return full_text
    return ""


def _row_has_backing_payload(base_dir: str | Path, row: dict[str, str]) -> bool:
    if row.get("doc_json"):
        path = resolve_doc_json_path(base_dir, row.get("doc_json"))
        return bool(path and path.exists())
    return bool(row.get("extracted_text"))
