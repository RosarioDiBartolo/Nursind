from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Iterable, Tuple

from drive_scanner.index_service import Index
from drive_scanner.io_json import load_json, write_json
from drive_scanner.schema import IndexFile, Outputs


def _ensure_outputs(raw: Any) -> Outputs | None:
    if not raw:
        return None
    if isinstance(raw, Outputs):
        return raw
    if isinstance(raw, dict):
        return Outputs(**raw)
    return None


def _as_index_file(
    raw: Any,
    employee: str | None,
    employee_id: str | None,
    default_type: str | None = None,
) -> IndexFile:
    payload: dict[str, Any] = {}

    if isinstance(raw, IndexFile):
        payload = raw.model_dump()
    elif isinstance(raw, dict):
        payload = dict(raw)

    # Force-fill employee fields when they're missing OR present-but-null/empty.
    # NOTE: dict.setdefault() won't override existing keys, even if value is None.
    if payload.get("employee") in (None, ""):
        payload["employee"] = employee or "unknown"
    if payload.get("employee_id") in (None, ""):
        payload["employee_id"] = employee_id

    if default_type and not payload.get("type"):
        payload["type"] = default_type

    if "outputs" in payload:
        payload["outputs"] = _ensure_outputs(payload.get("outputs"))

    return IndexFile(**payload)


def _iter_employee_files(
    entries: Iterable[dict],
    list_keys: Tuple[str, ...],
    default_type: str | None,
) -> list[IndexFile]:
    files: list[IndexFile] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue

        employee = entry.get("employee")
        employee_id = entry.get("employee_id")

        for key in list_keys:
            raw_files = entry.get(key) or []
            if not isinstance(raw_files, list):
                continue
            for raw in raw_files:
                files.append(_as_index_file(raw, employee, employee_id, default_type))
    return files


def _split_flat_files(items: list[Any]) -> tuple[list[IndexFile], list[IndexFile]]:
    included: list[IndexFile] = []
    excluded: list[IndexFile] = []
    for raw in items:
        file_item = _as_index_file(raw, None, None, None)
        if file_item.outputs:
            included.append(file_item)
        else:
            excluded.append(file_item)
    return included, excluded


def _extract_from_schema(data: Any) -> tuple[list[IndexFile], list[IndexFile]]:
    # Case 1: plain list of files
    if isinstance(data, list):
        return _split_flat_files(data)

    if not isinstance(data, dict):
        return [], []

    # Case 2: has included/excluded lists
    if "included" in data or "excluded" in data:
        included_raw = data.get("included") or []
        excluded_raw = data.get("excluded") or []

        # included can be either employee-buckets (each with "files") or flat files
        if (
            included_raw
            and isinstance(included_raw, list)
            and isinstance(included_raw[0], dict)
            and "files" in included_raw[0]
        ):
            included = _iter_employee_files(included_raw, ("files", "included"), None)
        else:
            included, _ = _split_flat_files(included_raw if isinstance(included_raw, list) else [])

        # excluded can be either employee-buckets (each with "files") or flat files
        if (
            excluded_raw
            and isinstance(excluded_raw, list)
            and isinstance(excluded_raw[0], dict)
            and "files" in excluded_raw[0]
        ):
            excluded = _iter_employee_files(excluded_raw, ("files", "excluded", "skipped"), "file")
        else:
            _, excluded = _split_flat_files(excluded_raw if isinstance(excluded_raw, list) else [])

        return included, excluded

    # Case 3: has employees list with per-employee included/excluded
    if "employees" in data:
        employees = data.get("employees") or []
        if not isinstance(employees, list):
            return [], []

        included = _iter_employee_files(
            employees,
            ("included", "files", "included_files"),
            None,
        )
        excluded = _iter_employee_files(
            employees,
            ("excluded", "skipped", "skipped_files", "excluded_files"),
            "file",
        )
        return included, excluded

    # Case 4: has files list
    if "files" in data and isinstance(data["files"], list):
        return _split_flat_files(data["files"])

    return [], []


def _build_index_meta(source: dict | None, files: list[IndexFile]) -> tuple[str | None, int]:
    root_id = None
    if isinstance(source, dict):
        root_id = source.get("root_id")

    employees = {item.employee for item in files if getattr(item, "employee", None)}
    employee_count = len(employees)
    return root_id, employee_count


def _write_index(out_path: Path, root_id: str | None, employee_count: int, files: list[IndexFile]) -> None:
    index = Index.generate_index(root_id, employee_count, files)
    write_json(str(out_path), index.model_dump())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert old index schemas to included/excluded index files."
    )
    parser.add_argument("--input", required=True, help="Input JSON (old schema)")
    parser.add_argument("--out-dir", default="downloads", help="Output directory")
    parser.add_argument("--included-out", default="included.index.json")
    parser.add_argument("--excluded-out", default="excluded.index.json")
    args = parser.parse_args()

    data = load_json(args.input)
    included, excluded = _extract_from_schema(data)

    source_meta = data if isinstance(data, dict) else None
    root_id, included_employee_count = _build_index_meta(source_meta, included)
    _root_id, excluded_employee_count = _build_index_meta(source_meta, excluded)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    _write_index(out_dir / args.included_out, root_id, included_employee_count, included)
    _write_index(out_dir / args.excluded_out, root_id, excluded_employee_count, excluded)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
