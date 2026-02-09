from __future__ import annotations

from typing import Any, Literal

from ..io_json import load_json
from .list_index import ListIndex
from .map_index import MapIndex

IndexKind = Literal["map", "list"]
DuplicatePolicy = Literal["error", "first", "last"]
AnyIndex = MapIndex | ListIndex


def detect_index_kind(payload: dict[str, Any]) -> IndexKind:
    files = payload.get("files")
    if isinstance(files, dict):
        return "map"
    if isinstance(files, list):
        return "list"
    raise ValueError("Unsupported index schema: expected 'files' as object or list")


def detect_index_kind_from_path(path: str) -> IndexKind:
    return detect_index_kind(load_json(path))


def map_to_list_index(source: MapIndex) -> ListIndex:
    files = [item.model_dump() for item in source.files.values()]
    return ListIndex.model_validate(
        {
            "root_id": source.root_id,
            "generated_at": source.generated_at,
            "employee_count": source.employee_count,
            "total_files": len(files),
            "files": files,
        }
    )


def list_to_map_index(
    source: ListIndex,
    duplicate_policy: DuplicatePolicy = "error",
) -> MapIndex:
    files: dict[str, dict[str, Any]] = {}
    for item in source.files:
        payload = item.model_dump()
        file_id = payload.get("file_id")
        if not file_id:
            raise ValueError("List index contains entry without file_id")
        if file_id in files:
            if duplicate_policy == "error":
                raise ValueError(f"Duplicate file_id in list index: {file_id}")
            if duplicate_policy == "first":
                continue
        files[file_id] = payload

    return MapIndex.model_validate(
        {
            "root_id": source.root_id,
            "generated_at": source.generated_at,
            "employee_count": source.employee_count,
            "total_files": len(files),
            "files": files,
        }
    )


def load_index_auto(path: str, *, strict: bool = True) -> tuple[IndexKind, AnyIndex]:
    kind = detect_index_kind_from_path(path)
    if kind == "map":
        return kind, MapIndex.load_index(path, strict=strict)
    return kind, ListIndex.load_index(path, strict=strict)


def convert_index(index: AnyIndex, *, target_kind: IndexKind, duplicate_policy: DuplicatePolicy) -> AnyIndex:
    if target_kind == "map":
        if isinstance(index, MapIndex):
            return index
        return list_to_map_index(index, duplicate_policy=duplicate_policy)

    if isinstance(index, ListIndex):
        return index
    return map_to_list_index(index)


def convert_index_file(
    *,
    input_path: str,
    output_path: str,
    target_kind: IndexKind | None,
    duplicate_policy: DuplicatePolicy = "error",
) -> tuple[IndexKind, IndexKind, int]:
    source_kind, source = load_index_auto(input_path, strict=True)
    resolved_target = target_kind or ("list" if source_kind == "map" else "map")
    converted = convert_index(
        source,
        target_kind=resolved_target,
        duplicate_policy=duplicate_policy,
    )
    converted.save_index(output_path)
    return source_kind, resolved_target, converted.total_files
