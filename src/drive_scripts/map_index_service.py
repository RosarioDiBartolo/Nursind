import logging
import os
import time
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict

from .io_json import load_json, write_json
from .schema import IndexFile

logger = logging.getLogger(__name__)

class MapIndex(BaseModel):
    model_config = ConfigDict(extra="forbid")

    root_id: Optional[str] = None
    generated_at: str
    employee_count: int
    total_files: int = 0
    files: dict[str, IndexFile]

    @staticmethod
    def generate_index(
        root_id: Optional[str],
        employee_count: int,
        files: dict[str, dict] | dict[str, IndexFile],
    ) -> "MapIndex":
        normalized_files = _normalize_files_map(files)
        return MapIndex.model_validate(
            {
                "root_id": root_id,
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "employee_count": employee_count,
                "total_files": len(normalized_files),
                "files": normalized_files,
            }
        )

    @staticmethod
    def load_index(path: str, strict: bool = False, allow_legacy: bool = False) -> "MapIndex":
        if not os.path.exists(path):
            if strict:
                raise RuntimeError(f"Index not found at {path}")
            return MapIndex(
                root_id=None,
                generated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                employee_count=0,
                files={},
            )

        data = load_json(path)
        files = data.get("files")
        if isinstance(files, list):
            if not allow_legacy:
                raise ValueError("Unsupported index schema: 'files' must be an object keyed by file_id")
            data["files"] = _list_to_map(files)
        elif not isinstance(files, dict):
            raise ValueError("Unsupported index schema: missing 'files' object")

        data["files"] = _normalize_files_map(data["files"])
        if "total_files" not in data:
            data["total_files"] = len(data["files"])
        return MapIndex.model_validate(data)

    def save_index(self, out_path: str) -> None:
        write_json(out_path, self.model_dump())


def _list_to_map(files: list[Any]) -> dict[str, dict]:
    file_map: dict[str, dict] = {}
    for entry in files:
        if isinstance(entry, BaseModel):
            entry = entry.model_dump()
        if not isinstance(entry, dict):
            raise ValueError("Index file entries must be objects")
        file_id = entry.get("file_id")
        if not file_id:
            raise ValueError("Index file entry missing file_id")
        if file_id in file_map:
            logger.warning("Duplicate file_id detected in legacy index: %s (last one wins)", file_id)
        file_map[file_id] = entry
    return file_map


def _normalize_files_map(files: dict[str, Any]) -> dict[str, dict]:
    normalized: dict[str, dict] = {}
    for key, entry in files.items():
        if not key:
            raise ValueError("Index file map contains empty file_id key")
        if isinstance(entry, BaseModel):
            entry = entry.model_dump()
        if not isinstance(entry, dict):
            raise ValueError("Index file map values must be objects")
        entry = dict(entry)
        entry_file_id = entry.get("file_id")
        if entry_file_id and entry_file_id != key:
            raise ValueError(f"File map key mismatch: {key} != {entry_file_id}")
        entry["file_id"] = key
        normalized[key] = entry
    return normalized
