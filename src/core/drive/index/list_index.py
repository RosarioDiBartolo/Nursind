from __future__ import annotations

import os
from typing import Any

from pydantic import BaseModel, ConfigDict

from ..io_json import load_json, write_json
from ..schema import IndexFile
from .common import utc_now_iso


class ListIndex(BaseModel):
    model_config = ConfigDict(extra="forbid")

    root_id: str | None = None
    generated_at: str
    employee_count: int
    total_files: int = 0
    files: list[IndexFile]

    @staticmethod
    def generate_index(
        root_id: str | None,
        employee_count: int,
        files: list[dict] | list[IndexFile],
    ) -> "ListIndex":
        normalized_files = _normalize_files_list(files)
        return ListIndex.model_validate(
            {
                "root_id": root_id,
                "generated_at": utc_now_iso(),
                "employee_count": employee_count,
                "total_files": len(normalized_files),
                "files": normalized_files,
            }
        )

    @staticmethod
    def load_index(path: str, strict: bool = False) -> "ListIndex":
        if not os.path.exists(path):
            if strict:
                raise RuntimeError(f"Index not found at {path}")
            return ListIndex(
                root_id=None,
                generated_at=utc_now_iso(),
                employee_count=0,
                files=[],
            )

        data = load_json(path)
        files = data.get("files")
        if not isinstance(files, list):
            raise ValueError("Unsupported index schema: missing 'files' list")

        data["files"] = _normalize_files_list(files)
        if "total_files" not in data:
            data["total_files"] = len(data["files"])
        return ListIndex.model_validate(data)

    def save_index(self, out_path: str) -> None:
        write_json(out_path, self.model_dump())


def _normalize_files_list(files: list[Any]) -> list[dict]:
    normalized: list[dict] = []
    for index, entry in enumerate(files):
        if isinstance(entry, BaseModel):
            entry = entry.model_dump()
        if not isinstance(entry, dict):
            raise ValueError("Index file list values must be objects")
        payload = dict(entry)
        file_id = payload.get("file_id")
        if not file_id:
            raise ValueError(f"Index file list entry at position {index} is missing file_id")
        payload["file_id"] = str(file_id)
        normalized.append(payload)
    return normalized
