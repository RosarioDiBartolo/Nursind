import os
import time
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict

from .io_json import load_json, write_json
from .schema import IndexFile  # <-- don't import Index here (name clash)


class Index(BaseModel):
    model_config = ConfigDict(extra="forbid")

    root_id: Optional[str] = None
    generated_at: str
    employee_count: int
    files: List[IndexFile]

    @staticmethod
    def generate_index(
        root_id: Optional[str],
        employee_count: int,
        files: list[dict] | list[IndexFile],
    ) -> "Index":
        return Index.model_validate(
            {
                "root_id": root_id,
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "employee_count": employee_count,
                "files": files,
            }
        )

    @staticmethod
    def load_index(path: str, strict: bool = False) -> "Index":
        if not os.path.exists(path):
            if strict:
                raise RuntimeError(f"Index not found at {path}")
            # return an empty-but-valid model instead of {}
            return Index(
                root_id=None,
                generated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                employee_count=0,
                files=[],
            )

        data = load_json(path)
        return Index.model_validate(data)

    def save_index(self, out_path: str) -> None:
        write_json(out_path, self.model_dump())


def _normalize_index_entry(
    entry: Any,
    employee: Optional[str],
    employee_id: Optional[str],
) -> Optional[dict]:
    if isinstance(entry, BaseModel):
        entry = entry.model_dump()
    if not isinstance(entry, dict):
        return None
    return {
        "employee": entry.get("employee") or employee,
        "employee_id": entry.get("employee_id") or employee_id,
        "file_id": entry.get("file_id"),
        "file_name": entry.get("file_name"),
        "mimeType": entry.get("mimeType"),
        "container": entry.get("container"),
        "reason": entry.get("reason"),
        "type": entry.get("type"),
    }


def extract_index_files(data: dict) -> List[dict]:
    """
    Extract file entries from the current flat index schema:
    {"files": [ {employee, file_id, file_name, ...}, ... ]}
    """
    if not isinstance(data, dict):
        raise ValueError("Index data must be a dict")

    items: List[dict] = []

    def add_entry(entry: Any, employee: Optional[str], employee_id: Optional[str]) -> None:
        normalized = _normalize_index_entry(entry, employee, employee_id)
        if normalized:
            items.append(normalized)

    files = data.get("files")
    if not isinstance(files, list):
        raise ValueError("Unsupported index schema: missing 'files' array")

    for entry in files:
        if isinstance(entry, dict):
            add_entry(entry, entry.get("employee"), entry.get("employee_id"))
        else:
            add_entry(entry, None, None)
    return items


def load_index_files(path: str) -> List[dict]:
    data = load_json(path)
    return extract_index_files(data)
