import os
import time
from typing import List, Optional

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
