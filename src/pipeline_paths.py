from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal

from src.drive_service.fs_utils import ensure_dir

PipelineStep = Literal[
    "root",
    "scan",
    "documents",
    "events",
    "shifts",
    "enrichment",
    "aggregation",
]

_PIPELINE_STEP_OUTPUT_ATTRS: dict[str, str] = {
    "root": "root_output",
    "scan": "scan_output",
    "documents": "documents_output",
    "events": "events_output",
    "shifts": "shifts_output",
    "enrichment": "enrichment_output",
    "aggregation": "aggregation_output",
}


@dataclass(slots=True)
class PipelinePaths:
    root_output: Path
    scan_output: Path
    documents_output: Path
    events_output: Path
    shifts_output: Path
    enrichment_output: Path
    aggregation_output: Path

    def ensure(self, step: PipelineStep) -> None:
        path_attr = _PIPELINE_STEP_OUTPUT_ATTRS.get(step)
        if path_attr is None:
            supported_steps = ", ".join(_PIPELINE_STEP_OUTPUT_ATTRS)
            raise ValueError(
                f"Unknown pipeline step {step!r}. Expected one of: {supported_steps}."
            )
        ensure_dir(str(getattr(self, path_attr)))

    def ensure_dirs(self) -> None:
        for path_attr in _PIPELINE_STEP_OUTPUT_ATTRS.values():
            ensure_dir(str(getattr(self, path_attr)))


def build_pipelines_paths(
    root_id: str | None = None,
    *,
    root_prefix: str | None = None,
    base_output: str | Path | None = None,
    create_dirs: bool = True,
) -> PipelinePaths:
    explicit_prefix = str(root_prefix).strip() if root_prefix is not None else ""
    explicit_root_id = str(root_id).strip() if root_id is not None else ""
    env_root_id = os.getenv("DRIVE_ROOT_FOLDER_ID", "").strip()
    candidate_root_id = explicit_root_id or env_root_id

    resolved_prefix = (
        explicit_prefix
        or os.getenv("OUTPUT_ROOT_PREFIX", "").strip()
        or os.getenv("PIPELINE_ROOT_PREFIX", "").strip()
        or _resolve_root_prefix_from_drive(candidate_root_id)
        or candidate_root_id
        or "default"
    )

    resolved_base_output = (
        Path(base_output)
        if base_output is not None
        else Path(os.getenv("OUTPUT_BASE_DIR", "output"))
    )
    root_output = resolved_base_output
    if resolved_prefix:
        root_output = root_output / Path(resolved_prefix)

    outputs = PipelinePaths(
        root_output=root_output,
        scan_output=root_output / "scan",
        documents_output=root_output / "documents",
        events_output=root_output / "events",
        shifts_output=root_output / "shifts",
        enrichment_output=root_output / "enrichment",
        aggregation_output=root_output / "aggregation",
    )
    if create_dirs:
        outputs.ensure_dirs()
    return outputs
 

@lru_cache(maxsize=128)
def _resolve_root_prefix_from_drive(root_id: str) -> str | None:
    root = str(root_id or "").strip()
    if not root:
        return None
    try:
        from src.drive_service.auth_service import load_creds
        from src.drive_service.drive_client import get_drive_service

        creds = load_creds()
        drive = get_drive_service(creds)
        res = drive.files().get(
            fileId=root,
            fields="name",
            supportsAllDrives=True,
        ).execute()
        name = str(res.get("name") or "").strip()
        return name or None
    except Exception:
        return None
