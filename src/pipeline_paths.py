from __future__ import annotations

import os
from functools import lru_cache
from dataclasses import dataclass
from pathlib import Path

from src.drive_service.fs_utils import ensure_dir


@dataclass(slots=True)
class PipelinePaths:
    root_output: Path
    scan_output: Path
    text_extraction_output: Path
    parsing_output: Path
    events_output: Path
    shifts_output: Path
    enrichment_output: Path
    aggregation_output: Path

    @property
    def evetns_output(self) -> Path:
        # Backward-compatible typo alias used in some notebooks.
        return self.events_output

    def ensure_dirs(self) -> None:
        for path in (
            self.root_output,
            self.scan_output,
            self.text_extraction_output,
            self.parsing_output,
            self.events_output,
            self.shifts_output,
            self.enrichment_output,
            self.aggregation_output,
        ):
            ensure_dir(str(path))


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
        text_extraction_output=root_output / "text_extracted",
        parsing_output=root_output / "days",
        events_output=root_output / "events",
        shifts_output=root_output / "shifts",
        enrichment_output=root_output / "enrichment",
        aggregation_output=root_output / "aggregation",
    )
    if create_dirs:
        outputs.ensure_dirs()
    return outputs


def build_output_paths(
    root_id: str | None = None,
    *,
    root_prefix: str | None = None,
    base_output: str | Path | None = None,
    create_dirs: bool = True,
) -> PipelinePaths:
    return build_pipelines_paths(
        root_id=root_id,
        root_prefix=root_prefix,
        base_output=base_output,
        create_dirs=create_dirs,
    )


# Backward-compatible alias; prefer PipelinePaths.
OutputPaths = PipelinePaths


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
