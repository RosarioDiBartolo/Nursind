from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .drive.names import safe_name
from .errors import ConfigurationError
from .paths import PipelinePaths, build_pipeline_paths


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    config_path: Path
    repository_root: Path
    name: str
    output_dir_stem: Path
    drive_root_id: str
    steps: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def base_output_dir(self) -> Path:
        return self.output_dir_stem

    @property
    def paths(self) -> PipelinePaths:
        return build_pipeline_paths(
            repository_root=self.repository_root,
            output_dir_stem=self.output_dir_stem,
        )

    def step(self, name: str) -> dict[str, Any]:
        value = self.steps.get(name, {})
        if not isinstance(value, dict):
            raise ConfigurationError(f"steps.{name} must be a JSON object")
        return dict(value)


def find_repository_root(start: str | Path | None = None) -> Path:
    current = Path(start or __file__).resolve()
    if current.is_file():
        current = current.parent
    for candidate in (current, *current.parents):
        if (candidate / "pipeline.json").exists() or (candidate / ".git").exists():
            return candidate
    raise ConfigurationError(f"Unable to locate repository root from {current}")


def _optional_text(value: Any) -> str:
    return str(value or "").strip()


def _fetch_drive_folder_name(root_id: str) -> str | None:
    from .drive.auth_service import load_creds
    from .drive.drive_client import get_drive_service
    from .drive.metadata import get_root_name

    creds = load_creds()
    return get_root_name(get_drive_service(creds), root_id)


def _resolve_pipeline_name(
    pipeline: dict[str, Any],
    drive: dict[str, Any],
    *,
    root_id: str,
) -> str:
    configured_name = _optional_text(pipeline.get("name"))
    if configured_name:
        return configured_name

    drive_folder_name = (
        _optional_text(drive.get("folder_name"))
        or _optional_text(drive.get("name"))
        or _optional_text(drive.get("root_name"))
    )
    if drive_folder_name:
        return drive_folder_name

    resolved_name = _fetch_drive_folder_name(root_id)
    if resolved_name:
        return resolved_name

    raise ConfigurationError(
        "Unable to resolve the Drive folder name from drive.root_id; "
        "set pipeline.name to override the output folder name."
    )


def load_pipeline_config(path: str | Path | None = None) -> PipelineConfig:
    repository_root = find_repository_root(path or __file__)
    config_path = Path(path).expanduser() if path is not None else repository_root / "pipeline.json"
    if not config_path.is_absolute():
        config_path = repository_root / config_path
    config_path = config_path.resolve()

    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigurationError(f"Pipeline config not found: {config_path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"Invalid JSON in {config_path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise ConfigurationError("Pipeline config must be a JSON object")
    pipeline = payload.get("pipeline", {})
    drive = payload.get("drive")
    steps = payload.get("steps", {})
    if not isinstance(pipeline, dict):
        raise ConfigurationError("pipeline must be a JSON object")
    if not isinstance(drive, dict):
        raise ConfigurationError("Missing drive configuration object")
    if not isinstance(steps, dict):
        raise ConfigurationError("steps must be a JSON object")

    root_id = _optional_text(drive.get("root_id"))
    if not root_id:
        raise ConfigurationError("drive.root_id is required")
    name = _resolve_pipeline_name(pipeline, drive, root_id=root_id)

    output_dir_stem = Path(safe_name(name)).expanduser()
    if not output_dir_stem.is_absolute():
        output_dir_stem = repository_root / output_dir_stem

    return PipelineConfig(
        config_path=config_path,
        repository_root=repository_root,
        name=name,
        output_dir_stem=output_dir_stem.resolve(),
        drive_root_id=root_id,
        steps={str(key): dict(value) for key, value in steps.items() if isinstance(value, dict)},
    )
