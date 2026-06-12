from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import ConfigurationError
from .paths import PipelinePaths, build_pipeline_paths


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    config_path: Path
    repository_root: Path
    name: str
    base_output_dir: Path
    drive_root_id: str
    steps: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def paths(self) -> PipelinePaths:
        return build_pipeline_paths(
            repository_root=self.repository_root,
            base_output_dir=self.base_output_dir,
            pipeline_name=self.name,
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
    pipeline = payload.get("pipeline")
    drive = payload.get("drive")
    steps = payload.get("steps", {})
    if not isinstance(pipeline, dict):
        raise ConfigurationError("Missing pipeline configuration object")
    if not isinstance(drive, dict):
        raise ConfigurationError("Missing drive configuration object")
    if not isinstance(steps, dict):
        raise ConfigurationError("steps must be a JSON object")

    name = str(pipeline.get("name") or "").strip()
    base_output = str(pipeline.get("base_output_dir") or "").strip()
    root_id = str(drive.get("root_id") or "").strip()
    if not name:
        raise ConfigurationError("pipeline.name is required")
    if not base_output:
        raise ConfigurationError("pipeline.base_output_dir is required")
    if not root_id:
        raise ConfigurationError("drive.root_id is required")

    base_output_dir = Path(base_output).expanduser()
    if not base_output_dir.is_absolute():
        base_output_dir = repository_root / base_output_dir

    return PipelineConfig(
        config_path=config_path,
        repository_root=repository_root,
        name=name,
        base_output_dir=base_output_dir.resolve(),
        drive_root_id=root_id,
        steps={str(key): dict(value) for key, value in steps.items() if isinstance(value, dict)},
    )
