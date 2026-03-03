from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.pipeline_paths import PipelinePaths, build_pipelines_paths

ROOT_ID_PLACEHOLDER = "<drive_root_id>"
DEFAULT_CONFIG_NAME = "shared_config.json"


@dataclass(slots=True)
class NotebookContext:
    repo_root: Path
    notebooks_dir: Path
    config_path: Path
    config: dict[str, Any]
    root_id: str
    root_prefix: str
    base_output_dir: Path
    paths: PipelinePaths

    def step(self, name: str) -> dict[str, Any]:
        value = self.config.get(name)
        if not isinstance(value, dict):
            raise KeyError(f"Missing '{name}' section in {self.config_path}")
        return dict(value)


def _default_config_path() -> Path:
    return Path(__file__).resolve().with_name(DEFAULT_CONFIG_NAME)


def load_notebook_context(config_path: str | Path | None = None) -> NotebookContext:
    resolved_config_path = (
        Path(config_path).expanduser().resolve()
        if config_path is not None
        else _default_config_path()
    )

    with resolved_config_path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    if not isinstance(config, dict):
        raise ValueError(f"Notebook config must be a JSON object: {resolved_config_path}")

    root_id = str(config.get("root_id") or "").strip()
    if not root_id or root_id == ROOT_ID_PLACEHOLDER:
        raise ValueError(
            f"Set 'root_id' in {resolved_config_path} before running the notebooks."
        )

    notebooks_dir = Path(__file__).resolve().parent
    repo_root = notebooks_dir.parent.parent

    base_output_value = str(config.get("base_output_dir") or "output").strip() or "output"
    base_output_dir = Path(base_output_value)
    if not base_output_dir.is_absolute():
        base_output_dir = repo_root / base_output_dir

    root_prefix = str(config.get("root_prefix") or "").strip() or root_id
    paths = build_pipelines_paths(
        root_id=root_id,
        root_prefix=root_prefix,
        base_output=base_output_dir,
        create_dirs=True,
    )

    return NotebookContext(
        repo_root=repo_root,
        notebooks_dir=notebooks_dir,
        config_path=resolved_config_path,
        config=dict(config),
        root_id=root_id,
        root_prefix=root_prefix,
        base_output_dir=base_output_dir,
        paths=paths,
    )


__all__ = [
    "DEFAULT_CONFIG_NAME",
    "NotebookContext",
    "ROOT_ID_PLACEHOLDER",
    "load_notebook_context",
]
