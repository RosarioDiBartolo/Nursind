from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cartellino_parser.pipeline_paths import PipelinePaths, _resolve_root_prefix_from_drive, build_pipeline_paths

ROOT_ID_PLACEHOLDER = "<drive_root_id>"
DEFAULT_CONFIG_NAME = "pipeline_config.json"


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
        value = self.config.get(name, {})
        if not isinstance(value, dict):
            raise KeyError(f"Missing '{name}' section in {self.config_path}")
        merged = dict(_canonical_step_defaults(self.paths).get(name, {}))
        merged.update(value)
        return merged


def _canonical_step_defaults(paths: PipelinePaths) -> dict[str, dict[str, Any]]:
    return {
        "scan": {
            "included_name": paths.scan.included_index.name,
            "filtered_name": paths.scan.filtered_index.name,
            "report_name": paths.scan.report_json.name,
        },
        "extract_documents": {
            "included_name": paths.extract_documents.included_index.name,
            "excluded_name": paths.extract_documents.excluded_index.name,
            "report_name": paths.extract_documents.report_json.name,
        },
        "extract_events": {
            "out_name": paths.extract_events.events_csv.name,
            "pages_name": paths.extract_events.pages_csv.name,
            "report_name": paths.extract_events.report_json.name,
        },
        "filter_midnight": {
            "events_name": paths.filter_midnight.events_csv.name,
            "out_name": paths.filter_midnight.cleaned_events_csv.name,
            "report_name": paths.filter_midnight.report_json.name,
            "removed_csv_name": paths.filter_midnight.removed_csv.name,
        },
        "pair_employee": {
            "events_name": paths.pair_employee.events_csv.name,
            "report_name": paths.pair_employee.report_json.name,
        },
        "turni_enrichment": {
            "report_name": paths.turni_enrichment.report_json.name,
        },
        "turni_employee_summary": {
            "out_name": paths.turni_employee_summary.summary_csv.name,
            "report_name": paths.turni_employee_summary.report_json.name,
        },
        "timbrature_missing_report": {
            "report_name": paths.timbrature_missing_report.report_json.name,
            "summary_name": paths.timbrature_missing_report.summary_csv.name,
            "findings_name": paths.timbrature_missing_report.findings_csv.name,
            "coverage_name": paths.timbrature_missing_report.coverage_csv.name,
        },
    }


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
    root_prefix = config.get("root_prefix")
    known_prefixes = config.get("known_prefixes", {})
    if not isinstance(known_prefixes, dict):
        known_prefixes = {}
    root_id_value = config.get("root_id")
    if not root_id_value and root_prefix:
        root_id_value = known_prefixes.get(root_prefix)
    root_id = str(root_id_value or "").strip()
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

    root_prefix = root_prefix or _resolve_root_prefix_from_drive(root_id) or root_id
    paths = build_pipeline_paths(
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

