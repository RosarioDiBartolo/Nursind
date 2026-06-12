from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.config import PipelineConfig, load_pipeline_config
from core.paths import PipelinePaths


@dataclass(frozen=True, slots=True)
class NotebookContext:
    pipeline: PipelineConfig

    @property
    def repo_root(self) -> Path:
        return self.pipeline.repository_root

    @property
    def notebooks_dir(self) -> Path:
        return Path(__file__).resolve().parent

    @property
    def config_path(self) -> Path:
        return self.pipeline.config_path

    @property
    def config(self) -> dict[str, Any]:
        return {"steps": self.pipeline.steps}

    @property
    def root_id(self) -> str:
        return self.pipeline.drive_root_id

    @property
    def root_prefix(self) -> str:
        return self.pipeline.name

    @property
    def base_output_dir(self) -> Path:
        return self.pipeline.base_output_dir

    @property
    def paths(self) -> PipelinePaths:
        return self.pipeline.paths

    def step(self, name: str) -> dict[str, Any]:
        values = self.pipeline.step(name)
        defaults = {
            "scan": {
                "included_name": self.paths.scan_included_index.name,
                "filtered_name": self.paths.scan_filtered_index.name,
                "report_name": self.paths.scan_report.name,
            },
            "extract_documents": {
                "included_name": self.paths.documents_included_index.name,
                "excluded_name": self.paths.documents_excluded_index.name,
                "report_name": self.paths.documents_report.name,
            },
            "extract_events": {
                "out_name": self.paths.events_csv.name,
                "pages_name": self.paths.pages_csv.name,
                "report_name": self.paths.events_report.name,
            },
            "filter_midnight": {
                "events_name": self.paths.events_csv.name,
                "out_name": self.paths.cleaned_events_csv.name,
                "removed_csv_name": self.paths.removed_midnight_csv.name,
                "report_name": self.paths.filter_report.name,
            },
            "pair_events": {"report_name": self.paths.pairing_report.name},
            "enrich_shifts": {"report_name": self.paths.enrichment_report.name},
            "summarize_shifts": {
                "out_name": self.paths.summary_csv.name,
                "report_name": self.paths.summary_report.name,
            },
        }
        return {**defaults.get(name, {}), **values}


def load_notebook_context(config_path: str | Path | None = None) -> NotebookContext:
    return NotebookContext(load_pipeline_config(config_path or "pipeline.json"))
