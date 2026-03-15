from __future__ import annotations

from dataclasses import dataclass

from src.pipeline_path_types import ArtifactScope, PipelineStage


@dataclass(frozen=True, slots=True)
class ScanArtifactsSpec:
    step: str = "scan"
    scope: ArtifactScope = ArtifactScope.STAGE_DIR
    stage: PipelineStage = "scan"
    included_index: str = "included.index.json"
    filtered_index: str = "filtered.index.json"
    report_json: str = "scan_directory.report.json"


SCAN_ARTIFACTS = ScanArtifactsSpec()

