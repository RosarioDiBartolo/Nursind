from __future__ import annotations

from dataclasses import dataclass

from cartellino_parser.pipeline_path_types import ArtifactRef, ArtifactScope, PipelineStage


@dataclass(frozen=True, slots=True)
class PairEmployeeArtifactsSpec:
    step: str = "pair_employee"
    scope: ArtifactScope = ArtifactScope.STAGE_DIR
    stage: PipelineStage = "shifts"
    input_dir: ArtifactRef = ArtifactRef(step="filter_midnight", artifact="dir")
    events_csv: ArtifactRef = ArtifactRef(step="filter_midnight", artifact="cleaned_events_csv")
    report_json: str = "pair_employee_events.report.json"


PAIR_EMPLOYEE_ARTIFACTS = PairEmployeeArtifactsSpec()


