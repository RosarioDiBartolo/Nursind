from __future__ import annotations

from dataclasses import dataclass

from cartellino_parser.pipeline_path_types import ArtifactRef, ArtifactScope, PipelineStage


@dataclass(frozen=True, slots=True)
class TurniEmployeeSummaryArtifactsSpec:
    step: str = "turni_employee_summary"
    scope: ArtifactScope = ArtifactScope.STAGE_DIR
    stage: PipelineStage = "aggregation"
    input_dir: ArtifactRef = ArtifactRef(step="turni_enrichment", artifact="dir")
    summary_csv: str = "turni_employee_summary.csv"
    report_json: str = "turni_employee_summary.report.json"


TURNI_EMPLOYEE_SUMMARY_ARTIFACTS = TurniEmployeeSummaryArtifactsSpec()


