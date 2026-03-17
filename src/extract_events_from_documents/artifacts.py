from __future__ import annotations

from dataclasses import dataclass

from cartellino_parser.pipeline_path_types import ArtifactRef, ArtifactScope, PipelineStage


@dataclass(frozen=True, slots=True)
class ExtractEventsArtifactsSpec:
    step: str = "extract_events"
    scope: ArtifactScope = ArtifactScope.STAGE_DIR
    stage: PipelineStage = "events"
    input_dir: ArtifactRef = ArtifactRef(step="extract_documents", artifact="dir")
    events_csv: str = "events.csv"
    pages_csv: str = "pages.csv"
    report_json: str = "extract_events.report.json"


EXTRACT_EVENTS_ARTIFACTS = ExtractEventsArtifactsSpec()


