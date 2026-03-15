from __future__ import annotations

from dataclasses import dataclass

from src.pipeline_path_types import ArtifactRef, ArtifactScope, PipelineStage


@dataclass(frozen=True, slots=True)
class FilterMidnightArtifactsSpec:
    step: str = "filter_midnight"
    scope: ArtifactScope = ArtifactScope.STAGE_DIR
    stage: PipelineStage = "events"
    dir_ref: ArtifactRef = ArtifactRef(step="extract_events", artifact="dir")
    input_dir: ArtifactRef = ArtifactRef(step="extract_events", artifact="dir")
    events_csv: ArtifactRef = ArtifactRef(step="extract_events", artifact="events_csv")
    cleaned_events_csv: str = "events.cleaned.csv"
    report_json: str = "events.clean_midnight.report.json"
    removed_csv: str = "events.midnight_removed.csv"


FILTER_MIDNIGHT_ARTIFACTS = FilterMidnightArtifactsSpec()

