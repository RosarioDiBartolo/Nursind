from __future__ import annotations

from dataclasses import dataclass

from cartellino_parser.pipeline_path_types import ArtifactRef, ArtifactScope, PipelineStage


@dataclass(frozen=True, slots=True)
class TurniEnrichmentArtifactsSpec:
    step: str = "turni_enrichment"
    scope: ArtifactScope = ArtifactScope.STAGE_DIR
    stage: PipelineStage = "enrichment"
    input_dir: ArtifactRef = ArtifactRef(step="pair_employee", artifact="dir")
    report_json: str = "turni_enrichment.stats.json"


TURNI_ENRICHMENT_ARTIFACTS = TurniEnrichmentArtifactsSpec()


