from __future__ import annotations

from dataclasses import dataclass

from src.pipeline_path_types import ArtifactRef, ArtifactScope, PipelineStage


@dataclass(frozen=True, slots=True)
class ExtractDocumentsArtifactsSpec:
    step: str = "extract_documents"
    scope: ArtifactScope = ArtifactScope.STAGE_DIR
    stage: PipelineStage = "documents"
    input_index: ArtifactRef = ArtifactRef(step="scan", artifact="included_index")
    included_index: str = "included_documents.index.json"
    excluded_index: str = "excluded_documents.index.json"
    report_json: str = "extract_documents_from_index.report.json"
    docs_dir: str = "docs"


EXTRACT_DOCUMENTS_ARTIFACTS = ExtractDocumentsArtifactsSpec()

