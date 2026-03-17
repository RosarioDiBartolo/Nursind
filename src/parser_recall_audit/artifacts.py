from __future__ import annotations

from dataclasses import dataclass

from cartellino_parser.pipeline_path_types import ArtifactScope


@dataclass(frozen=True, slots=True)
class ParserRecallAuditArtifactsSpec:
    step: str = "parser_recall_audit"
    scope: ArtifactScope = ArtifactScope.OUTPUT_ROOT
    report_json: str = "parser_recall_audit.report.json"
    suspicious_csv: str = "suspicious_pages.csv"


PARSER_RECALL_AUDIT_ARTIFACTS = ParserRecallAuditArtifactsSpec()


