from __future__ import annotations

from dataclasses import dataclass

from cartellino_parser.pipeline_path_types import ArtifactScope


@dataclass(frozen=True, slots=True)
class TimbratureMissingReportArtifactsSpec:
    step: str = "timbrature_missing_report"
    scope: ArtifactScope = ArtifactScope.PIPELINE_ROOT
    report_json: str = "missing_timbrature.report.json"
    summary_csv: str = "missing_timbrature.summary.csv"
    findings_csv: str = "missing_timbrature.findings.csv"
    coverage_csv: str = "missing_timbrature.coverage.csv"


TIMBRATURE_MISSING_REPORT_ARTIFACTS = TimbratureMissingReportArtifactsSpec()

