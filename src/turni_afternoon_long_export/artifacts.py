from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TurniAfternoonLongExportArtifactsSpec:
    step: str = "turni_afternoon_long_export"
    report_json: str = "turni_afternoon_long_export.report.json"
    file_suffix: str = ".afternoon_long.csv"
    output_dir_name: str = "afternoon_long_export"


TURNI_AFTERNOON_LONG_EXPORT_ARTIFACTS = TurniAfternoonLongExportArtifactsSpec()
