from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TurniAfternoonLongExportArtifactsSpec:
    step: str = "turni_afternoon_long_export"
    report_json: str = "turni_afternoon_long_export.report.json"
    filtered_file_suffix: str = ".pomeriggi.csv"
    pairs_file_suffix: str = ".csv"
    pdf_file_suffix: str = ".pdf"
    output_dir_name: str = "afternoon_long_export"


TURNI_AFTERNOON_LONG_EXPORT_ARTIFACTS = TurniAfternoonLongExportArtifactsSpec()
