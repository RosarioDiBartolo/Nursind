from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field

from .sources import DriveAuthConfig

_REPORT_KEYS = {"stage", "status", "inputs", "outputs", "stats", "row_totals", "items", "issues"}


class PublicModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class StageReport(PublicModel):
    stage: str
    status: str = "ok"
    inputs: dict[str, Any] = Field(default_factory=dict)
    outputs: dict[str, Any] = Field(default_factory=dict)
    stats: dict[str, Any] = Field(default_factory=dict)
    row_totals: dict[str, Any] = Field(default_factory=dict)
    items: list[dict[str, Any]] = Field(default_factory=list)
    issues: list[dict[str, Any]] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
        *,
        stage_override: str | None = None,
        outputs_override: Mapping[str, Any] | None = None,
        stats_override: Mapping[str, Any] | None = None,
    ) -> "StageReport":
        data = dict(payload)
        outputs = _coerce_mapping(data.get("outputs"))
        if outputs_override:
            outputs.update(dict(outputs_override))

        stats = _coerce_mapping(data.get("stats"))
        if stats_override:
            stats.update(dict(stats_override))

        return cls(
            stage=str(stage_override or data.get("stage") or "unknown"),
            status=str(data.get("status") or ("error" if data.get("issues") else "ok")),
            inputs=_coerce_mapping(data.get("inputs")),
            outputs=outputs,
            stats=stats,
            row_totals=_coerce_mapping(data.get("row_totals")),
            items=_coerce_list_of_mappings(data.get("items")),
            issues=_coerce_list_of_mappings(data.get("issues")),
            extra={key: value for key, value in data.items() if key not in _REPORT_KEYS},
        )


class PipelineRunResult(PublicModel):
    steps: list[StageReport] = Field(default_factory=list)


class ScanRequest(PublicModel):
    root_id: str
    out: Path | str | None = None
    included: Path | str | None = None
    filtered: Path | str | None = None
    report: Path | str | None = None
    workers: int = 6
    verbose: bool = False
    drive_auth: DriveAuthConfig | None = None


class ExtractDocumentsRequest(PublicModel):
    index: Path | str | None = None
    out: Path | str | None = None
    included: Path | str | None = None
    excluded: Path | str | None = None
    skip_included: bool = True
    reprocess_included: bool = False
    reprocess_excluded: bool = False
    workers: int = 8
    download_workers: int | None = None
    extract_workers: int | None = None
    max_in_flight: int = 128
    flush_every: int = 100
    limit: int = 0
    log_every: int = 50
    min_normal_score: float = 0.72
    min_score_delta: float = 0.08
    report: Path | str | None = None
    verbose: bool = False
    drive_auth: DriveAuthConfig | None = None


class ExtractEventsRequest(PublicModel):
    input_dir: Path | str | None = None
    output_dir: Path | str | None = None
    out_name: str = "events.csv"
    pages_name: str = "pages.csv"
    report_json: Path | str | None = None
    manifest_glob: str | None = None
    max_pattern_examples: int = 12
    max_unmatched_examples_per_file: int = 5
    verbose: bool = False


class FilterMidnightRequest(PublicModel):
    input_dir: Path | str | None = None
    events_name: str = "events.csv"
    out_name: str = "events.cleaned.csv"
    report_json: Path | str | None = None
    removed_csv: Path | str | None = None
    max_removed_examples_per_file: int = 10
    in_place: bool = False
    verbose: bool = False


class PairEmployeeEventsRequest(PublicModel):
    input_dir: Path | str | None = None
    output_dir: Path | str | None = None
    events_name: str = "events.cleaned.csv"
    report_json: Path | str | None = None
    max_gap_hours: float = 16.0
    employee_filter: str | None = None
    keep_inferred_column: bool = False
    verbose: bool = False


class TurniEnrichmentRequest(PublicModel):
    input_dir: Path | str | None = None
    output_dir: Path | str | None = None
    min_hours: float = 6.0
    include_holidays: bool = True
    report_json: Path | str | None = None
    verbose: bool = False


class TurniEmployeeSummaryRequest(PublicModel):
    enriched_dir: Path | str | None = None
    out: Path | str | None = None
    report_json: Path | str | None = None
    year_start: int | None = 2014
    year_end: int | None = 2025
    output_format: str = "csv"
    min_hours: float | None = None
    verbose: bool = False


class ParserRecallAuditRequest(PublicModel):
    root_dir: Path | str | None = None
    report_json: Path | str | None = None
    suspicious_csv: Path | str | None = None
    max_tiny_rows: int = 3
    min_large_rows: int = 10
    low_coverage_threshold: float = 0.25
    verbose: bool = False


class MissingTimbratureAuditRequest(PublicModel):
    pipeline_dir: Path | str | None = None
    report_json: Path | str | None = None
    summary_csv: Path | str | None = None
    findings_csv: Path | str | None = None
    coverage_csv: Path | str | None = None
    verbose: bool = False


def _coerce_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return {}


def _coerce_list_of_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, Mapping):
            rows.append({str(key): value for key, value in item.items()})
    return rows

