from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TypeAlias

from cartellino_parser.drive_service.fs_utils import ensure_dir
from cartellino_parser.pipeline_path_types import PipelineStage
from cartellino_parser.scan_directory.artifacts import SCAN_ARTIFACTS
from cartellino_parser.extract_documents_from_index.artifacts import EXTRACT_DOCUMENTS_ARTIFACTS
from cartellino_parser.extract_events_from_documents.artifacts import EXTRACT_EVENTS_ARTIFACTS
from cartellino_parser.filter_midnight_events.artifacts import FILTER_MIDNIGHT_ARTIFACTS
from cartellino_parser.pair_employee_events.artifacts import PAIR_EMPLOYEE_ARTIFACTS
from cartellino_parser.turni_enrichment.artifacts import TURNI_ENRICHMENT_ARTIFACTS
from cartellino_parser.turni_employee_summary.artifacts import TURNI_EMPLOYEE_SUMMARY_ARTIFACTS
from cartellino_parser.parser_recall_audit.artifacts import PARSER_RECALL_AUDIT_ARTIFACTS
from cartellino_parser.timbrature_missing_report.artifacts import TIMBRATURE_MISSING_REPORT_ARTIFACTS

PathLike: TypeAlias = str | Path
_StepOverrideMap: TypeAlias = dict[str, dict[str, PathLike]]

_LAYOUT_STAGE_ATTRS: dict[PipelineStage, str] = {
    "scan": "scan_dir",
    "documents": "documents_dir",
    "events": "events_dir",
    "shifts": "shifts_dir",
    "enrichment": "enrichment_dir",
    "aggregation": "aggregation_dir",
}


@dataclass(frozen=True, slots=True)
class PipelineLayout:
    output_root: Path
    pipeline_root: Path
    scan_dir: Path
    documents_dir: Path
    events_dir: Path
    shifts_dir: Path
    enrichment_dir: Path
    aggregation_dir: Path

    def ensure(self, stage: PipelineStage) -> None:
        path_attr = _LAYOUT_STAGE_ATTRS.get(stage)
        if path_attr is None:
            supported = ", ".join(_LAYOUT_STAGE_ATTRS)
            raise ValueError(f"Unknown pipeline stage {stage!r}. Expected one of: {supported}.")
        ensure_dir(str(getattr(self, path_attr)))

    def ensure_dirs(self) -> None:
        ensure_dir(str(self.pipeline_root))
        for path_attr in _LAYOUT_STAGE_ATTRS.values():
            ensure_dir(str(getattr(self, path_attr)))


@dataclass(frozen=True, slots=True)
class ScanPaths:
    dir: Path
    included_index: Path
    filtered_index: Path
    report_json: Path


@dataclass(frozen=True, slots=True)
class ExtractDocumentsPaths:
    dir: Path
    input_index: Path
    included_index: Path
    excluded_index: Path
    docs_dir: Path
    report_json: Path


@dataclass(frozen=True, slots=True)
class ExtractEventsPaths:
    dir: Path
    input_dir: Path
    events_csv: Path
    pages_csv: Path
    report_json: Path


@dataclass(frozen=True, slots=True)
class FilterMidnightPaths:
    dir: Path
    input_dir: Path
    events_csv: Path
    cleaned_events_csv: Path
    report_json: Path
    removed_csv: Path


@dataclass(frozen=True, slots=True)
class PairEmployeePaths:
    dir: Path
    input_dir: Path
    events_csv: Path
    report_json: Path


@dataclass(frozen=True, slots=True)
class TurniEnrichmentPaths:
    dir: Path
    input_dir: Path
    report_json: Path


@dataclass(frozen=True, slots=True)
class TurniEmployeeSummaryPaths:
    dir: Path
    input_dir: Path
    summary_csv: Path
    report_json: Path


@dataclass(frozen=True, slots=True)
class ParserRecallAuditPaths:
    root_dir: Path
    report_json: Path
    suspicious_csv: Path


@dataclass(frozen=True, slots=True)
class TimbratureMissingReportPaths:
    pipeline_dir: Path
    report_json: Path
    summary_csv: Path
    findings_csv: Path
    coverage_csv: Path


@dataclass(frozen=True, slots=True)
class _PipelineBuildState:
    root_id: str | None
    root_prefix: str
    base_output: Path
    create_dirs: bool
    overrides: _StepOverrideMap


@dataclass(frozen=True, slots=True)
class PipelinePaths:
    layout: PipelineLayout
    scan: ScanPaths
    extract_documents: ExtractDocumentsPaths
    extract_events: ExtractEventsPaths
    filter_midnight: FilterMidnightPaths
    pair_employee: PairEmployeePaths
    turni_enrichment: TurniEnrichmentPaths
    turni_employee_summary: TurniEmployeeSummaryPaths
    parser_recall_audit: ParserRecallAuditPaths
    timbrature_missing_report: TimbratureMissingReportPaths
    _state: _PipelineBuildState

    def ensure(self, stage: PipelineStage) -> None:
        self.layout.ensure(stage)

    def ensure_dirs(self) -> None:
        self.layout.ensure_dirs()

    @property
    def root_output(self) -> Path:
        return self.layout.pipeline_root

    @property
    def scan_output(self) -> Path:
        return self.layout.scan_dir

    @property
    def documents_output(self) -> Path:
        return self.layout.documents_dir

    @property
    def events_output(self) -> Path:
        return self.layout.events_dir

    @property
    def shifts_output(self) -> Path:
        return self.layout.shifts_dir

    @property
    def enrichment_output(self) -> Path:
        return self.layout.enrichment_dir

    @property
    def aggregation_output(self) -> Path:
        return self.layout.aggregation_dir


def _copy_overrides(overrides: _StepOverrideMap | None) -> _StepOverrideMap:
    if overrides is None:
        return {}
    return {step: dict(values) for step, values in overrides.items()}


def _normalize_override_map(**overrides: PathLike | None) -> dict[str, PathLike]:
    return {
        key: value
        for key, value in overrides.items()
        if value is not None and str(value).strip() != ""
    }


def _merge_step_overrides(
    overrides: _StepOverrideMap,
    step: str,
    values: dict[str, PathLike],
) -> _StepOverrideMap:
    merged = _copy_overrides(overrides)
    step_values = dict(merged.get(step, {}))
    step_values.update(values)
    merged[step] = step_values
    return merged


def _as_external_path(path: PathLike | None) -> Path | None:
    if path is None:
        return None
    value = Path(path)
    if str(value).strip() == "":
        return None
    return value


def _resolve_owned_path(base_dir: Path, default_name: str, override: PathLike | None) -> Path:
    candidate = _as_external_path(override)
    if candidate is None:
        return base_dir / default_name
    if candidate.is_absolute():
        return candidate
    return base_dir / candidate


def _resolve_input_path(default_path: Path, override: PathLike | None) -> Path:
    candidate = _as_external_path(override)
    if candidate is None:
        return default_path
    return candidate


def _resolve_pipeline_root(
    root_id: str | None = None,
    *,
    root_prefix: str | None = None,
    base_output: str | Path | None = None,
) -> tuple[Path, Path, str]:
    explicit_prefix = str(root_prefix).strip() if root_prefix is not None else ""
    explicit_root_id = str(root_id).strip() if root_id is not None else ""
    env_root_id = os.getenv("DRIVE_ROOT_FOLDER_ID", "").strip()
    candidate_root_id = explicit_root_id or env_root_id

    resolved_prefix = (
        explicit_prefix
        or os.getenv("OUTPUT_ROOT_PREFIX", "").strip()
        or os.getenv("PIPELINE_ROOT_PREFIX", "").strip()
        or _resolve_root_prefix_from_drive(candidate_root_id)
        or candidate_root_id
        or "default"
    )

    resolved_base_output = (
        Path(base_output)
        if base_output is not None
        else Path(os.getenv("OUTPUT_BASE_DIR", "output"))
    )
    pipeline_root = resolved_base_output / Path(resolved_prefix)
    return resolved_base_output, pipeline_root, resolved_prefix


def build_pipeline_layout(
    root_id: str | None = None,
    *,
    root_prefix: str | None = None,
    base_output: str | Path | None = None,
    create_dirs: bool = False,
) -> PipelineLayout:
    output_root, pipeline_root, _resolved_prefix = _resolve_pipeline_root(
        root_id=root_id,
        root_prefix=root_prefix,
        base_output=base_output,
    )
    layout = PipelineLayout(
        output_root=output_root,
        pipeline_root=pipeline_root,
        scan_dir=pipeline_root / "scan",
        documents_dir=pipeline_root / "documents",
        events_dir=pipeline_root / "events",
        shifts_dir=pipeline_root / "shifts",
        enrichment_dir=pipeline_root / "enrichment",
        aggregation_dir=pipeline_root / "aggregation",
    )
    if create_dirs:
        layout.ensure_dirs()
    return layout


def build_pipeline_paths(
    root_id: str | None = None,
    *,
    root_prefix: str | None = None,
    base_output: str | Path | None = None,
    create_dirs: bool = False,
    _overrides: _StepOverrideMap | None = None,
) -> PipelinePaths:
    output_root, pipeline_root, resolved_root_prefix = _resolve_pipeline_root(
        root_id=root_id,
        root_prefix=root_prefix,
        base_output=base_output,
    )
    layout = PipelineLayout(
        output_root=output_root,
        pipeline_root=pipeline_root,
        scan_dir=pipeline_root / "scan",
        documents_dir=pipeline_root / "documents",
        events_dir=pipeline_root / "events",
        shifts_dir=pipeline_root / "shifts",
        enrichment_dir=pipeline_root / "enrichment",
        aggregation_dir=pipeline_root / "aggregation",
    )
    if create_dirs:
        layout.ensure_dirs()

    overrides = _copy_overrides(_overrides)

    scan_overrides = overrides.get(SCAN_ARTIFACTS.step, {})
    scan_dir = _resolve_input_path(layout.scan_dir, scan_overrides.get("dir"))
    scan = ScanPaths(
        dir=scan_dir,
        included_index=_resolve_owned_path(
            scan_dir,
            SCAN_ARTIFACTS.included_index,
            scan_overrides.get("included_index"),
        ),
        filtered_index=_resolve_owned_path(
            scan_dir,
            SCAN_ARTIFACTS.filtered_index,
            scan_overrides.get("filtered_index"),
        ),
        report_json=_resolve_owned_path(
            scan_dir,
            SCAN_ARTIFACTS.report_json,
            scan_overrides.get("report_json"),
        ),
    )

    extract_documents_overrides = overrides.get(EXTRACT_DOCUMENTS_ARTIFACTS.step, {})
    extract_documents_dir = _resolve_input_path(
        layout.documents_dir,
        extract_documents_overrides.get("dir"),
    )
    extract_documents = ExtractDocumentsPaths(
        dir=extract_documents_dir,
        input_index=_resolve_input_path(
            scan.included_index,
            extract_documents_overrides.get("input_index"),
        ),
        included_index=_resolve_owned_path(
            extract_documents_dir,
            EXTRACT_DOCUMENTS_ARTIFACTS.included_index,
            extract_documents_overrides.get("included_index"),
        ),
        excluded_index=_resolve_owned_path(
            extract_documents_dir,
            EXTRACT_DOCUMENTS_ARTIFACTS.excluded_index,
            extract_documents_overrides.get("excluded_index"),
        ),
        docs_dir=_resolve_owned_path(
            extract_documents_dir,
            EXTRACT_DOCUMENTS_ARTIFACTS.docs_dir,
            extract_documents_overrides.get("docs_dir"),
        ),
        report_json=_resolve_owned_path(
            extract_documents_dir,
            EXTRACT_DOCUMENTS_ARTIFACTS.report_json,
            extract_documents_overrides.get("report_json"),
        ),
    )

    extract_events_overrides = overrides.get(EXTRACT_EVENTS_ARTIFACTS.step, {})
    extract_events_dir = _resolve_input_path(layout.events_dir, extract_events_overrides.get("dir"))
    extract_events = ExtractEventsPaths(
        dir=extract_events_dir,
        input_dir=_resolve_input_path(
            extract_documents.dir,
            extract_events_overrides.get("input_dir"),
        ),
        events_csv=_resolve_owned_path(
            extract_events_dir,
            EXTRACT_EVENTS_ARTIFACTS.events_csv,
            extract_events_overrides.get("events_csv"),
        ),
        pages_csv=_resolve_owned_path(
            extract_events_dir,
            EXTRACT_EVENTS_ARTIFACTS.pages_csv,
            extract_events_overrides.get("pages_csv"),
        ),
        report_json=_resolve_owned_path(
            extract_events_dir,
            EXTRACT_EVENTS_ARTIFACTS.report_json,
            extract_events_overrides.get("report_json"),
        ),
    )

    filter_midnight_overrides = overrides.get(FILTER_MIDNIGHT_ARTIFACTS.step, {})
    filter_midnight_dir = _resolve_input_path(
        extract_events.dir,
        filter_midnight_overrides.get("dir"),
    )
    filter_midnight = FilterMidnightPaths(
        dir=filter_midnight_dir,
        input_dir=_resolve_input_path(
            extract_events.dir,
            filter_midnight_overrides.get("input_dir"),
        ),
        events_csv=_resolve_input_path(
            extract_events.events_csv,
            filter_midnight_overrides.get("events_csv"),
        ),
        cleaned_events_csv=_resolve_owned_path(
            filter_midnight_dir,
            FILTER_MIDNIGHT_ARTIFACTS.cleaned_events_csv,
            filter_midnight_overrides.get("cleaned_events_csv"),
        ),
        report_json=_resolve_owned_path(
            filter_midnight_dir,
            FILTER_MIDNIGHT_ARTIFACTS.report_json,
            filter_midnight_overrides.get("report_json"),
        ),
        removed_csv=_resolve_owned_path(
            filter_midnight_dir,
            FILTER_MIDNIGHT_ARTIFACTS.removed_csv,
            filter_midnight_overrides.get("removed_csv"),
        ),
    )

    pair_employee_overrides = overrides.get(PAIR_EMPLOYEE_ARTIFACTS.step, {})
    pair_employee_dir = _resolve_input_path(layout.shifts_dir, pair_employee_overrides.get("dir"))
    pair_employee = PairEmployeePaths(
        dir=pair_employee_dir,
        input_dir=_resolve_input_path(
            filter_midnight.dir,
            pair_employee_overrides.get("input_dir"),
        ),
        events_csv=_resolve_input_path(
            filter_midnight.cleaned_events_csv,
            pair_employee_overrides.get("events_csv"),
        ),
        report_json=_resolve_owned_path(
            pair_employee_dir,
            PAIR_EMPLOYEE_ARTIFACTS.report_json,
            pair_employee_overrides.get("report_json"),
        ),
    )

    turni_enrichment_overrides = overrides.get(TURNI_ENRICHMENT_ARTIFACTS.step, {})
    turni_enrichment_dir = _resolve_input_path(
        layout.enrichment_dir,
        turni_enrichment_overrides.get("dir"),
    )
    turni_enrichment = TurniEnrichmentPaths(
        dir=turni_enrichment_dir,
        input_dir=_resolve_input_path(
            pair_employee.dir,
            turni_enrichment_overrides.get("input_dir"),
        ),
        report_json=_resolve_owned_path(
            turni_enrichment_dir,
            TURNI_ENRICHMENT_ARTIFACTS.report_json,
            turni_enrichment_overrides.get("report_json"),
        ),
    )

    turni_summary_overrides = overrides.get(TURNI_EMPLOYEE_SUMMARY_ARTIFACTS.step, {})
    turni_summary_dir = _resolve_input_path(layout.aggregation_dir, turni_summary_overrides.get("dir"))
    turni_employee_summary = TurniEmployeeSummaryPaths(
        dir=turni_summary_dir,
        input_dir=_resolve_input_path(
            turni_enrichment.dir,
            turni_summary_overrides.get("input_dir"),
        ),
        summary_csv=_resolve_owned_path(
            turni_summary_dir,
            TURNI_EMPLOYEE_SUMMARY_ARTIFACTS.summary_csv,
            turni_summary_overrides.get("summary_csv"),
        ),
        report_json=_resolve_owned_path(
            turni_summary_dir,
            TURNI_EMPLOYEE_SUMMARY_ARTIFACTS.report_json,
            turni_summary_overrides.get("report_json"),
        ),
    )

    parser_recall_overrides = overrides.get(PARSER_RECALL_AUDIT_ARTIFACTS.step, {})
    parser_recall_root = _resolve_input_path(
        layout.output_root,
        parser_recall_overrides.get("root_dir"),
    )
    parser_recall_audit = ParserRecallAuditPaths(
        root_dir=parser_recall_root,
        report_json=_resolve_owned_path(
            parser_recall_root,
            PARSER_RECALL_AUDIT_ARTIFACTS.report_json,
            parser_recall_overrides.get("report_json"),
        ),
        suspicious_csv=_resolve_owned_path(
            parser_recall_root,
            PARSER_RECALL_AUDIT_ARTIFACTS.suspicious_csv,
            parser_recall_overrides.get("suspicious_csv"),
        ),
    )

    timbrature_overrides = overrides.get(TIMBRATURE_MISSING_REPORT_ARTIFACTS.step, {})
    timbrature_pipeline_dir = _resolve_input_path(
        layout.pipeline_root,
        timbrature_overrides.get("pipeline_dir"),
    )
    timbrature_missing_report = TimbratureMissingReportPaths(
        pipeline_dir=timbrature_pipeline_dir,
        report_json=_resolve_owned_path(
            timbrature_pipeline_dir,
            TIMBRATURE_MISSING_REPORT_ARTIFACTS.report_json,
            timbrature_overrides.get("report_json"),
        ),
        summary_csv=_resolve_owned_path(
            timbrature_pipeline_dir,
            TIMBRATURE_MISSING_REPORT_ARTIFACTS.summary_csv,
            timbrature_overrides.get("summary_csv"),
        ),
        findings_csv=_resolve_owned_path(
            timbrature_pipeline_dir,
            TIMBRATURE_MISSING_REPORT_ARTIFACTS.findings_csv,
            timbrature_overrides.get("findings_csv"),
        ),
        coverage_csv=_resolve_owned_path(
            timbrature_pipeline_dir,
            TIMBRATURE_MISSING_REPORT_ARTIFACTS.coverage_csv,
            timbrature_overrides.get("coverage_csv"),
        ),
    )

    state = _PipelineBuildState(
        root_id=root_id,
        root_prefix=resolved_root_prefix,
        base_output=output_root,
        create_dirs=create_dirs,
        overrides=overrides,
    )
    return PipelinePaths(
        layout=layout,
        scan=scan,
        extract_documents=extract_documents,
        extract_events=extract_events,
        filter_midnight=filter_midnight,
        pair_employee=pair_employee,
        turni_enrichment=turni_enrichment,
        turni_employee_summary=turni_employee_summary,
        parser_recall_audit=parser_recall_audit,
        timbrature_missing_report=timbrature_missing_report,
        _state=state,
    )


def _rebuild_with_step_overrides(
    paths: PipelinePaths,
    step: str,
    **overrides: PathLike | None,
) -> PipelinePaths:
    normalized = _normalize_override_map(**overrides)
    merged = _merge_step_overrides(paths._state.overrides, step, normalized)
    return build_pipeline_paths(
        root_id=paths._state.root_id,
        root_prefix=paths._state.root_prefix,
        base_output=paths._state.base_output,
        create_dirs=paths._state.create_dirs,
        _overrides=merged,
    )


def with_scan_overrides(
    paths: PipelinePaths,
    *,
    dir: PathLike | None = None,
    included_index: PathLike | None = None,
    filtered_index: PathLike | None = None,
    report_json: PathLike | None = None,
) -> PipelinePaths:
    return _rebuild_with_step_overrides(
        paths,
        SCAN_ARTIFACTS.step,
        dir=dir,
        included_index=included_index,
        filtered_index=filtered_index,
        report_json=report_json,
    )


def with_extract_documents_overrides(
    paths: PipelinePaths,
    *,
    dir: PathLike | None = None,
    input_index: PathLike | None = None,
    included_index: PathLike | None = None,
    excluded_index: PathLike | None = None,
    docs_dir: PathLike | None = None,
    report_json: PathLike | None = None,
) -> PipelinePaths:
    return _rebuild_with_step_overrides(
        paths,
        EXTRACT_DOCUMENTS_ARTIFACTS.step,
        dir=dir,
        input_index=input_index,
        included_index=included_index,
        excluded_index=excluded_index,
        docs_dir=docs_dir,
        report_json=report_json,
    )


def with_extract_events_overrides(
    paths: PipelinePaths,
    *,
    dir: PathLike | None = None,
    input_dir: PathLike | None = None,
    events_csv: PathLike | None = None,
    pages_csv: PathLike | None = None,
    report_json: PathLike | None = None,
) -> PipelinePaths:
    return _rebuild_with_step_overrides(
        paths,
        EXTRACT_EVENTS_ARTIFACTS.step,
        dir=dir,
        input_dir=input_dir,
        events_csv=events_csv,
        pages_csv=pages_csv,
        report_json=report_json,
    )


def with_filter_midnight_overrides(
    paths: PipelinePaths,
    *,
    dir: PathLike | None = None,
    input_dir: PathLike | None = None,
    events_csv: PathLike | None = None,
    cleaned_events_csv: PathLike | None = None,
    report_json: PathLike | None = None,
    removed_csv: PathLike | None = None,
) -> PipelinePaths:
    return _rebuild_with_step_overrides(
        paths,
        FILTER_MIDNIGHT_ARTIFACTS.step,
        dir=dir,
        input_dir=input_dir,
        events_csv=events_csv,
        cleaned_events_csv=cleaned_events_csv,
        report_json=report_json,
        removed_csv=removed_csv,
    )


def with_pair_employee_overrides(
    paths: PipelinePaths,
    *,
    dir: PathLike | None = None,
    input_dir: PathLike | None = None,
    events_csv: PathLike | None = None,
    report_json: PathLike | None = None,
) -> PipelinePaths:
    return _rebuild_with_step_overrides(
        paths,
        PAIR_EMPLOYEE_ARTIFACTS.step,
        dir=dir,
        input_dir=input_dir,
        events_csv=events_csv,
        report_json=report_json,
    )


def with_turni_enrichment_overrides(
    paths: PipelinePaths,
    *,
    dir: PathLike | None = None,
    input_dir: PathLike | None = None,
    report_json: PathLike | None = None,
) -> PipelinePaths:
    return _rebuild_with_step_overrides(
        paths,
        TURNI_ENRICHMENT_ARTIFACTS.step,
        dir=dir,
        input_dir=input_dir,
        report_json=report_json,
    )


def with_turni_employee_summary_overrides(
    paths: PipelinePaths,
    *,
    dir: PathLike | None = None,
    input_dir: PathLike | None = None,
    summary_csv: PathLike | None = None,
    report_json: PathLike | None = None,
) -> PipelinePaths:
    return _rebuild_with_step_overrides(
        paths,
        TURNI_EMPLOYEE_SUMMARY_ARTIFACTS.step,
        dir=dir,
        input_dir=input_dir,
        summary_csv=summary_csv,
        report_json=report_json,
    )


def with_parser_recall_audit_overrides(
    paths: PipelinePaths,
    *,
    root_dir: PathLike | None = None,
    report_json: PathLike | None = None,
    suspicious_csv: PathLike | None = None,
) -> PipelinePaths:
    return _rebuild_with_step_overrides(
        paths,
        PARSER_RECALL_AUDIT_ARTIFACTS.step,
        root_dir=root_dir,
        report_json=report_json,
        suspicious_csv=suspicious_csv,
    )


def with_timbrature_missing_report_overrides(
    paths: PipelinePaths,
    *,
    pipeline_dir: PathLike | None = None,
    report_json: PathLike | None = None,
    summary_csv: PathLike | None = None,
    findings_csv: PathLike | None = None,
    coverage_csv: PathLike | None = None,
) -> PipelinePaths:
    return _rebuild_with_step_overrides(
        paths,
        TIMBRATURE_MISSING_REPORT_ARTIFACTS.step,
        pipeline_dir=pipeline_dir,
        report_json=report_json,
        summary_csv=summary_csv,
        findings_csv=findings_csv,
        coverage_csv=coverage_csv,
    )


@lru_cache(maxsize=128)
def _resolve_root_prefix_from_drive(root_id: str) -> str | None:
    root = str(root_id or "").strip()
    if not root:
        return None
    try:
        from cartellino_parser.drive_service.auth_service import load_creds
        from cartellino_parser.drive_service.drive_client import get_drive_service

        creds = load_creds()
        drive = get_drive_service(creds)
        res = drive.files().get(
            fileId=root,
            fields="name",
            supportsAllDrives=True,
        ).execute()
        name = str(res.get("name") or "").strip()
        return name or None
    except Exception:
        return None


__all__ = [
    "ExtractDocumentsPaths",
    "ExtractEventsPaths",
    "FilterMidnightPaths",
    "PairEmployeePaths",
    "ParserRecallAuditPaths",
    "PipelineLayout",
    "PipelinePaths",
    "PipelineStage",
    "ScanPaths",
    "TimbratureMissingReportPaths",
    "TurniEmployeeSummaryPaths",
    "TurniEnrichmentPaths",
    "_resolve_root_prefix_from_drive",
    "build_pipeline_layout",
    "build_pipeline_paths",
    "with_extract_documents_overrides",
    "with_extract_events_overrides",
    "with_filter_midnight_overrides",
    "with_pair_employee_overrides",
    "with_parser_recall_audit_overrides",
    "with_scan_overrides",
    "with_timbrature_missing_report_overrides",
    "with_turni_employee_summary_overrides",
    "with_turni_enrichment_overrides",
]

