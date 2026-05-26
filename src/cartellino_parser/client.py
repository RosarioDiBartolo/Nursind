from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, TypeVar

from .exceptions import CredentialsError
from .models import (
    ExtractDocumentsRequest,
    ExtractEventsRequest,
    FilterMidnightRequest,
    MissingTimbratureAuditRequest,
    PairEmployeeEventsRequest,
    ParserRecallAuditRequest,
    PipelineRunResult,
    ScanRequest,
    StageReport,
    TurniAfternoonLongExportRequest,
    TurniEmployeeSummaryRequest,
    TurniEnrichmentRequest,
)
from .sources import DriveAuthConfig

RequestT = TypeVar("RequestT")


class PipelineClient:
    """Public entrypoint for external modules importing the package."""

    def __init__(
        self,
        *,
        drive_auth: DriveAuthConfig | None = None,
        allow_env_credentials: bool = True,
        credentials_loader: Callable[[DriveAuthConfig | None], Any] | None = None,
        drive_service_factory: Callable[[Any], Any] | None = None,
    ) -> None:
        self._drive_auth = drive_auth
        self._allow_env_credentials = allow_env_credentials
        self._credentials_loader = credentials_loader
        self._drive_service_factory = drive_service_factory

    def build_paths(
        self,
        *,
        root_id: str | None = None,
        root_prefix: str | None = None,
        base_output: Path | str | None = None,
        create_dirs: bool = False,
    ):
        from cartellino_parser.pipeline_paths import build_pipeline_paths

        return build_pipeline_paths(
            root_id=root_id,
            root_prefix=root_prefix,
            base_output=base_output,
            create_dirs=create_dirs,
        )

    def scan(self, request: ScanRequest | None = None, **kwargs: Any) -> StageReport:
        from cartellino_parser.drive_service.fs_utils import ensure_parent_dir
        from cartellino_parser.pipeline_paths import build_pipeline_paths, with_scan_overrides
        from cartellino_parser.scan_directory.runtime import run_scan

        resolved = self._coerce_request(ScanRequest, request, kwargs)
        paths = with_scan_overrides(
            build_pipeline_paths(root_id=resolved.root_id, create_dirs=False),
            dir=self._path_value(resolved.out),
            included_index=self._path_value(resolved.included),
            filtered_index=self._path_value(resolved.filtered),
            report_json=self._path_value(resolved.report),
        )
        scan_paths = paths.scan
        ensure_parent_dir(str(scan_paths.included_index))
        ensure_parent_dir(str(scan_paths.filtered_index))
        ensure_parent_dir(str(scan_paths.report_json))

        creds = self._load_drive_credentials(resolved.drive_auth, require_credentials=True)
        drive = self._build_drive_service(creds)
        payload = run_scan(
            creds=creds,
            drive=drive,
            root_id=resolved.root_id,
            workers=resolved.workers,
            included_path=str(scan_paths.included_index),
            filtered_path=str(scan_paths.filtered_index),
            report_path=str(scan_paths.report_json),
        )
        return StageReport.from_payload(
            payload,
            stage_override="scan_directory",
            outputs_override={
                "included_index": str(scan_paths.included_index),
                "filtered_index": str(scan_paths.filtered_index),
                "report_json": str(scan_paths.report_json),
            },
            stats_override={
                "employee_total": payload.get("employee_total", 0),
                "employee_succeeded": payload.get("employee_succeeded", 0),
                "employee_failed": payload.get("employee_failed", 0),
                "included_total": payload.get("included_total", 0),
                "filtered_total": payload.get("filtered_total", 0),
            },
        )

    def extract_documents(
        self,
        request: ExtractDocumentsRequest | None = None,
        **kwargs: Any,
    ) -> StageReport:
        from cartellino_parser.extract_documents_from_index.options import ExtractDocumentsFromIndexOptions
        from cartellino_parser.extract_documents_from_index.runtime import run_extraction

        resolved = self._coerce_request(ExtractDocumentsRequest, request, kwargs)
        defaults = ExtractDocumentsFromIndexOptions()
        creds, auto_load_creds = self._resolve_drive_execution(resolved.drive_auth)
        payload = run_extraction(
            ExtractDocumentsFromIndexOptions(
                out=self._path_value(resolved.out, defaults.out),
                index=self._path_value(resolved.index, defaults.index),
                excluded=self._path_value(resolved.excluded, defaults.excluded),
                included=self._path_value(resolved.included, defaults.included),
                skip_included=resolved.skip_included,
                reprocess_included=resolved.reprocess_included,
                reprocess_excluded=resolved.reprocess_excluded,
                workers=resolved.workers,
                download_workers=resolved.download_workers,
                extract_workers=resolved.extract_workers or defaults.extract_workers,
                max_in_flight=resolved.max_in_flight,
                flush_every=resolved.flush_every,
                limit=resolved.limit,
                log_every=resolved.log_every,
                min_normal_score=resolved.min_normal_score,
                min_score_delta=resolved.min_score_delta,
                report=self._path_value(resolved.report, defaults.report),
                verbose=resolved.verbose,
            ),
            creds=creds,
            auto_load_creds=auto_load_creds,
            configure_logging=False,
            return_report=True,
        )
        return StageReport.from_payload(payload)

    def extract_events(
        self,
        request: ExtractEventsRequest | None = None,
        **kwargs: Any,
    ) -> StageReport:
        from cartellino_parser.extract_events_from_documents.options import (
            ExtractEventsFromTextOptions,
            DEFAULT_MANIFEST_GLOB,
        )
        from cartellino_parser.extract_events_from_documents.service import run_from_options

        resolved = self._coerce_request(ExtractEventsRequest, request, kwargs)
        defaults = ExtractEventsFromTextOptions()
        payload = run_from_options(
            ExtractEventsFromTextOptions(
                input_dir=self._path_value(resolved.input_dir, defaults.input_dir),
                output_dir=self._path_value(resolved.output_dir, defaults.output_dir),
                out_name=resolved.out_name,
                pages_name=resolved.pages_name,
                report_json=self._path_value(resolved.report_json, defaults.report_json),
                manifest_glob=resolved.manifest_glob or DEFAULT_MANIFEST_GLOB,
                max_pattern_examples=resolved.max_pattern_examples,
                max_unmatched_examples_per_file=resolved.max_unmatched_examples_per_file,
                verbose=resolved.verbose,
            )
        )
        return StageReport.from_payload(payload)

    def filter_midnight(
        self,
        request: FilterMidnightRequest | None = None,
        **kwargs: Any,
    ) -> StageReport:
        from cartellino_parser.filter_midnight_events.options import FilterMidnightEventsOptions
        from cartellino_parser.filter_midnight_events.service import run_from_options

        resolved = self._coerce_request(FilterMidnightRequest, request, kwargs)
        defaults = FilterMidnightEventsOptions()
        payload = run_from_options(
            FilterMidnightEventsOptions(
                input_dir=self._path_value(resolved.input_dir, defaults.input_dir),
                events_name=resolved.events_name,
                out_name=resolved.out_name,
                report_json=self._path_value(resolved.report_json, defaults.report_json),
                removed_csv=self._path_value(resolved.removed_csv, defaults.removed_csv),
                max_removed_examples_per_file=resolved.max_removed_examples_per_file,
                in_place=resolved.in_place,
                verbose=resolved.verbose,
            )
        )
        return StageReport.from_payload(payload)

    def pair_employee_events(
        self,
        request: PairEmployeeEventsRequest | None = None,
        **kwargs: Any,
    ) -> StageReport:
        from cartellino_parser.pair_employee_events.options import PairEmployeeEventsOptions
        from cartellino_parser.pair_employee_events.runtime import run_from_options

        resolved = self._coerce_request(PairEmployeeEventsRequest, request, kwargs)
        defaults = PairEmployeeEventsOptions()
        payload = run_from_options(
            PairEmployeeEventsOptions(
                input_dir=self._path_value(resolved.input_dir, defaults.input_dir),
                output_dir=self._path_value(resolved.output_dir, defaults.output_dir),
                events_name=resolved.events_name,
                report_json=self._path_value(resolved.report_json, defaults.report_json),
                max_gap_hours=resolved.max_gap_hours,
                employee_filter=resolved.employee_filter,
                keep_inferred_column=resolved.keep_inferred_column,
                verbose=resolved.verbose,
            )
        )
        return StageReport.from_payload(payload)

    def enrich_shifts(
        self,
        request: TurniEnrichmentRequest | None = None,
        **kwargs: Any,
    ) -> StageReport:
        from cartellino_parser.turni_enrichment.options import TurniEnrichmentOptions
        from cartellino_parser.turni_enrichment.service import run_from_options

        resolved = self._coerce_request(TurniEnrichmentRequest, request, kwargs)
        defaults = TurniEnrichmentOptions()
        payload = run_from_options(
            TurniEnrichmentOptions(
                input_dir=self._path_value(resolved.input_dir, defaults.input_dir),
                output_dir=self._path_value(resolved.output_dir, defaults.output_dir),
                min_hours=resolved.min_hours,
                include_holidays=resolved.include_holidays,
                report_json=self._path_value(resolved.report_json, defaults.report_json),
                verbose=resolved.verbose,
            )
        )
        return StageReport.from_payload(payload)

    def build_summary(
        self,
        request: TurniEmployeeSummaryRequest | None = None,
        **kwargs: Any,
    ) -> StageReport:
        from cartellino_parser.turni_employee_summary.options import TurniEmployeeSummaryOptions
        from cartellino_parser.turni_employee_summary.service import run_from_options

        resolved = self._coerce_request(TurniEmployeeSummaryRequest, request, kwargs)
        defaults = TurniEmployeeSummaryOptions()
        payload = run_from_options(
            TurniEmployeeSummaryOptions(
                enriched_dir=self._path_value(resolved.enriched_dir, defaults.enriched_dir),
                out=self._path_value(resolved.out, defaults.out),
                report_json=self._path_value(resolved.report_json, defaults.report_json),
                year_start=resolved.year_start,
                year_end=resolved.year_end,
                output_format=resolved.output_format,
                min_hours=resolved.min_hours,
                verbose=resolved.verbose,
            )
        )
        return StageReport.from_payload(payload)

    def export_afternoon_long_shifts(
        self,
        request: TurniAfternoonLongExportRequest | None = None,
        **kwargs: Any,
    ) -> StageReport:
        from cartellino_parser.turni_afternoon_long_export.options import (
            TurniAfternoonLongExportOptions,
        )
        from cartellino_parser.turni_afternoon_long_export.service import run_from_options

        resolved = self._coerce_request(TurniAfternoonLongExportRequest, request, kwargs)
        defaults = TurniAfternoonLongExportOptions()
        payload = run_from_options(
            TurniAfternoonLongExportOptions(
                enriched_dir=self._path_value(resolved.enriched_dir, defaults.enriched_dir),
                output_dir=self._path_value(resolved.output_dir, defaults.output_dir),
                report_json=self._path_value(resolved.report_json, defaults.report_json),
                verbose=resolved.verbose,
            )
        )
        return StageReport.from_payload(payload)

    def audit_parser_recall(
        self,
        request: ParserRecallAuditRequest | None = None,
        **kwargs: Any,
    ) -> StageReport:
        from cartellino_parser.parser_recall_audit.options import ParserRecallAuditOptions
        from cartellino_parser.parser_recall_audit.service import run_from_options

        resolved = self._coerce_request(ParserRecallAuditRequest, request, kwargs)
        defaults = ParserRecallAuditOptions()
        payload = run_from_options(
            ParserRecallAuditOptions(
                root_dir=self._path_value(resolved.root_dir, defaults.root_dir),
                report_json=self._path_value(resolved.report_json, defaults.report_json),
                suspicious_csv=self._path_value(resolved.suspicious_csv, defaults.suspicious_csv),
                max_tiny_rows=resolved.max_tiny_rows,
                min_large_rows=resolved.min_large_rows,
                low_coverage_threshold=resolved.low_coverage_threshold,
                verbose=resolved.verbose,
            )
        )
        return StageReport.from_payload(payload)

    def audit_missing_timbrature(
        self,
        request: MissingTimbratureAuditRequest | None = None,
        **kwargs: Any,
    ) -> StageReport:
        from cartellino_parser.timbrature_missing_report.options import TimbratureMissingReportOptions
        from cartellino_parser.timbrature_missing_report.service import run_from_options

        resolved = self._coerce_request(MissingTimbratureAuditRequest, request, kwargs)
        defaults = TimbratureMissingReportOptions()
        payload = run_from_options(
            TimbratureMissingReportOptions(
                pipeline_dir=self._path_value(resolved.pipeline_dir, defaults.pipeline_dir),
                report_json=self._path_value(resolved.report_json, defaults.report_json),
                summary_csv=self._path_value(resolved.summary_csv, defaults.summary_csv),
                findings_csv=self._path_value(resolved.findings_csv, defaults.findings_csv),
                coverage_csv=self._path_value(resolved.coverage_csv, defaults.coverage_csv),
                verbose=resolved.verbose,
            )
        )
        return StageReport.from_payload(payload)

    def run_pipeline(
        self,
        *,
        scan: ScanRequest | dict[str, Any] | None = None,
        extract_documents: ExtractDocumentsRequest | dict[str, Any] | None = None,
        extract_events: ExtractEventsRequest | dict[str, Any] | None = None,
        filter_midnight: FilterMidnightRequest | dict[str, Any] | None = None,
        pair_employee_events: PairEmployeeEventsRequest | dict[str, Any] | None = None,
        turni_enrichment: TurniEnrichmentRequest | dict[str, Any] | None = None,
        turni_employee_summary: TurniEmployeeSummaryRequest | dict[str, Any] | None = None,
        parser_recall_audit: ParserRecallAuditRequest | dict[str, Any] | None = None,
        timbrature_missing_report: MissingTimbratureAuditRequest | dict[str, Any] | None = None,
    ) -> PipelineRunResult:
        steps: list[StageReport] = []
        if scan is not None:
            steps.append(self.scan(**scan) if isinstance(scan, dict) else self.scan(scan))
        if extract_documents is not None:
            steps.append(
                self.extract_documents(**extract_documents)
                if isinstance(extract_documents, dict)
                else self.extract_documents(extract_documents)
            )
        if extract_events is not None:
            steps.append(
                self.extract_events(**extract_events)
                if isinstance(extract_events, dict)
                else self.extract_events(extract_events)
            )
        if filter_midnight is not None:
            steps.append(
                self.filter_midnight(**filter_midnight)
                if isinstance(filter_midnight, dict)
                else self.filter_midnight(filter_midnight)
            )
        if pair_employee_events is not None:
            steps.append(
                self.pair_employee_events(**pair_employee_events)
                if isinstance(pair_employee_events, dict)
                else self.pair_employee_events(pair_employee_events)
            )
        if turni_enrichment is not None:
            steps.append(
                self.enrich_shifts(**turni_enrichment)
                if isinstance(turni_enrichment, dict)
                else self.enrich_shifts(turni_enrichment)
            )
        if turni_employee_summary is not None:
            steps.append(
                self.build_summary(**turni_employee_summary)
                if isinstance(turni_employee_summary, dict)
                else self.build_summary(turni_employee_summary)
            )
        if parser_recall_audit is not None:
            steps.append(
                self.audit_parser_recall(**parser_recall_audit)
                if isinstance(parser_recall_audit, dict)
                else self.audit_parser_recall(parser_recall_audit)
            )
        if timbrature_missing_report is not None:
            steps.append(
                self.audit_missing_timbrature(**timbrature_missing_report)
                if isinstance(timbrature_missing_report, dict)
                else self.audit_missing_timbrature(timbrature_missing_report)
            )
        return PipelineRunResult(steps=steps)

    def _coerce_request(
        self,
        model_type: type[RequestT],
        request: RequestT | None,
        kwargs: dict[str, Any],
    ) -> RequestT:
        if request is not None:
            if kwargs:
                raise TypeError("Pass either a request object or keyword arguments, not both.")
            return request
        return model_type(**kwargs)

    def _resolve_drive_execution(
        self,
        request_auth: DriveAuthConfig | None,
    ) -> tuple[Any | None, bool]:
        effective_auth = request_auth or self._drive_auth
        if effective_auth is not None:
            return self._load_drive_credentials(effective_auth, require_credentials=True), False
        return None, self._allow_env_credentials

    def _load_drive_credentials(
        self,
        auth: DriveAuthConfig | None,
        *,
        require_credentials: bool,
    ) -> Any:
        effective_auth = auth or self._drive_auth
        if effective_auth is None and not self._allow_env_credentials:
            if require_credentials:
                raise CredentialsError(
                    "Drive credentials are required for this operation. "
                    "Pass drive_auth to PipelineClient or the request."
                )
            return None
        if self._credentials_loader is not None:
            return self._credentials_loader(effective_auth)

        from cartellino_parser.drive_service.auth_service import load_creds
        from cartellino_parser.drive_service.config import GoogleDriveSettings

        settings = None
        if effective_auth is not None:
            settings = GoogleDriveSettings(
                client_id=effective_auth.client_id or "",
                client_secret=effective_auth.client_secret or "",
                token_path=str(effective_auth.token_path),
                scopes=tuple(effective_auth.scopes),
            )
        return load_creds(settings=settings, load_env=effective_auth.load_env if effective_auth else True)

    def _build_drive_service(self, creds: Any) -> Any:
        if self._drive_service_factory is not None:
            return self._drive_service_factory(creds)
        from cartellino_parser.drive_service.drive_client import get_drive_service

        return get_drive_service(creds)

    @staticmethod
    def _path_value(value: Path | str | None, default: str | None = None) -> str | None:
        if value is None:
            return default
        return str(value)
