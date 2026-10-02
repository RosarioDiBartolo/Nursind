from __future__ import annotations

from collections.abc import Callable

from core.config import PipelineConfig
from core.config_values import optional_int
from core.documents.ocr_options import OcrOptions
from core.documents.options import ExtractDocumentsFromIndexOptions
from core.documents.runtime import run_extraction
from core.events.extraction.options import ExtractEventsFromTextOptions
from core.events.extraction.service import run_from_options as run_extract_events
from core.events.filtering.options import FilterMidnightEventsOptions
from core.events.filtering.service import run_from_options as run_filter_midnight
from core.shifts.enrichment.options import TurniEnrichmentOptions
from core.shifts.enrichment.service import run_from_options as run_enrich_shifts
from core.shifts.pairing.options import PairEmployeeEventsOptions
from core.shifts.pairing.runtime import run_from_options as run_pair_events
from core.shifts.summary.options import TurniEmployeeSummaryOptions
from core.shifts.summary.service import run_from_options as run_summarize_shifts

PipelineStepAction = Callable[[PipelineConfig, bool], None]


def run_scan_step(config: PipelineConfig, _verbose: bool) -> None:
    from core.drive.auth_service import load_creds
    from core.drive.drive_client import get_drive_service
    from core.drive.scan.runtime import run_scan

    paths = config.paths
    settings = config.step("scan")
    creds = load_creds()
    run_scan(
        creds=creds,
        drive=get_drive_service(creds),
        root_id=config.drive_root_id,
        workers=int(settings.get("workers", 8)),
        included_path=str(paths.scan_included_index),
        filtered_path=str(paths.scan_filtered_index),
        report_path=str(paths.scan_report),
    )


def run_extract_documents_step(config: PipelineConfig, verbose: bool) -> None:
    paths = config.paths
    settings = config.step("extract_documents")
    options = ExtractDocumentsFromIndexOptions(
        out=str(paths.documents_dir),
        index=str(paths.scan_included_index),
        included=str(paths.documents_included_index),
        excluded=str(paths.documents_excluded_index),
        report=str(paths.documents_report),
        workers=int(settings.get("workers", 8)),
        download_workers=settings.get("download_workers"),
        extract_workers=int(settings.get("extract_workers", 1)),
        max_in_flight=int(settings.get("max_in_flight", 128)),
        flush_every=int(settings.get("flush_every", 100)),
        limit=int(settings.get("limit", 0)),
        log_every=int(settings.get("log_every", 50)),
        ocr=OcrOptions(**settings.get("ocr", {})),
        reprocess_included=bool(settings.get("reprocess_included", False)),
        reprocess_excluded=bool(settings.get("reprocess_excluded", False)),
        min_normal_score=float(settings.get("min_normal_score", 0.72)),
        min_score_delta=float(settings.get("min_score_delta", 0.08)),
        verbose=verbose,
    )
    run_extraction(options, configure_logging=False)


def run_extract_events_step(config: PipelineConfig, verbose: bool) -> None:
    paths = config.paths
    settings = config.step("extract_events")
    run_extract_events(
        ExtractEventsFromTextOptions(
            input_dir=str(paths.documents_dir),
            output_dir=str(paths.events_dir),
            report_json=str(paths.events_report),
            max_pattern_examples=int(settings.get("max_pattern_examples", 12)),
            max_unmatched_examples_per_file=int(
                settings.get("max_unmatched_examples_per_file", 5)
            ),
            verbose=verbose,
        )
    )


def run_filter_midnight_step(config: PipelineConfig, verbose: bool) -> None:
    paths = config.paths
    settings = config.step("filter_midnight")
    run_filter_midnight(
        FilterMidnightEventsOptions(
            input_dir=str(paths.events_dir),
            report_json=str(paths.filter_report),
            removed_csv=str(paths.removed_midnight_csv),
            max_removed_examples_per_file=int(
                settings.get("max_removed_examples_per_file", 10)
            ),
            verbose=verbose,
        )
    )


def run_pair_events_step(config: PipelineConfig, verbose: bool) -> None:
    paths = config.paths
    settings = config.step("pair_events")
    run_pair_events(
        PairEmployeeEventsOptions(
            input_dir=str(paths.events_dir),
            output_dir=str(paths.shifts_dir),
            report_json=str(paths.pairing_report),
            max_gap_hours=float(settings.get("max_gap_hours", 16.0)),
            keep_inferred_column=bool(settings.get("keep_inferred_column", False)),
            verbose=verbose,
        )
    )


def run_enrich_shifts_step(config: PipelineConfig, verbose: bool) -> None:
    paths = config.paths
    settings = config.step("enrich_shifts")
    run_enrich_shifts(
        TurniEnrichmentOptions(
            input_dir=str(paths.shifts_dir),
            output_dir=str(paths.enrichment_dir),
            min_hours=float(settings.get("min_hours", 6.0)),
            include_holidays=bool(settings.get("include_holidays", True)),
            report_json=str(paths.enrichment_report),
            verbose=verbose,
        )
    )


def run_summarize_shifts_step(config: PipelineConfig, verbose: bool) -> None:
    paths = config.paths
    settings = config.step("summarize_shifts")
    output_format = str(settings.get("format", "csv"))
    output_path = paths.summary_csv
    if output_format == "json":
        output_path = output_path.with_suffix(".json")
    run_summarize_shifts(
        TurniEmployeeSummaryOptions(
            enriched_dir=str(paths.enrichment_dir),
            out=str(output_path),
            report_json=str(paths.summary_report),
            year_start=optional_int(settings.get("year_start")),
            year_end=optional_int(settings.get("year_end")),
            output_format=output_format,
            min_hours=settings.get("min_hours"),
            verbose=verbose,
        )
    )


STEP_ACTIONS: dict[str, PipelineStepAction] = {
    "scan": run_scan_step,
    "extract_documents": run_extract_documents_step,
    "extract_events": run_extract_events_step,
    "filter_midnight": run_filter_midnight_step,
    "pair_events": run_pair_events_step,
    "enrich_shifts": run_enrich_shifts_step,
    "summarize_shifts": run_summarize_shifts_step,
}

