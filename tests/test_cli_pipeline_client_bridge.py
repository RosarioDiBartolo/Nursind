from __future__ import annotations

from importlib import import_module
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cartellino_parser.models import (  # noqa: E402
    ExtractDocumentsRequest,
    ExtractEventsRequest,
    FilterMidnightRequest,
    MissingTimbratureAuditRequest,
    PairEmployeeEventsRequest,
    ParserRecallAuditRequest,
    StageReport,
    TurniEmployeeSummaryRequest,
    TurniEnrichmentRequest,
)


def _patch_client(monkeypatch, module, method_name: str, report: StageReport) -> dict[str, object]:
    captured: dict[str, object] = {}

    class FakeClient:
        pass

    def handler(self, request):
        captured["request"] = request
        return report

    setattr(FakeClient, method_name, handler)
    monkeypatch.setattr(module, "PipelineClient", FakeClient)
    monkeypatch.setattr(module, "setup_logging", lambda _verbose: None)
    return captured


@pytest.mark.parametrize(
    ("module_name", "method_name", "request_type", "argv_builder", "expected", "report"),
    [
        (
            "cartellino_parser.extract_documents_from_index.__main__",
            "extract_documents",
            ExtractDocumentsRequest,
            lambda tmp: [
                "--out",
                str(tmp / "documents"),
                "--index",
                str(tmp / "scan" / "included.index.json"),
                "--included",
                str(tmp / "documents" / "included_documents.index.json"),
                "--excluded",
                str(tmp / "documents" / "excluded_documents.index.json"),
                "--report",
                str(tmp / "documents" / "extract_documents_from_index.report.json"),
                "--workers",
                "4",
                "--download-workers",
                "2",
            ],
            lambda tmp: {
                "out": str(tmp / "documents"),
                "index": str(tmp / "scan" / "included.index.json"),
                "workers": 4,
                "download_workers": 2,
            },
            StageReport(
                stage="extract_documents_from_index",
                stats={"files_total": 5, "files_processed": 4, "files_error": 1},
            ),
        ),
        (
            "cartellino_parser.extract_events_from_documents.cli",
            "extract_events",
            ExtractEventsRequest,
            lambda tmp: [
                "--input-dir",
                str(tmp / "documents"),
                "--output-dir",
                str(tmp / "events"),
                "--out-name",
                "events.out.csv",
                "--pages-name",
                "pages.out.csv",
            ],
            lambda tmp: {
                "input_dir": str(tmp / "documents"),
                "output_dir": str(tmp / "events"),
                "out_name": "events.out.csv",
                "pages_name": "pages.out.csv",
            },
            StageReport(
                stage="extract_events_from_documents",
                stats={
                    "files_total": 2,
                    "files_processed": 2,
                    "files_error": 0,
                    "files_with_events": 2,
                    "events_extracted": 9,
                    "rows_with_events": 4,
                },
            ),
        ),
        (
            "cartellino_parser.filter_midnight_events.cli",
            "filter_midnight",
            FilterMidnightRequest,
            lambda tmp: [
                "--input-dir",
                str(tmp / "events"),
                "--out-name",
                "events.cleaned.custom.csv",
                "--removed-csv",
                str(tmp / "events" / "removed.csv"),
            ],
            lambda tmp: {
                "input_dir": str(tmp / "events"),
                "out_name": "events.cleaned.custom.csv",
                "removed_csv": str(tmp / "events" / "removed.csv"),
            },
            StageReport(
                stage="filter_midnight_events",
                stats={
                    "files_total": 1,
                    "files_processed": 1,
                    "files_error": 0,
                    "files_with_removed": 1,
                    "rows_removed_midnight": 3,
                },
            ),
        ),
        (
            "cartellino_parser.pair_employee_events.cli",
            "pair_employee_events",
            PairEmployeeEventsRequest,
            lambda tmp: [
                "--input-dir",
                str(tmp / "events"),
                "--output-dir",
                str(tmp / "shifts"),
                "--events-name",
                "events.cleaned.custom.csv",
                "--max-gap-hours",
                "12",
                "--employee",
                "Mario Rossi",
                "--keep-inferred-column",
            ],
            lambda tmp: {
                "input_dir": str(tmp / "events"),
                "output_dir": str(tmp / "shifts"),
                "events_name": "events.cleaned.custom.csv",
                "max_gap_hours": 12.0,
                "employee_filter": "Mario Rossi",
                "keep_inferred_column": True,
            },
            StageReport(
                stage="pair_employee_events",
                stats={
                    "employees_total": 1,
                    "employees_processed": 1,
                    "employees_with_pairs": 1,
                    "event_files_total": 1,
                    "event_files_loaded": 1,
                    "event_files_missing": 0,
                    "event_files_error": 0,
                    "events_deduped": 4,
                    "pairs_out": 2,
                    "pairs_deduped": 2,
                    "inferred_pairs": 0,
                    "rows_unmatched_after_close": 0,
                },
            ),
        ),
        (
            "cartellino_parser.turni_enrichment.cli",
            "enrich_shifts",
            TurniEnrichmentRequest,
            lambda tmp: [
                "--input-dir",
                str(tmp / "shifts"),
                "--out-dir",
                str(tmp / "enrichment"),
                "--min-hours",
                "7.5",
                "--no-holidays",
            ],
            lambda tmp: {
                "input_dir": str(tmp / "shifts"),
                "output_dir": str(tmp / "enrichment"),
                "min_hours": 7.5,
                "include_holidays": False,
            },
            StageReport(
                stage="turni_enrichment",
                stats={
                    "employees_total": 1,
                    "files_total": 1,
                    "files_processed": 1,
                    "files_error": 0,
                    "rows_total": 2,
                    "rows_complete": 2,
                    "rows_enriched": 2,
                    "overnight_fix": 0,
                },
            ),
        ),
        (
            "cartellino_parser.turni_employee_summary.cli",
            "build_summary",
            TurniEmployeeSummaryRequest,
            lambda tmp: [
                "--enriched-dir",
                str(tmp / "enrichment"),
                "--out",
                str(tmp / "aggregation" / "turni_employee_summary.csv"),
                "--year-start",
                "2016",
                "--year-end",
                "2024",
                "--format",
                "json",
            ],
            lambda tmp: {
                "enriched_dir": str(tmp / "enrichment"),
                "out": str(tmp / "aggregation" / "turni_employee_summary.csv"),
                "year_start": 2016,
                "year_end": 2024,
                "output_format": "json",
            },
            StageReport(
                stage="turni_employee_summary",
                stats={
                    "employees_total": 1,
                    "files_total": 1,
                    "files_processed": 1,
                    "files_error": 0,
                    "rows_total": 2,
                    "rows_classified": 2,
                },
            ),
        ),
        (
            "cartellino_parser.parser_recall_audit.cli",
            "audit_parser_recall",
            ParserRecallAuditRequest,
            lambda tmp: [
                "--root-dir",
                str(tmp / "output"),
                "--report-json",
                str(tmp / "output" / "parser_recall_audit.report.json"),
                "--suspicious-csv",
                str(tmp / "output" / "suspicious_pages.csv"),
                "--max-tiny-rows",
                "5",
            ],
            lambda tmp: {
                "root_dir": str(tmp / "output"),
                "report_json": str(tmp / "output" / "parser_recall_audit.report.json"),
                "suspicious_csv": str(tmp / "output" / "suspicious_pages.csv"),
                "max_tiny_rows": 5,
            },
            StageReport(
                stage="parser_recall_audit",
                stats={
                    "pipelines_total": 1,
                    "pages_total": 3,
                    "suspicious_pages_total": 1,
                    "tiny_page_total": 1,
                    "zero_event_page_total": 0,
                    "low_coverage_page_total": 0,
                    "missing_year_month_total": 0,
                    "likely_legitimate_no_events_total": 0,
                },
                outputs={
                    "report_json": "parser_recall_audit.report.json",
                    "suspicious_csv": "suspicious_pages.csv",
                },
            ),
        ),
        (
            "cartellino_parser.timbrature_missing_report.cli",
            "audit_missing_timbrature",
            MissingTimbratureAuditRequest,
            lambda tmp: [
                "--pipeline-dir",
                str(tmp / "pipeline"),
                "--report-json",
                str(tmp / "pipeline" / "missing_timbrature.report.json"),
                "--summary-csv",
                str(tmp / "pipeline" / "missing_timbrature.summary.csv"),
                "--findings-csv",
                str(tmp / "pipeline" / "missing_timbrature.findings.csv"),
                "--coverage-csv",
                str(tmp / "pipeline" / "missing_timbrature.coverage.csv"),
            ],
            lambda tmp: {
                "pipeline_dir": str(tmp / "pipeline"),
                "report_json": str(tmp / "pipeline" / "missing_timbrature.report.json"),
                "summary_csv": str(tmp / "pipeline" / "missing_timbrature.summary.csv"),
                "findings_csv": str(tmp / "pipeline" / "missing_timbrature.findings.csv"),
                "coverage_csv": str(tmp / "pipeline" / "missing_timbrature.coverage.csv"),
            },
            StageReport(
                stage="timbrature_missing_report",
                stats={
                    "employees_total": 1,
                    "employees_with_any_gaps": 1,
                    "missing_text_layer_files": 0,
                    "pages_missing_year_month": 0,
                    "coverage_gaps_total": 1,
                    "findings_total": 1,
                },
                outputs={
                    "report_json": "missing_timbrature.report.json",
                    "summary_csv": "missing_timbrature.summary.csv",
                    "findings_csv": "missing_timbrature.findings.csv",
                    "coverage_csv": "missing_timbrature.coverage.csv",
                },
            ),
        ),
    ],
)
def test_cli_modules_delegate_to_public_pipeline_client(
    monkeypatch,
    tmp_path: Path,
    module_name: str,
    method_name: str,
    request_type: type,
    argv_builder,
    expected,
    report: StageReport,
):
    module = import_module(module_name)
    captured = _patch_client(monkeypatch, module, method_name, report)

    rc = module.main(argv_builder(tmp_path))

    assert rc == 0
    request = captured["request"]
    assert isinstance(request, request_type)
    for field_name, value in expected(tmp_path).items():
        assert getattr(request, field_name) == value
