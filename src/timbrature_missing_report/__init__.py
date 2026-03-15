from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "DEFAULT_REPORT_JSON": (".options", "DEFAULT_REPORT_JSON"),
    "DEFAULT_SUMMARY_CSV": (".options", "DEFAULT_SUMMARY_CSV"),
    "DEFAULT_FINDINGS_CSV": (".options", "DEFAULT_FINDINGS_CSV"),
    "DEFAULT_COVERAGE_CSV": (".options", "DEFAULT_COVERAGE_CSV"),
    "SUMMARY_COLUMNS": (".service", "SUMMARY_COLUMNS"),
    "FINDING_COLUMNS": (".service", "FINDING_COLUMNS"),
    "COVERAGE_COLUMNS": (".service", "COVERAGE_COLUMNS"),
    "TimbratureMissingReportOptions": (".options", "TimbratureMissingReportOptions"),
    "build_parser": (".options", "build_parser"),
    "default_coverage_csv_path": (".options", "default_coverage_csv_path"),
    "default_findings_csv_path": (".options", "default_findings_csv_path"),
    "default_pipeline_dir": (".options", "default_pipeline_dir"),
    "default_report_json_path": (".options", "default_report_json_path"),
    "default_summary_csv_path": (".options", "default_summary_csv_path"),
    "parse_options": (".options", "parse_options"),
    "resolve_audit_inputs": (".service", "resolve_audit_inputs"),
    "audit_missing_timbrature_pipeline": (".service", "audit_missing_timbrature_pipeline"),
    "build_missing_timbrature_report": (".runtime", "build_missing_timbrature_report"),
    "run_from_options": (".runtime", "run_from_options"),
    "main": (".cli", "main"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(name)
    module_name, attr_name = _EXPORTS[name]
    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
