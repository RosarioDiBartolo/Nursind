from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "DEFAULT_LOW_COVERAGE_THRESHOLD": (".options", "DEFAULT_LOW_COVERAGE_THRESHOLD"),
    "DEFAULT_MAX_TINY_ROWS": (".options", "DEFAULT_MAX_TINY_ROWS"),
    "DEFAULT_MIN_LARGE_ROWS": (".options", "DEFAULT_MIN_LARGE_ROWS"),
    "SUSPICIOUS_PAGE_COLUMNS": (".service", "SUSPICIOUS_PAGE_COLUMNS"),
    "ParserRecallAuditOptions": (".options", "ParserRecallAuditOptions"),
    "audit_parser_recall_root": (".service", "audit_parser_recall_root"),
    "build_parser": (".options", "build_parser"),
    "build_parser_recall_report": (".runtime", "build_parser_recall_report"),
    "default_report_json_path": (".options", "default_report_json_path"),
    "default_root_dir": (".options", "default_root_dir"),
    "default_suspicious_csv_path": (".options", "default_suspicious_csv_path"),
    "main": (".cli", "main"),
    "parse_options": (".options", "parse_options"),
    "run_from_options": (".runtime", "run_from_options"),
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
