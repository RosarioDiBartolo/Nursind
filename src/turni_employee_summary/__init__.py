from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "TURNI": (".service", "TURNI"),
    "DEFAULT_YEAR_START": (".options", "DEFAULT_YEAR_START"),
    "DEFAULT_YEAR_END": (".options", "DEFAULT_YEAR_END"),
    "DEFAULT_OUTPUT_FORMAT": (".options", "DEFAULT_OUTPUT_FORMAT"),
    "TurniEmployeeSummaryOptions": (".options", "TurniEmployeeSummaryOptions"),
    "build_parser": (".options", "build_parser"),
    "default_enriched_dir": (".options", "default_enriched_dir"),
    "default_report_json_path": (".options", "default_report_json_path"),
    "default_summary_csv_path": (".options", "default_summary_csv_path"),
    "parse_options": (".options", "parse_options"),
    "process_one_enriched_file": (".service", "process_one_enriched_file"),
    "process_many_enriched_files": (".service", "process_many_enriched_files"),
    "build_turni_employee_summary_from_dir": (".runtime", "build_turni_employee_summary_from_dir"),
    "build_employee_turni_summary": (".runtime", "build_employee_turni_summary"),
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
