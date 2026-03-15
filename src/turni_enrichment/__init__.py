from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "DEFAULT_MIN_HOURS": (".options", "DEFAULT_MIN_HOURS"),
    "ENRICHED_COLUMNS": (".service", "ENRICHED_COLUMNS"),
    "TurniEnrichmentOptions": (".options", "TurniEnrichmentOptions"),
    "build_parser": (".options", "build_parser"),
    "default_input_dir": (".options", "default_input_dir"),
    "default_output_dir": (".options", "default_output_dir"),
    "default_report_json_path": (".options", "default_report_json_path"),
    "parse_options": (".options", "parse_options"),
    "process_one_pairs_file": (".service", "process_one_pairs_file"),
    "process_many_pairs_files": (".service", "process_many_pairs_files"),
    "build_turni_enrichment_from_dir": (".runtime", "build_turni_enrichment_from_dir"),
    "enrich_pairs_by_employee": (".runtime", "enrich_pairs_by_employee"),
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
