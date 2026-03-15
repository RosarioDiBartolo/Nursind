from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "DEFAULT_OUT_NAME": (".options", "DEFAULT_OUT_NAME"),
    "DEFAULT_PAGES_NAME": (".options", "DEFAULT_PAGES_NAME"),
    "DEFAULT_MANIFEST_GLOB": (".options", "DEFAULT_MANIFEST_GLOB"),
    "DEFAULT_MAX_PATTERN_EXAMPLES": (".options", "DEFAULT_MAX_PATTERN_EXAMPLES"),
    "DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE": (".options", "DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE"),
    "ExtractEventsFromTextOptions": (".options", "ExtractEventsFromTextOptions"),
    "build_parser": (".options", "build_parser"),
    "default_input_dir": (".options", "default_input_dir"),
    "default_output_dir": (".options", "default_output_dir"),
    "default_report_json_path": (".options", "default_report_json_path"),
    "parse_options": (".options", "parse_options"),
    "process_one_text_row": (".service", "process_one_text_row"),
    "process_many_text_rows": (".service", "process_many_text_rows"),
    "extract_events_from_documents_dir": (".runtime", "extract_events_from_documents_dir"),
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
