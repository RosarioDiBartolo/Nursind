from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "DEFAULT_EVENTS_NAME": (".options", "DEFAULT_EVENTS_NAME"),
    "DEFAULT_OUT_NAME": (".options", "DEFAULT_OUT_NAME"),
    "DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE": (".options", "DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE"),
    "FilterMidnightEventsOptions": (".options", "FilterMidnightEventsOptions"),
    "build_parser": (".options", "build_parser"),
    "default_input_dir": (".options", "default_input_dir"),
    "default_removed_csv_path": (".options", "default_removed_csv_path"),
    "default_report_json_path": (".options", "default_report_json_path"),
    "parse_options": (".options", "parse_options"),
    "process_one_events_file": (".service", "process_one_events_file"),
    "process_many_events_files": (".service", "process_many_events_files"),
    "build_filter_midnight_events_from_dir": (".runtime", "build_filter_midnight_events_from_dir"),
    "filter_midnight_events_dir": (".runtime", "filter_midnight_events_dir"),
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
