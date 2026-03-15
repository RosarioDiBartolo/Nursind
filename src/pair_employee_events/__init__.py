from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "DEFAULT_EVENTS_NAME": (".options", "DEFAULT_EVENTS_NAME"),
    "DEFAULT_MAX_GAP_HOURS": (".options", "DEFAULT_MAX_GAP_HOURS"),
    "PairEmployeeEventsOptions": (".options", "PairEmployeeEventsOptions"),
    "build_parser": (".options", "build_parser"),
    "default_input_dir": (".options", "default_input_dir"),
    "default_output_dir": (".options", "default_output_dir"),
    "default_report_json_path": (".options", "default_report_json_path"),
    "parse_options": (".options", "parse_options"),
    "normalize_employee": (".service", "normalize_employee"),
    "process_one_employee_events": (".service", "process_one_employee_events"),
    "process_many_employee_events": (".service", "process_many_employee_events"),
    "build_pair_employee_events_from_dir": (".runtime", "build_pair_employee_events_from_dir"),
    "pair_employee_events": (".runtime", "pair_employee_events"),
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
