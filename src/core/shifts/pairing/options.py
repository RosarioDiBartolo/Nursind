from __future__ import annotations

from dataclasses import dataclass

DEFAULT_EVENTS_NAME = "events.cleaned.csv"
DEFAULT_MAX_GAP_HOURS = 16.0


def default_output_dir() -> str:
    return "output/default/shifts"


def default_input_dir() -> str:
    return "output/default/events"


def default_report_json_path() -> str:
    return "output/default/shifts/pair_employee_events.report.json"


@dataclass(slots=True)
class PairEmployeeEventsOptions:
    input_dir: str | None
    output_dir: str
    events_name: str = DEFAULT_EVENTS_NAME
    report_json: str = "pair_employee_events.report.json"
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS
    employee_filter: str | None = None
    keep_inferred_column: bool = False
    verbose: bool = False
