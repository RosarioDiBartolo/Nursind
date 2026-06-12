from __future__ import annotations

from dataclasses import dataclass

DEFAULT_EVENTS_NAME = "events.csv"
DEFAULT_OUT_NAME = "events.cleaned.csv"
DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE = 10


def default_input_dir() -> str:
    return "output/default/events"


def default_report_json_path() -> str:
    return "output/default/events/events.clean_midnight.report.json"


def default_removed_csv_path() -> str:
    return "output/default/events/events.midnight_removed.csv"


@dataclass(slots=True)
class FilterMidnightEventsOptions:
    input_dir: str
    events_name: str = DEFAULT_EVENTS_NAME
    out_name: str = DEFAULT_OUT_NAME
    report_json: str = "events.clean_midnight.report.json"
    removed_csv: str = "events.midnight_removed.csv"
    max_removed_examples_per_file: int = DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE
    in_place: bool = False
    verbose: bool = False
