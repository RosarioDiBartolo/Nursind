from .cli import main
from .options import (
    DEFAULT_EVENTS_NAME,
    DEFAULT_INDEX,
    DEFAULT_INPUT_DIR,
    DEFAULT_MAX_GAP_HOURS,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_OUTPUTS,
    DEFAULT_REPORT_JSON,
    PairEmployeeEventsOptions,
    build_parser,
    parse_options,
)
from .runtime import (
    build_pair_employee_events_from_dir,
    pair_employee_events,
    run_from_options,
)
from .service import (
    normalize_employee,
    process_many_employee_events,
    process_one_employee_events,
)

__all__ = [
    "DEFAULT_OUTPUTS",
    "DEFAULT_INPUT_DIR",
    "DEFAULT_INDEX",
    "DEFAULT_OUTPUT_DIR",
    "DEFAULT_EVENTS_NAME",
    "DEFAULT_REPORT_JSON",
    "DEFAULT_MAX_GAP_HOURS",
    "PairEmployeeEventsOptions",
    "build_parser",
    "parse_options",
    "normalize_employee",
    "process_one_employee_events",
    "process_many_employee_events",
    "build_pair_employee_events_from_dir",
    "pair_employee_events",
    "run_from_options",
    "main",
]
