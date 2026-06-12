from __future__ import annotations

from dataclasses import dataclass

from core.drive.text_extraction_csv import TEXT_EXTRACTION_CSV_GLOB

DEFAULT_OUT_NAME = "events.csv"
DEFAULT_PAGES_NAME = "pages.csv"
DEFAULT_MANIFEST_GLOB = TEXT_EXTRACTION_CSV_GLOB
DEFAULT_MAX_PATTERN_EXAMPLES = 12
DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE = 5


@dataclass(slots=True)
class ExtractEventsFromTextOptions:
    input_dir: str
    output_dir: str
    out_name: str = DEFAULT_OUT_NAME
    pages_name: str = DEFAULT_PAGES_NAME
    report_json: str = "extract_events.report.json"
    manifest_glob: str = DEFAULT_MANIFEST_GLOB
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE
    verbose: bool = False


def default_output_dir() -> str:
    return "output/default/events"


def default_input_dir() -> str:
    return "output/default/documents"


def default_report_json_path() -> str:
    return "output/default/events/extract_events.report.json"
