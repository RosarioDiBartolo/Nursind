from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.drive_service.fs_utils import ensure_parent_dir

from .options import (
    DEFAULT_INPUT_DIR,
    DEFAULT_MAX_PATTERN_EXAMPLES,
    DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    DEFAULT_OUT_NAME,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REPORT_JSON,
    DEFAULT_TEXT_GLOB,
    ExtractEventsFromTextOptions,
)
from .service import process_many_text_files


def extract_events_from_text_dir(
    *,
    input_dir: str = DEFAULT_INPUT_DIR,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    report_json: str = DEFAULT_REPORT_JSON,
    text_glob: str = DEFAULT_TEXT_GLOB,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
) -> dict[str, Any]:
    input_base = Path(input_dir)
    text_files = sorted(input_base.rglob(text_glob))
    report = process_many_text_files(
        text_files,
        output_dir=output_dir,
        out_name=out_name,
        input_base=input_base,
        text_glob=text_glob,
        input_dir=input_dir,
        max_pattern_examples=max_pattern_examples,
        max_unmatched_examples_per_file=max_unmatched_examples_per_file,
    )
    ensure_parent_dir(report_json)
    with open(report_json, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return report


def run_from_options(options: ExtractEventsFromTextOptions) -> dict[str, Any]:
    return extract_events_from_text_dir(
        input_dir=options.input_dir,
        output_dir=options.output_dir,
        out_name=options.out_name,
        report_json=options.report_json,
        text_glob=options.text_glob,
        max_pattern_examples=options.max_pattern_examples,
        max_unmatched_examples_per_file=options.max_unmatched_examples_per_file,
    )
