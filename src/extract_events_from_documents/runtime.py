from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.drive_service.fs_utils import ensure_parent_dir
from src.drive_service.text_extraction_csv import (
    find_text_extraction_csvs,
    read_text_extraction_rows,
)

from .options import (
    DEFAULT_MANIFEST_GLOB,
    DEFAULT_MAX_PATTERN_EXAMPLES,
    DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    DEFAULT_OUT_NAME,
    DEFAULT_PAGES_NAME,
    ExtractEventsFromTextOptions,
    default_input_dir,
    default_output_dir,
    default_report_json_path,
)
from .service import process_many_text_rows


def extract_events_from_documents_dir(
    *,
    input_dir: str | None = None,
    output_dir: str | None = None,
    out_name: str = DEFAULT_OUT_NAME,
    pages_name: str = DEFAULT_PAGES_NAME,
    report_json: str | None = None,
    manifest_glob: str = DEFAULT_MANIFEST_GLOB,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
) -> dict[str, Any]:
    input_dir = input_dir or default_input_dir()
    output_dir = output_dir or default_output_dir()
    report_json = report_json or default_report_json_path()
    input_base = Path(input_dir)
    employee_csv_files = find_text_extraction_csvs(input_base)
    if not employee_csv_files:
        raise FileNotFoundError(
            "NO_DOCUMENT_EXTRACTION_MANIFESTS: no employee manifest CSV files were found in "
            f"{input_base}"
        )

    report = process_many_text_rows(
        read_text_extraction_rows(employee_csv_files, hydrate_text=False),
        output_dir=output_dir,
        out_name=out_name,
        pages_name=pages_name,
        input_dir=input_dir,
        manifest_glob=manifest_glob,
        max_pattern_examples=max_pattern_examples,
        max_unmatched_examples_per_file=max_unmatched_examples_per_file,
    )
    ensure_parent_dir(report_json)
    with open(report_json, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return report


def run_from_options(options: ExtractEventsFromTextOptions) -> dict[str, Any]:
    return extract_events_from_documents_dir(
        input_dir=options.input_dir,
        output_dir=options.output_dir,
        out_name=options.out_name,
        pages_name=options.pages_name,
        report_json=options.report_json,
        manifest_glob=options.manifest_glob,
        max_pattern_examples=options.max_pattern_examples,
        max_unmatched_examples_per_file=options.max_unmatched_examples_per_file,
    )
