from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.drive_service.fs_utils import ensure_parent_dir

from .options import (
    DEFAULT_INPUT_DIR,
    DEFAULT_MAX_NO_DAYS_FILES,
    DEFAULT_MAX_NO_DAYS_LINES,
    DEFAULT_OUT_DIR,
    DEFAULT_OUT_NAME,
    DEFAULT_REPORT_JSON,
    DEFAULT_TEXT_GLOB,
    ExtractDaysOptions,
)
from .service import process_many_text_files


def build_days_from_text_dir(
    *,
    input_dir: str = DEFAULT_INPUT_DIR,
    out_dir: str = DEFAULT_OUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    report_json: str = DEFAULT_REPORT_JSON,
    text_glob: str = DEFAULT_TEXT_GLOB,
    max_no_days_files: int = DEFAULT_MAX_NO_DAYS_FILES,
    max_no_days_lines: int = DEFAULT_MAX_NO_DAYS_LINES,
) -> dict[str, Any]:
    input_base = Path(input_dir)
    text_files = sorted(input_base.rglob(text_glob))
    report = process_many_text_files(
        text_files,
        out_dir=out_dir,
        out_name=out_name,
        input_base=input_base,
        text_glob=text_glob,
        input_dir=input_dir,
        max_no_days_files=max_no_days_files,
        max_no_days_lines=max_no_days_lines,
    )
    ensure_parent_dir(report_json)
    with open(report_json, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return report


def run_from_options(options: ExtractDaysOptions) -> dict[str, Any]:
    return build_days_from_text_dir(
        input_dir=options.input_dir,
        out_dir=options.out_dir,
        out_name=options.out_name,
        report_json=options.report_json,
        text_glob=options.text_glob,
        max_no_days_files=options.max_no_days_files,
        max_no_days_lines=options.max_no_days_lines,
    )
