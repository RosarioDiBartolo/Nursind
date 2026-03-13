from __future__ import annotations

import logging
from typing import Sequence

from src.drive_service.logging_utils import setup_logging

from .options import parse_options
from .runtime import run_from_options

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    options = parse_options(argv)
    setup_logging(options.verbose)
    report = run_from_options(options)
    stats = report["stats"]
    outputs = report.get("outputs") or {}
    logger.info(
        (
            "Completed: employees=%s with_issues=%s missing_text_layer=%s "
            "pages_missing_year_month=%s missing_months_after_pairing=%s issues=%s"
        ),
        stats["employees_total"],
        stats["employees_with_issues"],
        stats["missing_text_layer_files"],
        stats["pages_missing_year_month"],
        stats["months_missing_after_pairing_total"],
        stats["issues_total"],
    )
    if outputs:
        logger.info(
            "Outputs: report=%s employees=%s issues=%s non_ocr_dir=%s missing_months_dir=%s",
            outputs.get("report_json"),
            outputs.get("employee_summary_csv"),
            outputs.get("issues_csv"),
            outputs.get("non_ocr_files_dir"),
            outputs.get("missing_months_dir"),
        )
    return 0


__all__ = ["main"]
