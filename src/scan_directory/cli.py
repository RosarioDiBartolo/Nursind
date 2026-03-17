from __future__ import annotations

import argparse
from typing import Sequence

from cartellino_parser import PipelineClient
from cartellino_parser._cli_requests import request_from_object
from cartellino_parser.drive_service.logging_utils import get_logger, setup_logging
from cartellino_parser.models import ScanRequest

from .artifacts import SCAN_ARTIFACTS

logger = get_logger()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Scan Drive folders and build MapIndex outputs.")
    parser.add_argument("--root")
    parser.add_argument(
        "--out",
        "--output-dir",
        dest="out",
        default=None,
        help="Output directory for scan artifacts (defaults to the canonical scan stage dir).",
    )
    parser.add_argument(
        "--included",
        default=SCAN_ARTIFACTS.included_index,
        help=f"Included index output path relative to --out (default: {SCAN_ARTIFACTS.included_index})",
    )
    parser.add_argument(
        "--filtered",
        default=SCAN_ARTIFACTS.filtered_index,
        help=f"Filtered index output path relative to --out (default: {SCAN_ARTIFACTS.filtered_index})",
    )
    parser.add_argument(
        "--report",
        default=SCAN_ARTIFACTS.report_json,
        help=f"Report output path relative to --out (default: {SCAN_ARTIFACTS.report_json})",
    )
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    setup_logging(args.verbose)
    report = PipelineClient().scan(
        request_from_object(
            ScanRequest,
            args,
            rename={"root": "root_id"},
        )
    )
    stats = report.stats
    logger.info(
        "Completed: employees=%s included=%s filtered=%s",
        stats.get("employee_total", 0),
        stats.get("included_total", 0),
        stats.get("filtered_total", 0),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
