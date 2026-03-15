from __future__ import annotations

import argparse

from src.drive_service import config
from src.drive_service.auth_service import load_creds
from src.drive_service.drive_client import get_drive_service
from src.drive_service.fs_utils import ensure_parent_dir
from src.drive_service.logging_utils import get_logger, setup_logging
from src.pipeline_paths import build_pipeline_paths, with_scan_overrides

from .artifacts import SCAN_ARTIFACTS
from .runtime import run_scan

logger = get_logger()


def main() -> int:
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
    args = parser.parse_args()

    setup_logging(args.verbose)
    config.validate_env()

    paths = with_scan_overrides(
        build_pipeline_paths(root_id=args.root, create_dirs=False),
        dir=args.out,
        included_index=args.included,
        filtered_index=args.filtered,
        report_json=args.report,
    )
    scan = paths.scan
    ensure_parent_dir(str(scan.included_index))
    ensure_parent_dir(str(scan.filtered_index))
    ensure_parent_dir(str(scan.report_json))

    creds = load_creds()
    drive = get_drive_service(creds)
    run_scan(
        creds=creds,
        drive=drive,
        root_id=args.root,
        workers=args.workers,
        included_path=str(scan.included_index),
        filtered_path=str(scan.filtered_index),
        report_path=str(scan.report_json),
        logger_obj=logger,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
