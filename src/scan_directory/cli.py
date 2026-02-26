import argparse

from src.drive_service import config
from src.drive_service.auth_service import load_creds
from src.drive_service.drive_client import get_drive_service
from src.drive_service.fs_utils import ensure_dir, ensure_parent_dir
from src.drive_service.index_runtime import resolve_output_path
from src.drive_service.logging_utils import setup_logging, get_logger
from .runtime import run_scan

logger = get_logger()


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan Drive folders and build MapIndex outputs.")
    parser.add_argument("--root", default=config.DRIVE_ROOT_FOLDER_ID)
    parser.add_argument("--out", default=config.SCAN_OUT_DIR, help="Output directory")
    parser.add_argument(
        "--included",
        default=config.SCAN_INCLUDED_NAME,
        help="Included index filename (default: included.index.json)",
    )
    parser.add_argument(
        "--filtered",
        default=config.SCAN_FILTERED_NAME,
        help="Filtered index filename (default: filtered.index.json)",
    )
    parser.add_argument(
        "--report",
        default="scan_directory.report.json",
        help="Report filename (default: scan_directory.report.json)",
    )
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    config.validate_env()

    ensure_dir(args.out)
    included_path = resolve_output_path(args.out, args.included)
    filtered_path = resolve_output_path(args.out, args.filtered)
    report_path = resolve_output_path(args.out, args.report)
    ensure_parent_dir(included_path)
    ensure_parent_dir(filtered_path)
    ensure_parent_dir(report_path)

    creds = load_creds()
    drive = get_drive_service(creds)
    run_scan(
        creds=creds,
        drive=drive,
        root_id=args.root,
        workers=args.workers,
        included_path=included_path,
        filtered_path=filtered_path,
        report_path=report_path,
        logger_obj=logger,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
