import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import config
from auth_service import load_creds
from drive_client import get_drive_service, list_children
from fs_utils import ensure_dir
from logging_utils import setup_logging, get_logger
from report_service import write_manifest
from scan_service import build_employee_report, normalize_term

logger = get_logger()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=config.DRIVE_ROOT_FOLDER_ID)
    parser.add_argument("--out", default="./")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    config.validate_env()
    ensure_dir(args.out)
    creds = load_creds()

    drive = get_drive_service(creds)
    exclude_terms = [normalize_term(term) for term in config.EXCLUDE_TERMS]

    employees = [
        f for f in list_children(drive, args.root)
        if f["mimeType"] == "application/vnd.google-apps.folder"
    ]

    t0 = time.time()
    reports = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(build_employee_report, creds, emp, exclude_terms)
            for emp in employees
        ]
        for i, f in enumerate(as_completed(futures), 1):
            report = f.result()
            reports.append(report)
            total_included = sum(len(r["included"]) for r in reports)
            logger.info(
                "Progress %s/%s employees, %s files",
                i,
                len(futures),
                total_included,
            )

    logger.info("Done in %.1fs", time.time() - t0)
    write_manifest(args.out, args.root, reports)


if __name__ == "__main__":
    main()
