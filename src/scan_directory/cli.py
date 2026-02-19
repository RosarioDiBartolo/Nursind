import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.drive_service import config
from src.drive_service.auth_service import load_creds
from src.drive_service.drive_client import get_drive_service, list_children
from src.drive_service.fs_utils import ensure_dir, ensure_parent_dir
from src.drive_service.logging_utils import setup_logging, get_logger
from src.drive_service.index import MapIndex
from src.drive_service.schema import IndexFile
from .config import exclude_terms_normalized
from .scan_service import FOLDER_MIME, build_folder_report

logger = get_logger()


def _resolve_output_path(out_dir: str, name: str) -> str:
    if os.path.isabs(name):
        return name
    return os.path.join(out_dir, name)

 
def _get_root_name(drive, root_id: str) -> str | None:
    if not root_id:
        return None
    try:
        res = drive.files().get(
            fileId=root_id, fields="name", supportsAllDrives=True
        ).execute()
    except Exception as exc:
        logger.warning("Unable to resolve root folder name: %s", exc)
        return None
    return res.get("name") or None


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
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    config.validate_env()

    ensure_dir(args.out)
    included_path = _resolve_output_path(args.out, args.included)
    filtered_path = _resolve_output_path(args.out, args.filtered)
    ensure_parent_dir(included_path)
    ensure_parent_dir(filtered_path)

    creds = load_creds()
    drive = get_drive_service(creds)
 
    root_prefix = _get_root_name(drive, args.root)
    employees = [
        f for f in list_children(drive, args.root) if f["mimeType"] == FOLDER_MIME
    ]

    t0 = time.time()
    reports = []
    total_included = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(
                build_folder_report,
                creds,
                emp,
                exclude_terms_normalized,
                root_prefix=root_prefix,
            )
            for emp in employees
        ]
        for i, f in enumerate(as_completed(futures), 1):
            report = f.result()
            reports.append(report)
            total_included += report["counts"]["included"]
            logger.info(
                "Progress %s/%s employees, %s files",
                i,
                len(futures),
                total_included,
            )

    included_map: dict[str, IndexFile] = {}
    filtered_map: dict[str, IndexFile] = {}

    for report in reports:
        for item in report["included"]:
            file_id = item.get("file_id")
            if not file_id:
                continue
            if file_id in included_map:
                logger.warning("Duplicate file_id in included map: %s (last one wins)", file_id)
            included_map[file_id] = IndexFile(**item)
        for item in report["filtered"]:
            file_id = item.get("file_id")
            if not file_id:
                continue
            if file_id in filtered_map:
                logger.warning("Duplicate file_id in filtered map: %s (last one wins)", file_id)
            filtered_map[file_id] = IndexFile(**item)

    included_index = MapIndex.generate_index(args.root, len(employees), included_map)
    filtered_index = MapIndex.generate_index(args.root, len(employees), filtered_map)
    included_index.save_index(included_path)
    filtered_index.save_index(filtered_path)

    logger.info(
        "Done in %.1fs (included=%s, filtered=%s)",
        time.time() - t0,
        included_index.total_files,
        filtered_index.total_files,
    )
    logger.info("Included index: %s", included_path)
    logger.info("Filtered index: %s", filtered_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
