from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from src.drive_service.drive_client import list_children
from src.drive_service.index import MapIndex
from src.drive_service.io_json import write_json
from src.drive_service.logging_utils import get_logger
from src.drive_service.schema import IndexFile

from .config import exclude_terms_normalized
from .scan_service import FOLDER_MIME, build_folder_report

logger = get_logger()


def get_root_name(drive, root_id: str | None, logger_obj: Any = None) -> str | None:
    """Return Drive root display name when available."""
    log = logger_obj or logger
    if not root_id:
        return None
    try:
        res = drive.files().get(
            fileId=root_id, fields="name", supportsAllDrives=True
        ).execute()
    except Exception as exc:
        log.warning("Unable to resolve root folder name: %s", exc)
        return None
    return res.get("name") or None


def merge_reports_to_maps(
    reports: list[dict], logger_obj: Any = None
) -> tuple[dict[str, IndexFile], dict[str, IndexFile]]:
    """Merge folder reports into included/filtered file maps (last duplicate wins)."""
    log = logger_obj or logger
    included_map: dict[str, IndexFile] = {}
    filtered_map: dict[str, IndexFile] = {}

    for report in reports:
        for item in report.get("included", []):
            file_id = item.get("file_id")
            if not file_id:
                continue
            if file_id in included_map:
                log.warning("Duplicate file_id in included map: %s (last one wins)", file_id)
            included_map[file_id] = IndexFile(**item)
        for item in report.get("filtered", []):
            file_id = item.get("file_id")
            if not file_id:
                continue
            if file_id in filtered_map:
                log.warning("Duplicate file_id in filtered map: %s (last one wins)", file_id)
            filtered_map[file_id] = IndexFile(**item)

    return included_map, filtered_map


def run_scan(
    *,
    creds,
    drive,
    root_id: str,
    workers: int,
    included_path: str,
    filtered_path: str,
    report_path: str,
    exclude_terms: list[str] | None = None,
    logger_obj: Any = None,
) -> dict[str, Any]:
    """Run scan orchestration with continue-on-error employee handling."""
    log = logger_obj or logger
    terms = exclude_terms if exclude_terms is not None else exclude_terms_normalized
    root_prefix = get_root_name(drive, root_id, log)
    employees = [f for f in list_children(drive, root_id) if f.get("mimeType") == FOLDER_MIME]

    t0 = time.time()
    reports: list[dict] = []
    scan_errors: list[dict[str, str | None]] = []
    employees_without_included_files: list[dict[str, Any]] = []
    total_included = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_employee = {
            pool.submit(
                build_folder_report,
                creds,
                emp,
                terms,
                root_prefix=root_prefix,
            ): emp
            for emp in employees
        }
        for i, future in enumerate(as_completed(future_to_employee), 1):
            emp = future_to_employee[future]
            try:
                report = future.result()
            except Exception as exc:
                payload = {
                    "employee": emp.get("name"),
                    "employee_id": emp.get("id"),
                    "error": f"{type(exc).__name__}: {exc}",
                }
                scan_errors.append(payload)
                log.exception("Scan failed for employee %s", emp.get("name"))
                continue

            reports.append(report)
            total_included += report["counts"]["included"]
            if int(report["counts"].get("included", 0)) <= 0:
                employees_without_included_files.append(
                    {
                        "employee": report.get("employee"),
                        "employee_id": report.get("employee_id"),
                        "included": int(report["counts"].get("included", 0)),
                        "filtered_files": int(report["counts"].get("filtered_files", 0)),
                        "filtered_folders": int(report["counts"].get("filtered_folders", 0)),
                    }
                )
            log.info(
                "Progress %s/%s employees, %s files",
                i,
                len(future_to_employee),
                total_included,
            )

    included_map, filtered_map = merge_reports_to_maps(reports, log)
    included_index = MapIndex.generate_index(root_id, len(employees), included_map)
    filtered_index = MapIndex.generate_index(root_id, len(employees), filtered_map)
    included_index.save_index(included_path)
    filtered_index.save_index(filtered_path)

    duration_seconds = time.time() - t0
    report = {
        "root_id": root_id,
        "employee_total": len(employees),
        "employee_succeeded": len(reports),
        "employee_failed": len(scan_errors),
        "included_total": included_index.total_files,
        "filtered_total": filtered_index.total_files,
        "employees_without_included_files_count": len(employees_without_included_files),
        "employees_without_included_files": sorted(
            employees_without_included_files,
            key=lambda item: (
                str(item.get("employee") or ""),
                str(item.get("employee_id") or ""),
            ),
        ),
        "duration_seconds": round(duration_seconds, 3),
        "scan_errors": scan_errors,
        "included_path": included_path,
        "filtered_path": filtered_path,
    }
    write_json(report_path, report)

    log.info(
        "Done in %.1fs (included=%s, filtered=%s)",
        duration_seconds,
        included_index.total_files,
        filtered_index.total_files,
    )
    log.info("Included index: %s", included_path)
    log.info("Filtered index: %s", filtered_path)
    log.info("Scan report: %s", report_path)
    if scan_errors:
        log.warning("Employee scan errors: %s", len(scan_errors))

    return report
