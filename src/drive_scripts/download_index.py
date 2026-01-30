import argparse
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

from . import config
from .auth_service import load_creds
from .drive_client import get_drive_service
from .downloads import download_pdf_stream
from .fs_utils import ensure_dir
from .index_service import extract_index_files
from .io_json import load_json, write_json
from .logging_utils import setup_logging
from .names import safe_name


def _is_pdf(item: Dict[str, Any]) -> bool:
    if item.get("container") == "zip":
        return False
    mime_type = (item.get("mimeType") or "").lower()
    if mime_type and mime_type != "application/pdf":
        return False
    file_name = (item.get("file_name") or "").strip()
    if file_name:
        _, ext = os.path.splitext(file_name)
        if ext and ext.lower() != ".pdf":
            return False
    return True


def _build_output_path(
    out_dir: str,
    employee: str | None,
    file_id: str | None,
    file_name: str | None,
    flat: bool,
) -> str:
    base_name = safe_name((file_name or file_id or "unknown").strip())
    stem, ext = os.path.splitext(base_name)
    if not ext:
        ext = ".pdf"
    tag = stem
    if file_id:
        tag = f"{stem}__{file_id[:8]}"
    out_name = f"{tag}{ext}"
    if flat:
        return os.path.join(out_dir, out_name)
    emp_dir = safe_name(employee or "unknown")
    return os.path.join(out_dir, emp_dir, out_name)


def download_index_files(
    index_path: str,
    out_dir: str,
    *,
    workers: int = 6,
    overwrite: bool = False,
    skip_reasoned: bool = False,
    include_non_pdf: bool = False,
    flat: bool = False,
    dry_run: bool = False,
    report: str | None = None,
    logger: logging.Logger | None = None,
) -> dict:
    logger = logger or logging.getLogger(__name__)
    data = load_json(index_path)
    items = extract_index_files(data)

    ensure_dir(out_dir)

    seen: set[str] = set()
    queue: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []
    stats = {
        "total_items": len(items),
        "queued": 0,
        "downloaded": 0,
        "failed": 0,
        "skipped_existing": 0,
        "skipped_missing_id": 0,
        "skipped_reasoned": 0,
        "skipped_non_pdf": 0,
        "dry_run": 0,
        "duplicates": 0,
    }

    for item in items:
        file_id = item.get("file_id")
        if not file_id:
            stats["skipped_missing_id"] += 1
            results.append(
                {
                    "file_id": None,
                    "file_name": item.get("file_name"),
                    "employee": item.get("employee"),
                    "status": "skipped",
                    "reason": "missing file_id",
                }
            )
            continue
        if file_id in seen:
            stats["duplicates"] += 1
            continue
        seen.add(file_id)
        if skip_reasoned and item.get("reason"):
            stats["skipped_reasoned"] += 1
            results.append(
                {
                    "file_id": file_id,
                    "file_name": item.get("file_name"),
                    "employee": item.get("employee"),
                    "status": "skipped",
                    "reason": item.get("reason") or "reasoned",
                }
            )
            continue
        if not include_non_pdf and not _is_pdf(item):
            stats["skipped_non_pdf"] += 1
            results.append(
                {
                    "file_id": file_id,
                    "file_name": item.get("file_name"),
                    "employee": item.get("employee"),
                    "status": "skipped",
                    "reason": "non_pdf",
                }
            )
            continue
        queue.append(item)

    stats["queued"] = len(queue)

    creds = None
    if not dry_run:
        config.validate_env()
        creds = load_creds()

    stop_event = threading.Event()

    def _download_one(doc: Dict[str, Any]) -> Dict[str, Any]:
        file_id = doc.get("file_id")
        file_name = doc.get("file_name") or file_id or "unknown.pdf"
        employee = doc.get("employee")
        out_path = _build_output_path(out_dir, employee, file_id, file_name, flat)
        ensure_dir(os.path.dirname(out_path) or ".")

        if not overwrite and os.path.exists(out_path):
            return {
                "file_id": file_id,
                "file_name": file_name,
                "employee": employee,
                "out_path": out_path,
                "status": "skipped",
                "reason": "exists",
            }

        if dry_run:
            return {
                "file_id": file_id,
                "file_name": file_name,
                "employee": employee,
                "out_path": out_path,
                "status": "dry_run",
            }

        if stop_event.is_set():
            return {
                "file_id": file_id,
                "file_name": file_name,
                "employee": employee,
                "out_path": out_path,
                "status": "failed",
                "reason": "cancelled",
            }

        stream = None
        try:
            drive = get_drive_service(creds)
            stream = download_pdf_stream(drive, file_id, logger=logger)
            with open(out_path, "wb") as f:
                f.write(stream.read())
            return {
                "file_id": file_id,
                "file_name": file_name,
                "employee": employee,
                "out_path": out_path,
                "status": "downloaded",
            }
        except Exception as exc:
            return {
                "file_id": file_id,
                "file_name": file_name,
                "employee": employee,
                "out_path": out_path,
                "status": "failed",
                "reason": f"{type(exc).__name__}: {exc}",
            }
        finally:
            try:
                if stream is not None:
                    stream.close()
            except Exception:
                pass

    t0 = time.time()
    if queue:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_download_one, doc) for doc in queue]
            try:
                for i, f in enumerate(as_completed(futures), 1):
                    result = f.result()
                    results.append(result)
                    status = result.get("status")
                    if status == "downloaded":
                        stats["downloaded"] += 1
                    elif status == "failed":
                        stats["failed"] += 1
                    elif status == "skipped" and result.get("reason") == "exists":
                        stats["skipped_existing"] += 1
                    elif status == "dry_run":
                        stats["dry_run"] += 1
                    if i % 25 == 0 or i == len(futures):
                        logger.info("Progress %s/%s files", i, len(futures))
            except KeyboardInterrupt:
                stop_event.set()
                for future in futures:
                    future.cancel()
                logger.warning("Interrupted by user")
    else:
        logger.info("No files to download")

    payload = {
        "source_index": index_path,
        "out_dir": out_dir,
        "flat": flat,
        "skip_reasoned": skip_reasoned,
        "include_non_pdf": include_non_pdf,
        "dry_run": dry_run,
        "stats": stats,
        "items": results,
        "duration_s": round(time.time() - t0, 3),
    }

    if report:
        report_path = report if os.path.isabs(report) else os.path.join(out_dir, report)
        write_json(report_path, payload)
        logger.info("Report saved to %s", report_path)

    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download raw PDFs from a Drive index into an output folder."
    )
    parser.add_argument("--index", required=True, help="Path to index JSON")
    parser.add_argument("--out", default="downloads/raw", help="Output directory")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument(
        "--skip-reasoned",
        action="store_true",
        help="Skip entries that have a reason (failed/filtered)",
    )
    parser.add_argument(
        "--include-non-pdf",
        action="store_true",
        help="Download non-PDF items too",
    )
    parser.add_argument(
        "--flat",
        action="store_true",
        help="Store all PDFs directly under --out (no employee subfolders)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not download, only report")
    parser.add_argument(
        "--report",
        default="download_index.report.json",
        help="Report filename (relative to --out unless absolute)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    download_index_files(
        args.index,
        args.out,
        workers=args.workers,
        overwrite=args.overwrite,
        skip_reasoned=args.skip_reasoned,
        include_non_pdf=args.include_non_pdf,
        flat=args.flat,
        dry_run=args.dry_run,
        report=args.report,
        logger=logger,
    )


if __name__ == "__main__":
    main()
