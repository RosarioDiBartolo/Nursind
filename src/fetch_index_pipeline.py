import argparse
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from io import BytesIO

from drive_scripts.auth_service import load_creds
from drive_scripts.drive_client import get_drive_service
from drive_scripts.downloads import download_pdf_stream
from drive_scripts.fs_utils import ensure_dir
from drive_scripts.io_json import write_json
from drive_scripts.logging_utils import setup_logging, get_logger
from drive_scripts.map_index_service import MapIndex
from drive_scripts.names import safe_name
from drive_scripts.schema import IndexFile, Outputs
from parsing import parse_pdf

logger = get_logger()
#This is a commonly sign of a corrupted file that cannot be downloaded properly
DEFAULT_SKIP_REASON = "PdfminerException: No /Root object! - Is this really a PDF?"
_thread_local = threading.local()


def _get_drive(creds):
    drive = getattr(_thread_local, "drive", None)
    if drive is None:
        _thread_local.drive = get_drive_service(creds)
        drive = _thread_local.drive
    return drive


def _resolve_path(out_dir: str, path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(out_dir, path)


def _doc_attr(doc, name: str):
    if hasattr(doc, name):
        return getattr(doc, name)
    if isinstance(doc, dict):
        return doc.get(name)
    return None


def _build_output_paths(emp_name: str, file_name: str, file_id: str, out_dir: str):
    safe_emp = safe_name(emp_name)
    base_name = safe_name(file_name)
    if not base_name.lower().endswith(".pdf"):
        base_name = f"{base_name}.pdf"
    file_tag = f"{os.path.splitext(base_name)[0]}__{file_id[:8]}"
    emp_dir = os.path.join(out_dir, safe_emp)
    file_dir = os.path.join(emp_dir, file_tag)
    paths = {
        "file_dir": file_dir,
        "days_path": os.path.join(file_dir, "days.csv"),
        "pairs_path": os.path.join(file_dir, "pairs.csv"),
        "totals_path": os.path.join(file_dir, "totals.json"),
        "report_path": os.path.join(file_dir, "report.json"),
        "rel_outputs": {
            "days_csv": os.path.join(safe_emp, file_tag, "days.csv"),
            "pairs_csv": os.path.join(safe_emp, file_tag, "pairs.csv"),
            "totals_json": os.path.join(safe_emp, file_tag, "totals.json"),
            "report_json": os.path.join(safe_emp, file_tag, "report.json"),
        },
    }
    return paths


def _update_index_meta(index: MapIndex) -> None:
    index.generated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    employees = {
        item.employee for item in index.files.values() if getattr(item, "employee", None)
    }
    index.employee_count = len(employees)
    index.total_files = len(index.files)


def _download_pdf_bytes(creds, doc_info: dict, stop_event: threading.Event):
    if stop_event.is_set():
        return {"status": "failed", "reason": "cancelled", "doc": doc_info}
    file_id = doc_info.get("file_id")
    if not file_id:
        return {"status": "failed", "reason": "missing file_id", "doc": doc_info}
    try:
        drive = _get_drive(creds)
        stream = download_pdf_stream(drive, file_id, logger=logger)
        data = stream.read()
        stream.close()
        return {"status": "success", "data": data, "doc": doc_info}
    except Exception as exc:
        return {
            "status": "failed",
            "reason": f"{type(exc).__name__}: {exc}",
            "doc": doc_info,
        }


def _parse_and_write(pdf_bytes: bytes, doc_info: dict, out_dir: str):
    file_id = doc_info.get("file_id")
    file_name = doc_info.get("file_name") or file_id or "unknown.pdf"
    emp_name = doc_info.get("employee") or "unknown"
    emp_id = doc_info.get("employee_id")
    try:
        parsed = parse_pdf(BytesIO(pdf_bytes))
        paths = _build_output_paths(emp_name, file_name, file_id, out_dir)
        ensure_dir(paths["file_dir"])
        parsed.days_df.to_csv(paths["days_path"], index=False)
        parsed.pairs_df.to_csv(
            paths["pairs_path"],
            index=False,
            date_format="%Y-%m-%d %H:%M:%S",
        )
        write_json(paths["totals_path"], parsed.totals)
        write_json(
            paths["report_path"],
            {
                "meta": parsed.meta,
                "totals": parsed.totals,
                "validation": parsed.validation,
            },
        )
        return {
            "status": "success",
            "employee": emp_name,
            "employee_id": emp_id,
            "file_id": file_id,
            "file_name": file_name,
            "outputs": paths["rel_outputs"],
        }
    except Exception as exc:
        return {
            "status": "failed",
            "employee": emp_name,
            "employee_id": emp_id,
            "file_id": file_id,
            "file_name": file_name,
            "reason": f"{type(exc).__name__}: {exc}",
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch and parse index with a download/parse pipeline.")
    parser.add_argument("--out", default="downloads", help="Output directory (default: downloads)")
    parser.add_argument(
        "--index",
        default="index.scan.json",
        help="Input index filename (resolved under --out when relative)",
    )
    parser.add_argument(
        "--excluded",
        default="excluded.index.json",
        help="Excluded index output filename (default: excluded.index.json)",
    )
    parser.add_argument(
        "--included",
        default="included.index.json",
        help="Included index output filename (default: included.index.json)",
    )
    parser.add_argument(
        "--allow-legacy-index",
        action="store_true",
        help="Allow legacy index format with files as array (default: false)",
    )
    parser.add_argument(
        "--skip-existing-outputs",
        action="store_true",
        help="Skip processing when all output files already exist (default: false)",
    )
    parser.add_argument(
        "--skip-included",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip processing when file is already in included index (default: true)",
    )
    parser.add_argument(
        "--reprocess-included",
        action="store_true",
        help="Reprocess files already in included index (overrides --skip-included)",
    )
    parser.add_argument("--download-workers", type=int, default=8)
    parser.add_argument("--parse-workers", type=int, default=max(1, os.cpu_count() or 1))
    parser.add_argument(
        "--max-in-flight",
        type=int,
        default=128,
        help="Max number of parse tasks in flight (default: 128)",
    )
    parser.add_argument(
        "--flush-every",
        type=int,
        default=100,
        help="Write included/excluded indexes every N processed files (default: 100)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only the first N files (0 = no limit)",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=50,
        help="Log progress every N processed files (default: 50)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    global logger
    logger = get_logger()
    logger.info("Starting fetch_index_pipeline at out=%s", args.out)
    ensure_dir(args.out)

    logger.info("Loading credentials...")
    creds = load_creds()
    logger.info("Credentials loaded")

    index_path = _resolve_path(args.out, args.index)
    excluded_path = _resolve_path(args.out, args.excluded)
    included_path = _resolve_path(args.out, args.included)

    source = MapIndex.load_index(
        index_path,
        strict=False,
        allow_legacy=args.allow_legacy_index,
    )
    existing_excluded = MapIndex.load_index(excluded_path, strict=False, allow_legacy=True)
    existing_included = MapIndex.load_index(included_path, strict=False, allow_legacy=True)
    excluded_map: dict[str, IndexFile] = dict(existing_excluded.files)
    included_map: dict[str, IndexFile] = dict(existing_included.files)

    skip_included = args.skip_included and not args.reprocess_included
    total_docs = len(source.files)

    docs = []
    for doc in source.files.values():
        file_id = _doc_attr(doc, "file_id")
        if not file_id:
            logger.warning("Skipping entry with missing file_id")
            continue
        if file_id in excluded_map:
            continue
        if skip_included and file_id in included_map:
            continue
        reason = _doc_attr(doc, "reason")
        if reason == DEFAULT_SKIP_REASON:
            excluded_map[file_id] = doc
            continue
        docs.append(doc)

    if args.limit > 0:
        docs = docs[: args.limit]

    docs_to_download = []
    if args.skip_existing_outputs:
        logger.info("Skip-existing-outputs enabled")
    if skip_included:
        logger.info("Skip-included enabled")

    for doc in docs:
        file_id = _doc_attr(doc, "file_id")
        file_name = _doc_attr(doc, "file_name") or file_id or "unknown.pdf"
        emp_name = _doc_attr(doc, "employee") or "unknown"
        emp_id = _doc_attr(doc, "employee_id")
        if args.skip_existing_outputs:
            paths = _build_output_paths(emp_name, file_name, file_id, args.out)
            if all(
                os.path.exists(path)
                for path in (
                    paths["days_path"],
                    paths["pairs_path"],
                    paths["totals_path"],
                    paths["report_path"],
                )
            ):
                included_map[file_id] = IndexFile(
                    employee=emp_name,
                    employee_id=emp_id,
                    file_id=file_id,
                    file_name=file_name,
                    outputs=Outputs(
                        days_csv=paths["rel_outputs"]["days_csv"],
                        pairs_csv=paths["rel_outputs"]["pairs_csv"],
                        totals_json=paths["rel_outputs"]["totals_json"],
                        report_json=paths["rel_outputs"]["report_json"],
                    ),
                )
                continue
        docs_to_download.append(
            {
                "employee": emp_name,
                "employee_id": emp_id,
                "file_id": file_id,
                "file_name": file_name,
            }
        )

    filtered_docs = docs_to_download
    skipped_docs = total_docs - len(filtered_docs)
    logger.info("Found %d documents to process", len(filtered_docs))
    if skipped_docs:
        logger.info("Skipped %d documents (excluded/included/outputs/limit)", skipped_docs)
    if not filtered_docs:
        excluded_index = MapIndex.generate_index(source.root_id, source.employee_count, excluded_map)
        included_index = MapIndex.generate_index(source.root_id, source.employee_count, included_map)
        _update_index_meta(included_index)
        _update_index_meta(excluded_index)
        included_index.save_index(included_path)
        excluded_index.save_index(excluded_path)
        logger.info("No documents to process... exiting.")
        return 0

    t0 = time.time()
    flush_every = max(1, args.flush_every)
    log_every = max(1, args.log_every)
    processed = 0
    stop_event = threading.Event()

    excluded_index = MapIndex.generate_index(source.root_id, source.employee_count, excluded_map)
    included_index = MapIndex.generate_index(source.root_id, source.employee_count, included_map)

    parse_futures = {}

    with ThreadPoolExecutor(max_workers=args.download_workers) as download_pool, ProcessPoolExecutor(
        max_workers=args.parse_workers
    ) as parse_pool:
        download_futures = [
            download_pool.submit(_download_pdf_bytes, creds, doc_info, stop_event)
            for doc_info in filtered_docs
        ]
        try:
            for f in as_completed(download_futures):
                download_result = f.result()
                if download_result["status"] != "success":
                    doc_info = download_result["doc"]
                    file_id = doc_info.get("file_id")
                    if file_id:
                        excluded_map[file_id] = IndexFile(
                            employee=doc_info.get("employee") or "unknown",
                            employee_id=doc_info.get("employee_id"),
                            file_id=file_id,
                            file_name=doc_info.get("file_name"),
                            reason=download_result.get("reason"),
                            type="file",
                        )
                    processed += 1
                    continue

                while len(parse_futures) >= args.max_in_flight:
                    done = next(as_completed(parse_futures))
                    result = done.result()
                    _handle_parse_result(result, included_map, excluded_map)
                    parse_futures.pop(done, None)
                    processed += 1
                    _maybe_flush(
                        processed,
                        flush_every,
                        log_every,
                        t0,
                        included_index,
                        excluded_index,
                        included_map,
                        excluded_map,
                        included_path,
                        excluded_path,
                    )

                doc_info = download_result["doc"]
                parse_future = parse_pool.submit(
                    _parse_and_write,
                    download_result["data"],
                    doc_info,
                    args.out,
                )
                parse_futures[parse_future] = doc_info

            for done in as_completed(list(parse_futures)):
                result = done.result()
                _handle_parse_result(result, included_map, excluded_map)
                processed += 1
                _maybe_flush(
                    processed,
                    flush_every,
                    log_every,
                    t0,
                    included_index,
                    excluded_index,
                    included_map,
                    excluded_map,
                    included_path,
                    excluded_path,
                )
        except KeyboardInterrupt:
            stop_event.set()
            logger.warning("Interrupted by user, flushing report...")

    excluded_index.files = dict(excluded_map)
    included_index.files = dict(included_map)
    _update_index_meta(included_index)
    _update_index_meta(excluded_index)
    included_index.save_index(included_path)
    excluded_index.save_index(excluded_path)
    logger.info("Done in %.1fs", time.time() - t0)
    return 0


def _handle_parse_result(result, included_map, excluded_map):
    file_id = result.get("file_id")
    if result["status"] == "success":
        if file_id:
            included_map[file_id] = IndexFile(
                employee=result.get("employee"),
                employee_id=result.get("employee_id"),
                file_id=file_id,
                file_name=result.get("file_name"),
                outputs=Outputs(
                    days_csv=result["outputs"]["days_csv"],
                    pairs_csv=result["outputs"]["pairs_csv"],
                    totals_json=result["outputs"]["totals_json"],
                    report_json=result["outputs"]["report_json"],
                ),
            )
    else:
        if file_id:
            excluded_map[file_id] = IndexFile(
                employee=result.get("employee") or "unknown",
                employee_id=result.get("employee_id"),
                file_id=file_id,
                file_name=result.get("file_name"),
                reason=result.get("reason"),
                type="file",
            )


def _maybe_flush(
    processed: int,
    flush_every: int,
    log_every: int,
    start_ts: float,
    included_index: MapIndex,
    excluded_index: MapIndex,
    included_map: dict,
    excluded_map: dict,
    included_path: str,
    excluded_path: str,
):
    if processed % flush_every == 0:
        included_index.files = dict(included_map)
        excluded_index.files = dict(excluded_map)
        _update_index_meta(included_index)
        _update_index_meta(excluded_index)
        included_index.save_index(included_path)
        excluded_index.save_index(excluded_path)
    if processed % log_every == 0:
        elapsed = max(0.01, time.time() - start_ts)
        rate = processed / elapsed * 60.0
        logger.info("Progress %s files (%.1f files/min)", processed, rate)


if __name__ == "__main__":
    raise SystemExit(main())
