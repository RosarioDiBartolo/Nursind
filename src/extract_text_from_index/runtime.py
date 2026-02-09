from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from src.drive_service.auth_service import load_creds
from src.drive_service.fs_utils import ensure_dir
from src.drive_service.index_runtime import (
    maybe_flush_indexes,
    resolve_output_path,
    update_index_meta,
)
from src.drive_service.io_json import write_json
from src.drive_service.logging_utils import get_logger, setup_logging
from src.drive_service.map_index_service import MapIndex
from src.drive_service.schema import IndexFile

from .options import ExtractTextFromIndexOptions
from .planning import build_initial_stats, collect_docs
from .workers import download_pdf_bytes, extract_and_write


def _apply_result(
    result: dict,
    stats: dict,
    items: list,
    included_map: dict[str, IndexFile],
    excluded_map: dict[str, IndexFile],
) -> None:
    items.append(result)
    if result["status"] == "success":
        stats["succeeded"] += 1
        if result.get("selected_mode") == "vertical":
            stats["used_vertical"] += 1
        file_id = result.get("file_id")
        if file_id:
            included_map[file_id] = IndexFile(
                employee=result.get("employee") or "unknown",
                employee_id=result.get("employee_id"),
                file_id=file_id,
                file_name=result.get("file_name"),
                drive_path=result.get("drive_path"),
            )
            excluded_map.pop(file_id, None)
        return

    stats["failed"] += 1
    stage = result.get("stage")
    if stage == "download":
        stats["download_failed"] += 1
    elif stage == "extract":
        stats["extract_failed"] += 1

    doc_info = result.get("doc") or {}
    file_id = doc_info.get("file_id")
    if file_id:
        excluded_map[file_id] = IndexFile(
            employee=doc_info.get("employee") or "unknown",
            employee_id=doc_info.get("employee_id"),
            file_id=file_id,
            file_name=doc_info.get("file_name"),
            drive_path=doc_info.get("drive_path"),
            reason=result.get("reason"),
            type="file",
        )


def _flush_progress(
    *,
    processed: int,
    flush_every: int,
    log_every: int,
    start_ts: float,
    included_index,
    excluded_index,
    included_map: dict[str, IndexFile],
    excluded_map: dict[str, IndexFile],
    included_path: str,
    excluded_path: str,
    logger,
) -> None:
    maybe_flush_indexes(
        processed=processed,
        flush_every=flush_every,
        log_every=log_every,
        start_ts=start_ts,
        included_index=included_index,
        excluded_index=excluded_index,
        included_map=included_map,
        excluded_map=excluded_map,
        included_path=included_path,
        excluded_path=excluded_path,
        logger=logger,
    )


def run_extraction(options: ExtractTextFromIndexOptions) -> int:
    setup_logging(options.verbose)
    logger = get_logger()
    logger.info("Starting text extraction pipeline at out=%s", options.out)

    ensure_dir(options.out)
    index_path = options.index if os.path.isabs(options.index) else os.path.abspath(options.index)
    included_path = resolve_output_path(options.out, options.included)
    excluded_path = resolve_output_path(options.out, options.excluded)
    report_path = resolve_output_path(options.out, options.report)

    source = MapIndex.load_index(index_path, strict=True)
    existing_included = MapIndex.load_index(included_path, strict=False)
    existing_excluded = MapIndex.load_index(excluded_path, strict=False)
    included_map: dict[str, IndexFile] = dict(existing_included.files)
    excluded_map: dict[str, IndexFile] = dict(existing_excluded.files)

    skip_included = options.skip_included and not options.reprocess_included
    skip_excluded = not options.reprocess_excluded
    download_workers = max(
        1,
        options.download_workers if options.download_workers is not None else options.workers,
    )
    extract_workers = max(1, options.extract_workers)
    max_in_flight = max(1, options.max_in_flight)
    flush_every = max(1, options.flush_every)
    log_every = max(1, options.log_every)

    stats = build_initial_stats(len(source.files))
    docs = collect_docs(
        source.files,
        included_map,
        excluded_map,
        skip_included=skip_included,
        skip_excluded=skip_excluded,
        limit=options.limit,
        stats=stats,
    )

    logger.info("Queued %s files for extraction", stats["queued"])
    if not skip_excluded:
        logger.info("Reprocess-excluded enabled")
    if skip_included:
        logger.info("Skip-included enabled")
    logger.info(
        "Workers: download=%s, extract=%s, max_in_flight=%s",
        download_workers,
        extract_workers,
        max_in_flight,
    )

    included_index = MapIndex.generate_index(source.root_id, source.employee_count, included_map)
    excluded_index = MapIndex.generate_index(source.root_id, source.employee_count, excluded_map)

    items: list[dict] = []
    t0 = time.time()
    processed = 0
    stop_event = threading.Event()

    if docs:
        logger.info("Loading credentials...")
        creds = load_creds()
        logger.info("Credentials loaded")

        extract_futures: dict = {}
        with ThreadPoolExecutor(max_workers=download_workers) as download_pool, ProcessPoolExecutor(
            max_workers=extract_workers
        ) as extract_pool:
            download_futures = [
                download_pool.submit(download_pdf_bytes, creds, doc, stop_event)
                for doc in docs
            ]
            try:
                for future in as_completed(download_futures):
                    try:
                        download_result = future.result()
                    except Exception as exc:
                        download_result = {
                            "status": "failed",
                            "stage": "download",
                            "reason": f"{type(exc).__name__}: {exc}",
                            "doc": {},
                        }

                    if download_result["status"] != "success":
                        processed += 1
                        stats["processed"] = processed
                        _apply_result(download_result, stats, items, included_map, excluded_map)
                        _flush_progress(
                            processed=processed,
                            flush_every=flush_every,
                            log_every=log_every,
                            start_ts=t0,
                            included_index=included_index,
                            excluded_index=excluded_index,
                            included_map=included_map,
                            excluded_map=excluded_map,
                            included_path=included_path,
                            excluded_path=excluded_path,
                            logger=logger,
                        )
                        continue

                    while len(extract_futures) >= max_in_flight:
                        done = next(as_completed(extract_futures))
                        doc_info = extract_futures.pop(done, {})
                        try:
                            result = done.result()
                        except Exception as exc:
                            result = {
                                "status": "failed",
                                "stage": "extract",
                                "reason": f"{type(exc).__name__}: {exc}",
                                "doc": doc_info,
                            }
                        processed += 1
                        stats["processed"] = processed
                        _apply_result(result, stats, items, included_map, excluded_map)
                        _flush_progress(
                            processed=processed,
                            flush_every=flush_every,
                            log_every=log_every,
                            start_ts=t0,
                            included_index=included_index,
                            excluded_index=excluded_index,
                            included_map=included_map,
                            excluded_map=excluded_map,
                            included_path=included_path,
                            excluded_path=excluded_path,
                            logger=logger,
                        )

                    doc_info = download_result["doc"]
                    extract_future = extract_pool.submit(
                        extract_and_write,
                        download_result["data"],
                        doc_info,
                        options.out,
                        options.min_normal_score,
                        options.min_score_delta,
                    )
                    extract_futures[extract_future] = doc_info

                for done in as_completed(list(extract_futures)):
                    doc_info = extract_futures.pop(done, {})
                    try:
                        result = done.result()
                    except Exception as exc:
                        result = {
                            "status": "failed",
                            "stage": "extract",
                            "reason": f"{type(exc).__name__}: {exc}",
                            "doc": doc_info,
                        }
                    processed += 1
                    stats["processed"] = processed
                    _apply_result(result, stats, items, included_map, excluded_map)
                    _flush_progress(
                        processed=processed,
                        flush_every=flush_every,
                        log_every=log_every,
                        start_ts=t0,
                        included_index=included_index,
                        excluded_index=excluded_index,
                        included_map=included_map,
                        excluded_map=excluded_map,
                        included_path=included_path,
                        excluded_path=excluded_path,
                        logger=logger,
                    )
            except KeyboardInterrupt:
                stop_event.set()
                logger.warning("Interrupted by user, flushing indexes...")

    included_index.files = dict(included_map)
    excluded_index.files = dict(excluded_map)
    update_index_meta(included_index)
    update_index_meta(excluded_index)
    included_index.save_index(included_path)
    excluded_index.save_index(excluded_path)

    payload = {
        "source_index": index_path,
        "included_index": included_path,
        "excluded_index": excluded_path,
        "out_dir": options.out,
        "stats": stats,
        "items": items,
        "duration_s": round(time.time() - t0, 3),
    }
    ensure_dir(os.path.dirname(report_path) or ".")
    write_json(report_path, payload)
    logger.info("Report saved to %s", report_path)
    logger.info("Done in %.1fs", time.time() - t0)
    return 0
