from __future__ import annotations

import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from src.drive_service.auth_service import load_creds
from src.drive_service.index_runtime import (
    maybe_flush_indexes,
)
from src.drive_service.logging_utils import get_logger, setup_logging
from src.drive_service.schema import IndexFile

from .options import ExtractDocumentsFromIndexOptions
from .service import (
    apply_extraction_result,
    finalize_extraction_run,
    prepare_extraction_run,
)
from .workers import download_pdf_bytes, extract_and_write


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


def run_extraction(options: ExtractDocumentsFromIndexOptions) -> int:
    setup_logging(options.verbose)
    logger = get_logger()
    logger.info("Starting document extraction pipeline at out=%s", options.out)

    state = prepare_extraction_run(options)
    stats = state["stats"]
    docs = state["docs"]

    logger.info("Queued %s files for extraction", stats["queued"])
    if not state["skip_excluded"]:
        logger.info("Reprocess-excluded enabled")
    if state["skip_included"]:
        logger.info("Skip-included enabled")
    logger.info(
        "Workers: download=%s, extract=%s, max_in_flight=%s",
        state["download_workers"],
        state["extract_workers"],
        state["max_in_flight"],
    )

    items: list[dict] = []
    t0 = time.time()
    processed = 0
    stop_event = threading.Event()

    if docs:
        needs_drive = any(not bool(doc.get("local")) for doc in docs)
        if needs_drive:
            logger.info("Loading credentials...")
            creds = load_creds()
            logger.info("Credentials loaded")
        else:
            logger.info("No remote documents queued; skipping credential load")
            creds = None

        extract_futures: dict = {}
        with ThreadPoolExecutor(max_workers=state["download_workers"]) as download_pool, ProcessPoolExecutor(
            max_workers=state["extract_workers"]
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
                        apply_extraction_result(
                            download_result,
                            stats=stats,
                            items=items,
                            included_map=state["included_map"],
                            excluded_map=state["excluded_map"],
                            text_rows_by_file_id=state["text_rows_by_file_id"],
                        )
                        _flush_progress(
                            processed=processed,
                            flush_every=state["flush_every"],
                            log_every=state["log_every"],
                            start_ts=t0,
                            included_index=state["included_index"],
                            excluded_index=state["excluded_index"],
                            included_map=state["included_map"],
                            excluded_map=state["excluded_map"],
                            included_path=state["included_path"],
                            excluded_path=state["excluded_path"],
                            logger=logger,
                        )
                        continue

                    while len(extract_futures) >= state["max_in_flight"]:
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
                        apply_extraction_result(
                            result,
                            stats=stats,
                            items=items,
                            included_map=state["included_map"],
                            excluded_map=state["excluded_map"],
                            text_rows_by_file_id=state["text_rows_by_file_id"],
                        )
                        _flush_progress(
                            processed=processed,
                            flush_every=state["flush_every"],
                            log_every=state["log_every"],
                            start_ts=t0,
                            included_index=state["included_index"],
                            excluded_index=state["excluded_index"],
                            included_map=state["included_map"],
                            excluded_map=state["excluded_map"],
                            included_path=state["included_path"],
                            excluded_path=state["excluded_path"],
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
                    apply_extraction_result(
                        result,
                        stats=stats,
                        items=items,
                        included_map=state["included_map"],
                        excluded_map=state["excluded_map"],
                        text_rows_by_file_id=state["text_rows_by_file_id"],
                    )
                    _flush_progress(
                        processed=processed,
                        flush_every=state["flush_every"],
                        log_every=state["log_every"],
                        start_ts=t0,
                        included_index=state["included_index"],
                        excluded_index=state["excluded_index"],
                        included_map=state["included_map"],
                        excluded_map=state["excluded_map"],
                        included_path=state["included_path"],
                        excluded_path=state["excluded_path"],
                        logger=logger,
                    )
            except KeyboardInterrupt:
                stop_event.set()
                logger.warning("Interrupted by user, flushing indexes...")

    finalize_extraction_run(
        options=options,
        state=state,
        items=items,
        start_ts=t0,
    )
    logger.info("Report saved to %s", state["report_path"])
    logger.info("Done in %.1fs", time.time() - t0)
    return 0


__all__ = ["_flush_progress", "run_extraction"]
