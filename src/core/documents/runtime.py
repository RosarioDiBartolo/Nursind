from __future__ import annotations

import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import Any

from core.drive.auth_service import load_creds
from core.drive.index_runtime import maybe_flush_indexes
from core.drive.logging_utils import get_logger, setup_logging
from core.drive.schema import IndexFile
from core.errors import CredentialsError

from .options import ExtractDocumentsFromIndexOptions
from .service import apply_extraction_result, finalize_extraction_run, prepare_extraction_run
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


def _record_result(
    *,
    result: dict[str, Any],
    processed: int,
    stats: dict[str, Any],
    items: list[dict[str, Any]],
    state: dict[str, Any],
    start_ts: float,
    logger,
) -> int:
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
        start_ts=start_ts,
        included_index=state["included_index"],
        excluded_index=state["excluded_index"],
        included_map=state["included_map"],
        excluded_map=state["excluded_map"],
        included_path=state["included_path"],
        excluded_path=state["excluded_path"],
        logger=logger,
    )
    return processed


def _cancel_pending_futures(futures) -> int:
    cancelled = 0
    for future in list(futures):
        cancel = getattr(future, "cancel", None)
        if callable(cancel) and cancel():
            cancelled += 1
    return cancelled


def _drain_extract_futures(
    *,
    extract_futures: dict,
    processed: int,
    stats: dict[str, Any],
    items: list[dict[str, Any]],
    state: dict[str, Any],
    start_ts: float,
    logger,
    cancel_pending: bool = False,
) -> tuple[int, int]:
    cancelled = 0
    if cancel_pending:
        for future in list(extract_futures):
            cancel = getattr(future, "cancel", None)
            if callable(cancel) and cancel():
                extract_futures.pop(future, None)
                cancelled += 1

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
        processed = _record_result(
            result=result,
            processed=processed,
            stats=stats,
            items=items,
            state=state,
            start_ts=start_ts,
            logger=logger,
        )
    return processed, cancelled


def run_extraction(
    options: ExtractDocumentsFromIndexOptions,
    *,
    creds=None,
    auto_load_creds: bool = True,
    configure_logging: bool = True,
    return_report: bool = False,
) -> int | dict:
    if configure_logging:
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

    items: list[dict[str, Any]] = []
    t0 = time.time()
    processed = 0
    stop_event = threading.Event()

    if docs:
        needs_drive = any(not bool(doc.get("local")) for doc in docs)
        if needs_drive:
            if creds is None:
                if not auto_load_creds:
                    raise CredentialsError(
                        "Drive credentials are required for document extraction. "
                        "Pass explicit credentials or enable environment-based loading."
                    )
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
                        processed = _record_result(
                            result=download_result,
                            processed=processed,
                            stats=stats,
                            items=items,
                            state=state,
                            start_ts=t0,
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
                        processed = _record_result(
                            result=result,
                            processed=processed,
                            stats=stats,
                            items=items,
                            state=state,
                            start_ts=t0,
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

                processed, _ = _drain_extract_futures(
                    extract_futures=extract_futures,
                    processed=processed,
                    stats=stats,
                    items=items,
                    state=state,
                    start_ts=t0,
                    logger=logger,
                )
            except KeyboardInterrupt:
                stop_event.set()
                stats["interrupted"] = 1
                stats["cancelled_downloads"] += _cancel_pending_futures(download_futures)
                processed, cancelled_extracts = _drain_extract_futures(
                    extract_futures=extract_futures,
                    processed=processed,
                    stats=stats,
                    items=items,
                    state=state,
                    start_ts=t0,
                    logger=logger,
                    cancel_pending=True,
                )
                stats["cancelled_extracts"] += cancelled_extracts
                stats["not_processed_due_to_interrupt"] = max(
                    0,
                    int(stats.get("queued") or 0) - int(stats.get("processed") or 0),
                )
                state["runtime_issues"].append(
                    {
                        "code": "interrupt",
                        "message": (
                            "Interrupted by user; drained submitted extraction tasks "
                            "and cancelled pending work before finalization."
                        ),
                    }
                )
                logger.warning(
                    "Interrupted by user, drained in-flight extraction tasks and flushing indexes..."
                )

    report = finalize_extraction_run(
        options=options,
        state=state,
        items=items,
        start_ts=t0,
    )
    logger.info("Report saved to %s", state["report_path"])
    logger.info("Done in %.1fs", time.time() - t0)
    if return_report:
        return report
    return 0


__all__ = [
    "_cancel_pending_futures",
    "_drain_extract_futures",
    "_flush_progress",
    "_record_result",
    "run_extraction",
]
