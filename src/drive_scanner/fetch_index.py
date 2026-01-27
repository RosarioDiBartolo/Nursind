import os
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from drive_scanner.index_service import Index
from drive_scanner.names import safe_name 

from . import config
from .auth_service import load_creds
from .drive_client import get_drive_service
from .downloads import download_pdf_stream
from .fs_utils import ensure_dir 
from .io_json import write_json
from .logging_utils import setup_logging, get_logger
 
from .schema import IndexFile, Outputs
from parser_service import parse_pdf

logger = None


def _doc_attr(doc, name: str):
    if hasattr(doc, name):
        return getattr(doc, name)
    if isinstance(doc, dict):
        return doc.get(name)
    return None


def _file_key(file_id: str | None, file_name: str | None) -> str:
    if file_id:
        return f"id:{file_id}"
    return f"name:{file_name or 'unknown'}"


def _update_index_meta(index: Index) -> None:
    index.generated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    employees = {item.employee for item in index.files if getattr(item, "employee", None)}
    index.employee_count = len(employees)


def process_document(
    creds,
    doc: IndexFile,
    out_dir: str,
    stop_event: threading.Event,
):
    drive = get_drive_service(creds)
    emp_name = _doc_attr(doc, "employee") or "unknown"
    emp_id = _doc_attr(doc, "employee_id")
    file_id = _doc_attr(doc, "file_id")
    file_name = _doc_attr(doc, "file_name") or file_id or "unknown.pdf"
    reason = _doc_attr(doc, "reason")


    #Only for when source is excluded
    if reason =="PdfminerException: No /Root object! - Is this really a PDF?":
        return {
            "status": "failed",
            "employee": emp_name,
            "employee_id": emp_id,
            "file_id": file_id,
            "file_name": file_name,
            "reason": reason,
        }
    
    if stop_event.is_set():
        return {
            "status": "failed",
            "employee": emp_name,
            "employee_id": emp_id,
            "file_id": file_id,
            "file_name": file_name,
            "reason": "cancelled",
        }

    if not file_id:
        return {
            "status": "failed",
            "employee": emp_name,
            "employee_id": emp_id,
            "file_id": file_id,
            "file_name": file_name,
            "reason": "missing file_id",
        }

    safe_emp = safe_name(emp_name)
    base_name = safe_name(file_name)
    if not base_name.lower().endswith(".pdf"):
        base_name = f"{base_name}.pdf"
    file_tag = f"{os.path.splitext(base_name)[0]}__{file_id[:8]}"

    emp_dir = os.path.join(out_dir, safe_emp)
    file_dir = os.path.join(emp_dir, file_tag)
    ensure_dir(file_dir)

    try:
        if stop_event.is_set():
            raise RuntimeError("cancelled")
        stream = download_pdf_stream(drive, file_id, logger=logger)
        if stop_event.is_set():
            raise RuntimeError("cancelled")
        parsed = parse_pdf(stream)

        days_path = os.path.join(file_dir, "days.csv")
        pairs_path = os.path.join(file_dir, "pairs.csv")
        totals_path = os.path.join(file_dir, "totals.json")
        report_path = os.path.join(file_dir, "report.json")

        parsed.days_df.to_csv(days_path, index=False)
        parsed.pairs_df.to_csv(pairs_path, index=False)
        write_json(totals_path, parsed.totals)
        write_json(
            report_path,
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
            "outputs": {
                "days_csv": days_path,
                "pairs_csv": pairs_path,
                "totals_json": totals_path,
                "report_json": report_path,
            },
        }
    except Exception as exc:
        if isinstance(exc, RuntimeError) and str(exc) == "cancelled":
            reason = "cancelled"
        else:
            reason = f"{type(exc).__name__}: {exc}"
        return {
            "status": "failed",
            "employee": emp_name,
            "employee_id": emp_id,
            "file_id": file_id,
            "file_name": file_name,
            "reason": reason,
        }
    finally:
        try:
            if "stream" in locals():
                stream.close()
        except Exception:
            pass


def _resolve_report_path(out_dir: str, report: str) -> str:
    if os.path.isabs(report):
        return report
    return os.path.join(out_dir, report)


 


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="downloads", help="Output directory (default: downloads)")
    parser.add_argument("--excluded", default="excluded.index.json", help="Excluded index filename (default: excluded.index.json)")
    parser.add_argument("--included", default="included.index.json", help="Included index filename (default: included.index.json)")
 
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    global logger
    logger = get_logger()
    logger.info("Starting fetch_index at out=%s", args.out )
    config.validate_env()
    ensure_dir(args.out)

    logger.info("Loading credentials...")
    creds = load_creds()
    logger.info("Credentials loaded")

 
    excluded_path = _resolve_report_path(args.out, args.excluded)
    print(excluded_path)

    excluded = Index.load_index(excluded_path, strict=False)
    included_path = _resolve_report_path(args.out, args.included)
    included = Index.load_index(included_path, strict=False)
    docs = excluded.files
    excluded_map = {
        _file_key(getattr(item, "file_id", None), getattr(item, "file_name", None)): item
        for item in excluded.files
    }

    logger.info("Found %d documents to process", len(docs))
    if not docs:
        logger.info("No documents to process... exiting.")
        return
    


    t0 = time.time()
    flush_every = 25
    processed_since_flush = 0
    stop_event = threading.Event()

    interrupted = False


    new_excluded = Index.generate_index(excluded.root_id, excluded.employee_count, list(excluded_map.values()))
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        logger.info("Submitting %d tasks to ThreadPoolExecutor (workers=%d)", len(docs), args.workers)
        futures = [
            pool.submit(process_document, creds,  doc, args.out, stop_event)
            for doc in docs
        ]
        try:
            for i, f in enumerate(as_completed(futures), 1):
                result = f.result()

                key = _file_key(result.get("file_id"), result.get("file_name"))
                if result["status"] == "success":
                    included.files.append(
                        IndexFile(
                            employee=result.get("employee"),
                            employee_id=result.get("employee_id"),
                            file_id=result.get("file_id"),
                            file_name=result.get("file_name"),
                            outputs=Outputs(
                                days_csv=result["outputs"]["days_csv"],
                                pairs_csv=result["outputs"]["pairs_csv"],
                                totals_json=result["outputs"]["totals_json"],
                                report_json=result["outputs"]["report_json"],
                            ),
                        )
                         
                    )
                    excluded_map.pop(key, None)

                    
                else:    
                    excluded_map[key] = IndexFile(
                        employee=result.get("employee") or "unknown",
                        employee_id=result.get("employee_id"),
                        file_id=result.get("file_id"),
                        file_name=result.get("file_name"),
                        reason=result.get("reason"),
                        type="file",
                    )
                    
                processed_since_flush += 1
                if processed_since_flush >= flush_every:
                    new_excluded.files = list(excluded_map.values())
                    _update_index_meta(included)
                    _update_index_meta(new_excluded)
                    logger.info("Updating indexes, excluded: %s included: %s", excluded_path, included_path)
                    included.save_index(included_path)
                    new_excluded.save_index(excluded_path)
                    processed_since_flush = 0
                if i % 25 == 0 or i == len(futures):
                    logger.info("Progress %s/%s files", i, len(futures))
        except KeyboardInterrupt:
            stop_event.set()
            for future in futures:
                future.cancel()
            logger.warning("Interrupted by user, flushing report...")
            interrupted = True

    if interrupted:
        logger.info("Stopped after %.1fs", time.time() - t0)
    else:
        logger.info("Done in %.1fs", time.time() - t0)

    new_excluded.files = list(excluded_map.values())
    _update_index_meta(included)
    _update_index_meta(new_excluded)
    included.save_index(included_path)
    new_excluded.save_index(excluded_path)

      


if __name__ == "__main__":
    main()
