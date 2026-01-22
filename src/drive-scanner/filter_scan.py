import os
import json
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from googleapiclient.http import MediaIoBaseDownload

import config
from .auth_service import load_creds
from .drive_client import get_drive_service
from .fs_utils import ensure_dir
from .logging_utils import setup_logging, get_logger
from cartellino_parser.parser import parse_pdf

logger = get_logger()


def load_manifest(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_name(name: str, max_len: int = 120) -> str:
    name = name.strip()
    name = name.replace("\\", "_").replace("/", "_")
    name = name.replace(":", "_").replace("*", "_")
    name = name.replace("?", "_").replace('"', "_")
    name = name.replace("<", "_").replace(">", "_").replace("|", "_")
    if len(name) > max_len:
        name = name[:max_len]
    return name or "unnamed"


def download_file(drive, file_id: str, target_path: str):
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    with open(target_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request, chunksize=4 * 1024 * 1024)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def process_document(creds, employee: dict, doc: dict, out_dir: str, tmp_dir: str):
    drive = get_drive_service(creds)
    emp_name = employee.get("employee") or employee.get("name") or "unknown"
    file_id = doc.get("file_id")
    file_name = doc.get("file_name") or file_id or "unknown.pdf"

    if not file_id:
        return {
            "status": "excluded",
            "employee": emp_name,
            "file_id": file_id,
            "file_name": file_name,
            "reason": "missing file_id",
        }

    safe_emp = safe_name(emp_name)
    base_name = safe_name(file_name)
    if not base_name.lower().endswith(".pdf"):
        base_name = f"{base_name}.pdf"
    target_name = f"{os.path.splitext(base_name)[0]}__{file_id[:8]}.pdf"

    emp_dir = os.path.join(out_dir, safe_emp)
    ensure_dir(emp_dir)

    target_path = os.path.join(emp_dir, target_name)
    tmp_path = os.path.join(tmp_dir, f"{safe_name(file_id)}.pdf")

    try:
        download_file(drive, file_id, tmp_path)
        parse_pdf(tmp_path)
        os.replace(tmp_path, target_path)
        return {
            "status": "included",
            "employee": emp_name,
            "file_id": file_id,
            "file_name": file_name,
            "path": target_path,
        }
    except Exception as exc:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        return {
            "status": "excluded",
            "employee": emp_name,
            "file_id": file_id,
            "file_name": file_name,
            "reason": f"{type(exc).__name__}: {exc}",
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--out", default="downloads")
    parser.add_argument("--excluded-out", default="excluded.json")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    config.validate_env()
    ensure_dir(args.out)
    tmp_dir = os.path.join(args.out, ".tmp")
    ensure_dir(tmp_dir)

    creds = load_creds()
    manifest = load_manifest(args.manifest)

    employees = manifest.get("employees") or []

    docs = []
    for emp in employees:
        for doc in emp.get("included", []):
            docs.append((emp, doc))

    t0 = time.time()
    included = []
    excluded = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(process_document, creds, emp, doc, args.out, tmp_dir)
            for emp, doc in docs
        ]
        for i, f in enumerate(as_completed(futures), 1):
            result = f.result()
            if result["status"] == "excluded":
                excluded.append(result)
                logger.debug("Excluded %s (%s)", result["file_name"], result["reason"])
            else:
                included.append(result)
            if i % 25 == 0 or i == len(futures):
                logger.info("Progress %s/%s files", i, len(futures))

    logger.info("Done in %.1fs", time.time() - t0)

    excluded_path = args.excluded_out
    if not os.path.isabs(excluded_path):
        excluded_path = os.path.join(args.out, excluded_path)
    with open(excluded_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "manifest": args.manifest,
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "excluded": excluded,
            },
            f,
            indent=2,
        )

    logger.info("Excluded file list saved to %s", excluded_path)


if __name__ == "__main__":
    main()
