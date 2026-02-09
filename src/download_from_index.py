from __future__ import annotations

"""Download PDF files listed in a MapIndex into a local folder."""

import argparse
import os
 
from drive_service.auth_service import load_creds
from drive_service.downloads import download_pdf_stream
from drive_service.drive_client import get_drive_service
from drive_service.fs_utils import ensure_dir
from drive_service.logging_utils import get_logger, setup_logging
from drive_service.map_index_service import MapIndex
from drive_service.names import safe_name
 

DEFAULT_INDEX = "scan/samples.index.scan.map.json"
DEFAULT_OUT = "samples/from_index"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download files listed in a MapIndex to a local folder."
    )
    parser.add_argument("--index", default=DEFAULT_INDEX, help="Input MapIndex JSON path")
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output folder")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Download only first N files (0 = no limit)",
    )
    parser.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip files already present on disk (default: true)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser


def _build_output_path(out_dir: str, employee: str, file_name: str, file_id: str) -> str:
    safe_employee = safe_name(employee or "unknown")
    safe_file_name = safe_name(file_name or file_id or "unknown.pdf")
    if not safe_file_name.lower().endswith(".pdf"):
        safe_file_name = f"{safe_file_name}.pdf"
    employee_dir = os.path.join(out_dir, safe_employee)
    ensure_dir(employee_dir)
    return os.path.join(employee_dir, safe_file_name)


def main() -> int:
    args = build_parser().parse_args()
    setup_logging(args.verbose)
    logger = get_logger()

    ensure_dir(args.out)
    index_path = args.index if os.path.isabs(args.index) else os.path.abspath(args.index)
    out_path = args.out if os.path.isabs(args.out) else os.path.abspath(args.out)

    source = MapIndex.load_index(index_path, strict=True)
    docs = [
        (file_id, item)
        for file_id, item in source.files.items()
        if file_id and item.type != "folder"
    ]
    if args.limit > 0:
        docs = docs[: args.limit]

    logger.info("Index: %s", index_path)
    logger.info("Output: %s", out_path)
    logger.info("Files queued: %s", len(docs))
    if not docs:
        return 0

    creds = load_creds()
    drive = get_drive_service(creds)

    downloaded = 0
    skipped = 0
    failed = 0

    for idx, (file_id, item) in enumerate(docs, start=1):
        target_path = _build_output_path(
            out_path,
            employee=item.employee or "unknown",
            file_name=item.file_name or file_id,
            file_id=file_id,
        )

        if args.skip_existing and os.path.exists(target_path):
            skipped += 1
            continue

        stream = None
        try:
            stream = download_pdf_stream(drive, file_id, logger=logger)
            with open(target_path, "wb") as out_file:
                out_file.write(stream.read())
            downloaded += 1
        except Exception as exc:
            failed += 1
            logger.warning("Download failed for %s (%s): %s", file_id, item.file_name, exc)
        finally:
            if stream is not None:
                try:
                    stream.close()
                except Exception:
                    pass

        if args.verbose and idx % 25 == 0:
            logger.info(
                "Progress %s/%s (downloaded=%s skipped=%s failed=%s)",
                idx,
                len(docs),
                downloaded,
                skipped,
                failed,
            )

    logger.info(
        "Done (downloaded=%s skipped=%s failed=%s total=%s)",
        downloaded,
        skipped,
        failed,
        len(docs),
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
