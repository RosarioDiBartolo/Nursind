from __future__ import annotations

"""Download PDF files listed in a MapIndex into a local folder."""

import argparse
from hashlib import sha1
import os
import random
 
from src.drive_service.auth_service import load_creds
from src.drive_service.drive_client import get_drive_service
from src.drive_service.fs_utils import ensure_dir
from src.drive_service.index import MapIndex
from src.drive_service.index_downloads import download_pdf_bytes_for_index_item
from src.drive_service.logging_utils import get_logger, setup_logging
from src.drive_service.names import safe_name
 

DEFAULT_INDEX = "scan/samples.index.scan.map.json"
DEFAULT_OUT = "samples/from_index"
_ZIP_CACHE_MAX_ITEMS = 16


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
        "--random-sample",
        type=int,
        default=0,
        help="Randomly sample N files before download (0 = disabled)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed used with --random-sample",
    )
    parser.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip files already present on disk (default: true)",
    )
    parser.add_argument(
        "--flat-output",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Write files in a single flat output folder (default: true). "
            "Use --no-flat-output for per-employee subfolders."
        ),
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser


def _build_output_path(
    out_dir: str,
    employee: str,
    file_name: str,
    file_id: str,
    *,
    flat_output: bool,
) -> str:
    safe_employee = safe_name(employee or "unknown")
    safe_file_name = safe_name(file_name or file_id or "unknown.pdf")
    if not safe_file_name.lower().endswith(".pdf"):
        safe_file_name = f"{safe_file_name}.pdf"
    if flat_output:
        stem, ext = os.path.splitext(safe_file_name)
        file_hash = sha1((file_id or safe_file_name).encode("utf-8")).hexdigest()[:10]
        flat_name = safe_name(f"{safe_employee}_{stem}_{file_hash}")
        ensure_dir(out_dir)
        return os.path.join(out_dir, f"{flat_name}{ext}")
    employee_dir = os.path.join(out_dir, safe_employee)
    ensure_dir(employee_dir)
    return os.path.join(employee_dir, safe_file_name)


def _select_docs_for_download(
    docs: list[tuple[str, object]],
    *,
    random_sample: int,
    seed: int | None,
    limit: int,
) -> list[tuple[str, object]]:
    selected = list(docs)
    if random_sample > 0 and selected:
        sample_size = min(random_sample, len(selected))
        rng = random.Random(seed)
        selected = rng.sample(selected, sample_size)
    if limit > 0:
        selected = selected[:limit]
    return selected


def main() -> int:
    args = build_parser().parse_args()
    setup_logging(args.verbose)
    logger = get_logger()

    if args.limit < 0:
        raise ValueError("--limit must be >= 0")
    if args.random_sample < 0:
        raise ValueError("--random-sample must be >= 0")
    if args.seed is not None and args.random_sample == 0:
        logger.warning("--seed was provided without --random-sample; seed will be ignored")

    ensure_dir(args.out)
    index_path = args.index if os.path.isabs(args.index) else os.path.abspath(args.index)
    out_path = args.out if os.path.isabs(args.out) else os.path.abspath(args.out)

    source = MapIndex.load_index(index_path, strict=True)
    docs = [
        (file_id, item)
        for file_id, item in source.files.items()
        if file_id and item.type != "folder"
    ]
    docs = _select_docs_for_download(
        docs,
        random_sample=args.random_sample,
        seed=args.seed,
        limit=args.limit,
    )

    logger.info("Index: %s", index_path)
    logger.info("Output: %s", out_path)
    logger.info("Files queued: %s", len(docs))
    if not docs:
        return 0

    creds = None
    drive = None

    downloaded = 0
    skipped = 0
    failed = 0
    zip_cache: dict[str, bytes] = {}
    zip_cache_order: list[str] = []

    for idx, (file_id, item) in enumerate(docs, start=1):
        target_path = _build_output_path(
            out_path,
            employee=item.employee or "unknown",
            file_name=item.file_name or file_id,
            file_id=file_id,
            flat_output=args.flat_output,
        )

        if args.skip_existing and os.path.exists(target_path):
            skipped += 1
            continue

        try:
            is_local = bool(getattr(item, "local", False)) or str(file_id).startswith("local::")
            if not is_local and drive is None:
                creds = load_creds()
                drive = get_drive_service(creds)
            result = download_pdf_bytes_for_index_item(
                drive,
                file_id=file_id,
                local=is_local,
                drive_path=getattr(item, "drive_path", None),
                source_kind=getattr(item, "type", None),
                logger=logger,
                zip_cache=zip_cache,
                zip_cache_order=zip_cache_order,
                zip_cache_max_items=_ZIP_CACHE_MAX_ITEMS,
            )
            if result["status"] != "success":
                failed += 1
                logger.warning(
                    "Download failed for %s (%s): %s",
                    file_id,
                    item.file_name,
                    result.get("reason") or "download_failed",
                )
                continue
            with open(target_path, "wb") as out_file:
                out_file.write(result["data"])
            downloaded += 1
        except Exception as exc:
            failed += 1
            logger.warning("Download failed for %s (%s): %s", file_id, item.file_name, exc)

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
