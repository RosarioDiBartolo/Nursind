import argparse
import csv
import logging
import os
import random
import time
from typing import Any, Dict, List

from . import config
from .auth_service import load_creds
from .drive_client import get_drive_service
from .downloads import download_pdf_stream
from .fs_utils import ensure_dir
from .io_json import write_json
from .logging_utils import setup_logging
from .names import safe_name


def _read_rows(
    csv_path: str,
    id_col: str,
    name_col: str,
    type_col: str,
    required_type: str | None,
    skip_dot_underscore: bool,
    logger: logging.Logger,
) -> tuple[List[Dict[str, str]], Dict[str, int]]:
    stats = {
        "total_rows": 0,
        "missing_id": 0,
        "duplicate_id": 0,
        "type_mismatch": 0,
        "dot_underscore": 0,
    }
    rows: List[Dict[str, str]] = []
    seen: set[str] = set()

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or id_col not in reader.fieldnames:
            raise ValueError(f"Missing required column '{id_col}' in {csv_path}")

        has_type_col = type_col in reader.fieldnames
        if required_type and not has_type_col:
            logger.warning("Column '%s' not found; --type filter ignored", type_col)

        for row in reader:
            stats["total_rows"] += 1
            file_id = (row.get(id_col) or "").strip()
            if not file_id:
                stats["missing_id"] += 1
                continue
            if file_id in seen:
                stats["duplicate_id"] += 1
                continue

            file_name = (row.get(name_col) or "").strip()
            file_type = (row.get(type_col) or "").strip()

            if required_type and has_type_col and file_type and file_type != required_type:
                stats["type_mismatch"] += 1
                continue

            if skip_dot_underscore and file_name.startswith("._"):
                stats["dot_underscore"] += 1
                continue

            seen.add(file_id)
            rows.append(
                {
                    "file_id": file_id,
                    "file_name": file_name,
                    "type": file_type,
                }
            )

    return rows, stats


def _build_output_path(out_dir: str, file_id: str, file_name: str) -> str:
    base_name = (file_name or file_id).strip()
    stem, ext = os.path.splitext(base_name)
    if not ext:
        ext = ".pdf"
    safe_stem = safe_name(stem or file_id)
    out_name = f"{safe_stem}__{file_id[:8]}{ext}"
    return os.path.join(out_dir, out_name)


def main():
    ap = argparse.ArgumentParser(
        description="Download a random batch of Drive PDFs from a CSV file_id list."
    )
    ap.add_argument("--csv", required=True, help="CSV file containing file_id column")
    ap.add_argument("--out", default=os.path.join("downloads", "samples"), help="Output directory")
    ap.add_argument("--count", type=int, default=12, help="How many files to download")
    ap.add_argument("--seed", type=int, default=None, help="Random seed for reproducible sampling")
    ap.add_argument("--id-col", default="file_id", help="CSV column name for file_id")
    ap.add_argument("--name-col", default="file_name", help="CSV column name for file_name")
    ap.add_argument("--type-col", default="type", help="CSV column name for type")
    ap.add_argument("--type", dest="required_type", default="file", help="Filter by type column")
    ap.add_argument(
        "--include-dot-underscore",
        action="store_true",
        help="Include file names starting with ._ (macOS metadata files)",
    )
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    ap.add_argument(
        "--report",
        default="download_random.report.json",
        help="Report filename (relative to --out unless absolute)",
    )
    ap.add_argument("--dry-run", action="store_true", help="Do not download, just sample")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    if args.count <= 0:
        raise SystemExit("--count must be > 0")

    seed = args.seed if args.seed is not None else int(time.time())

    rows, read_stats = _read_rows(
        args.csv,
        args.id_col,
        args.name_col,
        args.type_col,
        args.required_type if args.required_type else None,
        not args.include_dot_underscore,
        logger,
    )

    if not rows:
        raise SystemExit("No rows available after filtering")

    if args.count > len(rows):
        logger.warning("Requested %d files but only %d available; using all", args.count, len(rows))
        sample = list(rows)
    else:
        rng = random.Random(seed)
        sample = rng.sample(rows, args.count)

    ensure_dir(args.out)
    report_path = args.report if os.path.isabs(args.report) else os.path.join(args.out, args.report)

    results: List[Dict[str, Any]] = []
    download_stats = {
        "downloaded": 0,
        "failed": 0,
        "skipped_existing": 0,
        "dry_run": 0,
    }

    if args.dry_run:
        logger.info("Dry run enabled; no files will be downloaded")
    else:
        config.validate_env()
        creds = load_creds()
        drive = get_drive_service(creds)

    for i, row in enumerate(sample, start=1):
        file_id = row["file_id"]
        file_name = row.get("file_name") or file_id
        out_path = _build_output_path(args.out, file_id, file_name)
        stream = None

        if not args.overwrite and os.path.exists(out_path):
            download_stats["skipped_existing"] += 1
            results.append(
                {
                    "file_id": file_id,
                    "file_name": file_name,
                    "out_path": out_path,
                    "status": "skipped",
                    "reason": "exists",
                }
            )
            logger.info("[%d/%d] Skipping existing %s", i, len(sample), out_path)
            continue

        if args.dry_run:
            download_stats["dry_run"] += 1
            results.append(
                {
                    "file_id": file_id,
                    "file_name": file_name,
                    "out_path": out_path,
                    "status": "dry_run",
                }
            )
            logger.info("[%d/%d] Would download %s", i, len(sample), file_name)
            continue

        logger.info("[%d/%d] Downloading %s", i, len(sample), file_name)
        try:
            stream = download_pdf_stream(drive, file_id, logger=logger)
            with open(out_path, "wb") as f:
                f.write(stream.read())
            download_stats["downloaded"] += 1
            results.append(
                {
                    "file_id": file_id,
                    "file_name": file_name,
                    "out_path": out_path,
                    "status": "downloaded",
                }
            )
        except Exception as exc:
            download_stats["failed"] += 1
            results.append(
                {
                    "file_id": file_id,
                    "file_name": file_name,
                    "out_path": out_path,
                    "status": "failed",
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            )
            logger.exception("Download failed for %s", file_id)
        finally:
            try:
                if stream is not None:
                    stream.close()
            except Exception:
                pass

    payload = {
        "source_csv": args.csv,
        "out_dir": args.out,
        "seed": seed,
        "count_requested": args.count,
        "count_available": len(rows),
        "count_sampled": len(sample),
        "filters": {
            "required_type": args.required_type,
            "skip_dot_underscore": not args.include_dot_underscore,
            "id_col": args.id_col,
            "name_col": args.name_col,
            "type_col": args.type_col,
        },
        "read_stats": read_stats,
        "download_stats": download_stats,
        "items": results,
    }
    write_json(report_path, payload)
    logger.info("Report saved to %s", report_path)


if __name__ == "__main__":
    main()
