from __future__ import annotations

import argparse
import csv
import json
import logging
import math
from pathlib import Path

from cartellino_parser.parse_days import parse_days as parse_cartellino_days
from parser_service.router import analyze_detection
from parser_shared.extract import extract_text
from timbrature_elenco_compact_parser.parse_days import parse_days as parse_compact_days
from timbrature_elenco_parser.parse_days import parse_days as parse_elenco_days


LOGGER = logging.getLogger(__name__)

PARSERS = {
    "cartellino": parse_cartellino_days,
    "timbrature_compact": parse_compact_days,
    "timbrature_elenco": parse_elenco_days,
}


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")


def _iter_pdfs(folder: Path) -> list[Path]:
    return sorted(folder.glob("*.pdf"))


def _pick_sample(paths: list[Path], sample_ratio: float, limit: int | None) -> list[Path]:
    if not paths:
        return []
    count = max(1, math.ceil(len(paths) * sample_ratio))
    if limit is not None:
        count = min(count, limit)
    return paths[:count]


def _count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return 0
        return sum(1 for _ in reader)


def _load_text(output_dir: Path, pdf_path: Path, allow_extract: bool) -> tuple[str | None, str]:
    extracted_path = output_dir / "extracted.normal.txt"
    if extracted_path.exists():
        return extracted_path.read_text(encoding="utf-8", errors="replace"), "artifact"
    if allow_extract:
        return extract_text(pdf_path), "extracted"
    return None, "missing"


def _load_report_is_ok(report_path: Path) -> bool | None:
    if not report_path.exists():
        return None
    data = json.loads(report_path.read_text(encoding="utf-8"))
    validation = data.get("validation", {})
    is_ok = validation.get("is_ok")
    return is_ok if isinstance(is_ok, bool) else None


def _check_group(
    name: str,
    samples_root: Path,
    output_root: Path,
    sample_ratio: float,
    limit: int | None,
    allow_extract: bool,
) -> int:
    parser = PARSERS[name]
    pdfs = _iter_pdfs(samples_root / name)
    sample = _pick_sample(pdfs, sample_ratio, limit)

    missing_outputs: list[str] = []
    day_mismatches: list[str] = []
    validation_failures: list[str] = []
    detection_issues: list[str] = []
    missing_text: list[str] = []

    for pdf_path in sample:
        stem = pdf_path.stem
        output_dir = output_root / name / stem
        days_path = output_dir / "days.csv"
        report_path = output_dir / "report.json"
        totals_path = output_dir / "totals.json"
        pairs_path = output_dir / "pairs.csv"

        missing = [
            str(path.name)
            for path in (days_path, pairs_path, totals_path, report_path)
            if not path.exists()
        ]
        if missing:
            missing_outputs.append(f"{pdf_path.name}: missing {', '.join(missing)}")
            continue

        text, text_source = _load_text(output_dir, pdf_path, allow_extract)
        if text is None:
            missing_text.append(pdf_path.name)
            continue

        records = parser(text.splitlines(), None, None)
        expected_days = len(records)
        actual_days = _count_csv_rows(days_path)
        if expected_days != actual_days:
            day_mismatches.append(
                f"{pdf_path.name}: extracted_days={expected_days} output_days={actual_days} ({text_source})"
            )

        is_ok = _load_report_is_ok(report_path)
        if is_ok is False:
            validation_failures.append(pdf_path.name)

        detect_info = analyze_detection(text)
        detected_family = detect_info.get("detected_family")
        if name == "cartellino" and detected_family not in {"cartellino"}:
            detection_issues.append(f"{pdf_path.name}: detected={detected_family}")
        if name != "cartellino" and detected_family not in {"timbrature"}:
            detection_issues.append(f"{pdf_path.name}: detected={detected_family}")

    LOGGER.info(
        "%s: total=%d sampled=%d missing_outputs=%d mismatched_days=%d validation_fail=%d missing_text=%d detection_issues=%d",
        name,
        len(pdfs),
        len(sample),
        len(missing_outputs),
        len(day_mismatches),
        len(validation_failures),
        len(missing_text),
        len(detection_issues),
    )

    def _emit(label: str, items: list[str]) -> None:
        if not items:
            return
        LOGGER.info("%s (%d):", label, len(items))
        for item in items[:10]:
            LOGGER.info("  %s", item)
        if len(items) > 10:
            LOGGER.info("  ... %d more", len(items) - 10)

    _emit(f"{name} missing outputs", missing_outputs)
    _emit(f"{name} day count mismatches", day_mismatches)
    _emit(f"{name} validation failures", validation_failures)
    _emit(f"{name} missing extracted text", missing_text)
    _emit(f"{name} detection issues", detection_issues)

    return len(missing_outputs) + len(day_mismatches) + len(validation_failures)


def main() -> int:
    parser = argparse.ArgumentParser(description="Check sample outputs against extracted text.")
    parser.add_argument("--samples", default="samples", help="Samples root folder")
    parser.add_argument("--out", default="samples/output", help="Output root folder")
    parser.add_argument(
        "--sample",
        type=float,
        default=0.7,
        help="Sample ratio per folder (majority). Default: 0.7",
    )
    parser.add_argument("--limit", type=int, help="Max PDFs to check per folder")
    parser.add_argument(
        "--extract-if-missing",
        action="store_true",
        help="Re-extract text when extracted.normal.txt is missing",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    _configure_logging(args.verbose)

    samples_root = Path(args.samples)
    output_root = Path(args.out)
    issues = 0
    for name in ("cartellino", "timbrature_compact", "timbrature_elenco"):
        issues += _check_group(
            name,
            samples_root,
            output_root,
            args.sample,
            args.limit,
            args.extract_if_missing,
        )

    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
