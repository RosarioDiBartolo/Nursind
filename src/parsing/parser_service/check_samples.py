from __future__ import annotations

import argparse
import csv
import json
import logging
import math
from pathlib import Path
from datetime import datetime

from ..parsers.cartellino.parse_days import parse_days as parse_cartellino_days
from ..core.detect import analyze_detection
from ..core.extractor import extract_text
from ..parsers.timbrature_compact.parse_days import parse_days as parse_compact_days
from ..parsers.timbrature_elenco.parse_days import parse_days as parse_elenco_days
from ..timbrature_shared.day_values import extract_day_values


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


def _load_report(report_path: Path) -> dict | None:
    if not report_path.exists():
        return None
    return json.loads(report_path.read_text(encoding="utf-8"))


def _diagnose_overcount(records, totals: dict, max_lines: int) -> list[str]:
    ore_lavorate_total = totals.get("ore_lavorate")
    if ore_lavorate_total is None or not records:
        return []
    avg = float(ore_lavorate_total) / max(1, len(records))
    items = []
    for record in records:
        extracted = extract_day_values(record.raw)
        if not extracted:
            continue
        _day, _dow, values, has_event = extracted
        excess = record.mo_lav - avg
        items.append(
            {
                "day": record.day,
                "dow": record.dow,
                "mo_lav": record.mo_lav,
                "excess": excess,
                "values": values,
                "has_event": has_event,
                "raw": record.raw,
            }
        )

    items.sort(key=lambda item: item["excess"], reverse=True)
    lines: list[str] = []
    for item in items[:max_lines]:
        lines.append(
            f"{item['day']:02d} {item['dow']} mo_lav={item['mo_lav']:.2f} "
            f"excess={item['excess']:.2f} values={item['values']} "
            f"event={item['has_event']} raw={item['raw']}"
        )
    return lines


def _check_group(
    name: str,
    samples_root: Path,
    output_root: Path,
    sample_ratio: float,
    limit: int | None,
    allow_extract: bool,
    diagnose_overcount: bool,
    max_lines: int,
) -> int:
    parser = PARSERS[name]
    pdfs = _iter_pdfs(samples_root / name)
    sample = _pick_sample(pdfs, sample_ratio, limit)

    missing_outputs: list[str] = []
    day_mismatches: list[str] = []
    validation_failures: list[str] = []
    detection_issues: list[str] = []
    missing_text: list[str] = []
    pair_issues: list[str] = []

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

        incomplete_pairs = 0
        missing_duration = 0
        duration_mismatch = 0
        total_pairs = 0
        with pairs_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_pairs += 1
                entry_ts = row.get("entry_ts") or ""
                exit_ts = row.get("exit_ts") or ""
                duration_hhmm = row.get("duration_hhmm") or ""
                if not entry_ts or not exit_ts:
                    incomplete_pairs += 1
                    continue
                if not duration_hhmm:
                    missing_duration += 1
                    continue
                if entry_ts and exit_ts and duration_hhmm:
                    try:
                        hh, mm = duration_hhmm.split(":", 1)
                        duration_hours = int(hh) + int(mm) / 60.0
                    except Exception:
                        duration_hours = None
                    try:
                        entry = datetime.fromisoformat(entry_ts)
                        exit_time = datetime.fromisoformat(exit_ts)
                        diff_hours = (exit_time - entry).total_seconds() / 3600.0
                    except Exception:
                        diff_hours = None
                    if duration_hours is not None and diff_hours is not None:
                        if abs(diff_hours - duration_hours) > 0.2:
                            duration_mismatch += 1

        if missing_duration or duration_mismatch:
            pair_issues.append(
                f"{pdf_path.name}: pairs={total_pairs} incomplete_pairs={incomplete_pairs} "
                f"missing_duration={missing_duration} duration_mismatch={duration_mismatch}"
            )

        report_data = _load_report(report_path)
        is_ok = None
        totals = {}
        validation = {}
        if report_data is not None:
            totals = report_data.get("totals", {}) or {}
            validation = report_data.get("validation", {}) or {}
            is_ok = validation.get("is_ok") if isinstance(validation.get("is_ok"), bool) else None
        if is_ok is False:
            validation_failures.append(pdf_path.name)
            if diagnose_overcount and totals and validation:
                diff = validation.get("ore_lavorate_diff")
                row_sum = validation.get("ore_lavorate_pair_sum")
                total = validation.get("ore_lavorate_total")
                LOGGER.info(
                    "%s overcount %s: row_sum=%s total=%s diff=%s",
                    name,
                    pdf_path.name,
                    row_sum,
                    total,
                    diff,
                )
                for line in _diagnose_overcount(records, totals, max_lines):
                    LOGGER.info("  %s", line)

        detect_info = analyze_detection(text)
        detected_family = detect_info.get("detected_family")
        if name == "cartellino" and detected_family not in {"cartellino"}:
            detection_issues.append(f"{pdf_path.name}: detected={detected_family}")
        if name != "cartellino" and detected_family not in {"timbrature"}:
            detection_issues.append(f"{pdf_path.name}: detected={detected_family}")

    LOGGER.info(
        "%s: total=%d sampled=%d missing_outputs=%d mismatched_days=%d validation_fail=%d missing_text=%d detection_issues=%d pair_issues=%d",
        name,
        len(pdfs),
        len(sample),
        len(missing_outputs),
        len(day_mismatches),
        len(validation_failures),
        len(missing_text),
        len(detection_issues),
        len(pair_issues),
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
    _emit(f"{name} pair issues", pair_issues)

    return (
        len(missing_outputs)
        + len(day_mismatches)
        + len(validation_failures)
        + len(pair_issues)
    )


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
    parser.add_argument(
        "--diagnose-overcount",
        action="store_true",
        help="Print top day lines contributing to ore_lavorate overcount",
    )
    parser.add_argument(
        "--max-lines",
        type=int,
        default=8,
        help="Max lines to print per file when diagnosing overcount (default: 8)",
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
            args.diagnose_overcount,
            args.max_lines,
        )

    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
