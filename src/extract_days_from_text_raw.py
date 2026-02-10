from __future__ import annotations

"""Build days.csv files from extracted raw text documents."""

import argparse
import csv
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
 
from drive_service.fs_utils import ensure_parent_dir
from drive_service.logging_utils import setup_logging
from raw_text_parsing import (
    DAY_PREFIX_RE,
    QTA_RE,
    detect_doc_format,
    line_has_event,
    normalize_text,
    parse_day_header,
    resolve_year_month,
)
 
logger = logging.getLogger(__name__)

DEFAULT_INPUT_DIR = "output/text_extracted"
DEFAULT_OUT_DIR = "output/parsed_from_text"
DEFAULT_OUT_NAME = "days.csv"
DEFAULT_REPORT_JSON = "output/parsed_from_text/extract_days_from_text_raw.report.json"
DEFAULT_TEXT_GLOB = "*.txt"
DEFAULT_MAX_NO_DAYS_FILES = 80
DEFAULT_MAX_NO_DAYS_LINES = 8

NUMBER_RE = re.compile(r"^[+-]?\d+(?:[.,]\d+)?$")
HHMM_RE = re.compile(r"^[+-]?\d{1,3}:\d{2}$")


@dataclass(frozen=True)
class ParsedRow:
    day: int
    dow: str
    raw: str
    line_no: int
    has_event: bool
    mo_f: float | None
    mo_t: float | None
    mo_lav: float | None


def _format_float(value: float | None) -> str:
    if value is None:
        return ""
    text = f"{value:.6f}".rstrip("0").rstrip(".")
    return text or "0"


def _parse_hhmm(token: str) -> float | None:
    clean = token.strip()
    sign = -1.0 if clean.startswith("-") else 1.0
    clean = clean.lstrip("+-")
    if not HHMM_RE.fullmatch(clean):
        return None
    hour_s, minute_s = clean.split(":")
    hour = int(hour_s)
    minute = int(minute_s)
    if not (0 <= minute <= 59):
        return None
    return sign * (hour + minute / 60.0)


def _parse_decimal(token: str) -> float | None:
    clean = token.strip()
    if not NUMBER_RE.fullmatch(clean):
        return None
    normalized = clean.replace(",", ".")
    value = float(normalized)
    if "." not in normalized:
        return value

    sign = -1.0 if value < 0 else 1.0
    abs_value = abs(value)
    hours = int(abs_value)
    minutes = int(round((abs_value - hours) * 100))
    if 0 <= minutes <= 59:
        return sign * (hours + minutes / 60.0)
    return value


def _parse_numeric_token(token: str, *, allow_hhmm: bool) -> float | None:
    clean = token.strip().strip("|,;")
    if not clean:
        return None
    if allow_hhmm:
        hhmm = _parse_hhmm(clean)
        if hhmm is not None:
            return hhmm
    return _parse_decimal(clean)


def _extract_leading_values(value_text: str, *, allow_hhmm: bool) -> list[float]:
    values: list[float] = []
    for token in value_text.split():
        parsed = _parse_numeric_token(token, allow_hhmm=allow_hhmm)
        if parsed is not None:
            values.append(parsed)
    return values


def _extract_trailing_values(value_text: str, *, allow_hhmm: bool) -> list[float]:
    tokens = value_text.split()
    out_rev: list[float] = []
    collecting = False
    for raw_token in reversed(tokens):
        parsed = _parse_numeric_token(raw_token, allow_hhmm=allow_hhmm)
        if parsed is None:
            if collecting:
                break
            continue
        collecting = True
        out_rev.append(parsed)
    out_rev.reverse()
    return out_rev


def _assign_cartellino(values: list[float]) -> tuple[float | None, float | None, float | None]:
    if len(values) >= 3:
        return values[-3], values[-2], values[-1]
    if len(values) == 2:
        return values[0], values[1], values[1]
    if len(values) == 1:
        return values[0], values[0], values[0]
    return None, None, None


def _assign_timbrature(
    values: list[float],
    *,
    has_event: bool,
    any_event: bool,
) -> tuple[float | None, float | None, float | None]:
    contratt: float | None = None
    lavorato: float | None = None

    if values:
        if not has_event:
            contratt = values[0]
            lavorato = values[1] if len(values) >= 2 else values[0]
        elif len(values) >= 2:
            contratt = values[0]
            lavorato = values[1]
        else:
            lavorato = values[0]

    mo_f = 0.0 if contratt is None else contratt
    mo_t = 0.0 if lavorato is None else lavorato
    mo_lav = mo_t
    if any_event and not has_event:
        mo_lav = 0.0
    return mo_f, mo_t, mo_lav


def _assign_situazione(
    values: list[float],
    *,
    has_event: bool,
    any_event: bool,
) -> tuple[float | None, float | None, float | None]:
    # Expected order: dovuto, contrattuale, lavorato, saldo, straordinario.
    contratt: float | None = None
    lavorato: float | None = None
    if len(values) >= 3:
        contratt = values[1] if len(values) >= 4 else values[0]
        lavorato = values[2]
    elif len(values) == 2:
        contratt, lavorato = values[0], values[1]
    elif len(values) == 1:
        contratt, lavorato = values[0], values[0]

    mo_f = 0.0 if contratt is None else contratt
    mo_t = 0.0 if lavorato is None else lavorato
    mo_lav = mo_t
    if any_event and not has_event:
        mo_lav = 0.0
    return mo_f, mo_t, mo_lav


def _parse_rows_for_file(text: str, *, doc_format: str) -> tuple[list[ParsedRow], dict[str, Any]]:
    lines = text.splitlines()
    candidates: list[tuple[int, str, bool]] = []
    for idx, line in enumerate(lines, start=1):
        raw = line.strip()
        if not raw:
            continue
        header = parse_day_header(raw)
        if header is None:
            continue
        has_event = line_has_event(raw)
        candidates.append((idx, raw, has_event))

    any_event = any(has_event for _, _, has_event in candidates)
    rows: list[ParsedRow] = []

    for line_no, raw, has_event in candidates:
        header = parse_day_header(raw)
        if header is None:
            continue
        day, dow = header
        norm = normalize_text(raw)

        if doc_format == "situazione_mensile":
            values = _extract_trailing_values(norm, allow_hhmm=True)
            mo_f, mo_t, mo_lav = _assign_situazione(
                values,
                has_event=has_event,
                any_event=any_event,
            )
        elif doc_format == "timbrature_web":
            rest = DAY_PREFIX_RE.sub("", norm, count=1)
            rest = QTA_RE.sub("", rest)
            values = _extract_leading_values(rest, allow_hhmm=False)
            mo_f, mo_t, mo_lav = _assign_timbrature(
                values,
                has_event=has_event,
                any_event=any_event,
            )
        elif doc_format == "cartellino_classic":
            values = _extract_trailing_values(norm, allow_hhmm=False)
            mo_f, mo_t, mo_lav = _assign_cartellino(values)
        else:
            rest = DAY_PREFIX_RE.sub("", norm, count=1)
            rest = QTA_RE.sub("", rest)
            values = _extract_leading_values(rest, allow_hhmm=False)
            mo_f, mo_t, mo_lav = _assign_timbrature(
                values,
                has_event=has_event,
                any_event=any_event,
            )

        rows.append(
            ParsedRow(
                day=day,
                dow=dow,
                raw=raw,
                line_no=line_no,
                has_event=has_event,
                mo_f=mo_f,
                mo_t=mo_t,
                mo_lav=mo_lav,
            )
        )

    deduped: list[ParsedRow] = []
    seen: set[tuple[int, str, str]] = set()
    for row in rows:
        key = (row.day, row.dow, row.raw)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    deduped.sort(key=lambda item: (item.day, item.line_no))

    stats = {
        "lines_total": len(lines),
        "candidate_day_lines": len(candidates),
        "rows_parsed": len(deduped),
        "rows_with_event": sum(1 for item in deduped if item.has_event),
        "rows_without_event": sum(1 for item in deduped if not item.has_event),
    }
    return deduped, stats


def _write_days_csv(
    rows: list[ParsedRow],
    out_csv: Path,
    *,
    year: int | None,
    month: int | None,
) -> None:
    ensure_parent_dir(str(out_csv))
    columns = ["year", "month", "day", "dow", "mo_f", "mo_t", "mo_lav", "raw"]
    with open(out_csv, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "year": "" if year is None else year,
                    "month": "" if month is None else month,
                    "day": row.day,
                    "dow": row.dow,
                    "mo_f": _format_float(row.mo_f),
                    "mo_t": _format_float(row.mo_t),
                    "mo_lav": _format_float(row.mo_lav),
                    "raw": row.raw,
                }
            )


def build_days_from_text_dir(
    *,
    input_dir: str = DEFAULT_INPUT_DIR,
    out_dir: str = DEFAULT_OUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    report_json: str = DEFAULT_REPORT_JSON,
    text_glob: str = DEFAULT_TEXT_GLOB,
    max_no_days_files: int = DEFAULT_MAX_NO_DAYS_FILES,
    max_no_days_lines: int = DEFAULT_MAX_NO_DAYS_LINES,
) -> dict[str, Any]:
    input_base = Path(input_dir)
    out_base = Path(out_dir)
    text_files = sorted(input_base.rglob(text_glob))

    totals: dict[str, Any] = {
        "files_total": len(text_files),
        "files_processed": 0,
        "files_error": 0,
        "files_with_days": 0,
        "files_without_days": 0,
        "rows_total": 0,
        "rows_with_event": 0,
        "rows_without_event": 0,
        "input_dir": os.path.abspath(input_dir),
        "out_dir": os.path.abspath(out_dir),
        "text_glob": text_glob,
        "out_name": out_name,
    }
    format_counts: dict[str, int] = {}
    files_without_days: list[dict[str, Any]] = []
    file_errors: list[dict[str, str]] = []

    for index, txt_path in enumerate(text_files, start=1):
        try:
            text = txt_path.read_text(encoding="utf-8", errors="replace")
            doc_format = detect_doc_format(text)
            format_counts[doc_format] = format_counts.get(doc_format, 0) + 1
            year, month = resolve_year_month(text, txt_path)
            rows, file_stats = _parse_rows_for_file(text, doc_format=doc_format)

            rel = txt_path.relative_to(input_base)
            out_csv = out_base / rel.with_suffix("") / out_name
            _write_days_csv(rows, out_csv, year=year, month=month)

            totals["files_processed"] += 1
            totals["rows_total"] += int(file_stats["rows_parsed"])
            totals["rows_with_event"] += int(file_stats["rows_with_event"])
            totals["rows_without_event"] += int(file_stats["rows_without_event"])

            if rows:
                totals["files_with_days"] += 1
            else:
                totals["files_without_days"] += 1
                if len(files_without_days) < max_no_days_files:
                    samples: list[str] = []
                    if max_no_days_lines > 0:
                        for line in text.splitlines():
                            candidate = line.strip()
                            if not candidate:
                                continue
                            if len(samples) >= max_no_days_lines:
                                break
                            samples.append(candidate)
                    files_without_days.append(
                        {
                            "source_txt": str(txt_path),
                            "output_days_csv": str(out_csv),
                            "doc_format": doc_format,
                            "year": year,
                            "month": month,
                            "candidate_day_lines": int(file_stats["candidate_day_lines"]),
                            "sample_lines": samples,
                        }
                    )

            if index % 500 == 0:
                logger.info(
                    "Processati %s/%s file txt (rows=%s)",
                    index,
                    len(text_files),
                    totals["rows_total"],
                )
        except Exception as exc:
            totals["files_error"] += 1
            file_errors.append({"source_txt": str(txt_path), "error": f"{type(exc).__name__}: {exc}"})
            logger.exception("Errore elaborando %s", txt_path)

    report = {
        "stats": totals,
        "format_counts": format_counts,
        "files_without_days": files_without_days,
        "file_errors": file_errors,
    }
    ensure_parent_dir(report_json)
    with open(report_json, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Parse extracted .txt documents and generate per-file days.csv outputs."
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help="Root folder containing extracted text files (default: output/text_extracted)",
    )
    parser.add_argument(
        "--out-dir",
        default=DEFAULT_OUT_DIR,
        help="Root folder where parsed per-file days.csv will be written (default: output/parsed_from_text)",
    )
    parser.add_argument(
        "--text-glob",
        default=DEFAULT_TEXT_GLOB,
        help="Glob pattern used when searching text files recursively (default: *.txt)",
    )
    parser.add_argument(
        "--out-name",
        default=DEFAULT_OUT_NAME,
        help="Output csv filename written per source file (default: days.csv)",
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help=(
            "Path of final JSON report "
            "(default: output/parsed_from_text/extract_days_from_text_raw.report.json)"
        ),
    )
    parser.add_argument(
        "--max-no-days-files",
        type=int,
        default=DEFAULT_MAX_NO_DAYS_FILES,
        help="Maximum number of no-days files kept in report examples (default: 80)",
    )
    parser.add_argument(
        "--max-no-days-lines",
        type=int,
        default=DEFAULT_MAX_NO_DAYS_LINES,
        help="Maximum sample lines per no-days file in report (default: 8)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose)
    report = build_days_from_text_dir(
        input_dir=args.input_dir,
        out_dir=args.out_dir,
        out_name=args.out_name,
        report_json=args.report_json,
        text_glob=args.text_glob,
        max_no_days_files=max(0, int(args.max_no_days_files)),
        max_no_days_lines=max(0, int(args.max_no_days_lines)),
    )
    stats = report["stats"]
    logger.info(
        (
            "Completato: files=%s processati=%s errori=%s "
            "files_with_days=%s rows=%s rows_with_event=%s"
        ),
        stats["files_total"],
        stats["files_processed"],
        stats["files_error"],
        stats["files_with_days"],
        stats["rows_total"],
        stats["rows_with_event"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
