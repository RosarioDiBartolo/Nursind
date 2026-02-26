from __future__ import annotations

import csv
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from src.drive_service.fs_utils import ensure_parent_dir
from src.raw_text_parsing import (
    DAY_PREFIX_RE,
    QTA_RE,
    detect_doc_format,
    line_has_event,
    normalize_text,
    parse_day_header,
    resolve_year_month,
)

from .options import (
    DEFAULT_MAX_NO_DAYS_FILES,
    DEFAULT_MAX_NO_DAYS_LINES,
    DEFAULT_OUT_DIR,
    DEFAULT_OUT_NAME,
    DEFAULT_TEXT_GLOB,
)

logger = logging.getLogger(__name__)

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


def _header_preview(text: str, *, max_lines: int = 3, max_chars: int = 240) -> str:
    lines: list[str] = []
    for raw in text.splitlines():
        clean = raw.strip()
        if not clean:
            continue
        lines.append(clean)
        if len(lines) >= max_lines:
            break
    preview = " | ".join(lines)
    if len(preview) > max_chars:
        return f"{preview[:max_chars].rstrip()}..."
    return preview


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


def _build_days_output_path(
    txt_path: Path,
    *,
    input_base: Path | None,
    out_base: Path,
    out_name: str,
) -> Path:
    suffix = out_name.strip() or "days.csv"
    if input_base is None:
        rel = Path(txt_path.name)
    else:
        try:
            rel = txt_path.relative_to(input_base)
        except ValueError:
            rel = Path(txt_path.name)
    return out_base / rel.with_suffix(f".{suffix}")


def process_one_text_file(
    txt_path: str | Path,
    *,
    out_dir: str = DEFAULT_OUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    input_base: str | Path | None = None,
    max_no_days_lines: int = DEFAULT_MAX_NO_DAYS_LINES,
) -> dict[str, Any]:
    source_path = Path(txt_path)
    out_base = Path(out_dir)
    input_base_path = Path(input_base) if input_base is not None else None

    base_result: dict[str, Any] = {
        "status": "error",
        "source_txt": str(source_path),
        "output_days_csv": None,
        "doc_format": None,
        "year": None,
        "month": None,
        "candidate_day_lines": 0,
        "rows_parsed": 0,
        "rows_with_event": 0,
        "rows_without_event": 0,
        "sample_lines": [],
        "error_code": None,
        "error": None,
        "header_preview": None,
    }

    try:
        text = source_path.read_text(encoding="utf-8", errors="replace")
        doc_format = detect_doc_format(text)
        base_result["doc_format"] = doc_format
        year, month = resolve_year_month(text, source_path)
        base_result["year"] = year
        base_result["month"] = month
        if year is None or month is None:
            base_result["error_code"] = "missing_year_month"
            base_result["error"] = (
                "MISSING_YEAR_MONTH: unable to resolve month/year from text and filename"
            )
            base_result["header_preview"] = _header_preview(text)
            return base_result

        rows, file_stats = _parse_rows_for_file(text, doc_format=doc_format)
        out_csv = _build_days_output_path(
            source_path,
            input_base=input_base_path,
            out_base=out_base,
            out_name=out_name,
        )
        _write_days_csv(rows, out_csv, year=year, month=month)

        base_result["status"] = "ok"
        base_result["output_days_csv"] = str(out_csv)
        base_result["candidate_day_lines"] = int(file_stats["candidate_day_lines"])
        base_result["rows_parsed"] = int(file_stats["rows_parsed"])
        base_result["rows_with_event"] = int(file_stats["rows_with_event"])
        base_result["rows_without_event"] = int(file_stats["rows_without_event"])

        if not rows and max_no_days_lines > 0:
            samples: list[str] = []
            for line in text.splitlines():
                candidate = line.strip()
                if not candidate:
                    continue
                if len(samples) >= max_no_days_lines:
                    break
                samples.append(candidate)
            base_result["sample_lines"] = samples

        return base_result
    except Exception as exc:
        base_result["error_code"] = "processing_error"
        base_result["error"] = f"{type(exc).__name__}: {exc}"
        return base_result


def process_many_text_files(
    text_files: Iterable[str | Path],
    *,
    out_dir: str = DEFAULT_OUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    input_base: str | Path | None = None,
    text_glob: str = DEFAULT_TEXT_GLOB,
    input_dir: str | None = None,
    max_no_days_files: int = DEFAULT_MAX_NO_DAYS_FILES,
    max_no_days_lines: int = DEFAULT_MAX_NO_DAYS_LINES,
) -> dict[str, Any]:
    normalized_input_base = Path(input_base) if input_base is not None else None
    normalized_files = sorted(Path(path) for path in text_files)

    resolved_input_dir = input_dir
    if resolved_input_dir is None and normalized_input_base is not None:
        resolved_input_dir = str(normalized_input_base)

    totals: dict[str, Any] = {
        "files_total": len(normalized_files),
        "files_processed": 0,
        "files_error": 0,
        "files_missing_year_month": 0,
        "files_with_days": 0,
        "files_without_days": 0,
        "rows_total": 0,
        "rows_with_event": 0,
        "rows_without_event": 0,
        "input_dir": os.path.abspath(resolved_input_dir) if resolved_input_dir else None,
        "out_dir": os.path.abspath(out_dir),
        "text_glob": text_glob,
        "out_name": out_name,
    }
    format_counts: dict[str, int] = {}
    files_without_days: list[dict[str, Any]] = []
    files_missing_year_month: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    items: list[dict[str, Any]] = []

    for index, txt_path in enumerate(normalized_files, start=1):
        result = process_one_text_file(
            txt_path,
            out_dir=out_dir,
            out_name=out_name,
            input_base=normalized_input_base,
            max_no_days_lines=max_no_days_lines,
        )
        items.append(result)

        doc_format = result.get("doc_format")
        if isinstance(doc_format, str) and doc_format:
            format_counts[doc_format] = format_counts.get(doc_format, 0) + 1

        if result["status"] != "ok":
            totals["files_error"] += 1
            if result.get("error_code") == "missing_year_month":
                totals["files_missing_year_month"] += 1
                files_missing_year_month.append(
                    {
                        "source_txt": result["source_txt"],
                        "doc_format": result["doc_format"],
                        "header_preview": result.get("header_preview"),
                    }
                )
            errors.append(
                {
                    "source_txt": str(result["source_txt"]),
                    "error": str(result["error"]),
                }
            )
            logger.error("Errore elaborando %s: %s", result["source_txt"], result["error"])
            continue

        totals["files_processed"] += 1
        totals["rows_total"] += int(result["rows_parsed"])
        totals["rows_with_event"] += int(result["rows_with_event"])
        totals["rows_without_event"] += int(result["rows_without_event"])

        if int(result["rows_parsed"]) > 0:
            totals["files_with_days"] += 1
        else:
            totals["files_without_days"] += 1
            if len(files_without_days) < max_no_days_files:
                files_without_days.append(
                    {
                        "source_txt": result["source_txt"],
                        "output_days_csv": result["output_days_csv"],
                        "doc_format": result["doc_format"],
                        "year": result["year"],
                        "month": result["month"],
                        "candidate_day_lines": result["candidate_day_lines"],
                        "sample_lines": list(result.get("sample_lines") or []),
                    }
                )

        if index % 500 == 0:
            logger.info(
                "Processati %s/%s file txt (rows=%s)",
                index,
                len(normalized_files),
                totals["rows_total"],
            )

    return {
        "stats": totals,
        "items": items,
        "errors": errors,
        "format_counts": format_counts,
        "files_missing_year_month": files_missing_year_month,
        "files_without_days": files_without_days,
        "file_errors": errors,
    }
