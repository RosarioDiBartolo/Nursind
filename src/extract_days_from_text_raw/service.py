from __future__ import annotations

import csv
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from src.event_hints_slots import hint_columns, serialize_hint_slots
from src.drive_service.fs_utils import ensure_parent_dir
from src.raw_text_parsing import (
    line_has_event,
    parse_day_header,
    resolve_year_month,
)

from .parsers import ParseContext, resolve_parser
from .parsers.base import BaseFormatParser, EventHint
from .parsers.common import normalized_raw
from .options import (
    DEFAULT_MAX_NO_DAYS_FILES,
    DEFAULT_MAX_NO_DAYS_LINES,
    DEFAULT_OUT_DIR,
    DEFAULT_OUT_NAME,
    DEFAULT_TEXT_GLOB,
)

logger = logging.getLogger(__name__)


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
    parser_id: str
    event_hints: tuple[EventHint, ...]


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


def _parse_rows_for_file(
    text: str,
    *,
    parser: BaseFormatParser,
) -> tuple[list[ParsedRow], dict[str, Any]]:
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
        values = parser.parse_row(
            raw,
            has_event=has_event,
            any_event=any_event,
            ctx=ParseContext(normalized_raw=normalized_raw(raw)),
        )

        rows.append(
            ParsedRow(
                day=day,
                dow=dow,
                raw=raw,
                line_no=line_no,
                has_event=has_event,
                mo_f=values.values.mo_f,
                mo_t=values.values.mo_t,
                mo_lav=values.values.mo_lav,
                parser_id=parser.parser_id,
                event_hints=values.event_hints,
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
    columns = [
        "year",
        "month",
        "day",
        "dow",
        "mo_f",
        "mo_t",
        "mo_lav",
        "raw",
        *hint_columns(),
    ]
    with open(out_csv, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            payload = {
                "year": "" if year is None else year,
                "month": "" if month is None else month,
                "day": row.day,
                "dow": row.dow,
                "mo_f": _format_float(row.mo_f),
                "mo_t": _format_float(row.mo_t),
                "mo_lav": _format_float(row.mo_lav),
                "raw": row.raw,
            }
            payload.update(
                serialize_hint_slots(
                    parser_id=row.parser_id,
                    event_hints=row.event_hints,
                )
            )
            writer.writerow(payload)


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
        parser = resolve_parser(text)
        base_result["doc_format"] = parser.legacy_doc_format
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

        rows, file_stats = _parse_rows_for_file(text, parser=parser)
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
