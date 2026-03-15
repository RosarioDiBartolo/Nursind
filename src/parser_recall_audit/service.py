from __future__ import annotations

import csv
import logging
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from src.drive_service.io_json import load_json
from src.drive_service.text_extraction_csv import find_text_extraction_csvs, read_text_extraction_rows
from src.extract_events_from_documents.page_analysis import resolve_page_texts
from src.extract_events_from_documents.writers import write_rows_csv
from src.raw_text_parsing import line_has_event, normalize_text
from src.reporting import build_stage_report, compact_stage_report, resolve_output_path, write_json_report

from .options import (
    ParserRecallAuditOptions,
    default_report_json_path,
    default_root_dir,
    default_suspicious_csv_path,
)

logger = logging.getLogger(__name__)

ABSENCE_KEYWORDS: tuple[str, ...] = (
    "ferie",
    "facolt",
    "interdizione",
    "malatt",
    "conged",
    "matern",
    "allatt",
    "permess",
    "aspett",
    "infort",
    "assenza",
    "riposo",
    "rec",
    "rrr",
)
TIME_TOKEN_RE = re.compile(r"\b(?:[01]?\d|2[0-4])[:.,][0-5]\d\b")

SUSPICIOUS_PAGE_COLUMNS = [
    "pipeline",
    "issue_bucket",
    "suspicion_score",
    "likely_legitimate_no_events",
    "source_file_link",
    "source_drive_path",
    "source_file_name",
    "source_file_id",
    "source_doc_json",
    "page_ref",
    "page_no",
    "page_year",
    "page_month",
    "parser_id",
    "decision",
    "decision_reason",
    "rows_considered",
    "rows_with_events",
    "rows_without_events",
    "events_extracted",
    "events_dropped_missing_year_month",
    "coverage_ratio_page",
    "neighbor_avg_coverage",
    "prev_coverage",
    "next_coverage",
    "zero_run_length",
    "file_pages_total",
    "file_avg_coverage",
    "file_zero_event_pages",
    "file_low_coverage_pages",
    "absence_keyword_hits",
    "absence_keywords_found",
    "time_token_count",
    "time_line_count",
    "event_candidate_line_count",
    "page_text_found",
    "header_preview",
    "detail",
]


def _parse_bool(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _parse_int(value: object) -> int:
    try:
        return int(str(value or "").strip())
    except Exception:
        return 0


def _parse_float(value: object) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _resolve_output_root(root_dir: str | Path) -> Path:
    return Path(root_dir).resolve()


def _discover_pipeline_dirs(root_dir: str | Path) -> list[Path]:
    root = _resolve_output_root(root_dir)
    discovered: list[Path] = []
    seen: set[Path] = set()

    def register(candidate: Path) -> None:
        resolved = candidate.resolve()
        if resolved in seen:
            return
        if (resolved / "events" / "pages.csv").exists():
            seen.add(resolved)
            discovered.append(resolved)

    register(root)
    if root.exists():
        for child in sorted(root.iterdir(), key=lambda item: item.name.lower()):
            if child.is_dir():
                register(child)
    return discovered


def _read_pages_csv(path: Path, *, pipeline_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for raw_row in csv.DictReader(handle):
            row = dict(raw_row)
            row["pipeline"] = pipeline_name
            row["page_no"] = _parse_int(row.get("page_no"))
            row["page_year"] = _parse_int(row.get("page_year")) or None
            row["page_month"] = _parse_int(row.get("page_month")) or None
            row["rows_considered"] = _parse_int(row.get("rows_considered"))
            row["rows_with_events"] = _parse_int(row.get("rows_with_events"))
            row["rows_without_events"] = _parse_int(row.get("rows_without_events"))
            row["events_extracted"] = _parse_int(row.get("events_extracted"))
            row["events_dropped_missing_year_month"] = _parse_int(
                row.get("events_dropped_missing_year_month")
            )
            row["relevant_for_coverage"] = _parse_bool(row.get("relevant_for_coverage"))
            coverage_ratio_page = _parse_float(row.get("coverage_ratio_page"))
            if coverage_ratio_page is None and row["rows_considered"] > 0:
                coverage_ratio_page = round(
                    row["rows_with_events"] / row["rows_considered"],
                    6,
                )
            row["coverage_ratio_page"] = coverage_ratio_page
            rows.append(row)
    return rows


def _build_manifest_lookup(pipeline_dir: Path) -> dict[str, dict[str, dict[str, str]]]:
    manifest_rows = read_text_extraction_rows(
        find_text_extraction_csvs(pipeline_dir / "documents"),
        hydrate_text=False,
    )
    by_file_id: dict[str, dict[str, str]] = {}
    by_doc_json: dict[str, dict[str, str]] = {}
    by_doc_name: dict[str, dict[str, str]] = {}
    for row in manifest_rows:
        file_id = str(row.get("file_id") or "").strip()
        doc_json = str(row.get("doc_json") or "").strip()
        if file_id:
            by_file_id[file_id] = row
        if doc_json:
            by_doc_json[doc_json] = row
            by_doc_name[Path(doc_json).name] = row
    return {
        "by_file_id": by_file_id,
        "by_doc_json": by_doc_json,
        "by_doc_name": by_doc_name,
    }


def _backfill_page_row_from_manifest(
    row: dict[str, Any],
    *,
    manifest_lookup: dict[str, dict[str, dict[str, str]]],
) -> None:
    manifest_row = None
    source_file_id = str(row.get("source_file_id") or "").strip()
    source_doc_json = str(row.get("source_doc_json") or "").strip()
    if source_file_id:
        manifest_row = manifest_lookup["by_file_id"].get(source_file_id)
    if manifest_row is None and source_doc_json:
        manifest_row = manifest_lookup["by_doc_json"].get(source_doc_json)
    if manifest_row is None and source_doc_json:
        manifest_row = manifest_lookup["by_doc_name"].get(Path(source_doc_json).name)
    if manifest_row is None:
        return

    for source_key, manifest_key in (
        ("source_file_link", "file_link"),
        ("source_drive_path", "drive_path"),
        ("source_file_name", "file_name"),
        ("source_employee", "employee"),
        ("source_file_id", "file_id"),
    ):
        if str(row.get(source_key) or "").strip():
            continue
        resolved = str(manifest_row.get(manifest_key) or "").strip()
        if resolved:
            row[source_key] = resolved


def _file_key(row: dict[str, Any]) -> tuple[str, str]:
    token = (
        str(row.get("source_doc_json") or "").strip()
        or str(row.get("source_file_id") or "").strip()
        or str(row.get("source_file_name") or "").strip()
        or str(row.get("page_ref") or "").strip()
    )
    return str(row.get("pipeline") or ""), token


def _is_large_zero_event_page(row: dict[str, Any], *, min_large_rows: int) -> bool:
    return bool(
        row.get("relevant_for_coverage")
        and int(row.get("rows_considered") or 0) >= min_large_rows
        and int(row.get("events_extracted") or 0) == 0
    )


def _enrich_file_context(
    rows: list[dict[str, Any]],
    *,
    min_large_rows: int,
    low_coverage_threshold: float,
) -> None:
    file_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("relevant_for_coverage"):
            file_groups[_file_key(row)].append(row)

    for group in file_groups.values():
        group.sort(key=lambda item: int(item.get("page_no") or 0))
        coverage_values = [
            float(item["coverage_ratio_page"])
            for item in group
            if item.get("coverage_ratio_page") is not None
        ]
        file_avg_coverage = (
            round(sum(coverage_values) / len(coverage_values), 6) if coverage_values else None
        )
        file_zero_event_pages = sum(
            1 for item in group if _is_large_zero_event_page(item, min_large_rows=min_large_rows)
        )
        file_low_coverage_pages = sum(
            1
            for item in group
            if int(item.get("rows_considered") or 0) >= min_large_rows
            and item.get("coverage_ratio_page") is not None
            and float(item["coverage_ratio_page"]) < low_coverage_threshold
        )

        idx = 0
        while idx < len(group):
            row = group[idx]
            if not _is_large_zero_event_page(row, min_large_rows=min_large_rows):
                row["zero_run_length"] = 0
                idx += 1
                continue
            start = idx
            while idx < len(group) and _is_large_zero_event_page(
                group[idx], min_large_rows=min_large_rows
            ):
                idx += 1
            run_length = idx - start
            for run_idx in range(start, idx):
                group[run_idx]["zero_run_length"] = run_length

        for index, row in enumerate(group):
            prev_coverage = next(
                (
                    float(group[candidate]["coverage_ratio_page"])
                    for candidate in range(index - 1, -1, -1)
                    if group[candidate].get("coverage_ratio_page") is not None
                ),
                None,
            )
            next_coverage = next(
                (
                    float(group[candidate]["coverage_ratio_page"])
                    for candidate in range(index + 1, len(group))
                    if group[candidate].get("coverage_ratio_page") is not None
                ),
                None,
            )
            neighbor_values = [
                value for value in (prev_coverage, next_coverage) if value is not None
            ]
            neighbor_avg_coverage = (
                round(sum(neighbor_values) / len(neighbor_values), 6)
                if neighbor_values
                else None
            )
            row["prev_coverage"] = prev_coverage
            row["next_coverage"] = next_coverage
            row["neighbor_avg_coverage"] = neighbor_avg_coverage
            row["file_pages_total"] = len(group)
            row["file_avg_coverage"] = file_avg_coverage
            row["file_zero_event_pages"] = file_zero_event_pages
            row["file_low_coverage_pages"] = file_low_coverage_pages
            row.setdefault("zero_run_length", 0)


def _resolve_doc_json_path(
    *,
    pipeline_dir: Path,
    source_doc_json: str | None,
) -> Path | None:
    raw = str(source_doc_json or "").strip()
    if not raw:
        return None

    raw_path = Path(raw)
    candidates = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        candidates.extend(
            [
                (Path.cwd() / raw_path).resolve(),
                (pipeline_dir / raw_path).resolve(),
                (pipeline_dir / "documents" / "docs" / raw_path.name).resolve(),
            ]
        )

    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.exists():
            return resolved
    return None


def _load_page_text(
    *,
    pipeline_dir: Path,
    source_doc_json: str | None,
    page_no: int,
    doc_cache: dict[Path, dict[int, str] | None],
) -> str:
    doc_path = _resolve_doc_json_path(pipeline_dir=pipeline_dir, source_doc_json=source_doc_json)
    if doc_path is None:
        return ""
    if doc_path not in doc_cache:
        try:
            payload = load_json(str(doc_path))
        except Exception:
            logger.exception("Failed loading document JSON from %s", doc_path)
            doc_cache[doc_path] = None
        else:
            if isinstance(payload, dict):
                doc_cache[doc_path] = resolve_page_texts(payload)
            else:
                doc_cache[doc_path] = None
    page_texts = doc_cache.get(doc_path) or {}
    return str(page_texts.get(page_no) or "")


def _page_signal_metrics(page_text: str) -> tuple[int, str, int, int, int]:
    if not page_text.strip():
        return 0, "", 0, 0, 0

    keywords_found: Counter[str] = Counter()
    time_token_count = 0
    time_line_count = 0
    event_candidate_line_count = 0

    for raw_line in page_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        normalized = normalize_text(line)
        if not normalized:
            continue
        matched_keywords = [keyword for keyword in ABSENCE_KEYWORDS if keyword in normalized]
        for keyword in matched_keywords:
            keywords_found[keyword] += 1
        time_matches = TIME_TOKEN_RE.findall(line)
        time_token_count += len(time_matches)
        if time_matches:
            time_line_count += 1
        if line_has_event(line):
            event_candidate_line_count += 1

    return (
        sum(keywords_found.values()),
        ",".join(sorted(keywords_found)),
        time_token_count,
        time_line_count,
        event_candidate_line_count,
    )


def _issue_bucket(
    row: dict[str, Any],
    *,
    max_tiny_rows: int,
    min_large_rows: int,
    low_coverage_threshold: float,
) -> str | None:
    if (
        int(row.get("events_dropped_missing_year_month") or 0) > 0
        or str(row.get("decision_reason") or "").strip() == "missing_page_year_month"
    ):
        return "missing_year_month"
    if not row.get("relevant_for_coverage"):
        return None
    rows_considered = int(row.get("rows_considered") or 0)
    if rows_considered <= max_tiny_rows:
        return "tiny_page"
    if rows_considered >= min_large_rows and int(row.get("events_extracted") or 0) == 0:
        return "zero_event_page"
    coverage_ratio_page = row.get("coverage_ratio_page")
    if rows_considered >= min_large_rows and coverage_ratio_page is not None:
        if float(coverage_ratio_page) < low_coverage_threshold:
            return "low_coverage_page"
    return None


def _likely_legitimate_no_events(row: dict[str, Any]) -> bool:
    if row.get("issue_bucket") != "zero_event_page":
        return False
    absence_hits = int(row.get("absence_keyword_hits") or 0)
    zero_run_length = int(row.get("zero_run_length") or 0)
    return absence_hits >= 5 or zero_run_length >= 4


def _suspicion_score(row: dict[str, Any]) -> int:
    bucket = str(row.get("issue_bucket") or "")
    rows_considered = int(row.get("rows_considered") or 0)
    absence_hits = int(row.get("absence_keyword_hits") or 0)
    zero_run_length = int(row.get("zero_run_length") or 0)
    time_token_count = int(row.get("time_token_count") or 0)
    neighbor_avg_coverage = float(row.get("neighbor_avg_coverage") or 0.0)
    coverage_ratio_page = float(row.get("coverage_ratio_page") or 0.0)

    if bucket == "missing_year_month":
        score = 100 + min(rows_considered, 20)
    elif bucket == "tiny_page":
        score = 90 - min(rows_considered, 3) * 15 + round(neighbor_avg_coverage * 20)
        if time_token_count >= 2:
            score += 10
    elif bucket == "zero_event_page":
        score = 75 + min(rows_considered, 31) // 2 + round(neighbor_avg_coverage * 25)
        score -= min(absence_hits * 3, 45)
        if zero_run_length >= 4:
            score -= min((zero_run_length - 3) * 8, 32)
    elif bucket == "low_coverage_page":
        score = 60 + round((1.0 - coverage_ratio_page) * 50) + round(
            neighbor_avg_coverage * 20
        )
        score -= min(absence_hits * 2, 20)
    else:
        score = 0
    return max(0, min(100, int(score)))


def _build_detail(row: dict[str, Any]) -> str:
    bucket = str(row.get("issue_bucket") or "")
    rows_considered = int(row.get("rows_considered") or 0)
    rows_with_events = int(row.get("rows_with_events") or 0)
    events_extracted = int(row.get("events_extracted") or 0)
    coverage_ratio_page = row.get("coverage_ratio_page")
    neighbor_avg_coverage = row.get("neighbor_avg_coverage")
    absence_keywords_found = str(row.get("absence_keywords_found") or "")
    pieces: list[str] = []

    if bucket == "missing_year_month":
        pieces.append("Events were dropped because page year/month could not be resolved")
    elif bucket == "tiny_page":
        pieces.append(f"Tiny relevant page: rows_considered={rows_considered}")
    elif bucket == "zero_event_page":
        pieces.append(
            f"Large relevant page with zero extracted events: rows_considered={rows_considered}"
        )
        pieces.append(f"zero_run_length={int(row.get('zero_run_length') or 0)}")
    elif bucket == "low_coverage_page":
        pieces.append(
            "Low coverage page: "
            f"rows_with_events={rows_with_events}/{rows_considered} "
            f"(coverage_ratio_page={coverage_ratio_page})"
        )

    pieces.append(f"events_extracted={events_extracted}")
    if neighbor_avg_coverage is not None:
        pieces.append(f"neighbor_avg_coverage={neighbor_avg_coverage}")
    if absence_keywords_found:
        pieces.append(f"absence_keywords={absence_keywords_found}")
    if row.get("likely_legitimate_no_events"):
        pieces.append("likely_legitimate_no_events=true")
    return "; ".join(pieces)


def audit_parser_recall_root(
    root_dir: str | Path,
    *,
    max_tiny_rows: int,
    min_large_rows: int,
    low_coverage_threshold: float,
) -> dict[str, Any]:
    root = _resolve_output_root(root_dir)
    artifact_errors: list[dict[str, str]] = []
    pipeline_dirs = _discover_pipeline_dirs(root)
    all_rows: list[dict[str, Any]] = []

    for pipeline_dir in pipeline_dirs:
        pages_csv_path = pipeline_dir / "events" / "pages.csv"
        manifest_lookup: dict[str, dict[str, dict[str, str]]] = {
            "by_file_id": {},
            "by_doc_json": {},
            "by_doc_name": {},
        }
        try:
            manifest_lookup = _build_manifest_lookup(pipeline_dir)
        except Exception as exc:
            artifact_errors.append(
                {
                    "artifact": str(pipeline_dir / "documents"),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            logger.exception("Failed reading manifest CSVs from %s", pipeline_dir / "documents")
        try:
            pipeline_rows = _read_pages_csv(pages_csv_path, pipeline_name=pipeline_dir.name)
            for row in pipeline_rows:
                _backfill_page_row_from_manifest(row, manifest_lookup=manifest_lookup)
            all_rows.extend(pipeline_rows)
        except Exception as exc:
            artifact_errors.append(
                {
                    "artifact": str(pages_csv_path),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            logger.exception("Failed reading pages CSV from %s", pages_csv_path)

    _enrich_file_context(
        all_rows,
        min_large_rows=min_large_rows,
        low_coverage_threshold=low_coverage_threshold,
    )

    pipeline_dir_map = {item.name: item for item in pipeline_dirs}
    doc_cache: dict[Path, dict[int, str] | None] = {}
    suspicious_rows: list[dict[str, Any]] = []

    for row in all_rows:
        bucket = _issue_bucket(
            row,
            max_tiny_rows=max_tiny_rows,
            min_large_rows=min_large_rows,
            low_coverage_threshold=low_coverage_threshold,
        )
        if bucket is None:
            continue

        pipeline_name = str(row.get("pipeline") or "")
        row_pipeline_dir = pipeline_dir_map.get(pipeline_name)
        page_text = (
            _load_page_text(
                pipeline_dir=row_pipeline_dir,
                source_doc_json=str(row.get("source_doc_json") or ""),
                page_no=int(row.get("page_no") or 0),
                doc_cache=doc_cache,
            )
            if row_pipeline_dir is not None
            else ""
        )
        (
            absence_keyword_hits,
            absence_keywords_found,
            time_token_count,
            time_line_count,
            event_candidate_line_count,
        ) = _page_signal_metrics(page_text)

        suspicious_row = {
            "pipeline": pipeline_name,
            "issue_bucket": bucket,
            "source_file_link": row.get("source_file_link"),
            "source_drive_path": row.get("source_drive_path"),
            "source_file_name": row.get("source_file_name"),
            "source_file_id": row.get("source_file_id"),
            "source_doc_json": row.get("source_doc_json"),
            "page_ref": row.get("page_ref"),
            "page_no": row.get("page_no"),
            "page_year": row.get("page_year"),
            "page_month": row.get("page_month"),
            "parser_id": row.get("parser_id"),
            "decision": row.get("decision"),
            "decision_reason": row.get("decision_reason"),
            "rows_considered": row.get("rows_considered"),
            "rows_with_events": row.get("rows_with_events"),
            "rows_without_events": row.get("rows_without_events"),
            "events_extracted": row.get("events_extracted"),
            "events_dropped_missing_year_month": row.get(
                "events_dropped_missing_year_month"
            ),
            "coverage_ratio_page": row.get("coverage_ratio_page"),
            "neighbor_avg_coverage": row.get("neighbor_avg_coverage"),
            "prev_coverage": row.get("prev_coverage"),
            "next_coverage": row.get("next_coverage"),
            "zero_run_length": row.get("zero_run_length", 0),
            "file_pages_total": row.get("file_pages_total"),
            "file_avg_coverage": row.get("file_avg_coverage"),
            "file_zero_event_pages": row.get("file_zero_event_pages"),
            "file_low_coverage_pages": row.get("file_low_coverage_pages"),
            "absence_keyword_hits": absence_keyword_hits,
            "absence_keywords_found": absence_keywords_found,
            "time_token_count": time_token_count,
            "time_line_count": time_line_count,
            "event_candidate_line_count": event_candidate_line_count,
            "page_text_found": bool(page_text.strip()),
            "header_preview": row.get("header_preview"),
        }
        suspicious_row["likely_legitimate_no_events"] = _likely_legitimate_no_events(
            suspicious_row
        )
        suspicious_row["suspicion_score"] = _suspicion_score(suspicious_row)
        suspicious_row["detail"] = _build_detail(suspicious_row)
        suspicious_rows.append(suspicious_row)

    suspicious_rows.sort(
        key=lambda item: (
            -int(item.get("suspicion_score") or 0),
            -int(item.get("rows_considered") or 0),
            str(item.get("pipeline") or ""),
            str(item.get("source_file_name") or ""),
            int(item.get("page_no") or 0),
        )
    )

    counts_by_bucket = dict(
        sorted(Counter(str(row.get("issue_bucket") or "") for row in suspicious_rows).items())
    )
    counts_by_pipeline = dict(
        sorted(Counter(str(row.get("pipeline") or "") for row in suspicious_rows).items())
    )
    counts_by_parser = dict(
        sorted(Counter(str(row.get("parser_id") or "") for row in suspicious_rows).items())
    )

    stats = {
        "pipelines_total": len(pipeline_dirs),
        "pages_total": len(all_rows),
        "relevant_pages_total": sum(1 for row in all_rows if row.get("relevant_for_coverage")),
        "suspicious_pages_total": len(suspicious_rows),
        "likely_legitimate_no_events_total": sum(
            1 for row in suspicious_rows if row.get("likely_legitimate_no_events")
        ),
        "tiny_page_total": counts_by_bucket.get("tiny_page", 0),
        "zero_event_page_total": counts_by_bucket.get("zero_event_page", 0),
        "low_coverage_page_total": counts_by_bucket.get("low_coverage_page", 0),
        "missing_year_month_total": counts_by_bucket.get("missing_year_month", 0),
        "artifact_errors_total": len(artifact_errors),
        "max_tiny_rows": max_tiny_rows,
        "min_large_rows": min_large_rows,
        "low_coverage_threshold": low_coverage_threshold,
    }

    artifacts = {
        "root_dir": str(root),
        "pipeline_dirs": [str(path) for path in pipeline_dirs],
        "errors": artifact_errors,
    }

    return {
        "stats": stats,
        "artifacts": artifacts,
        "suspicious_rows": suspicious_rows,
        "counts_by_bucket": counts_by_bucket,
        "counts_by_pipeline": counts_by_pipeline,
        "counts_by_parser": counts_by_parser,
    }


def build_parser_recall_report(
    *,
    root_dir: str | None = None,
    report_json: str | None = None,
    suspicious_csv: str | None = None,
    max_tiny_rows: int,
    min_large_rows: int,
    low_coverage_threshold: float,
) -> dict[str, Any]:
    root_dir = root_dir or default_root_dir()
    report_json = report_json or default_report_json_path()
    suspicious_csv = suspicious_csv or default_suspicious_csv_path()
    audit = audit_parser_recall_root(
        root_dir,
        max_tiny_rows=max_tiny_rows,
        min_large_rows=min_large_rows,
        low_coverage_threshold=low_coverage_threshold,
    )

    report_path = resolve_output_path(root_dir, report_json)
    suspicious_csv_path = resolve_output_path(root_dir, suspicious_csv)
    write_rows_csv(
        rows=audit["suspicious_rows"],
        out_csv=suspicious_csv_path,
        columns=SUSPICIOUS_PAGE_COLUMNS,
    )

    report = build_stage_report(
        stage="parser_recall_audit",
        inputs={
            "root_dir": str(Path(root_dir).resolve()),
            "max_tiny_rows": max_tiny_rows,
            "min_large_rows": min_large_rows,
            "low_coverage_threshold": low_coverage_threshold,
        },
        outputs={
            "report_json": str(report_path.resolve()),
            "suspicious_csv": str(suspicious_csv_path.resolve()),
        },
        stats={
            **audit["stats"],
            "counts_by_bucket": audit["counts_by_bucket"],
            "counts_by_pipeline": audit["counts_by_pipeline"],
            "counts_by_parser": audit["counts_by_parser"],
        },
        row_totals={"items": len(audit["suspicious_rows"]), "issues": len(audit["artifacts"]["errors"])},
        items=audit["suspicious_rows"],
        issues=list(audit["artifacts"]["errors"]),
    )
    write_json_report(report_path, compact_stage_report(report))
    return report


def run_from_options(options: ParserRecallAuditOptions) -> dict[str, Any]:
    return build_parser_recall_report(
        root_dir=options.root_dir,
        report_json=options.report_json,
        suspicious_csv=options.suspicious_csv,
        max_tiny_rows=options.max_tiny_rows,
        min_large_rows=options.min_large_rows,
        low_coverage_threshold=options.low_coverage_threshold,
    )


__all__ = [
    "SUSPICIOUS_PAGE_COLUMNS",
    "audit_parser_recall_root",
    "build_parser_recall_report",
    "run_from_options",
]
