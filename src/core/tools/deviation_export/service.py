"""Load post-filter pairs, audit comparison coverage, and export reports."""
from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from pathlib import Path

from core.drive.names import safe_name
from core.events.extraction.writers import write_rows_csv
from core.tools.missing_report.inputs import read_csv_rows as read_csv
from core.reporting import build_stage_report, write_json_report
from core.tools.deviation_export.comparison import compare_shift, validate_schedules
from core.tools.deviation_export.pdf_report import write_report
from core.tools.deviation_export.source_codes import SourceCodes


def export_deviations(pipeline_root: Path, repository_root: Path, *, threshold: float = 10,
                      schedules_path: Path | None = None) -> dict:
    if not math.isfinite(threshold) or threshold < 0:
        raise ValueError("Threshold must be finite and non-negative")
    schedules_path = schedules_path or Path(__file__).with_name("schedules.json")
    config = json.loads(schedules_path.read_text(encoding="utf-8"))
    schedules = config["schedules"]
    validate_schedules(schedules)
    cleaned_path = pipeline_root / "events/events.cleaned.csv"
    cleaned = {row["source_event_ref"]: row for row in read_csv(cleaned_path)}
    pair_files = sorted((pipeline_root / "shifts").glob("*.pairs.csv"))
    if not pair_files or not cleaned:
        raise ValueError("Missing post-filter events or paired shifts; rerun the pipeline first")
    output = pipeline_root / "deviation_export"
    output.mkdir(parents=True, exist_ok=True)
    sources = SourceCodes(repository_root)
    issues, items = [], []
    quality = []
    for relative in ("events/extract_events.report.json", "shifts/pair_employee_events.report.json"):
        path = pipeline_root / relative
        if path.exists():
            upstream = json.loads(path.read_text(encoding="utf-8"))
            if upstream.get("issues") or upstream.get("status") != "ok":
                if relative.startswith("events/"):
                    count = upstream.get("stats", {}).get("pages_error", 0)
                    quality.append(f"Nell'archivio complessivo risultano {count} pagine con data mancante "
                                   "o incoerente. La copertura dei cartellini e incompleta.")
                else:
                    count = upstream.get("stats", {}).get("rows_unmatched_after_close", 0)
                    quality.append(f"Nell'archivio complessivo restano {count} timbrature senza coppia, "
                                   "escluse dal confronto dei turni completi.")
        else:
            quality.append(f"Report di qualita mancante: {relative}")
    for pair_file in pair_files:
        employee = pair_file.name.removesuffix(".pairs.csv")
        compared, excluded = [], []
        pairs = read_csv(pair_file)
        entry_lines = Counter(row.get("entry_event_ref", "").split(":w", 1)[0] for row in pairs)
        for row in pairs:
            try:
                if entry_lines[row.get("entry_event_ref", "").split(":w", 1)[0]] > 1:
                    raise ValueError("Multiple paired intervals on the same source line; manual review required")
                for kind, timestamp, reference in (("E", "entry_ts", "entry_event_ref"),
                                                    ("U", "exit_ts", "exit_event_ref")):
                    event = cleaned.get(row.get(reference, ""))
                    if not event or event["event_ts"] != row[timestamp] or event["event_kind"] != kind:
                        raise ValueError("Pair does not match current post-filter events; rerun pairing")
                    if event["source_employee"] != employee:
                        raise ValueError("Employee mismatch in source event")
                if Path(row["source_csv"]).resolve() != cleaned_path.resolve():
                    raise ValueError("Pair was not generated from current events.cleaned.csv")
                codes = [sources.code(row[key]) for key in ("entry_event_ref", "exit_event_ref")]
                compared.append(compare_shift(row, *codes, schedules, threshold))
            except (ValueError, KeyError, FileNotFoundError) as exc:
                excluded.append({**row, "reason": str(exc)})
        selected = [row for row in compared if row["over_threshold"]]
        prefix = safe_name(employee)
        columns = ["entry_ts", "exit_ts", "code", "theoretical_entry", "theoretical_exit",
                   "diff_entry_minutes", "diff_exit_minutes", "over_threshold",
                   "entry_event_ref", "exit_event_ref", "file_id", "file_name"]
        write_rows_csv(out_csv=output / f"{prefix}.confronti.csv", rows=compared, columns=columns)
        write_rows_csv(out_csv=output / f"{prefix}.scostamenti.csv", rows=selected, columns=columns)
        write_rows_csv(out_csv=output / f"{prefix}.esclusi.csv", rows=excluded,
                  columns=["entry_ts", "exit_ts", "reason", "entry_event_ref", "exit_event_ref"])
        pdf_path = output / f"{prefix}.scostamenti.pdf"
        write_report(pdf_path, employee, compared, excluded, threshold, config["source"], quality)
        if excluded:
            issues.append({"code": "uncomparable_shifts", "employee": employee,
                           "count": len(excluded), "reasons": dict(Counter(row["reason"] for row in excluded))})
        items.append({"employee": employee, "rows_total": len(compared)+len(excluded),
                      "rows_comparable": len(compared), "rows_selected": len(selected),
                      "rows_excluded": len(excluded), "pdf": str(pdf_path.resolve())})
    for message in quality:
        issues.append({"code": "upstream_quality_warning", "message": message})
    inputs = [cleaned_path, schedules_path, *pair_files, *sources.documents.keys()]
    report = build_stage_report(
        stage="deviation_export", status="warning" if issues else "ok",
        inputs={"pipeline_root": str(pipeline_root.resolve()), "threshold_minutes": threshold,
                "schedule_source": config["source"],
                "sha256": {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest() for path in inputs}},
        outputs={"output_dir": str(output.resolve())},
        stats={"employees": len(items), **{key: sum(item[key] for item in items)
               for key in ("rows_total", "rows_comparable", "rows_selected", "rows_excluded")}},
        issues=issues, items=items,
    )
    write_json_report(output / "deviation_export.report.json", report)
    return report
