"""Independent, fixed expectations: never derived from pipeline output."""

import hashlib
import json

import pandas as pd

from core.config import PipelineConfig
from core.drive.io_json import write_json

EXPECTED_EVENTS = [
    ("2026-09-01 08:00:00", "E"),
    ("2026-09-01 16:00:00", "U"),
    ("2026-09-02 20:00:00", "E"),
    ("2026-09-03 07:00:00", "U"),
    ("2026-09-04 14:00:00", "U"),
]
EXPECTED_SHIFTS = [
    ("2026-09-01 08:00:00", "2026-09-01 16:00:00", 8.0, "M"),
    ("2026-09-02 20:00:00", "2026-09-03 07:00:00", 11.0, "N"),
]


def verify_demo(config: PipelineConfig) -> dict:
    p = config.paths
    checks = []

    def check(label, condition):
        checks.append({"check": label, "passed": bool(condition)})

    events = pd.read_csv(p.events_csv)
    cleaned = pd.read_csv(p.cleaned_events_csv)
    check(
        "Exact five timestamp/kind pairs",
        list(events[["event_ts", "event_kind"]].itertuples(index=False, name=None))
        == EXPECTED_EVENTS,
    )
    check(
        "Cleanup retains all five events",
        list(cleaned[["event_ts", "event_kind"]].itertuples(index=False, name=None))
        == EXPECTED_EVENTS,
    )
    check(
        "Every event has source page, line, slot and document",
        events[["source_page_no", "source_line_id", "source_slot", "source_doc_json"]]
        .notna()
        .all()
        .all(),
    )
    check("Expected monthly parser", set(events.parser_id) == {"workflow_monthly"})
    pages = pd.read_csv(p.pages_csv)
    check(
        "One dated events page",
        len(pages) == 1
        and int(pages.iloc[0].events_extracted) == 5
        and pages.iloc[0].year_month_source == "workflow_header",
    )
    manifest = pd.read_csv(p.documents_dir / "DEMO-001.csv")
    doc = json.loads((p.documents_dir / manifest.iloc[0].doc_json).read_text(encoding="utf-8"))
    check(
        "Native PDF JSON with layout",
        doc["document"]["page_count"] == 1
        and bool(doc["layout"]["pages"][0]["words"])
        and doc["extraction"]["has_text_layer"]
        and doc["extraction"].get("ocr_pages", 0) == 0,
    )
    pairs = pd.read_csv(p.shifts_dir / "DEMO-001.pairs.csv")
    check(
        "Exactly two complete shifts",
        list(pairs[["entry_ts", "exit_ts"]].itertuples(index=False, name=None))
        == [row[:2] for row in EXPECTED_SHIFTS],
    )
    enriched = pd.read_csv(p.enrichment_dir / "DEMO-001.enriched.csv")
    check(
        "Exact durations and buckets",
        list(
            enriched[["entry_ts", "exit_ts", "duration_hours", "turno_bucket"]].itertuples(
                index=False, name=None
            )
        )
        == EXPECTED_SHIFTS,
    )
    report = json.loads(p.pairing_report.read_text(encoding="utf-8"))
    check("One unmatched punch reported", report["stats"]["rows_unmatched_after_close"] == 1)
    summary = pd.read_csv(p.summary_csv)
    counts = summary.set_index("turno")["2026"].to_dict()
    check(
        "Summary M=1 N=1 and no other counts",
        counts.get("M") == 1
        and counts.get("N") == 1
        and sum(counts.values()) == 2
        and set(summary.employee) == {"DEMO-001"},
    )
    workbook = pd.read_excel(p.summary_csv.with_suffix(".xlsx"))
    check("CSV and Excel agree", summary.equals(workbook))
    hashes = {
        str(f.relative_to(p.pipeline_root)): hashlib.sha256(f.read_bytes()).hexdigest()
        for f in sorted(p.pipeline_root.rglob("*"))
        if f.is_file()
        and f.suffix in {".pdf", ".csv", ".xlsx", ".json"}
        and f.name != "verification.json"
    }
    result = {"passed": all(c["passed"] for c in checks), "checks": checks, "sha256": hashes}
    write_json(str(p.pipeline_root / "verification.json"), result)
    failures = [c["check"] for c in checks if not c["passed"]]
    if failures:
        raise ValueError(f"Demo verification failed: {failures}")
    return result
