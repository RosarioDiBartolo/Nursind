"""Build restartable employee views from the mapping CSVs."""
import csv
import hashlib
import json
import os
from collections import defaultdict
from dataclasses import replace
from pathlib import Path
from tempfile import mkdtemp

from core.events.extraction.writers import write_rows_csv
from core.pipeline.steps import STEP_ACTIONS


def read_table(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return reader.fieldnames, list(reader)


def group_employees(config, identities=None):
    paths = config.paths
    approved = {}
    if identities:
        _, rows = read_table(Path(identities))
        for row in rows:
            key = row["file_id"], row["employee"].strip().casefold()
            value = row["employee_id"].strip()
            if not value or approved.setdefault(key, value) != value:
                raise ValueError(f"Inconsistent employee_id for {key}")
    tables = {name: read_table(paths.events_dir / f"{name}.csv") for name in ("files", "pages", "events")}
    groups = defaultdict(lambda: {"files": [], "pages": [], "events": []})
    destinations = defaultdict(set)
    page_groups = {}
    for page in tables["pages"][1]:
        fid = page["file_id"]
        name = page["employee"].strip()
        group = "unresolved"
        if name and page["is_timbrature"] == "True":
            identity = approved.get((fid, name.casefold())) or page.get("employee_identifier")
            key = ("employee", identity) if identity else ("document", fid, name.casefold())
            digest = hashlib.sha256(json.dumps(key).encode()).hexdigest()[:20]
            group = f"employees/id-{digest}"
        groups[group]["pages"].append(page)
        destinations[fid].add(group)
        page_groups[fid, page["page_no"]] = group
    for event in tables["events"][1]:
        group = page_groups[event["source_file_id"], event["source_page_no"]]
        groups[group]["events"].append(event)
    for item in tables["files"][1]:
        for group in destinations[item["file_id"]] or {"unresolved"}:
            groups[group]["files"].append(item)
    base = paths.pipeline_root / "grouped"
    base.mkdir(exist_ok=True)
    snapshot = Path(mkdtemp(prefix="run-", dir=base))
    for group, artifacts in groups.items():
        for name, rows in artifacts.items():
            write_rows_csv(rows=rows, columns=tables[name][0], out_csv=snapshot / group / "events" / f"{name}.csv")
    pointer = snapshot / "pointer.tmp"
    pointer.write_text(snapshot.name, encoding="utf-8")
    os.replace(pointer, base / "current.txt")


def run_employee_step(config, step, verbose):
    base = config.paths.pipeline_root / "grouped"
    current = base / (base / "current.txt").read_text(encoding="utf-8") / "employees"
    for employee in sorted(current.glob("id-*")):
        STEP_ACTIONS[step](replace(config, output_dir_stem=employee), verbose)
