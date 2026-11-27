"""Count complete employee histories across the source lots of one delivery group."""

import hashlib
import json
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pandas as pd

from core.events.extraction.writers import EVENT_COLUMNS
from core.shifts.pairing.service import process_one_employee_events
from core.shifts.enrichment.service import process_one_pairs_file
from core.table_outputs import write_csv_and_excel
from .inventory import read_rows
from .recovery import save


def employee_counts(name, csv_path, folder, columns):
    pair = process_one_employee_events(
        {"employee": name, "files": [{"events_csv": str(csv_path)}]},
        output_dir=str(folder / "shifts"),
    )
    if pair["status"] != "ok":
        raise RuntimeError(pair["error"])
    enriched = process_one_pairs_file(pair["output_csv"], output_dir=str(folder / "enrichment"))
    if enriched["status"] != "ok":
        raise RuntimeError(enriched["error"])
    df = pd.read_csv(enriched["output_enriched_csv"])
    rows = []
    for year, part in df.loc[df.duration_hours > 0].groupby("year"):
        night = part.is_night
        afternoon = part.is_afternoon & ~night
        morning = ~night & ~afternoon
        masks = {"M": morning, "P": afternoon, "N": night, "MF": morning & part.is_holiday}
        row = {"dipendente": name, "anno": int(year)}
        row.update({k: int(v.sum()) for k, v in masks.items() if k in columns})
        row["totale"] = sum(row[k] for k in masks if k in row)
        if "di_cui_oltre_6_ore" in columns:
            row["di_cui_oltre_6_ore"] = int((part.duration_hours > 6).sum())
        if row["totale"]:
            rows.append(row)
    return rows


def event_keys(frame):
    # Situazione mensile also uses row and document identity when closing a pair.
    columns = [
        "event_ts",
        "event_kind",
        "event_raw",
        "parser_id",
        "source_file_id",
        "source_page_no",
        "source_line_id",
    ]
    return set(pd.util.hash_pandas_object(frame[columns], index=False).values)


def prior_histories(group):
    histories = defaultdict(set)
    for path in sorted((group / "Lotti uniti").glob("parte-*/events/events.cleaned.csv")):
        for frame in pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            chunksize=30000,
            usecols=[
                "source_employee",
                "event_ts",
                "event_kind",
                "event_raw",
                "parser_id",
                "source_file_id",
                "source_page_no",
                "source_line_id",
            ],
        ):
            for name, part in frame.groupby("source_employee"):
                histories[name].update(event_keys(part))
    return histories


def count_job(job):
    name, folder, columns, fingerprint, version = job
    rows = employee_counts(name, folder / "events.cleaned.csv", folder, columns)
    entry = {"hash": fingerprint, "rows": rows, "code": version}
    save(folder / "risultato.json", entry)
    return name, entry


def recount(group, delivery, roots, output, extra, workers, *, from_sources=False):
    coverage = {
        r.get("id") or r.get("id_file"): r["dipendente"]
        for r in read_rows(delivery / "copertura_file.csv")
    }
    paths = [p / "events/events.cleaned.csv" for p in roots]
    if extra:
        paths.append(extra)
    with (delivery / "conteggi.csv").open(encoding="utf-8-sig") as stream:
        columns = stream.readline().strip().split(",")
    review = group / "Verifica date e duplicati" / "pagine.json"
    reviewed_pages = json.loads(review.read_bytes()) if review.exists() else []
    core = Path(__file__).resolve().parents[2]
    code = hashlib.sha256()
    for f in [Path(__file__), core / "shift_logic.py", *sorted((core / "shifts").rglob("*.py"))]:
        code.update(f.read_bytes())
    state_file = output / "conteggi.state.json"
    previous = json.loads(state_file.read_bytes()) if state_file.exists() else {}
    inputs = [
        [str(p), p.stat().st_size, p.stat().st_mtime_ns]
        for p in paths
        + [delivery / "copertura_file.csv", delivery / "conteggi.csv"]
        + ([review] if reviewed_pages else [])
    ]
    if (
        previous.get("inputs") == inputs
        and previous.get("code") == code.hexdigest()
        and all((output / f).is_file() for f in ("conteggi.csv", "conteggi.xlsx"))
    ):
        return 0
    groups = defaultdict(list)
    for path in paths:
        for frame in pd.read_csv(
            path, dtype=str, keep_default_na=False, chunksize=20000, usecols=EVENT_COLUMNS
        ):
            for page in reviewed_pages:
                mask = (frame.source_file_id == page["id"]) & (
                    frame.source_page_no == str(page["pagina"])
                )
                action = page["azione"]
                if not mask.any():
                    continue
                if action == "anno futuro non verificato" or "gia conteggiata" in action:
                    frame = frame.loc[~mask].copy()
                elif len(action) == 7 and action[4] == "-":
                    corrected = pd.to_datetime(
                        action + frame.loc[mask, "event_ts"].str.slice(7), errors="raise"
                    )
                    frame.loc[mask, "event_ts"] = corrected.dt.strftime("%Y-%m-%d %H:%M:%S")
                    frame.loc[mask, "dow"] = corrected.dt.dayofweek.map(
                        dict(enumerate(("LU", "MA", "ME", "GI", "VE", "SA", "DO")))
                    )
                else:
                    raise ValueError(f"Rettifica sconosciuta: {action}")
            frame = frame.assign(
                source_employee=frame.source_file_id.map(coverage).fillna(frame.source_employee)
            )
            for name, rows in frame.groupby("source_employee"):
                if not name.strip():
                    raise ValueError(f"Dipendente mancante in {path}")
                groups[name].append(rows)
    employees, jobs = {}, []
    baseline = prior_histories(group) if not previous and not from_sources else {}
    old_rows = defaultdict(list)
    for row in read_rows(delivery / "conteggi.csv"):
        old_rows[row["dipendente"]].append(
            {k: int(v) if k != "dipendente" else v for k, v in row.items()}
        )
    for name, parts in groups.items():
        frame = pd.concat(parts, ignore_index=True)
        fingerprint = hashlib.sha256(
            pd.util.hash_pandas_object(frame, index=False).values.tobytes()
        ).hexdigest()
        folder = output / "dipendenti" / hashlib.sha256(name.encode()).hexdigest()[:20]
        result_path = folder / "risultato.json"
        entry = previous.get("employees", {}).get(name, {})
        if result_path.exists():
            cached = json.loads(result_path.read_bytes())
            if cached.get("code") == code.hexdigest() and cached["hash"] == fingerprint:
                entry = cached
        if entry.get("hash") == fingerprint and entry.get("code") == code.hexdigest():
            employees[name] = entry
        elif name in baseline and name in old_rows and event_keys(frame) == baseline[name]:
            employees[name] = {
                "hash": fingerprint,
                "code": code.hexdigest(),
                "rows": old_rows[name],
            }
        else:
            folder.mkdir(parents=True, exist_ok=True)
            frame.to_csv(folder / "events.cleaned.csv", index=False)
            jobs.append((name, folder, columns, fingerprint, code.hexdigest()))
    groups.clear()
    if jobs:
        with ProcessPoolExecutor(max_workers=min(workers, len(jobs))) as pool:
            employees.update(pool.map(count_job, jobs))
    results = [row for name in sorted(employees) for row in employees[name]["rows"]]
    write_csv_and_excel(results, output / "conteggi.csv", columns=columns)
    save(
        state_file,
        {"inputs": inputs, "code": code.hexdigest(), "columns": columns, "employees": employees},
    )
    return len(jobs)
