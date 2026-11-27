"""Resume a delivery group after its latest midnight filtering run."""

import hashlib
import json
from pathlib import Path
from core.drive.text_extraction_csv import build_doc_json_rel_path

from core.events.extraction.writers import EVENT_COLUMNS, write_rows_csv
from core.events.filtering.service import process_one_events_file
from .counts import recount
from .inventory import candidates, read_rows, ensure_filtered
from .recovery import Originals, parser_version, recover, save


def run(block, groups, retry, offline, workers):
    originals = Originals(offline)
    version = parser_version()
    deliveries = sorted((block / "Consegna").glob("*/copertura_file.csv"))
    known = {p.parent.name for p in deliveries}
    if not known or set(groups) - known:
        raise ValueError(f"Gruppi disponibili: {sorted(known)}")
    pending = 0
    for coverage in deliveries:
        name = coverage.parent.name
        if groups and name not in groups:
            continue
        output = block / "Ricalcolo" / name
        try:
            pending += run_group(
                block / name, coverage.parent, output, version, originals, retry, workers
            )
        except Exception as exc:
            output.mkdir(parents=True, exist_ok=True)
            save(output / "errore_gruppo.json", {"gruppo": name, "errore": str(exc)})
            print(f"{name}: ricalcolo incompleto: {exc}", flush=True)
            pending += 1
        else:
            (output / "errore_gruppo.json").unlink(missing_ok=True)
    return pending


def run_group(group, delivery, output, version, originals, retry, workers):
    pending = 0
    roots = ensure_filtered(group, delivery)
    extra = None
    if retry or (output / "files").exists():
        present = set()
        recorded = set()
        for root in roots:
            recorded.update(r["source_file_id"] for r in read_rows(root / "events/pages.csv"))
            present.update(row["source_file_id"] for row in read_rows(root / "events/events.csv"))
        rows, issues = [], []
        for item in candidates(roots, delivery):
            if item["file_id"] in present:
                continue
            if not retry:
                cache = output / "files" / Path(build_doc_json_rel_path(item["file_id"])).name
                if not cache.exists():
                    continue
            item["page_recorded"] = item["file_id"] in recorded
            result = recover(item, output, version, originals if retry else Originals(True))
            rows.extend(result["events"])
            if not result["events"] or result.get("error") or result.get("partial_pages"):
                issues.append(
                    {
                        "file_id": item["file_id"],
                        "nome": item["file_name"],
                        "motivo": result.get("error")
                        or result.get("partial_pages")
                        or "Nessuna timbratura riconosciuta",
                    }
                )
        save(output / "lacune_residue.json", issues)
        pending += len(issues)
        if rows:
            source = output / "recupero/events.csv"
            digest = hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()
            checkpoint = source.parent / "stato.json"
            extra = source.with_name("events.cleaned.csv")
            if (
                not extra.exists()
                or not checkpoint.exists()
                or json.loads(checkpoint.read_bytes()) != digest
            ):
                write_rows_csv(rows=rows, out_csv=source, columns=EVENT_COLUMNS)
                filtered = process_one_events_file(source, output_dir=str(source.parent))
                if filtered["status"] != "ok":
                    raise RuntimeError(filtered["error"])
                extra = Path(filtered["output_events_csv"])
                save(checkpoint, digest)
    changed = recount(group, delivery, roots, output, extra, workers)
    print(f"{group.name}: {len(roots)} sorgenti, {changed} dipendenti ricalcolati", flush=True)
    return pending
