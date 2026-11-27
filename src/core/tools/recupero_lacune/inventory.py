"""Select zero-event attendance candidates using the delivery inventory."""

import csv
import json
import re

from core.drive.text_extraction_csv import build_doc_json_rel_path


def read_rows(path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        yield from csv.DictReader(stream)


def candidates(roots, delivery):
    """Use current scan IDs and the canonical delivery employee mapping."""
    coverage = {
        r.get("id") or r.get("id_file"): r for r in read_rows(delivery / "copertura_file.csv")
    }
    seen = set()
    for source in roots:
        files = json.loads((source / "scan/included.index.json").read_bytes())["files"]
        for fid, metadata in files.items():
            if fid in seen or metadata.get("type") == "folder":
                continue
            name = metadata.get("file_name") or ""
            path = metadata.get("drive_path") or ""
            explicit = re.search(r"cartell|timbr|presenz|jcart", name, re.I)
            if not explicit and re.search(
                r"bust[ae]|cedolin|paghe|stipend|modulistic|bonifico|document|mandato|identit|mensa|procura|revoca|ricorso",
                path,
                re.I,
            ):
                continue
            hints = re.search(r"cartell|timbr|presenz", path, re.I) or re.search(
                r"20\d{2}|gen|feb|mar|apr|mag|giu|lug|ago|sett|ott|nov|dic", name, re.I
            )
            if not explicit and not hints:
                continue
            seen.add(fid)
            yield {
                "file_id": fid,
                "file_name": name,
                "employee": coverage.get(fid, {}).get("dipendente") or metadata.get("employee"),
                "employee_id": metadata.get("employee_id"),
                "drive_path": path,
                "source": source,
                "document": source / "documents" / build_doc_json_rel_path(fid),
                "explicit": bool(explicit),
            }


def ensure_filtered(group, delivery):
    from core.drive.text_extraction_csv import find_text_extraction_csvs
    from core.events.extraction.options import ExtractEventsFromTextOptions
    from core.events.extraction.service import run_from_options as extract_events
    from core.events.filtering.options import FilterMidnightEventsOptions
    from core.events.filtering.service import run_from_options as filter_midnight

    lots = {r.get("lotto", "") for r in read_rows(delivery / "copertura_cartelle.csv")}
    sources = []
    for lot in sorted(lots):
        roots = [group / lot / "artifacts", group / lot]
        if len(lots) == 1:
            roots.append(group / "artifacts")
        root = next(
            (p for p in roots if (p / "documents").is_dir() or (p / "events/events.csv").exists()),
            None,
        )
        if root is None:
            raise FileNotFoundError(f"Sorgente assente: {group.name} {lot}")
        directory = root / "events"
        raw = directory / "events.csv"
        if not raw.exists():
            if not find_text_extraction_csvs(root / "documents"):
                raise ValueError(f"Manifest dei documenti assente: {root}")
            report = extract_events(
                ExtractEventsFromTextOptions(
                    str(root / "documents"),
                    str(directory),
                    report_json=str(directory / "extract_events.report.json"),
                )
            )
            if report["stats"]["files_error"]:
                raise ValueError(f"Errori nell'estrazione eventi: {directory}")
        clean = directory / "events.cleaned.csv"
        if not clean.exists() or clean.stat().st_mtime_ns < raw.stat().st_mtime_ns:
            report = filter_midnight(
                FilterMidnightEventsOptions(
                    str(directory),
                    report_json=str(directory / "events.clean_midnight.report.json"),
                    removed_csv=str(directory / "events.midnight_removed.csv"),
                )
            )
            if report["status"] != "ok":
                raise ValueError(f"Errore nel filtro mezzanotte: {directory}")
        sources.append(root)
    return sources
