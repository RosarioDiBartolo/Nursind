"""Trace delivery files and employee folders to the current scan and event tables."""

import json
from collections import Counter, defaultdict

from core.events.extraction.writers import write_rows_csv
from core.tools.recupero_lacune.inventory import read_rows
from .scopes import employee


def extraction_errors(path):
    # Extraction metadata precedes the much larger document layout.
    with path.open(encoding="utf-8-sig") as stream:
        prefix = []
        for line in stream:
            if line.startswith('  "document":'):
                metadata = json.loads("".join(prefix).rstrip().removesuffix(",") + "}")
                break
            prefix.append(line)
        else:
            metadata = json.loads("".join(prefix))
    return [str(item["page_no"]) for item in metadata.get("extraction", {}).get("page_errors", [])]


def inventory(group, roots, folder):
    files, folders, links = [], {}, []
    for root in roots:
        scan = json.loads((root / "scan/included.index.json").read_bytes())
        root_id = scan["root_id"]
        lot = root.parent.name if root.name == "artifacts" and root.parent.name != group else group
        links.append(f"- [{lot}](https://drive.google.com/drive/folders/{root_id})")
        inc = json.loads((root / "documents/included_documents.index.json").read_bytes())["files"]
        exc = json.loads((root / "documents/excluded_documents.index.json").read_bytes())["files"]
        events_dir = root / "events"
        pages, errors = Counter(), defaultdict(list)
        for row in read_rows(events_dir / "pages.csv"):
            fid = row["source_file_id"]
            pages[fid] += 1
            if row["parse_error"] or row["decision"].startswith("error"):
                errors[fid].append(row["page_no"])
        raw = Counter(r["source_file_id"] for r in read_rows(events_dir / "events.csv"))
        clean = Counter(r["source_file_id"] for r in read_rows(events_dir / "events.cleaned.csv"))
        if clean - raw or raw.keys() - scan["files"].keys():
            raise ValueError(f"ID o eventi incoerenti in {root}")
        scan_report = root / "scan/scan_directory.report.json"
        found = (
            json.loads(scan_report.read_bytes()).get("employees_found", [])
            if scan_report.exists()
            else []
        )

        def register(meta):
            key = (root_id, meta.get("employee_id") or meta["employee"])
            if key not in folders:
                folders[key] = dict(
                    lotto=lot,
                    id_radice=root_id,
                    cartella=meta["employee"],
                    dipendente=employee(group, meta["employee"], meta.get("employee_id")),
                    id_cartella=meta.get("employee_id") or "",
                    link=f"https://drive.google.com/drive/folders/{meta.get('employee_id') or root_id}",
                    file_inclusi=0,
                    file_estratti=0,
                    file_con_eventi=0,
                )
            return folders[key]

        for meta in found:
            register(meta)
        # Partial OCR errors are recorded by the document stage separately from parsing errors.
        report_file = root / "documents/extract_documents_from_index.report.json"
        partial = defaultdict(list)
        if report_file.exists():
            for issue in json.loads(report_file.read_bytes()).get("issues", []):
                if issue.get("code") == "page_extraction_error":
                    partial[issue["source_file_id"]].append(str(issue["page_no"]))
        for fid, meta in scan["files"].items():
            record = register(meta)
            if fid in inc and (doc := inc[fid]["outputs"].get("doc_json")):
                partial[fid] = extraction_errors(root / "documents" / doc)
            record["file_inclusi"] += 1
            record["file_estratti"] += int(fid in inc)
            record["file_con_eventi"] += int(raw[fid] > 0)
            status = []
            if fid not in inc:
                status.append(exc.get(fid, {}).get("reason") or "Estrazione non disponibile")
            elif not raw[fid]:
                status.append("Senza eventi estratti")
            if errors[fid]:
                status.append("Errore di parsing pagina")
            if partial[fid]:
                status.append("Estrazione parziale")
            if raw[fid] > clean[fid]:
                status.append("Timbrature di mezzanotte filtrate")
            files.append(
                dict(
                    lotto=lot,
                    dipendente=record["dipendente"],
                    nome=meta["file_name"],
                    id=fid,
                    id_cartella=record["id_cartella"],
                    percorso=meta["drive_path"],
                    link=f"https://drive.google.com/open?id={fid}",
                    pagine=pages[fid],
                    eventi=raw[fid],
                    mezzanotte_filtrate=raw[fid] - clean[fid],
                    pagine_errori=", ".join(errors[fid]),
                    pagine_errori_estrazione=", ".join(partial[fid]),
                    esito=" / ".join(status) or "Con eventi",
                )
            )
    if not files:
        raise ValueError(f"Indice vuoto: {group}")
    write_rows_csv(rows=files, columns=list(files[0]), out_csv=folder / "copertura_file.csv")
    write_rows_csv(
        rows=list(folders.values()),
        columns=list(next(iter(folders.values()))),
        out_csv=folder / "copertura_cartelle.csv",
    )
    return files, links
