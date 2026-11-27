"""Build the Blocco 1 delivery in a staging directory before publishing it."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from shutil import copyfile

from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate
from xml.sax.saxutils import escape

from core.events.extraction.writers import write_rows_csv
from core.tools.recupero_lacune.counts import recount
from core.tools.recupero_lacune.inventory import read_rows
from core.tools.turni_custom_counts.delivery_pdf import export_counts_pdf
from .coverage import inventory
from .scopes import GROUPS, QUESTIONS


def finish_reports(group, folder, files, links, cache):
    used = set()
    paired_times = set()
    state = json.loads((cache / "conteggi.state.json").read_bytes())
    import hashlib

    for name in state["employees"]:
        key = hashlib.sha256(name.encode()).hexdigest()[:20]
        (pairs,) = (cache / "dipendenti" / key / "shifts").glob("*.pairs.csv")
        for row in read_rows(pairs):
            if row["entry_ts"] >= row["exit_ts"]:
                continue
            for side, kind in (("entry", "E"), ("exit", "U")):
                if ref := row.get(side + "_event_ref"):
                    used.add(ref)
                paired_times.add((name, kind, row[side + "_ts"]))
    by_id = {r["id"]: r for r in files}
    for r in files:
        r["senza_coppia"] = 0
    gaps = {}
    for name in state["employees"]:
        key = hashlib.sha256(name.encode()).hexdigest()[:20]
        for event in read_rows(cache / "dipendenti" / key / "events.cleaned.csv"):
            if (
                event["source_event_ref"] in used
                or (name, event["event_kind"], event["event_ts"]) in paired_times
            ):
                continue
            row = by_id[event["source_file_id"]]
            row["senza_coppia"] += 1
            gaps.setdefault(row["id"], set()).add(int(event["source_page_no"] or 1))
    for r in files:
        r["pagine_senza_coppia"] = ", ".join(map(str, sorted(gaps.get(r["id"], ()))))
        if r["senza_coppia"]:
            r["esito"] += " / Timbrature senza coppia"
    issues = [
        r
        for r in files
        if not r["eventi"]
        or r["pagine_errori"]
        or r["pagine_errori_estrazione"]
        or r["senza_coppia"]
        or r["mezzanotte_filtrate"]
    ]
    for filename, rows in (("copertura_file.csv", files), ("lacune_file.csv", issues)):
        write_rows_csv(rows=rows, columns=list(files[0]), out_csv=folder / filename)
    total = sum(int(r["totale"]) for r in read_rows(folder / "conteggi.csv"))
    status = f"{total:,} turni rispondenti al quesito.".replace(",", ".")
    question = QUESTIONS[GROUPS[group][0]]
    if group.startswith("Ragusa"):
        question = "Quanti turni di mattina, pomeriggio e notte risultano per dipendente e anno e quanti superano le sei ore?"
    (folder / "QUESITO.md").write_text(f"# {group}\n\n{question}\n\n{status}\n", encoding="utf-8")
    (folder / "verifica_copertura.md").write_text(
        f"# Copertura di {group}\n\n{len(files)} file indicizzati, {sum(bool(r['eventi']) for r in files)} con eventi. "
        f"I conteggi usano l'anno di ingresso e i turni di durata positiva.\n\n"
        "Il CSV delle lacune indica file a zero eventi, errori, eventi filtrati e timbrature prive di coppia. "
        "Timbrature identiche dello stesso dipendente, verso e istante sono coperte dalla stessa coppia.\n\n"
        + "\n".join(links)
        + "\n",
        encoding="utf-8",
    )
    export_counts_pdf(folder, status)
    styles = getSampleStyleSheet()
    story = [Paragraph(f"Lacune - {escape(group)}", styles["Title"])]
    for r in issues:
        story.append(
            Paragraph(
                escape(
                    f"{r['dipendente']} | {r['nome']} | {r['esito']} | eventi {r['eventi']}, senza coppia {r['senza_coppia']}"
                ),
                styles["BodyText"],
            )
        )
    if not issues:
        story.append(Paragraph("Nessuna lacuna registrata.", styles["BodyText"]))
    SimpleDocTemplate(
        str(folder / "lacune_file.pdf"), leftMargin=14 * mm, rightMargin=14 * mm
    ).build(story)
    return total


def build(block: Path, target: Path, workers: int):
    if target.exists():
        raise FileExistsError(
            f"Destinazione gia presente: {target}. Specificare --destinazione con un percorso libero."
        )
    if target == block or block not in target.parents or target.parent != block:
        raise ValueError(
            "La destinazione deve essere una nuova cartella direttamente sotto il blocco."
        )
    cache_root = block / "Ricalcolo consegna"
    if target == cache_root:
        raise ValueError("Destinazione riservata agli artefatti di verifica.")
    with TemporaryDirectory(prefix=".consegna-", dir=block) as tmp:
        staging = Path(tmp) / "consegna"
        for group, (categories, lots) in GROUPS.items():
            folder = staging / group
            folder.mkdir(parents=True)
            roots = [block / group / lot for lot in lots]
            for root in roots:
                raw, clean = (root / "events" / f for f in ("events.csv", "events.cleaned.csv"))
                if not clean.is_file() or clean.stat().st_mtime_ns < raw.stat().st_mtime_ns:
                    raise ValueError(f"Filtro mezzanotte mancante o obsoleto: {root}")
            files, links = inventory(group, roots, folder)
            fields = ["dipendente", "anno", *categories.split(), "totale"]
            if group.startswith("Ragusa"):
                fields.append("di_cui_oltre_6_ore")
            write_rows_csv(rows=[], columns=fields, out_csv=folder / "conteggi.csv")
            cache = cache_root / group
            recount(block / group, folder, roots, cache, None, workers, from_sources=True)
            for ext in ("csv", "xlsx"):
                copyfile(cache / f"conteggi.{ext}", folder / f"conteggi.{ext}")
            total = finish_reports(group, folder, files, links, cache)
            print(f"{group}: {len(files)} file, {total} turni", flush=True)
        staging.rename(target)
