"""Render a delivery's published CSV values without reclassifying shifts."""
import csv
from pathlib import Path
from tempfile import TemporaryDirectory
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import LongTable, Paragraph, SimpleDocTemplate, Spacer, TableStyle


LABELS = {
    "dipendente": "Dipendente", "lotto": "Lotto", "anno": "Anno",
    "M": "M", "P": "P", "N": "N", "MF": "MF", "totale": "Totale",
    "coppie_durata_positiva": "Turni", "di_cui_oltre_6_ore": "Di cui oltre 6 ore",
}


def export_counts_pdf(folder: Path, status: str) -> Path:
    with (folder / "conteggi.csv").open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        fields = reader.fieldnames or []
        if not {"dipendente", "anno"}.issubset(fields) or set(fields) - LABELS.keys():
            raise ValueError(f"Colonne conteggi non riconosciute: {fields}")
        rows = list(reader)
    if not rows:
        raise ValueError("Il CSV dei conteggi è vuoto.")
    question = next(line for line in (folder / "QUESITO.md").read_text(
        encoding="utf-8-sig", errors="replace").splitlines() if line.strip().endswith("?"))
    styles = getSampleStyleSheet()
    cell = ParagraphStyle("Nome", fontName="Helvetica", fontSize=8, leading=10)
    data = [[LABELS[key] for key in fields]]
    for row in rows:
        data.append([Paragraph(escape(row[key]), cell) if key in ("dipendente", "lotto")
                     else str(int(row[key])) for key in fields])
    widths = {"anno": 40, "lotto": 78, "coppie_durata_positiva": 72,
              "di_cui_oltre_6_ore": 95}
    available = A4[0] - 80
    widths["dipendente"] = available - sum(widths.get(key, 40) for key in fields if key != "dipendente")
    table = LongTable(data, colWidths=[widths.get(key, 40) for key in fields], repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#204654")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (fields.index("anno"), 0), (-1, -1), "RIGHT"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#eef4f4")]),
    ]))
    story = [Paragraph(escape(folder.name), styles["Title"]),
             Paragraph(escape(question), styles["Heading3"]),
             Paragraph(escape(status), styles["BodyText"])]
    legend = {"M": "M = mattina", "P": "P = pomeriggio", "N": "N = notte",
              "MF": "MF = mattine di domenica o festivo infrasettimanale"}
    if any(key in fields for key in legend):
        story.append(Paragraph(" · ".join(value for key, value in legend.items() if key in fields),
                               styles["BodyText"]))
    story.extend([Spacer(1, 12), table])

    def footer(canvas, doc):
        canvas.setFont("Helvetica", 8)
        canvas.drawString(40, 24, folder.name)
        canvas.drawRightString(A4[0] - 40, 24, f"Pagina {doc.page}")

    target = folder / "conteggi.pdf"
    with TemporaryDirectory(dir=folder) as scratch:
        draft = Path(scratch) / target.name
        doc = SimpleDocTemplate(str(draft), pagesize=A4, leftMargin=34, rightMargin=34,
                                topMargin=38, bottomMargin=40, title=f"Conteggi - {folder.name}")
        doc.build(story, onFirstPage=footer, onLaterPages=footer)
        draft.replace(target)
    return target
