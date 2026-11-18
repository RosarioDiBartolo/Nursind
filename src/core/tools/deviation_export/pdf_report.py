"""Paginated, per-employee deviation report, including coverage limits."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from html import escape
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle, PageBreak


def write_report(path: Path, employee: str, compared: list[dict], excluded: list[dict],
                 threshold: float, schedule_source: str, quality: list[str]) -> None:
    styles = getSampleStyleSheet()
    body = styles["BodyText"]
    body.fontSize, body.leading = 9, 13
    doc = SimpleDocTemplate(str(path), pagesize=landscape(A4), leftMargin=16*mm,
                            rightMargin=16*mm, topMargin=16*mm, bottomMargin=16*mm,
                            title=f"Scostamenti turni - {employee}", author="Nursind")
    selected = sum(row["over_threshold"] for row in compared)
    story = [Paragraph(f"Scostamenti turni - {escape(employee)}", styles["Title"]),
             Paragraph("Confronto tra orari teorici e timbrature ricostruite dopo il filtro di mezzanotte.", body),
             Spacer(1, 6*mm),
             Paragraph(f"Turni completi: {len(compared)+len(excluded)} | Confrontabili: {len(compared)} | "
                       f"Oltre {threshold:g} minuti: {selected} | Esclusi: {len(excluded)}", body),
             Spacer(1, 4*mm),
             Paragraph("Segno negativo: anticipo; positivo: ritardo rispetto all'orario teorico. "
                       "La soglia si applica al valore assoluto, in entrata o in uscita. "
                       "Gli scostamenti non costituiscono automaticamente ore di straordinario o irregolarita.", body),
             Spacer(1, 4*mm), Paragraph(escape(schedule_source), body), Spacer(1, 4*mm)]
    for message in quality:
        story.append(Paragraph(f"Avviso: {escape(message)}", body))
    if excluded:
        story.append(Paragraph("I turni non confrontabili sono elencati nel CSV esclusi; "
                               "non sono conteggiati come turni senza scostamento.", body))
    months: dict[str, list[dict]] = defaultdict(list)
    for row in compared:
        months[row["month"]].append(row)
    month_names = ("", "Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
                   "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre")
    for month, rows in sorted(months.items()):
        year, number = map(int, month.split("-"))
        selected_rows = [row for row in rows if row["over_threshold"]]
        story.extend([PageBreak(), Paragraph(f"Dettaglio scostamenti - {month_names[number]} {year}",
                                            styles["Heading1"]),
                      Paragraph(f"Turni confrontabili: {len(rows)} | Oltre {threshold:g} minuti: {len(selected_rows)}. "
                                "Le notti sono riportate alla data di entrata; (+1) indica il giorno successivo.", body),
                      Spacer(1, 4*mm)])
        if not selected_rows:
            story.append(Paragraph("Nessun turno oltre la soglia nel mese.", body))
            continue
        data = [["Data", "Codice", "Teorico E", "Reale E", "Diff. E", "Teorico U", "Reale U", "Diff. U"]]
        for row in selected_rows:
            entry = datetime.fromisoformat(row["entry_ts"])
            def time_label(key):
                value = datetime.fromisoformat(row[key])
                suffix = " (+1)" if value.date() > entry.date() else ""
                return value.strftime("%H:%M") + suffix
            data.append([entry.strftime("%d/%m/%Y"), row["code"], time_label("theoretical_entry"),
                         time_label("entry_ts"), f"{row['diff_entry_minutes']:+g} min",
                         time_label("theoretical_exit"), time_label("exit_ts"), f"{row['diff_exit_minutes']:+g} min"])
        table = Table(data, colWidths=[27*mm, 49*mm, 27*mm, 27*mm, 25*mm, 27*mm, 27*mm, 25*mm], repeatRows=1)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#263F58")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8), ("LEADING", (0, 0), (-1, -1), 11),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F0F4F8")]),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#BCC8D5")),
        ]))
        story.append(table)
    def footer(canvas, document):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#607080"))
        canvas.drawString(16*mm, 9*mm, f"Analisi documentale - {employee} - dati con limiti di copertura")
        canvas.drawRightString(281*mm, 9*mm, f"Pagina {document.page}")
        canvas.restoreState()
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
