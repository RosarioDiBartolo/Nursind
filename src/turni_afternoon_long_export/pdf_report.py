from __future__ import annotations

from pathlib import Path

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

PDF_TITLE = "Report Pomeriggi oltre le 6 ore"


def write_employee_pdf_report(
    *,
    employee: str,
    rows: pd.DataFrame,
    output_path: str | Path,
) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        alignment=TA_CENTER,
        fontSize=16,
        leading=20,
        spaceAfter=8,
    )
    subtitle_style = ParagraphStyle(
        "EmployeeSubtitle",
        parent=styles["Heading2"],
        alignment=TA_CENTER,
        fontSize=12,
        leading=15,
        spaceAfter=12,
    )
    body_style = ParagraphStyle(
        "ReportBody",
        parent=styles["BodyText"],
        alignment=TA_LEFT,
        fontSize=8,
        leading=10,
    )

    doc = SimpleDocTemplate(
        str(path),
        pagesize=landscape(A4),
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=12 * mm,
        bottomMargin=12 * mm,
        title=f"{PDF_TITLE} - {employee}",
    )

    story = [
        Paragraph(PDF_TITLE, title_style),
        Paragraph(str(employee or "unknown"), subtitle_style),
        _summary_table(rows),
        Spacer(1, 8),
    ]

    if rows.empty:
        story.append(Paragraph("Nessun pomeriggio oltre le 6 ore trovato.", body_style))
    else:
        story.append(_data_table(rows, body_style))

    doc.build(story)


def _summary_table(rows: pd.DataFrame) -> Table:
    summary = _summary_values(rows)
    table = Table(
        [
            ["Pomeriggi", "Ore totali", "Periodo", "Festivi"],
            [
                str(summary["row_count"]),
                summary["total_duration"],
                summary["period"],
                str(summary["holiday_count"]),
            ],
        ],
        colWidths=[35 * mm, 35 * mm, 70 * mm, 35 * mm],
        hAlign="CENTER",
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E8EDF3")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#1F2937")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#B8C1CC")),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return table


def _summary_values(rows: pd.DataFrame) -> dict[str, object]:
    if rows.empty:
        return {
            "row_count": 0,
            "total_duration": "00:00",
            "period": "",
            "holiday_count": 0,
        }

    dates = []
    if "Data" in rows.columns:
        parsed_dates = pd.to_datetime(rows["Data"], errors="coerce")
        dates = [value.strftime("%Y-%m-%d") for value in parsed_dates.dropna()]

    holiday_count = 0
    if "Festivo" in rows.columns:
        holiday_count = int(rows["Festivo"].fillna("").astype(str).str.lower().eq("festivo").sum())

    return {
        "row_count": int(len(rows)),
        "total_duration": _total_duration(rows.get("durata turno")),
        "period": _date_period(dates),
        "holiday_count": holiday_count,
    }


def _total_duration(values: pd.Series | None) -> str:
    if values is None:
        return "00:00"

    total_minutes = 0
    for raw in values.fillna("").astype(str):
        parts = raw.split(":", maxsplit=1)
        if len(parts) != 2:
            continue
        try:
            total_minutes += int(parts[0]) * 60 + int(parts[1])
        except ValueError:
            continue
    hours, minutes = divmod(total_minutes, 60)
    return f"{hours:02d}:{minutes:02d}"


def _date_period(dates: list[str]) -> str:
    if not dates:
        return ""
    first = min(dates)
    last = max(dates)
    if first == last:
        return first
    return f"{first} - {last}"


def _data_table(rows: pd.DataFrame, body_style: ParagraphStyle) -> Table:
    columns = list(rows.columns)
    data: list[list[object]] = [[Paragraph(str(column), body_style) for column in columns]]
    for _, row in rows.iterrows():
        data.append([Paragraph(str(row.get(column, "")), body_style) for column in columns])

    table = Table(
        data,
        repeatRows=1,
        colWidths=[36 * mm, 39 * mm, 39 * mm, 25 * mm, 30 * mm, 30 * mm, 28 * mm],
        hAlign="CENTER",
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#DDE5EE")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#CBD5E1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return table


__all__ = ["PDF_TITLE", "write_employee_pdf_report"]
