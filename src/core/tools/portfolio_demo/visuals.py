"""Render evidence cards from verified artifacts; never fabricate table values."""

import json
from pathlib import Path

import pandas as pd
from PIL import Image, ImageDraw, ImageFont
import pypdfium2 as pdfium

from core.config import PipelineConfig
from core.tools.portfolio_demo.verification import verify_demo


def create_visuals(config: PipelineConfig) -> None:
    verify_demo(config)
    p = config.paths
    out = p.pipeline_root / "visuals"
    out.mkdir(exist_ok=True)
    font_path = Path("C:/Windows/Fonts/arial.ttf")
    if not font_path.exists():
        font_path = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")

    def font(size):
        return ImageFont.truetype(str(font_path), size)

    def card(title, subtitle):
        im = Image.new("RGB", (1600, 1000), "#101d30")
        d = ImageDraw.Draw(im)
        d.text((60, 38), "NURSIND   /   PIPELINE DOCUMENTALE PYTHON", font=font(20), fill="#64dbc1")
        d.text((60, 88), title, font=font(42), fill="white")
        d.text((60, 151), subtitle, font=font(23), fill="#b6c5d9")
        d.line((60, 920, 1540, 920), fill="#345069", width=2)
        d.text(
            (60, 944),
            "DATI SINTETICI  |  1 PDF nativo, 1 pagina  |  Layout WORKFLOW  |  OCR disattivato",
            font=font(20),
            fill="#b6c5d9",
        )
        return im, d

    def table(d, headers, rows, y, widths):
        x = 60
        for header, width in zip(headers, widths):
            d.rectangle((x, y, x + width, y + 48), fill="#244760")
            d.text((x + 12, y + 10), str(header), font=font(21), fill="white")
            x += width
        for i, row in enumerate(rows):
            x = 60
            for value, width in zip(row, widths):
                top = y + 48 + i * 56
                d.rectangle(
                    (x, top, x + width, top + 56), fill="#1b3048" if i % 2 == 0 else "#203850"
                )
                d.text((x + 12, top + 14), str(value), font=font(21), fill="white")
                x += width

    with pdfium.PdfDocument(str(p.pipeline_root / "input/synthetic-monthly.pdf")) as pdf:
        page = pdf[0]
        rendered = page.render(scale=2).to_pil().convert("RGB")
        rendered.save(out / "input-page.png")
        page.close()
    im, d = card(
        "Dal documento ai dati verificabili",
        "Input generato localmente; tabella letta dall'output reale della pipeline.",
    )
    crop = rendered.crop((35, 45, 1150, 645))
    crop.thumbnail((1000, 450))
    im.paste(crop, (60, 215))
    d.text((1100, 240), "5 eventi estratti", font=font(28), fill="#64dbc1")
    d.text((1100, 300), "2 turni completi", font=font(28), fill="white")
    d.text((1100, 360), "1 evento isolato", font=font(28), fill="#ffcc7a")
    enriched = pd.read_csv(p.enrichment_dir / "DEMO-001.enriched.csv")
    table(
        d,
        ("Entrata", "Uscita", "Ore", "Categoria"),
        enriched[["entry_ts", "exit_ts", "duration_hours", "turno_bucket"]].values.tolist(),
        690,
        (460, 460, 180, 300),
    )
    im.save(out / "01-input-output.png")

    events = pd.read_csv(p.events_csv)
    im, d = card(
        "Ogni evento conserva la propria origine",
        "Estratto di events.csv: timestamp, tipo, pagina, riga e colonna del PDF.",
    )
    table(
        d,
        ("Timestamp", "Tipo", "Pagina", "Riga", "Colonna"),
        events[
            ["event_ts", "event_kind", "source_page_no", "source_line_id", "source_slot"]
        ].values.tolist(),
        250,
        (460, 140, 180, 300, 320),
    )
    d.text(
        (60, 660),
        "Parser rilevato: " + str(events.iloc[0].parser_id),
        font=font(27),
        fill="#64dbc1",
    )
    d.text(
        (60, 720),
        "Il JSON intermedio conserva testo e coordinate; gli eventi lo referenziano.",
        font=font(26),
        fill="white",
    )
    d.text(
        (60, 780),
        "Notte: 2 settembre 20:00 -> 3 settembre 07:00 (11 ore).",
        font=font(26),
        fill="white",
    )
    im.save(out / "02-traceability.png")

    report = json.loads(p.pairing_report.read_text(encoding="utf-8"))
    summary = pd.read_csv(p.summary_csv)
    im, d = card(
        "Un'anomalia resta visibile",
        "L'uscita senza entrata non genera un turno e non aumenta il riepilogo.",
    )
    isolated = events[events.event_ts == "2026-09-04 14:00:00"]
    table(
        d,
        ("Evento conservato", "Tipo", "Colonna"),
        isolated[["event_ts", "event_kind", "source_slot"]].values.tolist(),
        235,
        (700, 250, 450),
    )
    d.text(
        (60, 410),
        "Report pairing: rows_unmatched_after_close = "
        + str(report["stats"]["rows_unmatched_after_close"]),
        font=font(29),
        fill="#ffcc7a",
    )
    d.text(
        (60, 475),
        "Limite: il report conta l'anomalia; la revisione usa il CSV degli eventi.",
        font=font(26),
        fill="white",
    )
    table(d, ("Identificativo", "Categoria", "2026"), summary.values.tolist(), 570, (700, 350, 350))
    im.save(out / "03-anomaly-summary.png")

    im, d = card(
        "Una pipeline, artefatti ispezionabili",
        "Servizi Python esistenti; nessuna applicazione web e nessun accesso a Drive.",
    )
    stages = [
        ("01 / Preparazione locale", "PDF sintetico + indice locale (nessuna scansione Drive)"),
        ("02 / extract_documents", "PDF nativo -> JSON testo/layout + manifest CSV"),
        ("03 / extract_events", "Parser WORKFLOW -> events.csv + pages.csv"),
        ("04 / filter_midnight", "events.cleaned.csv + audit delle rimozioni (zero nel campione)"),
        ("05 / pair_events", "Due coppie complete + report con un evento non abbinato"),
        ("06 / enrich_shifts", "Durate 8 e 11 ore; categorie M e N"),
        ("07 / summarize_shifts", "Riepilogo CSV + Excel; confronto automatico delle aspettative"),
    ]
    for i, (label, detail) in enumerate(stages):
        y = 220 + i * 94
        d.rounded_rectangle((60, y, 1540, y + 78), radius=10, fill="#203850")
        d.text((80, y + 23), label, font=font(23), fill="#64dbc1")
        d.text((510, y + 23), detail, font=font(22), fill="white")
    im.save(out / "04-pipeline.png")
    print(out)
