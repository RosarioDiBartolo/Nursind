"""Create a synthetic native-text PDF and isolated local configuration."""

from pathlib import Path

from reportlab.pdfgen import canvas

from core.config import load_pipeline_config
from core.drive.index.build_local_pdf_index import build_local_pdf_index
from core.drive.io_json import write_json


def prepare_demo(repository: Path, name: str) -> Path:
    root = repository / f"{name}.out_dir"
    if root.exists():
        raise FileExistsError(f"Use a new --name; existing artifacts are preserved: {root}")
    inputs = root / "input"
    inputs.mkdir(parents=True)
    pdf = canvas.Canvas(str(inputs / "synthetic-monthly.pdf"), pagesize=(595, 842), invariant=1)
    pdf.setTitle("Nursind - synthetic demonstration")
    pdf.setAuthor("Synthetic demo")
    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(30, 800, "NURSIND / DOCUMENT PIPELINE")
    pdf.setFont("Helvetica", 11)
    pdf.drawString(30, 775, "DATI INTERAMENTE SINTETICI - DEMO-001")
    pdf.drawString(30, 750, "WORKFLOW CARTELLINO MENSILE SETTEMBRE 2026")
    pdf.drawString(30, 720, "Campione dimostrativo: due turni e una timbratura isolata.")
    pdf.setFont("Helvetica", 5)
    for i, label in enumerate(("Ent 1", "Usc 1", "Ent 2", "Usc 2", "Ent 3", "Usc 3")):
        pdf.drawString(59.61 + i * 22, 685, label)
    pdf.drawString(210, 685, "Nota dimostrativa")
    pdf.setFont("Helvetica", 6)
    for y, day, weekday, slots, note in (
        (665, "1", "M", {0: "08:00", 1: "16:00"}, "Diurno completo"),
        (640, "2", "M", {0: "20:00", 1: "07:00"}, "Notturno: uscita il giorno successivo"),
        (615, "4", "V", {1: "14:00"}, "Anomalia: entrata assente"),
    ):
        pdf.drawString(31.58, y, day)
        pdf.drawString(44.61, y, weekday)
        for slot, value in slots.items():
            pdf.drawString(59.61 + slot * 22, y, value)
        pdf.drawString(210, y, note)
    pdf.setFont("Helvetica", 10)
    pdf.drawString(30, 560, "Layout vincolato al parser mensile; nessun dato personale reale.")
    pdf.drawString(30, 540, "Le celle vuote restano vuote: non sono orari da ricostruire.")
    pdf.showPage()
    pdf.save()
    config_path = root / "demo-config.json"
    write_json(
        str(config_path),
        {
            "pipeline": {"name": name},
            "drive": {"root_id": "synthetic-local-only"},
            "steps": {
                "extract_documents": {
                    "workers": 1,
                    "extract_workers": 1,
                    "max_in_flight": 1,
                    "ocr": {"enabled": False},
                },
                "pair_events": {"max_gap_hours": 16, "keep_inferred_column": True},
                "enrich_shifts": {"min_hours": 6, "include_holidays": False},
            },
        },
    )
    config = load_pipeline_config(config_path)
    index, _ = build_local_pdf_index(inputs, identity="DEMO-001")
    config.paths.scan_dir.mkdir(parents=True)
    index.save_index(str(config.paths.scan_included_index))
    return config_path
