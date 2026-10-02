"""Offline regression tests for mixed PDF extraction and OCR failure handling."""

from io import BytesIO
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

from PIL import Image
import pytest
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen.canvas import Canvas

from core.documents import ocr as engine
from core.documents.ocr import OcrError
from core.documents.ocr_options import OcrOptions
from core.documents.page_extraction import extract_document_pages
from core.documents.workers import extract_and_write
from core.pdf import extract_layout


TSV = (
    "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n"
    "5\t1\t1\t1\t1\t1\t100\t200\t80\t30\t91.5\t01/03/2024\n"
    "5\t1\t1\t1\t1\t2\t400\t201\t70\t30\t85.5\t07:58\n"
    "5\t1\t1\t1\t1\t3\t800\t200\t70\t30\t60.0\t14:12\n"
)


def make_pdf(*kinds: str) -> bytes:
    stream = BytesIO()
    canvas = Canvas(stream, pagesize=(300, 400))
    for kind in kinds:
        if kind in {"scan", "header_scan"}:
            image = Image.new("RGB", (300, 350), "white")
            canvas.drawImage(ImageReader(image), 0, 0, width=300, height=350)
        if kind in {"native", "header_scan"}:
            canvas.drawString(20, 380, "RIEPILOGO PRESENZE MARZO 2024")
        if kind == "numeric":
            canvas.drawString(20, 350, "07:58 14:12")
        if kind == "native":
            canvas.drawString(20, 350, "01/03/2024 08:00 14:00")
        canvas.showPage()
    canvas.save()
    return stream.getvalue()


def extract(data: bytes) -> dict:
    return extract_document_pages(
        data,
        min_normal_score=0.72,
        min_score_delta=0.08,
        ocr=OcrOptions(enabled=True),
    )


@pytest.fixture
def fake_tesseract(monkeypatch):
    calls = []
    monkeypatch.setattr(engine, "check_tesseract", lambda _options: "tesseract")

    def run(arguments, options):
        path = Path(arguments[1])
        with Image.open(path) as image:
            calls.append((path, image.size))
        return TSV

    monkeypatch.setattr(engine, "_run_tesseract", run)
    return calls


def test_mixed_pdf_keeps_native_text_page_numbers_coordinates_and_blank_pages(fake_tesseract):
    data = make_pdf("native", "scan", "blank")
    result = extract(data)
    assert len(fake_tesseract) == 1
    assert result["mode"] == "mixed"
    assert result["ocr_pages"] == 1
    assert result["has_text_layer"] is True
    assert [p["method"] for p in result["pages"]] == ["normal", "ocr", "blank"]
    assert "08:00 14:00" in result["text"]
    assert "07:58 14:12" in result["text"]
    assert result["layout"]["pages"][0] == extract_layout(data)["pages"][0]
    scan = result["layout"]["pages"][1]
    assert scan["lines"][0]["line_id"] == "p2_l1"
    assert scan["lines"][0]["text"] == "01/03/2024 07:58 14:12"
    assert scan["words"][0]["x0"] == 24.0
    assert scan["words"][0]["confidence"] == 91.5
    assert result["pages"][1]["ocr_confidence"] == 79.0
    assert not fake_tesseract[0][0].exists()


def test_native_header_does_not_hide_scanned_body(fake_tesseract):
    result = extract(make_pdf("header_scan"))
    assert len(fake_tesseract) == 1
    assert result["pages"][0]["ocr_reason"] == "image_region_without_sufficient_text"
    assert "07:58" in result["text"]


@pytest.mark.parametrize("kind", ["native", "numeric"])
def test_short_native_pdf_needs_no_tesseract(monkeypatch, kind):
    monkeypatch.setattr(engine, "check_tesseract", lambda _: pytest.fail("unexpected OCR"))
    result = extract(make_pdf(kind))
    assert result["ocr_pages"] == 0
    assert result["mode"] == "normal"


def test_scanned_artifact_records_original_text_layer_and_layout(tmp_path, fake_tesseract):
    result = extract_and_write(
        make_pdf("scan"),
        {"file_id": "local::scan", "employee": "Test"},
        str(tmp_path),
        0.72,
        0.08,
        OcrOptions(enabled=True),
    )
    assert result["status"] == "success"
    assert result["has_text_layer"] is False
    payload = json.loads((tmp_path / result["doc_json"]).read_text(encoding="utf-8"))
    assert payload["extraction"]["selected_mode"] == "ocr"
    assert payload["extraction"]["pages"][0]["method"] == "ocr"
    assert payload["document"]["full_text"] == "01/03/2024 07:58 14:12"
    assert payload["layout"]["pages"][0]["lines"][0]["line_id"] == "p1_l1"


@pytest.mark.parametrize("failure", ["ocr_timeout", "ocr_missing_languages", "ocr_unavailable"])
def test_failed_scanned_page_does_not_write_partial_native_document(tmp_path, monkeypatch, failure):
    def fail(_options):
        raise OcrError(failure)

    monkeypatch.setattr(engine, "check_tesseract", fail)
    result = extract_and_write(
        make_pdf("native", "scan"),
        {"file_id": "local::mixed"},
        str(tmp_path),
        0.72,
        0.08,
        OcrOptions(enabled=True),
    )
    assert result["status"] == "failed"
    assert failure in result["reason"]
    assert not list(tmp_path.rglob("*.json"))


def test_disabled_ocr_retains_missing_text_layer_behavior(tmp_path):
    result = extract_and_write(make_pdf("scan"), {"file_id": "scan"}, str(tmp_path), 0.72, 0.08)
    assert result["reason"] == "missing_text_layer"


def test_empty_ocr_output_is_not_a_success(monkeypatch, fake_tesseract):
    monkeypatch.setattr(engine, "_run_tesseract", lambda *_: TSV.splitlines()[0] + "\n")
    with pytest.raises(OcrError, match="ocr_empty_page"):
        extract(make_pdf("scan"))


def test_tsv_keeps_literal_quotes_and_rejects_invalid_output():
    result = engine.layout_from_tsv(
        TSV.replace("01/03/2024", '"marzo"'),
        page_no=4,
        width=300,
        height=400,
        image_size=(1200, 1600),
        origin=(10, 20),
    )
    assert result["words"][0]["text"] == '"marzo"'
    assert result["words"][0]["x0"] == 35
    assert result["words"][0]["y0"] == 70
    with pytest.raises(OcrError, match="ocr_invalid_output"):
        engine.layout_from_tsv("nonsense", page_no=1, width=1, height=1, image_size=(1, 1))


def test_tesseract_timeout_and_thread_limit(monkeypatch):
    def timeout(arguments, **kwargs):
        assert kwargs["env"]["OMP_THREAD_LIMIT"] == "1"
        assert kwargs["timeout"] == 12
        assert kwargs.get("shell", False) is False
        raise subprocess.TimeoutExpired(arguments, 12)

    monkeypatch.setattr(engine.subprocess, "run", timeout)
    with pytest.raises(OcrError, match="ocr_timeout"):
        engine._run_tesseract(["tesseract"], OcrOptions(timeout_seconds=12))


def test_missing_language_fails_even_if_tesseract_can_run(monkeypatch):
    engine.check_tesseract.cache_clear()
    monkeypatch.setattr(engine.shutil, "which", lambda _: "tesseract")
    monkeypatch.setattr(engine, "_run_tesseract", lambda *_: "List of languages (1):\neng\n")
    with pytest.raises(OcrError, match="ocr_missing_languages: ita"):
        engine.check_tesseract(OcrOptions(enabled=True))
    engine.check_tesseract.cache_clear()


def test_oversized_page_is_rejected_before_render(monkeypatch):
    monkeypatch.setattr(engine, "check_tesseract", lambda _: "tesseract")
    page = SimpleNamespace(width=10000, height=10000, page_number=1)
    with pytest.raises(OcrError, match="ocr_page_too_large"):
        engine.recognize_page(page, OcrOptions())


@pytest.mark.parametrize(
    "values",
    [
        {"enabled": "false"},
        {"dpi": 0},
        {"dpi": True},
        {"psm": 0},
        {"timeout_seconds": float("nan")},
        {"timeout_seconds": -1},
        {"max_pixels": 0},
        {"language": ""},
        {"executable": None},
    ],
)
def test_invalid_ocr_settings_fail_early(values):
    with pytest.raises(ValueError):
        OcrOptions(**values)
