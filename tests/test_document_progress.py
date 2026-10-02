from concurrent.futures import Future
import logging
import os
from pathlib import Path
import subprocess
import sys
import threading

from core.documents.progress import completed_with_progress


def test_wait_reports_pending_work_then_delivers_completion(caplog):
    future = Future()
    with caplog.at_level(logging.INFO):
        iterator = completed_with_progress([future], stage="downloads", interval=0.01)
        timer = threading.Timer(0.06, lambda: future.set_result("done"))
        timer.start()
        try:
            assert list(iterator) == [future]
        finally:
            timer.join()
    assert "Waiting for downloads: 1 tasks outstanding" in caplog.text
    assert future.result() == "done"


def test_windows_spawn_worker_emits_document_and_page_progress(tmp_path):
    # Real spawn, not an executor mock: INFO logging must survive process startup.
    from io import BytesIO
    from reportlab.pdfgen.canvas import Canvas

    stream = BytesIO()
    canvas = Canvas(stream)
    canvas.drawString(40, 750, "RIEPILOGO PRESENZE MARZO 2024")
    canvas.save()
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(stream.getvalue())
    script = tmp_path / "check_worker.py"
    script.write_text(
        """
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path
import sys
from core.drive.logging_utils import setup_logging
from core.documents.workers import extract_and_write
from core.documents.ocr_options import OcrOptions

if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=1, mp_context=get_context("spawn"),
                             initializer=setup_logging, initargs=(False,)) as pool:
        result = pool.submit(extract_and_write, Path(sys.argv[1]).read_bytes(),
                             {"file_id": "sample", "file_name": "sample.pdf"},
                             sys.argv[2], .72, .08, OcrOptions(enabled=True)).result(timeout=30)
        assert result["status"] == "success", result
""",
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, str(script), str(pdf_path), str(tmp_path / "output")],
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stderr
    for message in (
        "Extraction started: sample.pdf",
        "Page 1/1 started:",
        "Page 1/1 finished:",
        "Extraction saved: sample.pdf",
    ):
        assert message in result.stderr
