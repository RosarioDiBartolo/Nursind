from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import cartellino_parser.extract_documents_from_index.runtime as runtime  # noqa: E402
import cartellino_parser.extract_documents_from_index.service as service  # noqa: E402
from cartellino_parser.extract_documents_from_index.options import (  # noqa: E402
    ExtractDocumentsFromIndexOptions,
)


class _FakeFuture:
    def __init__(self, *, payload=None, error: Exception | None = None, cancel_allowed: bool = True):
        self._payload = payload
        self._error = error
        self._cancel_allowed = cancel_allowed
        self._cancelled = False
        self._started = False
        self.kind = "generic"

    def result(self):
        self._started = True
        if self._cancelled:
            raise RuntimeError("cancelled")
        if self._error is not None:
            raise self._error
        return self._payload

    def cancel(self):
        if not self._cancel_allowed or self._cancelled or self._started:
            return False
        self._cancelled = True
        return True


class _FakeDownloadPool:
    def __init__(self, *args, **kwargs):
        self._futures: list[_FakeFuture] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, fn, *args):
        future = _FakeFuture(payload=lambda: fn(*args))
        future.kind = "download"
        self._futures.append(future)
        return future


class _FakeExtractPool:
    def __init__(self, *args, **kwargs):
        self._futures: list[_FakeFuture] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, fn, *args):
        future = _FakeFuture(payload=lambda: fn(*args), cancel_allowed=False)
        future.kind = "extract"
        self._futures.append(future)
        return future


def _resolve_future_payload(future: _FakeFuture):
    payload = future._payload
    if callable(payload):
        future._payload = payload()


def test_run_extraction_drains_in_flight_extracts_on_keyboard_interrupt(monkeypatch, tmp_path: Path):
    docs = [
        {"file_id": "file-1", "employee": "Mario Rossi", "local": True},
        {"file_id": "file-2", "employee": "Giulia Bianchi", "local": True},
        {"file_id": "file-3", "employee": "Luca Neri", "local": True},
    ]
    state = {
        "stats": {
            "queued": 3,
            "processed": 0,
            "succeeded": 0,
            "failed": 0,
            "download_failed": 0,
            "extract_failed": 0,
            "excluded_missing_text_layer": 0,
            "used_vertical": 0,
            "interrupted": 0,
            "cancelled_downloads": 0,
            "cancelled_extracts": 0,
            "not_processed_due_to_interrupt": 0,
        },
        "docs": docs,
        "skip_excluded": True,
        "skip_included": True,
        "download_workers": 1,
        "extract_workers": 1,
        "max_in_flight": 8,
        "flush_every": 1,
        "log_every": 1,
        "included_map": {},
        "excluded_map": {},
        "text_rows_by_file_id": {},
        "included_index": SimpleNamespace(),
        "excluded_index": SimpleNamespace(),
        "included_path": str(tmp_path / "included.json"),
        "excluded_path": str(tmp_path / "excluded.json"),
        "report_path": str(tmp_path / "report.json"),
        "index_path": str(tmp_path / "source.index.json"),
        "runtime_issues": [],
    }
    captured = {}

    monkeypatch.setattr(runtime, "prepare_extraction_run", lambda _options: state)
    monkeypatch.setattr(runtime, "ThreadPoolExecutor", _FakeDownloadPool)
    monkeypatch.setattr(runtime, "ProcessPoolExecutor", _FakeExtractPool)
    monkeypatch.setattr(runtime, "_flush_progress", lambda **kwargs: None)

    def fake_download_pdf_bytes(_creds, doc, _stop_event):
        return {"status": "success", "data": b"%PDF-1.4", "doc": doc}

    def fake_extract_and_write(_pdf_bytes, doc, _out_dir, _min_normal_score, _min_score_delta):
        return {
            "status": "success",
            "employee": doc["employee"],
            "employee_id": None,
            "local": True,
            "file_id": doc["file_id"],
            "file_name": f'{doc["file_id"]}.pdf',
            "drive_path": None,
            "source_kind": "local_pdf",
            "archive_file_id": None,
            "archive_member_path": None,
            "source_text_ref": f'{doc["employee"]}/{doc["file_id"]}.txt',
            "doc_json": f'docs/{doc["file_id"]}.json',
            "has_text_layer": True,
            "selected_mode": "normal",
            "tried_vertical": False,
            "normal_quality": 0.9,
            "vertical_quality": None,
        }

    def fake_apply_extraction_result(
        result,
        *,
        stats,
        items,
        included_map,
        excluded_map,
        text_rows_by_file_id,
    ):
        items.append(dict(result))
        if result["status"] == "success":
            stats["succeeded"] += 1
            included_map[result["file_id"]] = {"doc_json": result["doc_json"]}
            text_rows_by_file_id[result["file_id"]] = {"doc_json": result["doc_json"]}
        else:
            stats["failed"] += 1
            excluded_map[(result.get("doc") or {}).get("file_id") or "missing"] = result

    def fake_finalize_extraction_run(*, options, state, items, start_ts):
        captured["state"] = state
        captured["items"] = list(items)
        return {
            "stage": "extract_documents_from_index",
            "status": "interrupted",
            "stats": dict(state["stats"]),
            "issues": list(state["runtime_issues"]),
            "row_totals": {"items": len(items), "issues": len(state["runtime_issues"])},
        }

    def fake_as_completed(futures):
        futures = list(futures)
        if futures and all(getattr(future, "kind", "") == "download" for future in futures):
            first = futures[0]
            second = futures[1]
            _resolve_future_payload(first)
            second._error = KeyboardInterrupt()
            return iter([first, second])

        ready = []
        for future in futures:
            if getattr(future, "_cancelled", False):
                continue
            _resolve_future_payload(future)
            ready.append(future)
        return iter(ready)

    monkeypatch.setattr(runtime, "download_pdf_bytes", fake_download_pdf_bytes)
    monkeypatch.setattr(runtime, "extract_and_write", fake_extract_and_write)
    monkeypatch.setattr(runtime, "apply_extraction_result", fake_apply_extraction_result)
    monkeypatch.setattr(runtime, "finalize_extraction_run", fake_finalize_extraction_run)
    monkeypatch.setattr(runtime, "as_completed", fake_as_completed)

    report = runtime.run_extraction(
        ExtractDocumentsFromIndexOptions(out=str(tmp_path), index="ignored"),
        auto_load_creds=False,
        configure_logging=False,
        return_report=True,
    )

    assert report["status"] == "interrupted"
    assert captured["state"]["stats"]["interrupted"] == 1
    assert captured["state"]["stats"]["cancelled_downloads"] == 1
    assert captured["state"]["stats"]["cancelled_extracts"] == 0
    assert captured["state"]["stats"]["not_processed_due_to_interrupt"] == 2
    assert [item["file_id"] for item in captured["items"]] == ["file-1"]
    assert captured["state"]["runtime_issues"] == [
        {
            "code": "interrupt",
            "message": (
                "Interrupted by user; drained submitted extraction tasks "
                "and cancelled pending work before finalization."
            ),
        }
    ]


def test_finalize_extraction_run_marks_interrupted_status(monkeypatch, tmp_path: Path):
    class DummyIndex:
        def __init__(self):
            self.files = {}
            self.saved_paths: list[str] = []

        def save_index(self, path):
            self.saved_paths.append(path)

    payload_holder = {}
    monkeypatch.setattr(service, "update_index_meta", lambda _index: None)
    monkeypatch.setattr(service, "write_text_extraction_rows", lambda _out, _rows: [])
    monkeypatch.setattr(service, "prune_stale_text_extraction_docs", lambda _out, _rows: None)
    monkeypatch.setattr(
        service,
        "write_json",
        lambda path, payload: payload_holder.update({"path": path, "payload": payload}),
    )

    options = ExtractDocumentsFromIndexOptions(out=str(tmp_path / "documents"), index="source.json")
    state = {
        "included_index": DummyIndex(),
        "excluded_index": DummyIndex(),
        "included_map": {},
        "excluded_map": {},
        "text_rows_by_file_id": {},
        "included_path": str(tmp_path / "documents" / "included.json"),
        "excluded_path": str(tmp_path / "documents" / "excluded.json"),
        "report_path": str(tmp_path / "documents" / "extract_documents_from_index.report.json"),
        "index_path": str(tmp_path / "scan" / "included.index.json"),
        "skip_included": True,
        "skip_excluded": True,
        "stats": {
            "queued": 2,
            "processed": 1,
            "succeeded": 1,
            "failed": 0,
            "download_failed": 0,
            "extract_failed": 0,
            "excluded_missing_text_layer": 0,
            "used_vertical": 0,
            "interrupted": 1,
            "cancelled_downloads": 1,
            "cancelled_extracts": 0,
            "not_processed_due_to_interrupt": 1,
        },
        "runtime_issues": [{"code": "interrupt", "message": "drained before finalization"}],
    }

    report = service.finalize_extraction_run(
        options=options,
        state=state,
        items=[],
        start_ts=0.0,
    )

    assert report["status"] == "interrupted"
    assert report["row_totals"]["issues"] == 1
    assert report["issues"] == [{"code": "interrupt", "message": "drained before finalization"}]
    assert payload_holder["payload"]["status"] == "interrupted"
