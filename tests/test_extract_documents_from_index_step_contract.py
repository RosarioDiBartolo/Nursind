from __future__ import annotations

from pathlib import Path

import src.extract_documents_from_index.service as service
from src.extract_documents_from_index.service import process_many_index_documents, process_one_index_document
from tests.step_contract import assert_process_many_contract, assert_process_one_contract


def test_extract_documents_process_one_contract(monkeypatch, tmp_path: Path) -> None:
    doc_info = {
        "employee": "Mario Rossi",
        "employee_id": "emp-1",
        "file_id": "file-1",
        "file_name": "sample.pdf",
        "source_text_ref": "Mario Rossi/sample.txt",
    }

    monkeypatch.setattr(
        service,
        "download_pdf_bytes",
        lambda *_args, **_kwargs: {
            "status": "success",
            "data": b"%PDF-1.4",
            "doc": doc_info,
        },
    )
    monkeypatch.setattr(
        service,
        "extract_and_write",
        lambda *_args, **_kwargs: {
            "status": "success",
            "employee": "Mario Rossi",
            "file_id": "file-1",
            "file_name": "sample.pdf",
            "doc_json": "docs/sample.json",
            "selected_mode": "normal",
            "source_text_ref": "Mario Rossi/sample.txt",
        },
    )

    result = process_one_index_document(
        doc_info,
        creds=None,
        out_dir=str(tmp_path),
        min_normal_score=0.72,
        min_score_delta=0.08,
    )

    assert_process_one_contract(result, source_key="source_file_id")
    assert result["status"] == "ok"
    assert result["doc_json"] == "docs/sample.json"


def test_extract_documents_process_many_contract(monkeypatch, tmp_path: Path) -> None:
    success_doc = {
        "employee": "Mario Rossi",
        "file_id": "file-1",
        "file_name": "sample.pdf",
    }
    failure_doc = {
        "employee": "Giulia Bianchi",
        "file_id": "file-2",
        "file_name": "missing.pdf",
    }

    def fake_download(_creds, doc_info, _stop_event):
        if doc_info["file_id"] == "file-2":
            return {
                "status": "failed",
                "stage": "download",
                "reason": "not_found",
                "doc": doc_info,
            }
        return {
            "status": "success",
            "data": b"%PDF-1.4",
            "doc": doc_info,
        }

    monkeypatch.setattr(service, "download_pdf_bytes", fake_download)
    monkeypatch.setattr(
        service,
        "extract_and_write",
        lambda *_args, **_kwargs: {
            "status": "success",
            "employee": "Mario Rossi",
            "file_id": "file-1",
            "file_name": "sample.pdf",
            "doc_json": "docs/sample.json",
            "selected_mode": "normal",
            "source_text_ref": "Mario Rossi/sample.txt",
        },
    )

    report = process_many_index_documents(
        [success_doc, failure_doc],
        creds=None,
        out_dir=str(tmp_path),
        min_normal_score=0.72,
        min_score_delta=0.08,
    )

    assert_process_many_contract(report)
    assert int(report["stats"]["files_total"]) == 2
    assert int(report["stats"]["files_processed"]) == 1
    assert int(report["stats"]["files_error"]) == 1

