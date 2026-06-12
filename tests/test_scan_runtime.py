from __future__ import annotations

from pathlib import Path

from core.drive.io_json import load_json
from core.drive.scan import runtime


class _Files:
    def __init__(self, name: str = "Root"):
        self.name = name

    def get(self, **_kwargs):
        return self

    def execute(self):
        return {"name": self.name}


class _Drive:
    def files(self):
        return _Files()


def _report(employee: dict, included: int = 1) -> dict:
    files = []
    if included:
        files.append(
            {
                "employee": employee["name"],
                "employee_id": employee["id"],
                "file_id": f"{employee['id']}-file",
                "file_name": "sample.pdf",
                "drive_path": f"/Root/{employee['name']}/sample.pdf",
                "type": "file",
            }
        )
    return {
        "employee": employee["name"],
        "employee_id": employee["id"],
        "counts": {"included": included, "filtered_files": 0, "filtered_folders": 0},
        "included": files,
        "filtered": [],
    }


def test_scan_writes_nested_outputs(monkeypatch, tmp_path: Path) -> None:
    employee = {"id": "e1", "name": "Alice", "mimeType": runtime.FOLDER_MIME}
    monkeypatch.setattr(runtime, "list_children", lambda *_: [employee])
    monkeypatch.setattr(runtime, "build_folder_report", lambda *_args, **_kwargs: _report(employee))
    scan = tmp_path / "pipeline" / "scan"

    report = runtime.run_scan(
        creds=object(),
        drive=_Drive(),
        root_id="root",
        workers=1,
        included_path=str(scan / "included.index.json"),
        filtered_path=str(scan / "filtered.index.json"),
        report_path=str(scan / "report.json"),
        exclude_terms=[],
    )

    assert report["included_total"] == 1
    assert load_json(str(scan / "report.json"))["employee_total"] == 1


def test_scan_continues_after_employee_failure(monkeypatch, tmp_path: Path) -> None:
    employees = [
        {"id": "e1", "name": "Alice", "mimeType": runtime.FOLDER_MIME},
        {"id": "e2", "name": "Bob", "mimeType": runtime.FOLDER_MIME},
    ]
    monkeypatch.setattr(runtime, "list_children", lambda *_: employees)

    def build(_creds, employee, _terms, **_kwargs):
        if employee["id"] == "e2":
            raise RuntimeError("boom")
        return _report(employee)

    monkeypatch.setattr(runtime, "build_folder_report", build)
    report = runtime.run_scan(
        creds=object(),
        drive=_Drive(),
        root_id="root",
        workers=2,
        included_path=str(tmp_path / "included.json"),
        filtered_path=str(tmp_path / "filtered.json"),
        report_path=str(tmp_path / "report.json"),
        exclude_terms=[],
    )

    assert report["employee_succeeded"] == 1
    assert report["employee_failed"] == 1
    assert report["scan_errors"][0]["employee"] == "Bob"


def test_scan_ignores_non_employee_files(monkeypatch, tmp_path: Path) -> None:
    employee = {"id": "e1", "name": "Alice", "mimeType": runtime.FOLDER_MIME}
    monkeypatch.setattr(
        runtime,
        "list_children",
        lambda *_: [employee, {"id": "pdf", "name": "loose.pdf", "mimeType": "application/pdf"}],
    )
    seen = []
    monkeypatch.setattr(
        runtime,
        "build_folder_report",
        lambda _creds, item, _terms, **_kwargs: seen.append(item["id"]) or _report(item, 0),
    )
    runtime.run_scan(
        creds=object(),
        drive=_Drive(),
        root_id="root",
        workers=1,
        included_path=str(tmp_path / "included.json"),
        filtered_path=str(tmp_path / "filtered.json"),
        report_path=str(tmp_path / "report.json"),
        exclude_terms=[],
    )
    assert seen == ["e1"]


def test_scan_reports_employees_without_included_files(monkeypatch, tmp_path: Path) -> None:
    employee = {"id": "e1", "name": "Alice", "mimeType": runtime.FOLDER_MIME}
    monkeypatch.setattr(runtime, "list_children", lambda *_: [employee])
    monkeypatch.setattr(runtime, "build_folder_report", lambda *_args, **_kwargs: _report(employee, 0))
    report = runtime.run_scan(
        creds=object(),
        drive=_Drive(),
        root_id="root",
        workers=1,
        included_path=str(tmp_path / "included.json"),
        filtered_path=str(tmp_path / "filtered.json"),
        report_path=str(tmp_path / "report.json"),
        exclude_terms=[],
    )
    assert report["employees_without_included_files_count"] == 1


def test_scan_merge_is_deterministic_for_duplicate_ids() -> None:
    first = _report({"id": "e1", "name": "Alice"}, 1)
    second = _report({"id": "e1", "name": "Alice"}, 1)
    second["included"][0]["file_name"] = "new.pdf"
    included, _filtered = runtime.merge_reports_to_maps([first, second])
    assert included["e1-file"].file_name == "new.pdf"
