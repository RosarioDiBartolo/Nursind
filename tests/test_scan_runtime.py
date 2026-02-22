from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.drive_service.io_json import load_json  # noqa: E402
from src.scan_directory import runtime  # noqa: E402


class _FakeFilesApi:
    def __init__(self, name: str = "RootFolder"):
        self._name = name

    def get(self, **_kwargs):
        return self

    def execute(self):
        return {"name": self._name}


class _FakeDrive:
    def __init__(self, name: str = "RootFolder"):
        self._files = _FakeFilesApi(name=name)

    def files(self):
        return self._files


class _FakeFailingFilesApi:
    def get(self, **_kwargs):
        return self

    def execute(self):
        raise RuntimeError("cannot fetch root name")


class _FakeFailingDrive:
    def files(self):
        return _FakeFailingFilesApi()


def test_merge_reports_to_maps_last_wins():
    reports = [
        {
            "included": [
                {
                    "employee": "A",
                    "employee_id": "1",
                    "file_id": "f1",
                    "file_name": "a.pdf",
                    "drive_path": "/A/a.pdf",
                    "type": "file",
                }
            ],
            "filtered": [],
        },
        {
            "included": [
                {
                    "employee": "A",
                    "employee_id": "1",
                    "file_id": "f1",
                    "file_name": "a-new.pdf",
                    "drive_path": "/A/a-new.pdf",
                    "type": "file",
                }
            ],
            "filtered": [],
        },
    ]

    included_map, filtered_map = runtime.merge_reports_to_maps(reports)
    assert len(included_map) == 1
    assert included_map["f1"].file_name == "a-new.pdf"
    assert filtered_map == {}


def test_get_root_name_returns_none_on_error():
    root_name = runtime.get_root_name(_FakeFailingDrive(), "root-1")
    assert root_name is None


def test_get_root_name_returns_none_when_root_is_missing():
    root_name = runtime.get_root_name(_FakeDrive(), None)
    assert root_name is None


def test_run_scan_happy_path(monkeypatch, tmp_path):
    employees = [
        {"id": "e1", "name": "Alice", "mimeType": runtime.FOLDER_MIME},
        {"id": "e2", "name": "Bob", "mimeType": runtime.FOLDER_MIME},
    ]
    monkeypatch.setattr(runtime, "list_children", lambda _drive, _root: employees)

    def fake_build_folder_report(_creds, emp, _terms, *, root_prefix=None):
        return {
            "employee": emp["name"],
            "employee_id": emp["id"],
            "counts": {"included": 1, "filtered_files": 0, "filtered_folders": 0},
            "included": [
                {
                    "employee": emp["name"],
                    "employee_id": emp["id"],
                    "file_id": f"{emp['id']}-f1",
                    "file_name": "doc.pdf",
                    "drive_path": f"/{root_prefix or 'Root'}/{emp['name']}/doc.pdf",
                    "type": "file",
                }
            ],
            "filtered": [],
        }

    monkeypatch.setattr(runtime, "build_folder_report", fake_build_folder_report)

    included_path = tmp_path / "included.index.json"
    filtered_path = tmp_path / "filtered.index.json"
    report_path = tmp_path / "scan_directory.report.json"

    report = runtime.run_scan(
        creds=object(),
        drive=_FakeDrive(),
        root_id="root-1",
        workers=2,
        included_path=str(included_path),
        filtered_path=str(filtered_path),
        report_path=str(report_path),
        exclude_terms=[],
    )

    assert included_path.exists()
    assert filtered_path.exists()
    assert report_path.exists()
    assert report["employee_total"] == 2
    assert report["employee_succeeded"] == 2
    assert report["employee_failed"] == 0
    assert report["included_total"] == 2
    assert report["filtered_total"] == 0
    saved_report = load_json(str(report_path))
    assert saved_report["employee_total"] == 2
    assert saved_report["included_path"] == str(included_path)
    assert saved_report["filtered_path"] == str(filtered_path)
    assert isinstance(saved_report["duration_seconds"], float)


def test_run_scan_partial_failure_continues(monkeypatch, tmp_path):
    employees = [
        {"id": "e1", "name": "Alice", "mimeType": runtime.FOLDER_MIME},
        {"id": "e2", "name": "Bob", "mimeType": runtime.FOLDER_MIME},
    ]
    monkeypatch.setattr(runtime, "list_children", lambda _drive, _root: employees)

    def fake_build_folder_report(_creds, emp, _terms, *, root_prefix=None):
        if emp["id"] == "e2":
            raise RuntimeError("boom")
        return {
            "employee": emp["name"],
            "employee_id": emp["id"],
            "counts": {"included": 1, "filtered_files": 0, "filtered_folders": 0},
            "included": [
                {
                    "employee": emp["name"],
                    "employee_id": emp["id"],
                    "file_id": f"{emp['id']}-f1",
                    "file_name": "doc.pdf",
                    "drive_path": f"/{root_prefix or 'Root'}/{emp['name']}/doc.pdf",
                    "type": "file",
                }
            ],
            "filtered": [],
        }

    monkeypatch.setattr(runtime, "build_folder_report", fake_build_folder_report)

    included_path = tmp_path / "included.index.json"
    filtered_path = tmp_path / "filtered.index.json"
    report_path = tmp_path / "scan_directory.report.json"

    report = runtime.run_scan(
        creds=object(),
        drive=_FakeDrive(),
        root_id="root-1",
        workers=2,
        included_path=str(included_path),
        filtered_path=str(filtered_path),
        report_path=str(report_path),
        exclude_terms=[],
    )

    assert report["employee_total"] == 2
    assert report["employee_succeeded"] == 1
    assert report["employee_failed"] == 1
    assert report["included_total"] == 1
    assert len(report["scan_errors"]) == 1
    assert report["scan_errors"][0]["employee"] == "Bob"


def test_run_scan_ignores_non_folder_children(monkeypatch, tmp_path):
    children = [
        {"id": "e1", "name": "Alice", "mimeType": runtime.FOLDER_MIME},
        {"id": "f1", "name": "loose.pdf", "mimeType": "application/pdf"},
    ]
    monkeypatch.setattr(runtime, "list_children", lambda _drive, _root: children)

    seen_employee_ids: list[str] = []

    def fake_build_folder_report(_creds, emp, _terms, *, root_prefix=None):
        seen_employee_ids.append(emp["id"])
        return {
            "employee": emp["name"],
            "employee_id": emp["id"],
            "counts": {"included": 0, "filtered_files": 0, "filtered_folders": 0},
            "included": [],
            "filtered": [],
        }

    monkeypatch.setattr(runtime, "build_folder_report", fake_build_folder_report)

    report = runtime.run_scan(
        creds=object(),
        drive=_FakeDrive(),
        root_id="root-1",
        workers=2,
        included_path=str(tmp_path / "included.index.json"),
        filtered_path=str(tmp_path / "filtered.index.json"),
        report_path=str(tmp_path / "scan_directory.report.json"),
        exclude_terms=[],
    )

    assert seen_employee_ids == ["e1"]
    assert report["employee_total"] == 1


def test_run_scan_uses_default_exclude_terms_when_none(monkeypatch, tmp_path):
    monkeypatch.setattr(
        runtime,
        "exclude_terms_normalized",
        ["term-a", "term-b"],
    )
    monkeypatch.setattr(
        runtime,
        "list_children",
        lambda _drive, _root: [{"id": "e1", "name": "Alice", "mimeType": runtime.FOLDER_MIME}],
    )

    received_terms = []

    def fake_build_folder_report(_creds, _emp, terms, *, root_prefix=None):
        received_terms.append(list(terms))
        return {
            "employee": "Alice",
            "employee_id": "e1",
            "counts": {"included": 0, "filtered_files": 0, "filtered_folders": 0},
            "included": [],
            "filtered": [],
        }

    monkeypatch.setattr(runtime, "build_folder_report", fake_build_folder_report)

    runtime.run_scan(
        creds=object(),
        drive=_FakeDrive(),
        root_id="root-1",
        workers=1,
        included_path=str(tmp_path / "included.index.json"),
        filtered_path=str(tmp_path / "filtered.index.json"),
        report_path=str(tmp_path / "scan_directory.report.json"),
        exclude_terms=None,
    )

    assert received_terms == [["term-a", "term-b"]]
