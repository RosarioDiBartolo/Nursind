from drive_scanner.filter_scan import (
    _build_base_employees,
    _finalize_employees,
    _merge_report_into_base,
)


def test_merge_legacy_files_report_into_existing_employee():
    manifest_employees = [
        {
            "employee": "Alice Rossi",
            "employee_id": "E001",
            "included": [{"file_id": "a1", "file_name": "A.pdf"}],
            "skipped": [],
            "excluded_folders": [],
        }
    ]
    base = _build_base_employees(manifest_employees)
    legacy_report = {
        "files": [
            {
                "status": "success",
                "employee": "Alice Rossi",
                "file_id": "a2",
                "file_name": "B.pdf",
                "outputs": {"report_json": "out/report.json"},
            }
        ]
    }

    _merge_report_into_base(base, legacy_report)
    merged = _finalize_employees(base, manifest_employees)

    assert len(merged) == 1
    assert merged[0]["employee"] == "Alice Rossi"
    ids = {item["file_id"] for item in merged[0]["included"]}
    assert ids == {"a2"}


def test_merge_new_style_report_dedupes_by_id():
    manifest_employees = [
        {
            "employee": "Bob Bianchi",
            "employee_id": "E002",
            "included": [],
            "skipped": [],
            "excluded_folders": [],
        }
    ]
    base = _build_base_employees(manifest_employees)
    report = {
        "employees": [
            {
                "employee": "Bob Bianchi",
                "employee_id": "E002",
                "included": [
                    {"file_id": "b1", "file_name": "X.pdf"},
                    {"file_id": "b1", "file_name": "X.pdf"},
                ],
                "skipped": [],
                "excluded_folders": [],
            }
        ]
    }

    _merge_report_into_base(base, report)
    merged = _finalize_employees(base, manifest_employees)

    assert len(merged) == 1
    assert len(merged[0]["included"]) == 1
