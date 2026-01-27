from drive_scanner.reports import (
    build_base_employees,
    finalize_index,
    merge_report_into_base,
)


def test_merge_report_into_existing_employee():
    index_employees = [
        {
            "employee": "Alice Rossi",
            "employee_id": "E001",
            "files": [{"file_id": "a1", "file_name": "A.pdf"}],
        }
    ]
    base = build_base_employees(index_employees)
    report = {
        "root_id": "root",
        "generated_at": "2024-01-01T00:00:00Z",
        "employee_count": 1,
        "included": [
            {
                "employee": "Alice Rossi",
                "employee_id": "E001",
                "files": [{"file_id": "a2", "file_name": "B.pdf"}],
            }
        ],
        "excluded": [],
    }

    merge_report_into_base(base, report)
    merged = finalize_index(base, index_employees, "root")

    assert len(merged["included"]) == 1
    assert merged["included"][0]["employee"] == "Alice Rossi"
    ids = {item["file_id"] for item in merged["included"][0]["files"]}
    assert ids == {"a2"}


def test_merge_report_dedupes_by_id():
    index_employees = [
        {
            "employee": "Bob Bianchi",
            "employee_id": "E002",
            "files": [],
        }
    ]
    base = build_base_employees(index_employees)
    report = {
        "root_id": "root",
        "generated_at": "2024-01-01T00:00:00Z",
        "employee_count": 1,
        "included": [
            {
                "employee": "Bob Bianchi",
                "employee_id": "E002",
                "files": [
                    {"file_id": "b1", "file_name": "X.pdf"},
                    {"file_id": "b1", "file_name": "X.pdf"},
                ],
            }
        ],
        "excluded": [],
    }

    merge_report_into_base(base, report)
    merged = finalize_index(base, index_employees, "root")

    assert len(merged["included"]) == 1
    assert len(merged["included"][0]["files"]) == 1
