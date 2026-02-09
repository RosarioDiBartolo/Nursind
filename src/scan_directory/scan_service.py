from __future__ import annotations

from typing import Iterable, List, Tuple

from drive_service.drive_client import get_drive_service, list_children
from drive_service.logging_utils import get_logger
from drive_service.names import normalize_term

logger = get_logger()

PDF_MIME = "application/pdf"
ZIP_MIME_TYPES = {"application/zip", "application/x-zip-compressed"}
FOLDER_MIME = "application/vnd.google-apps.folder"


def find_excluding_term(value: str, exclude_terms: Iterable[str]) -> str | None:
    if not exclude_terms:
        return None
    normalized = normalize_term(value)
    for term in exclude_terms:
        if term == normalized:
            return term
    return None


def folder_excluded(name: str, exclude_terms: Iterable[str]) -> str | None:
    return find_excluding_term(name, exclude_terms)


def file_excluded(filename: str, exclude_terms: Iterable[str]) -> str | None:
    if not exclude_terms:
        return None
    n = filename.lower()
    for term in exclude_terms:
        if term in n:
            return term
    return None


def _is_pdf_or_zip(item: dict) -> bool:
    mime_type = item.get("mimeType")
    if mime_type == PDF_MIME:
        return True
    if mime_type in ZIP_MIME_TYPES:
        return True
    name = (item.get("name") or "").lower()
    return name.endswith(".zip")


def collect_files_recursive(
    drive,
    emp: dict,
    exclude_terms: Iterable[str],
    *,
    root_prefix: str | None = None,
) -> Tuple[List[dict], List[dict]]:
    base_path = f"/{emp['name']}" if not root_prefix else f"/{root_prefix}/{emp['name']}"
    stack = [(emp["id"], emp["name"], base_path)]
    files: List[dict] = []
    excluded_folders: List[dict] = []

    while stack:
        fid, name, path = stack.pop()
        term = folder_excluded(name, exclude_terms)
        if term:
            logger.debug("[%s] skipping folder: %s", emp["name"], name)
            excluded_folders.append(
                {
                    "file_id": fid,
                    "file_name": name,
                    "drive_path": path,
                    "type": "folder",
                    "reason": term,
                }
            )
            continue
        for item in list_children(drive, fid):
            if item["mimeType"] == FOLDER_MIME:
                child_path = f"{path}/{item['name']}"
                stack.append((item["id"], item["name"], child_path))
            elif _is_pdf_or_zip(item):
                file_path = f"{path}/{item['name']}"
                files.append(
                    {
                        "file_id": item["id"],
                        "file_name": item["name"],
                        "drive_path": file_path,
                    }
                )

    return files, excluded_folders


def build_employee_report(
    creds, emp: dict, exclude_terms: Iterable[str], *, root_prefix: str | None = None
) -> dict:
    drive = get_drive_service(creds)
    files, excluded_folders = collect_files_recursive(
        drive, emp, exclude_terms, root_prefix=root_prefix
    )
    included: List[dict] = []
    filtered: List[dict] = []

    for item in files:
        fname = item["file_name"]
        term = file_excluded(fname, exclude_terms)
        payload = {
            "employee": emp["name"],
            "employee_id": emp["id"],
            "file_id": item["file_id"],
            "file_name": fname,
            "drive_path": item.get("drive_path"),
            "type": "file",
        }
        if term:
            payload["reason"] = term
            filtered.append(payload)
        else:
            included.append(payload)

    filtered.extend(
        [
            {
                "employee": emp["name"],
                "employee_id": emp["id"],
                "file_id": item["file_id"],
                "file_name": item["file_name"],
                "drive_path": item.get("drive_path"),
                "type": "folder",
                "reason": item.get("reason"),
            }
            for item in excluded_folders
        ]
    )

    return {
        "employee": emp["name"],
        "employee_id": emp["id"],
        "counts": {
            "included": len(included),
            "filtered_files": len([f for f in filtered if f.get("type") == "file"]),
            "filtered_folders": len([f for f in filtered if f.get("type") == "folder"]),
        },
        "included": included,
        "filtered": filtered,
    }
