from typing import Iterable, List, Tuple

from .drive_client import get_drive_service, list_children
from .logging_utils import get_logger

logger = get_logger()

PDF_MIME = "application/pdf"
ZIP_MIME_TYPES = {"application/zip", "application/x-zip-compressed"}

def normalize_term(value: str) -> str:
    value = value.lower().strip().replace("_", " ").replace("-", " ")
    return " ".join(value.split())


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


def collect_files_recursive(
    drive, emp, exclude_terms: Iterable[str]
) -> Tuple[List[dict], List[dict]]:
    stack = [(emp["id"], emp["name"])]
    files = []
    excluded_folders = []

    while stack:
        fid, name = stack.pop()
        term = folder_excluded(name, exclude_terms)
        if term:
            logger.debug("[%s] skipping folder: %s", emp["name"], name)
            excluded_folders.append(
                {"folder_id": fid, "folder_name": name, "reason": term}
            )
            continue
        for item in list_children(drive, fid):
            if item["mimeType"] == "application/vnd.google-apps.folder":
                stack.append((item["id"], item["name"]))
            elif item["mimeType"] == PDF_MIME:
                files.append(
                    {
                        "file_id": item["id"],
                        "file_name": item["name"],
                        "mimeType": item["mimeType"],
                    }
                )
            elif item["mimeType"] in ZIP_MIME_TYPES or item["name"].lower().endswith(".zip"):
                files.append(
                    {
                        "file_id": item["id"],
                        "file_name": item["name"],
                        "mimeType": item["mimeType"],
                        "container": "zip",
                    }
                )

    return files, excluded_folders


def file_excluded(filename: str, exclude_terms: Iterable[str]) -> str | None:
    if not exclude_terms:
        return None
    n = filename.lower()
    for term in exclude_terms:
        if term in n:
            return term
    return None


def build_employee_report(creds, emp, exclude_terms: Iterable[str]):
    drive = get_drive_service(creds)
    files, excluded_folders = collect_files_recursive(drive, emp, exclude_terms)
    included = []
    skipped = []

    for item in files:
        fname = item["file_name"]
        term = file_excluded(fname, exclude_terms)
        if term:
            skipped.append(
                {
                    "file_id": item["file_id"],
                    "file_name": fname,
                    "mimeType": item.get("mimeType"),
                    "container": item.get("container"),
                    "reason": term,
                }
            )
        else:
            included.append(
                {
                    "file_id": item["file_id"],
                    "file_name": fname,
                    "mimeType": item.get("mimeType"),
                    "container": item.get("container"),
                }
            )

    return {
        "employee": emp["name"],
        "employee_id": emp["id"],
        "counts": {
            "included": len(included),
            "skipped_files": len(skipped),
            "excluded_folders": len(excluded_folders),
        },
        "included": included,
        "skipped": skipped,
        "excluded_folders": excluded_folders,
    }
