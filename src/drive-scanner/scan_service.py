from typing import Iterable, List, Tuple

from drive_client import get_drive_service, list_children
from logging_utils import get_logger

logger = get_logger()


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


def collect_pdfs_recursive(
    drive, emp, exclude_terms: Iterable[str]
) -> Tuple[List[Tuple[str, str]], List[dict]]:
    stack = [(emp["id"], emp["name"])]
    pdfs = []
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
            elif item["mimeType"] == "application/pdf":
                pdfs.append((item["id"], item["name"]))

    return pdfs, excluded_folders


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
    pdfs, excluded_folders = collect_pdfs_recursive(drive, emp, exclude_terms)
    included = []
    skipped = []

    for fid, fname in pdfs:
        term = file_excluded(fname, exclude_terms)
        if term:
            skipped.append({"file_id": fid, "file_name": fname, "reason": term})
        else:
            included.append({"file_id": fid, "file_name": fname})

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
