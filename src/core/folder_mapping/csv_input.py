"""Import the same inventory from explicitly selected Drive files."""
import csv
import re
from urllib.parse import parse_qs, urlsplit

from googleapiclient.errors import HttpError

from core.reporting import write_json_report
from core.drive.names import is_macos_metadata
from core.folder_mapping.scan import save_inventory


def drive_id(row):
    raw = (row.get("file_id") or "").strip()
    link = (row.get("file_url") or row.get("file_link") or "").strip()
    if link:
        url = urlsplit(link)
        match = re.search(r"/d/([\w-]+)(?:/|$)", url.path)
        linked = match[1] if match else parse_qs(url.query).get("id", [""])[0]
        if url.hostname not in {"drive.google.com", "docs.google.com"} or not linked or (raw and raw != linked):
            raise ValueError("Invalid Drive link or conflicting file_id")
        raw = linked
    if not re.fullmatch(r"[A-Za-z0-9_-]+", raw):
        raise ValueError("Missing or invalid Drive file ID")
    return raw


def scan_csv(drive, config, path):
    items = {}
    duplicates = []
    with open(path, encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not {"file_id", "file_url", "file_link"}.intersection(reader.fieldnames or []):
            raise ValueError("Expected file_id or file_url")
        for row in reader:
            try:
                key = drive_id(row)
            except ValueError as exc:
                raise ValueError(f"CSV line {reader.line_num}: {exc}") from exc
            if key in items:
                duplicates.append({"file_id": key, "line": reader.line_num})
                continue
            item = dict(file_id=key, relative_path=key, is_pdf=False)
            try:
                meta = drive.files().get(fileId=key, fields="name,mimeType", supportsAllDrives=True).execute()
                name = meta["name"]
                item.update(file_name=name, relative_path=row.get("relative_path") or name, mime_type=meta["mimeType"],
                            is_pdf=meta["mimeType"] == "application/pdf" or name.lower().endswith(".pdf"))
                if is_macos_metadata(item["relative_path"]):
                    item.update(is_pdf=False, reason="macos_metadata")
            except HttpError as exc:
                if exc.resp.status not in (403, 404):
                    raise
                item.update(reason="inaccessible", error=str(exc))
            items[key] = item
    result = save_inventory(config, list(items.values()))
    write_json_report(config.paths.scan_dir / "duplicates.json", {"rows": duplicates})
    return result
