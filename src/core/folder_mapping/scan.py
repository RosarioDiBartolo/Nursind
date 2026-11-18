"""Inventory every Drive file, adapting PDFs to the existing extraction index."""
from __future__ import annotations

import logging

from core.drive.drive_client import list_children
from core.drive.index.map_index import MapIndex
from core.drive.names import is_macos_metadata
from core.reporting import write_json_report

FOLDER_MIME = "application/vnd.google-apps.folder"
logger = logging.getLogger(__name__)


def scan_folder(drive, config) -> dict:
    logger.info("Drive inventory started: root=%s", config.drive_root_id)
    inventory = []
    stack = [(config.drive_root_id, "")]
    visited = set()
    while stack:
        folder_id, parent_path = stack.pop()
        if folder_id in visited:
            continue
        visited.add(folder_id)
        logger.info("Scanning folder %s: %s [%s] | files found=%s | folders pending=%s",
                    len(visited), parent_path or "/", folder_id, len(inventory), len(stack))
        # A listing failure stops the scan: a partial inventory must not look complete.
        for item in sorted(list_children(drive, folder_id), key=lambda item: (item["name"], item["id"])):
            path = f"{parent_path}/{item['name']}".lstrip("/")
            if is_macos_metadata(path):
                continue
            if item["mimeType"] == FOLDER_MIME:
                stack.append((item["id"], path))
                continue
            inventory.append({
                "file_id": item["id"], "file_name": item["name"],
                "relative_path": path, "mime_type": item["mimeType"],
                "is_pdf": item["mimeType"] == "application/pdf" or item["name"].lower().endswith(".pdf"),
            })
    inventory.sort(key=lambda item: (item["relative_path"], item["file_id"]))
    index = MapIndex.generate_index(config.drive_root_id, 0, {
        item["file_id"]: {
            "employee": "unknown", "local": False, "file_id": item["file_id"],
            "file_name": item["file_name"], "drive_path": item["relative_path"], "type": "file",
        }
        for item in inventory if item["is_pdf"]
    })
    config.paths.scan_dir.mkdir(parents=True, exist_ok=True)
    index.save_index(str(config.paths.scan_included_index))
    report = {"root_id": config.drive_root_id, "files": inventory}
    write_json_report(config.paths.scan_dir / "inventory.json", report)
    logger.info("Drive inventory saved: folders=%s files=%s PDFs=%s | %s",
                len(visited), len(inventory), index.total_files,
                config.paths.scan_dir / "inventory.json")
    return report
