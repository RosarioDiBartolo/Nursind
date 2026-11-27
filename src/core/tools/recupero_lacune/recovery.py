"""Resume document and event recovery with per-file checkpoints."""

import hashlib
import json
from pathlib import Path
from threading import Event
from uuid import uuid4

from core.documents.ocr_options import OcrOptions
from core.documents.workers import download_pdf_bytes, extract_and_write
from core.drive.archive_utils import parse_archive_member_id
from core.drive.auth_service import load_creds
from core.drive.text_extraction_csv import build_doc_json_rel_path
from core.events.extraction.service import _process_one_document


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def parser_version():
    core = Path(__file__).resolve().parents[2]
    files = [core / "parsing.py", *sorted((core / "events/extraction").rglob("*.py"))]
    return hashlib.sha256(b"".join(f.read_bytes() for f in files)).hexdigest(), max(
        f.stat().st_mtime_ns for f in files
    )


class Originals:
    def __init__(self, offline):
        self.offline = offline
        self.creds = None
        self.auth_error = None

    def fetch(self, item):
        source = Path(item["drive_path"])
        if source.is_file():
            return source.read_bytes()
        if self.offline:
            raise RuntimeError("Originale non presente localmente (--offline)")
        if self.auth_error:
            raise RuntimeError(self.auth_error)
        if self.creds is None:
            try:
                self.creds = load_creds()
            except Exception as exc:
                self.auth_error = f"Drive non disponibile: {type(exc).__name__}"
                raise RuntimeError(self.auth_error) from exc
        info = dict(item)
        archive = parse_archive_member_id(item["file_id"])
        if archive:
            info.update(
                source_kind="zip_member", archive_file_id=archive[0], archive_member_path=archive[1]
            )
        result = download_pdf_bytes(self.creds, info, Event())
        if result["status"] != "success":
            raise RuntimeError(result.get("reason") or "Download fallito")
        return result["data"]


def recover(item, output, version, originals):
    original = item["document"]
    relative = Path(build_doc_json_rel_path(item["file_id"]))
    cache = output / "files" / relative.name
    version_hash, parser_modified = version
    original_time = original.stat().st_mtime_ns if original.exists() else 0
    stamp = [version_hash, original_time, item["employee"]]
    prior = json.loads(cache.read_bytes()) if cache.exists() else {}
    if prior.get("stamp") != stamp:
        prior = {}
    if prior.get("complete"):
        return prior
    document = output / "documents" / relative

    def parse(path):
        row = {**item, "doc_json": relative.as_posix()}
        info, events, pages, _, _ = _process_one_document(
            row, input_dir=str(path.parent.parent), max_unmatched_examples_per_file=2
        )
        if info["status"] != "ok":
            raise RuntimeError(info["error"])
        return events, pages

    try:
        if not prior and original.exists():
            # The caller already checked that the current event table contains no punches.
            page_csv = item["source"] / "events/pages.csv"
            if item.get("page_recorded") and page_csv.stat().st_mtime_ns >= max(
                parser_modified, original_time
            ):
                events, pages = [], []
            else:
                events, pages = parse(original)
            scanned = (
                item["explicit"]
                and not events
                and any(
                    p.get("method") == "ocr"
                    for p in json.loads(original.read_bytes())
                    .get("extraction", {})
                    .get("pages", [])
                )
            )
            prior = {
                "events": events,
                "stage": "parser",
                "pages_error": sum(
                    bool(p.get("parse_error")) or str(p["decision"]).startswith("error")
                    for p in pages
                ),
                "complete": not scanned,
                "stamp": stamp,
            }
            save(cache, prior)
        if not prior.get("complete"):
            if not document.exists() or document.stat().st_mtime_ns < original_time:
                data = originals.fetch(item)
                result = extract_and_write(
                    data,
                    item,
                    str(output / "documents"),
                    0.72,
                    0.08,
                    OcrOptions(enabled=True, psm=1),
                )
                if result["status"] != "success":
                    raise RuntimeError(result["reason"])
            events, pages = parse(document)
            errors = json.loads(document.read_bytes())["extraction"].get("page_errors", [])
            prior = {
                "events": events,
                "stage": "ocr",
                "partial_pages": errors,
                "pages": len(pages),
                "complete": True,
                "stamp": stamp,
            }
            save(cache, prior)
        return prior
    except Exception as exc:
        return {**prior, "events": prior.get("events", []), "complete": False, "error": str(exc)}
