from __future__ import annotations

from hashlib import sha1
from io import BytesIO
from urllib.parse import quote, unquote
from zipfile import BadZipFile, ZipFile


def normalize_zip_member_path(member_path: str) -> str:
    if not member_path:
        raise ValueError("ZIP member path cannot be empty")
    normalized = member_path.replace("\\", "/").lstrip("/")
    parts = [part for part in normalized.split("/") if part and part != "."]
    if any(part == ".." for part in parts):
        raise ValueError(f"ZIP member path escapes root: {member_path}")
    if not parts:
        raise ValueError(f"ZIP member path is invalid: {member_path}")
    return "/".join(parts)


def build_archive_member_id(archive_file_id: str, member_path: str) -> str:
    normalized_member = normalize_zip_member_path(member_path)
    encoded_path = quote(normalized_member, safe="")
    digest = sha1(normalized_member.encode("utf-8")).hexdigest()[:10]
    return f"zip::{archive_file_id}::{digest}::{encoded_path}"


def parse_archive_member_id(member_id: str) -> tuple[str, str] | None:
    if not member_id.startswith("zip::"):
        return None
    parts = member_id.split("::", 3)
    if len(parts) != 4:
        return None
    archive_file_id = parts[1]
    encoded_path = parts[3]
    if not archive_file_id or not encoded_path:
        return None
    return archive_file_id, unquote(encoded_path)


def list_pdf_members(zip_bytes: bytes) -> list[str]:
    members: list[str] = []
    with ZipFile(BytesIO(zip_bytes)) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            normalized = normalize_zip_member_path(info.filename)
            if normalized.lower().endswith(".pdf"):
                members.append(normalized)
    return sorted(set(members))


def extract_zip_member_bytes(zip_bytes: bytes, member_path: str) -> bytes:
    normalized_member = normalize_zip_member_path(member_path)
    with ZipFile(BytesIO(zip_bytes)) as zf:
        with zf.open(normalized_member, "r") as stream:
            return stream.read()


__all__ = [
    "BadZipFile",
    "build_archive_member_id",
    "extract_zip_member_bytes",
    "list_pdf_members",
    "normalize_zip_member_path",
    "parse_archive_member_id",
]
