from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from ..fs_utils import ensure_parent_dir
from ..logging_utils import get_logger, setup_logging
from .map_index import MapIndex

DEFAULT_INDEX_NAME = "index.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a MapIndex from PDF files already present in a local folder."
    )
    parser.add_argument(
        "--folder",
        required=True,
        help="Folder containing PDF files to index.",
    )
    parser.add_argument(
        "--name",
        default=DEFAULT_INDEX_NAME,
        help=(
            "Index filename (or relative path) saved inside the target folder. "
            "Default: index.json"
        ),
    )
    parser.add_argument(
        "--identity",
        default=None,
        help=(
            "Optional override used for both root_id and the single employee "
            "name/id. Default: folder name."
        ),
    )
    parser.add_argument(
        "--recursive",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Include PDFs from nested subfolders (default: false).",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser


def build_local_pdf_index(
    folder: str | Path,
    *,
    output_name: str = DEFAULT_INDEX_NAME,
    identity: str | None = None,
    recursive: bool = False,
) -> tuple[MapIndex, Path]:
    folder_path = Path(folder).expanduser()
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder}")
    if not folder_path.is_dir():
        raise NotADirectoryError(f"Expected a folder, got: {folder}")

    folder_path = folder_path.resolve()
    output_path = _resolve_output_path(folder_path, output_name)
    folder_name = _resolve_identity(folder_path, identity)
    pdf_files = _discover_pdf_files(folder_path, recursive=recursive)

    files: dict[str, dict[str, str]] = {}
    for pdf_path in pdf_files:
        relative_path = pdf_path.relative_to(folder_path).as_posix()
        file_id = f"local::{relative_path}"
        files[file_id] = {
            "employee": folder_name,
            "employee_id": folder_name,
            "local": True,
            "file_id": file_id,
            "file_name": pdf_path.name,
            "drive_path": str(pdf_path),
            "type": "file",
        }

    index = MapIndex.generate_index(
        root_id=folder_name,
        employee_count=1,
        files=files,
    )
    ensure_parent_dir(str(output_path))
    index.save_index(str(output_path))
    return index, output_path


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.verbose)
    logger = get_logger()

    index, output_path = build_local_pdf_index(
        args.folder,
        output_name=args.name,
        identity=args.identity,
        recursive=args.recursive,
    )
    logger.info(
        "Built local PDF index for %s (%s files): %s",
        index.root_id,
        index.total_files,
        output_path,
    )
    return 0


def _discover_pdf_files(folder_path: Path, *, recursive: bool) -> list[Path]:
    iterator = folder_path.rglob("*") if recursive else folder_path.iterdir()
    pdf_files = [
        path.resolve()
        for path in iterator
        if path.is_file() and path.suffix.lower() == ".pdf"
    ]
    return sorted(
        pdf_files,
        key=lambda path: path.relative_to(folder_path).as_posix().lower(),
    )


def _folder_name(folder_path: Path) -> str:
    name = folder_path.name.strip()
    if name:
        return name
    anchor = folder_path.anchor.rstrip("\\/")
    return anchor or str(folder_path)


def _resolve_identity(folder_path: Path, identity: str | None) -> str:
    if identity is None:
        return _folder_name(folder_path)

    token = str(identity).strip()
    if token:
        return token
    raise ValueError("--identity cannot be empty")


def _resolve_output_path(folder_path: Path, output_name: str) -> Path:
    target = Path(output_name)
    if target.is_absolute():
        raise ValueError("--name must be a file name or relative path inside --folder")

    output_path = (folder_path / target).resolve()
    if output_path != folder_path and folder_path not in output_path.parents:
        raise ValueError("--name must resolve inside --folder")
    return output_path


if __name__ == "__main__":
    raise SystemExit(main())
