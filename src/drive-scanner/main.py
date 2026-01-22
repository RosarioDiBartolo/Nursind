import os
import json
from dotenv import load_dotenv

from .client import get_drive_service
from .drive_helpers import FOLDER_MIME, get_file_meta, list_children
from .drive_scanner import scan_tree_collect_files_by_path
from .utils import safe_filename

load_dotenv()


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_csv(key: str, default: list[str]) -> list[str]:
    v = os.getenv(key)
    if not v:
        return default
    raw = v.replace("\n", ",").replace(";", ",")
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def main():
    root_folder_id = os.getenv("DRIVE_ROOT_FOLDER_ID", "").strip()
    if not root_folder_id:
        raise SystemExit("Missing DRIVE_ROOT_FOLDER_ID in .env")

    out_dir = (os.getenv("OUT_DIR") or "cartellini_out").strip() or "cartellini_out"
    os.makedirs(out_dir, exist_ok=True)

    follow_shortcuts = _env_bool("FOLLOW_SHORTCUT_FOLDERS", False)
    also_write_combined = _env_bool("WRITE_COMBINED_INDEX", True)

    # Include terms: substring match anywhere in full path (folders + filename)
    include_terms = _env_csv(
        "INCLUDE_TERMS",
        default=["cartellino", "cartellini"],
    )

    # Exclude terms: exact match on ANY parent folder segment (normalized)
    # Keep it ONLY to folder categories you truly want excluded.
    exclude_terms = _env_csv(
        "EXCLUDE_TERMS",
        default=["buste paga", "buste_paga"],
    )

    service = get_drive_service()

    root_meta = get_file_meta(service, root_folder_id)
    root_name = root_meta.get("name") or root_folder_id

    # direct subfolders of root
    children = list_children(service, root_folder_id)
    subfolders = [c for c in children if c.get("mimeType") == FOLDER_MIME]

    combined_index = {
        "root": {"id": root_folder_id, "name": root_name},
        "includeTerms": include_terms,
        "excludeTerms": exclude_terms,
        "subfolders": [],
    }

    for sf in subfolders:
        sf_id = sf["id"]
        sf_name = sf.get("name") or sf_id

        result = scan_tree_collect_files_by_path(
            service=service,
            start_folder_id=sf_id,
            start_folder_name=sf_name,
            include_terms=include_terms,
            exclude_terms=exclude_terms,
            follow_shortcut_folders=follow_shortcuts,
        )

        payload = {
            "rootFolder": {"id": root_folder_id, "name": root_name},
            "subfolder": {"id": sf_id, "name": sf_name},
            "result": result,
        }

        filename = f"{safe_filename(sf_name)}__{sf_id}.json"
        filepath = os.path.join(out_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        stats = result.get("stats", {})
        print(f"✅ {sf_name}: {stats.get('filesCollected', 0)} files -> {filepath}")

        combined_index["subfolders"].append(
            {
                "id": sf_id,
                "name": sf_name,
                "filesCollected": stats.get("filesCollected", 0),
                # ✅ matches the scanner's stat name in the final logic
                "filesExcludedByExactFolderTerm": stats.get("filesExcludedByExactFolderTerm", 0),
                "jsonFile": filename,
            }
        )

    if also_write_combined:
        combined_path = os.path.join(out_dir, f"_combined_index__{root_folder_id}.json")
        with open(combined_path, "w", encoding="utf-8") as f:
            json.dump(combined_index, f, ensure_ascii=False, indent=2)
        print(f"📦 Combined index -> {combined_path}")


if __name__ == "__main__":
    main()
