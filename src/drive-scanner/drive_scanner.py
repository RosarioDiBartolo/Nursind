from typing import Any, Dict, List, Optional, Tuple
import re

from .drive_helpers import FOLDER_MIME, SHORTCUT_MIME, list_children


def _normalize(s: str) -> str:
    """
    Normalize for comparisons:
    - lowercase
    - "_" and "-" become spaces
    - collapse whitespace
    """
    s = (s or "").lower()
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _contains_any_term_substring(haystack: str, terms: List[str]) -> bool:
    """Substring match (normalized) for include behavior."""
    h = _normalize(haystack)
    return any(_normalize(t) in h for t in terms)


def _folder_segments_contain_exclude(
    folder_segments: List[str],
    exclude_terms: List[str],
) -> bool:
    """
    EXCLUDE RULE:
    Return True if ANY folder segment equals (exact match) any exclude term after normalization.
    """
    exclude_set = {_normalize(t) for t in exclude_terms}
    for seg in folder_segments:
        if _normalize(seg) in exclude_set:
            return True
    return False


def scan_tree_collect_files_by_path(
    service: Any,
    start_folder_id: str,
    start_folder_name: str,
    include_terms: List[str],
    follow_shortcut_folders: bool = False,
    exclude_terms: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    INCLUDE:
      - include a file if ANY include_terms appears anywhere in the FULL path
        (folders or filename), using substring match.

    EXCLUDE (fixed):
      - exclude ONLY if ANY PARENT FOLDER SEGMENT equals one of exclude_terms
        (exact segment match after normalization).
      - DO NOT exclude based on filename.
      - This excludes:  .../buste_paga/cedolino.pdf
      - This does NOT exclude: .../buste_paga_e_cartellini/.../cartellino.pdf
    """

    exclude_terms = exclude_terms or ["buste paga", "buste_paga"]

    # stack: (folder_id, path_parts_of_folders)
    stack: List[Tuple[str, List[str]]] = [(start_folder_id, [start_folder_name])]
    visited_folders = set()

    matches: List[Dict[str, Any]] = []
    stats = {
        "foldersScanned": 0,
        "itemsScanned": 0,
        "filesCollected": 0,
        "filesExcludedByExactFolderTerm": 0,
    }

    while stack:
        folder_id, folder_path_parts = stack.pop()

        if folder_id in visited_folders:
            continue
        visited_folders.add(folder_id)

        stats["foldersScanned"] += 1
        children = list_children(service, folder_id)
        stats["itemsScanned"] += len(children)

        for item in children:
            item_id = item.get("id")
            item_name = item.get("name") or ""
            mime = item.get("mimeType")

            # shortcuts
            if mime == SHORTCUT_MIME:
                sd = item.get("shortcutDetails") or {}
                target_id = sd.get("targetId")
                target_mime = sd.get("targetMimeType")
                if follow_shortcut_folders and target_id and target_mime == FOLDER_MIME:
                    stack.append((target_id, folder_path_parts + [item_name]))
                continue

            # folders
            if mime == FOLDER_MIME:
                stack.append((item_id, folder_path_parts + [item_name]))
                continue

            # files
            full_path = "/".join(folder_path_parts + [item_name])

            # EXCLUDE: check only folder segments (parents), exact match
            if _folder_segments_contain_exclude(folder_path_parts, exclude_terms):
                stats["filesExcludedByExactFolderTerm"] += 1
                continue

            # INCLUDE: substring match on full path
            if not _contains_any_term_substring(full_path, include_terms):
                continue

            node: Dict[str, Any] = {
                "id": item_id,
                "name": item_name,
                "mimeType": mime,
                "path": full_path,
                "modifiedTime": item.get("modifiedTime"),
                "createdTime": item.get("createdTime"),
                "matched": {
                    "byPathSubstring": True,
                    "excludeAppliedAsExactFolderMatch": True,
                },
            }

            if "size" in item:
                try:
                    node["size"] = int(item["size"])
                except Exception:
                    node["size"] = item["size"]

            matches.append(node)
            stats["filesCollected"] += 1

    return {
        "startFolder": {"id": start_folder_id, "name": start_folder_name},
        "includeTerms": include_terms,
        "excludeTerms": exclude_terms,
        "stats": stats,
        "matches": matches,
    }
