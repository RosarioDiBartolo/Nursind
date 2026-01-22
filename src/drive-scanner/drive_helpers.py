from typing import Any, Dict, List

FOLDER_MIME = "application/vnd.google-apps.folder"
SHORTCUT_MIME = "application/vnd.google-apps.shortcut"


def get_file_meta(service: Any, file_id: str) -> Dict[str, Any]:
    return (
        service.files()
        .get(
            fileId=file_id,
            fields="id,name,mimeType",
            supportsAllDrives=True,
        )
        .execute()
    )


def list_children(service: Any, parent_id: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    page_token = None

    fields = (
        "nextPageToken, files("
        "id, name, mimeType, size, modifiedTime, createdTime, "
        "shortcutDetails(targetId,targetMimeType)"
        ")"
    )

    q = f"'{parent_id}' in parents and trashed = false"

    while True:
        resp = (
            service.files()
            .list(
                q=q,
                fields=fields,
                pageSize=1000,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )

        items.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    # folders first, then name
    items.sort(
        key=lambda x: (
            0 if x.get("mimeType") == FOLDER_MIME else 1,
            (x.get("name") or "").lower(),
        )
    )
    return items


def name_matches(name: str, target_terms: List[str]) -> bool:
    n = (name or "").lower()
    return any(term.lower() in n for term in target_terms)
