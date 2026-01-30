import threading

from googleapiclient.discovery import build


_thread_local = threading.local()


def get_drive_service(creds):
    if not hasattr(_thread_local, "drive"):
        _thread_local.drive = build(
            "drive", "v3", credentials=creds, cache_discovery=False
        )
    return _thread_local.drive


def list_children(drive, folder_id: str):
    items = []
    token = None
    while True:
        res = drive.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, mimeType)",
            pageSize=1000,
            pageToken=token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        items.extend(res.get("files", []))
        token = res.get("nextPageToken")
        if not token:
            break
    return items
