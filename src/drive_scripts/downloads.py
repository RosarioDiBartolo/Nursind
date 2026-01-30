import io

from googleapiclient.http import MediaIoBaseDownload


def download_pdf_stream(drive, file_id: str, logger=None) -> io.BytesIO:
    if logger:
        logger.debug("Starting download for %s", file_id)
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    stream = io.BytesIO()
    downloader = MediaIoBaseDownload(stream, request, chunksize=4 * 1024 * 1024)
    done = False
    try:
        while not done:
            _, done = downloader.next_chunk()
    except Exception as exc:
        if logger:
            logger.exception("Download failed for %s: %s", file_id, exc)
        raise
    stream.seek(0)
    if logger:
        logger.debug("Finished download for %s", file_id)
    return stream
