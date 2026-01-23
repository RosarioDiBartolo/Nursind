import io
import json
import argparse
from typing import Any, Dict, List

from pypdf import PdfReader, PdfWriter
from googleapiclient.http import MediaIoBaseDownload

from  .client import get_drive_service

PDF_MIME = "application/pdf"


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def download_pdf_bytesio(service: Any, file_id: str) -> io.BytesIO:
    """
    Download a PDF from Drive into an in-memory BytesIO stream.
    PdfReader accepts file-like objects (read/seek), per docs.
    """
    stream = io.BytesIO()
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    downloader = MediaIoBaseDownload(stream, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    stream.seek(0)
    return stream


def main():
    ap = argparse.ArgumentParser(description="Merge PDFs (doc-aligned: PdfReader + PdfWriter.append)")
    ap.add_argument("--json", required=True, help="One person's JSON output from your scanner")
    ap.add_argument("--out", required=True, help="Output merged PDF path")
    ap.add_argument("--strict", action="store_true", help="Use PdfReader(strict=True). Default False.")
    args = ap.parse_args()

    service = get_drive_service()  # must have drive.readonly scope

    payload = load_json(args.json)
    matches: List[Dict[str, Any]] = payload["result"]["matches"]

    pdf_items = [m for m in matches if m.get("mimeType") == PDF_MIME]
    if not pdf_items:
        raise SystemExit("No PDFs found in JSON matches.")

    # Keep stable order (path-based). You can swap to modifiedTime if you want.
    pdf_items.sort(key=lambda m: (m.get("path") or "").lower())

    writer = PdfWriter()

    # IMPORTANT:
    # Keep streams alive until writer.write() completes, because writer keeps references.
    open_streams: List[io.BytesIO] = []

    for i, item in enumerate(pdf_items, start=1):
        file_id = item["id"]
        path = item.get("path") or item.get("name") or file_id
        print(f"[{i}/{len(pdf_items)}] {path}")

        stream = download_pdf_bytesio(service, file_id)
        open_streams.append(stream)

        reader = PdfReader(stream, strict=args.strict)  # signature per docs :contentReference[oaicite:5]{index=5}
        writer.append(reader)  # merging pattern per pypdf docs :contentReference[oaicite:6]{index=6}

    with open(args.out, "wb") as f:
        writer.write(f)

    # Streams will get GC’d after script ends, but closing is clean:
    for s in open_streams:
        try:
            s.close()
        except Exception:
            pass

    print(f"\n✅ Merged PDF saved to: {args.out}")


if __name__ == "__main__":
    main()
