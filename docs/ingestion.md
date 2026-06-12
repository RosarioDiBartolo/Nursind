# Ingestion

```powershell
.\.venv\Scripts\python.exe src\scripts\scan.py
.\.venv\Scripts\python.exe src\scripts\extract_documents.py
```

Both scripts read root `pipeline.json`. Scan writes included/filtered indexes
under `scan/`. Document extraction consumes the included index and writes
employee manifests plus `documents/docs/*.json`.

Implementation: `core.drive.scan`, `core.documents`, and shared `core.drive`
helpers.
