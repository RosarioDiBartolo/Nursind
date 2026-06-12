# Afternoon Long-Shift Export

Optional utility:

```powershell
.\.venv\Scripts\python.exe src\scripts\enrich_shifts.py
.\.venv\Scripts\python.exe src\scripts\tools\afternoon_export.py
```

The export requires `enrichment/*.enriched.csv`. It exits with an error and
writes an explanatory report when the enrichment step has not been run.

For each employee it writes:

- `<employee>/<employee>.pomeriggi.csv`
- `<employee>/<employee>.csv`
- `<employee>/<employee>.pdf`

Implementation: `core.tools.afternoon_export`.
