# Shift Aggregation

```powershell
.\.venv\Scripts\python.exe src\scripts\summarize_shifts.py
```

Reads enriched employee CSV files and writes the canonical yearly summary
under `aggregation/`. Settings come from `steps.summarize_shifts`.

Implementation: `core.shifts.summary`.
