# Shift Aggregation

```powershell
.\.venv\Scripts\python.exe src\scripts\summarize_shifts.py
```

Reads enriched employee CSV files and writes the canonical yearly summary
under `aggregation/` as both CSV and Excel workbook files. By default, year
columns are discovered from the enriched rows; `steps.summarize_shifts.year_start`
and `year_end` can constrain an explicit range.

Implementation: `core.shifts.summary`.
