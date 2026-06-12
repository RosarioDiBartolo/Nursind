# Shift Enrichment

```powershell
.\.venv\Scripts\python.exe src\scripts\enrich_shifts.py
```

Reads `shifts/*.pairs.csv`, fixes overnight exits, computes duration, and
assigns holiday/time-of-day classifications. Settings come from
`steps.enrich_shifts` in `pipeline.json`.

Implementation: `core.shifts.enrichment` and `core.shift_logic`.
