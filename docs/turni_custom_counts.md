# Custom Shift Counts

```powershell
.\.venv\Scripts\python.exe src\scripts\tools\turni_custom_counts.py
```

Reads enriched employee CSV files and writes custom yearly counts under
`turni_custom_counts/`.

Rows:
- `P`: all afternoon shifts
- `N`: all night shifts
- `M`: Saturday morning shifts
- `MF`: holiday or Sunday morning shifts

Settings come from `steps.turni_custom_counts` when present.

Implementation: `core.tools.turni_custom_counts`.
