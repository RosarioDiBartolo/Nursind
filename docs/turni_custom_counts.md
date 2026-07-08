# Custom Shift Counts

```powershell
.\.venv\Scripts\python.exe src\scripts\tools\turni_custom_counts.py
```

Reads enriched employee CSV files and writes custom yearly counts under
`turni_custom_counts/` as both CSV and Excel workbook files.

Rows:
- `P`: long afternoon shifts
- `N`: long night shifts
- `M`: long Saturday morning shifts
- `MF`: long holiday or Sunday morning shifts

Short shifts marked as `S` by enrichment are excluded from all custom counts.

By default, year columns are discovered from the enriched rows. Settings come
from `steps.turni_custom_counts` when present, and `year_start`/`year_end` can
constrain an explicit range.

Implementation: `core.tools.turni_custom_counts`.
