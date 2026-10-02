# Clean Shifts Export

```powershell
.\.venv\Scripts\python.exe src\scripts\tools\clean_shifts.py
```

Reads employee `enrichment/*.enriched.csv` files and writes one cleaned Excel
workbook per employee under `clean_shifts/`.

Rows with `turno_bucket` equal to `M` are excluded.

Each output workbook contains:

- `Data`
- `Giorno`
- `Entrata`
- `Uscita`
- `Durata`
- `Festivo`
- `Turno`

Implementation: `core.tools.clean_shifts_export`.
