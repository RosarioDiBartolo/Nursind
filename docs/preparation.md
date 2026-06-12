# Event Preparation

```powershell
.\.venv\Scripts\python.exe src\scripts\extract_events.py
.\.venv\Scripts\python.exe src\scripts\filter_midnight.py
.\.venv\Scripts\python.exe src\scripts\pair_events.py
```

Event extraction writes `events.csv` and `pages.csv`. Cleanup writes
`events.cleaned.csv` and the removed-row audit. Pairing writes one
`*.pairs.csv` per employee.

Implementation: `core.events.extraction`, `core.events.filtering`, and
`core.shifts.pairing`.
