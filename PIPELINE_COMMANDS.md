# Pipeline Commands

All commands read `pipeline.json` by default:

```powershell
$python = ".\.venv\Scripts\python.exe"

& $python src\scripts\scan.py
& $python src\scripts\extract_documents.py
& $python src\scripts\extract_events.py
& $python src\scripts\filter_midnight.py
& $python src\scripts\pair_events.py
& $python src\scripts\enrich_shifts.py
& $python src\scripts\summarize_shifts.py
```

Run selected steps in canonical order with the pipeline runner:

```powershell
& $python src\scripts\run_pipeline.py scan extract_events summarize_shifts
& $python src\scripts\run_pipeline.py --from extract_events --through summarize_shifts
& $python src\scripts\run_pipeline.py --steps scan,pairing,summary --skip pairing
```

Use another configuration with `--config path\to\pipeline.json`. Add
`--verbose` for debug logging.

Optional tools:

```powershell
& $python src\scripts\tools\parser_recall.py
& $python src\scripts\tools\missing_timbrature.py
& $python src\scripts\tools\afternoon_export.py
& $python src\scripts\tools\clean_shifts.py
& $python src\scripts\tools\download_from_index.py --help
```

Verification:

```powershell
& $python -m pytest
& $python -m compileall -q src
& $python -m ruff check src tests
```
