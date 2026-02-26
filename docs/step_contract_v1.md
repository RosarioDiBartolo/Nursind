# Step Contract v1

Purpose: define one consistent, function-first structure for every pipeline step so each stage is easy to test on one file or small batches.

## Required Package Layout
- `src/<step_name>/options.py`: defaults, typed options, parser builders.
- `src/<step_name>/service.py`: pure processing logic (`process_one`, `process_many`).
- `src/<step_name>/runtime.py`: folder scanning + report writing orchestration.
- `src/<step_name>/cli.py`: thin CLI wrapper (`parse -> setup logging -> run`).
- `src/<step_name>/__init__.py`: stable public API exports.
- `src/<step_name>/__main__.py`: `python -m "src.<step_name>"` entrypoint.

## Required Function API
- `process_one_<unit>(path, **kwargs) -> dict`
- `process_many_<unit>(paths, **kwargs) -> dict`
- `build_<step>_from_dir(...) -> dict` (runtime helper for directory mode)

Function API is the primary interface for notebook/small-sample validation.

## `process_one` Return Contract
Minimum required keys:
- `status`: `"ok"` or `"error"`
- `error`: `None` or string
- `error_code`: `None` or string
- one source key (`source_*`)

If `status == "ok"`, step-specific output keys must be present (for example output file path).

## `process_many` Return Contract
Minimum required top-level keys:
- `stats`: dict
- `items`: list of `process_one` payloads
- `errors`: list

Minimum required `stats` keys:
- `files_total`
- `files_processed`
- `files_error`

Consistency invariants:
- `files_total == len(items)`
- `files_error == len(errors)`
- `files_processed + files_error == files_total`

## Backward Compatibility Rules
- Keep `python -m "src.<step_name>"` working.
- Keep existing public imports working from `src.<step_name>`.
- During migration, allow legacy wrapper modules only if needed; remove wrappers in a planned cleanup.

## Test Contract
Each step must have:
- one test validating `process_one` contract shape
- one test validating `process_many` contract shape and invariants
- at least one functional sample test with real temp files
