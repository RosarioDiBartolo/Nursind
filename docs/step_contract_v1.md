# Step Contract v1

Purpose: define one consistent, function-first structure for every pipeline step so each stage is easy to test on one file or small batches.

## Supported Step Shapes
Every pipeline step must fit one of these supported shapes:

1. Directory-batch step
- Package layout:
  `options.py`, `service.py`, `runtime.py`, `cli.py`, `__init__.py`, `__main__.py`
- Public function API:
  `process_one_<unit>(...)`, `process_many_<unit>(...)`, `build_<step>_from_dir(...)`
- Examples:
  midnight filtering, pairing, enrichment, summary generation

2. Index-driven extraction step
- Package layout:
  `options.py`, `service.py`, `runtime.py`, worker/planning helpers as needed, `__init__.py`, `__main__.py`
- Public function API:
  `process_one_<unit>(...)`, `process_many_<unit>(...)`, plus runtime entrypoints that coordinate concurrency and index/report writes
- Notes:
  worker and planning modules may exist, but public callers should go through `service.py` or `runtime.py`

3. Pipeline-audit step
- Package layout:
  `options.py`, `service.py`, `runtime.py`, `cli.py`, `__init__.py`, `__main__.py`
- Public function API:
  one primary audit function such as `audit_<step>(...)`, plus a runtime/report writer such as `build_<step>_report(...)`
- Notes:
  these steps may not naturally expose `process_one`; the audit entrypoint is the contract surface

Function API is the primary interface for notebook and small-sample validation.

## Standard `process_one` Return Contract
Minimum required keys:
- `status`: `"ok"` or `"error"`
- `error`: `None` or string
- `error_code`: `None` or string
- one source key (`source_*`)

If `status == "ok"`, step-specific output keys must be present.

## Standard `process_many` Return Contract
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

## Audit-Step Return Contract
Audit-oriented steps must expose:
- one primary top-level `stats` dict
- one or more report row collections
- explicit output metadata when a runtime/report writer persists files

## Backward Compatibility Rules
- Keep `python -m "src.<step_name>"` working.
- Keep existing public imports working from `src.<step_name>`.
- Prefer adapting the contract doc to the real supported step shape over adding fake wrappers.

## Test Contract
Each supported step must have:
- one contract test for its public function surface
- one functional sample test with real temp files
- one runtime/report-writing test when the step persists artifacts
