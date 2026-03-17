# Step Contract v1

Purpose: define one consistent, function-first structure for every pipeline step so each stage is easy to test on one file or small batches.

## Supported Step Shapes
Every pipeline step must fit one of these supported shapes:

1. Directory-batch step
- Package layout:
  `options.py`, `service.py`, `cli.py`, `__main__.py`
- Public function API:
  `process_one_<unit>(...)`, `process_many_<unit>(...)`, `build_<step>_from_dir(...)`
- Examples:
  midnight filtering, pairing, enrichment, summary generation

2. Index-driven extraction step
- Package layout:
  `options.py`, `service.py`, `runtime.py`, worker/planning helpers as needed, `__main__.py`
- Public function API:
  `process_one_<unit>(...)`, `process_many_<unit>(...)`, plus runtime entrypoints that coordinate concurrency and index/report writes
- Notes:
  worker and planning modules may exist, but public callers should go through the concrete module that owns the entrypoint

3. Pipeline-audit step
- Package layout:
  `options.py`, `service.py`, `cli.py`, `__main__.py`
- Public function API:
  one primary audit function such as `audit_<step>(...)`, plus a report writer such as `build_<step>_report(...)`
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
- `stage`
- `status`
- `inputs`
- `outputs`
- `stats`: dict
- `row_totals`: dict
- `items`: list of `process_one` payloads
- `issues`: list

Minimum required `stats` keys:
- `files_total`
- `files_processed`
- `files_error`

Consistency invariants:
- `files_total == len(items)`
- `row_totals["items"] == len(items)`
- `files_error == len(issues)`
- `row_totals["issues"] == len(issues)`
- `files_processed + files_error == files_total`

## Audit-Step Return Contract
Audit-oriented steps must expose:
- `stage`, `status`, `inputs`, `outputs`, `stats`, `row_totals`, `issues`
- `items` only when row-level detail is genuinely useful in memory
- explicit output metadata when a report writer persists files

## Import Rules
- Keep `python -m "src.<step_name>"` working for implementation compatibility.
- Public package consumers should import the documented `cartellino_parser.*` surface instead of `src.*`.
- Concrete modules such as `src.<step_name>.service` or `src.<step_name>.options` are appropriate only for internal tests, repo notebooks, and step-contract validation.
- README and integration docs should not teach `src.*` as the package-facing API.
- Prefer adapting the contract doc to the real supported step shape over adding fake wrappers or compatibility aliases.

## Test Contract
Each supported step must have:
- one contract test for its public function surface
- one functional sample test with real temp files
- one runtime/report-writing test when the step persists artifacts
