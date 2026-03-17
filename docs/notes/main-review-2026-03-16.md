# Main Review Notes (2026-03-16)

Scope: review of `main` as-is, across six parallel tracks:
- security
- code quality
- bugs
- race
- test flakiness
- maintainability

## Concrete Findings

### 1. Missing-timbrature audit does not ingest pairing results

Severity: high

The missing-timbrature audit reads `pair_report_payload["by_employee"]`, but the pairing step writes a stage report with `items`. In practice this means pairing failures and missing pair outputs are never surfaced by the audit.

Relevant code:
- `src/timbrature_missing_report/service.py:314`
- `src/pair_employee_events/runtime.py:145`
- `src/reporting.py:23`

Impact:
- `pairing_failed` findings are skipped
- `pair_output_missing` findings are skipped
- audit output can falsely report fewer downstream issues than actually exist

### 2. `doc_json` paths can escape the pipeline root

Severity: medium

Manifest-supplied `doc_json` values are joined directly with the base directory and then read. A crafted manifest row can point outside the canonical `documents/docs` area and make the pipeline load arbitrary local JSON files.

Relevant code:
- `src/drive_service/text_extraction_csv.py:254`
- `src/extract_events_from_documents/source_context.py:128`
- `src/parser_recall_audit/service.py:315`

Impact:
- unintended local file reads
- audit and event extraction trust paths beyond the intended artifact root

### 3. Interrupted extraction can leave outputs inconsistent

Severity: medium

On `KeyboardInterrupt`, extraction sets the stop flag and exits the scheduling loop, but it does not drain already-submitted extraction futures before finalizing. Worker processes may already have written `docs/*.json` while indexes and manifest CSVs are finalized without those results.

Relevant code:
- `src/extract_documents_from_index/runtime.py:89`

Impact:
- partial runs can leave orphaned document JSON files
- included/excluded indexes and manifests can diverge from the actual extracted artifacts

### 4. Zip-cache tests share mutable module-global state

Severity: low

The zip-support tests mutate `workers._thread_local.zip_cache` and `zip_cache_order` directly and do not reset them after each case. That creates order dependence across tests that touch the same worker module.

Relevant code:
- `tests/test_extract_documents_zip_support.py:81`
- `tests/test_extract_documents_zip_support.py:132`

Impact:
- low-confidence test order sensitivity
- future tests touching the same module state may become flaky

## Point-by-Point Summary

### 1. Security issue

Found: yes

The main security-relevant issue is manifest-driven `doc_json` path traversal / arbitrary local JSON reads.

### 2. Code quality

Found: yes

The strongest example is report contract drift between the pairing report producer and the missing-timbrature consumer (`items` vs `by_employee`). There is also duplicated doc-json resolution logic between shared helpers and parser recall.

### 3. Bugs

Found: yes

Pairing failures are currently invisible to the missing-timbrature audit because it reads the wrong report shape.

### 4. Race

Found: yes

The extraction runtime has an interruption/concurrency consistency bug around in-flight worker futures during shutdown. I did not confirm a separate shared-memory race beyond that.

### 5. Test flakiness

Found: low-confidence risk

The zip-cache tests share mutable worker-module state and may become order-dependent.

### 6. Maintainability of the code

Found: yes

The following modules remain structurally heavy and mixed in responsibility:
- `src/parser_recall_audit/service.py`
- `src/timbrature_missing_report/service.py`
- `src/extract_events_from_documents/page_analysis.py`

The report-contract mismatch is likely a symptom of this drift.

## Validation Limitation

I could not run the targeted pytest subset in this environment because the repo `python` / `pytest` shims resolve to a missing Python 3.12 install, while the available `python3` does not have `pytest` installed.
