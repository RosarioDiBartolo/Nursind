# Architecture Brainstorming Notes

This file is archival brainstorming only. It is not part of the canonical workflow documentation; use `README.md` and `docs/*` for the supported pipeline.

## Goal

Design a pipeline architecture that is clean, structured, and strong enough to showcase in a CV.

## Pipeline Step Interface

A `step_interface` abstraction should be implemented for the pipeline.

Each pipeline step must follow a consistent structure and contract.

## Pipeline Paths Service

A dedicated service will handle pipeline paths so that steps can easily share state during runtime.

- Current location: `src/pipeline_paths.py`
- Responsibility:
  - centralized path management
  - shared state access via filesystem
  - clean separation of storage logic from step logic

## Step Structure

Each step should be a Python package exposing a `__main__` entry point.

### Execution Pattern

`__main__.py` should act as a thin wrapper around a CLI module:

```python
from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
```

- `__main__` is only a wrapper.
- The actual runtime logic lives in `cli.py`.

This ensures:

- clear separation of concerns
- easy CLI testing
- clean entry point behavior

## Versioning Strategy

Each step must have a version.

### Rules

- outputs are tied to the step version
- any stored output must be invalidated if the version changes
- version mismatch overrides persistence automatically

## Step Execution Rules

Each step depends on the previous step.

A step must not run if:

1. The previous step's output is missing.
2. The previous step's output version does not match its declared version.

In both cases:

- the user should be prompted to run the previous step first

## Explicit Requirements Per Step

Each step must:

- explicitly validate that the previous step has successfully run
- throw a fatal error if required inputs are missing
- clearly document expected input and output structure
- be easy to test in isolation

## Design Principles

- deterministic execution
- explicit state validation
- version-driven invalidation
- clear documentation
- testable behavior
- minimal hidden coupling
