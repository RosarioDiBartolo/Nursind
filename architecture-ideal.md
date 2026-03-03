 

# Architecture Brainstorming Notes

> This file is for personal use only.
> It contains temporary architecture ideas, notes, and structure concepts.
> It will be refined over time throughout the development process.

---

## Goal

Design a pipeline architecture that is clean, structured, and strong enough to showcase in my CV.

---

## Pipeline Step Interface

A `step_interface` abstraction should be implemented for the pipeline.

Each pipeline step must follow a consistent structure and contract.

---

## Pipeline Paths Service

A dedicated service will handle pipeline paths so that steps can easily share state during runtime.

* Current location:
  `src/pipeline_paths.py`
* Responsibility:

  * Centralized path management
  * Shared state access via filesystem
  * Clean separation of storage logic from step logic

---

## Step Structure

Each step should be a Python package exposing a `__main__` entry point.

### Execution Pattern

`__main__.py` should act as a thin wrapper around a CLI module:

```python
from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
```

* `__main__` is only a wrapper.
* The actual runtime logic lives in `cli.py`.

This ensures:

* Clear separation of concerns
* Easy CLI testing
* Clean entry point behavior

---

## Versioning Strategy

Each step must have a version.

### Rules

* Outputs are tied to the step version.
* Any stored output must be invalidated if the version changes.
* Version mismatch overrides persistence automatically.

---

## Step Execution Rules

Each step depends on the previous step.

A step **must not run** if:

1. The previous step's output is missing.
2. The previous step’s output version does not match its declared version.

In both cases:

* The user should be prompted to run the previous step first.

---

## Explicit Requirements Per Step

Each step must:

* Explicitly validate that the previous step has successfully run.
* Throw a fatal error if required inputs are missing.
* Clearly document:

  * Expected input structure (files/folders)
  * Produced output structure (files/folders)
* Be easy to test in isolation.

---

## Design Principles

* Deterministic execution
* Explicit state validation
* Version-driven invalidation
* Clear documentation
* Testable behavior
* Minimal hidden coupling

 