# LLM Navigation Guide

1. Read `AGENTS.md` and `.github/copilot-instructions.md`.
2. Read `README.md`, `docs/codebase_map.md`, and
   `docs/shared_logic_registry.md`.
3. For configuration or paths, open `src/core/config.py` and
   `src/core/paths.py`.
4. For a pipeline step, read its script first, then the corresponding core
   service.
5. Ignore generated outputs, `graphify-out/`, notebook cell outputs, and
   virtual environments unless the task explicitly targets them.

Tests should protect observable behavior and artifacts. Do not recreate
package API, CLI-delegation, notebook-source, or generic result-shape tests.
