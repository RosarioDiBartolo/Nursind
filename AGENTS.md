# READ .github\copilot-instructions.md

## Shared Logic First
- Prefer shared utilities over copy/paste logic.
- Before adding new helpers, search for existing implementations in `src/drive_service`, `src/raw_text_parsing.py`, `src/pdf_text_extraction.py`, and `src/shift_services.py`.
- Read `docs/shared_logic_registry.md` before introducing new shared helpers.
- If logic is used by 2 or more files, extract it into a shared module and reuse it.
- When you add/move shared logic, update `docs/shared_logic_registry.md` in the same change.
- Keep CLI files thin: argument parsing + orchestration only. Move business logic into importable functions.

## Discovery Protocol (Required Before New Helpers)
- Run these searches before adding any helper/feature implementation:
  - `rg -n "keyword|synonym|feature_name" src docs`
  - `rg --files src | rg "service|parser|helper|runtime|utils"`
  - `rg -n "def .*candidate_name|class .*candidate_name" src`
- Then read:
  - `docs/shared_logic_registry.md`
  - The nearest matching module among `src/drive_service/`, `src/raw_text_parsing.py`, `src/pdf_text_extraction.py`, `src/shift_services.py`
- In PRs, confirm the discovery protocol was run and shared logic was reused where available.

## File Design Preferences
- Prefer small files with high cohesion and a single clear purpose.
- Keep modules functional and as independent as practical; avoid unnecessary coupling across files.
- Split code by logic boundaries (parsing, transformation, I/O, orchestration) instead of by convenience.
- Maximum 3 responsibilities per file/module; if a 4th appears, split the file.
- When a file grows mixed responsibilities, split it into focused modules with explicit interfaces.

## Commit Workflow
- After any substantial code change, propose a descriptive commit message that summarizes the actual changes.
- Do not create a commit for substantial changes until the user explicitly confirms they are satisfied and wants the commit created.
