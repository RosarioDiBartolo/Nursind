# READ .github\copilot-instructions.md

## Shared Logic First
- Prefer shared utilities over copy/paste logic.
- Before adding new helpers, search for existing implementations in `src/drive_service`, `src/raw_text_parsing.py`, `src/pdf_text_extraction.py`, and `src/shift_services.py`.
- Read `docs/shared_logic_registry.md` before introducing new shared helpers.
- If logic is used by 2 or more files, extract it into a shared module and reuse it.
- When you add/move shared logic, update `docs/shared_logic_registry.md` in the same change.
- Keep CLI files thin: argument parsing + orchestration only. Move business logic into importable functions.

## File Design Preferences
- Prefer small files with high cohesion and a single clear purpose.
- Keep modules functional and as independent as practical; avoid unnecessary coupling across files.
- Split code by logic boundaries (parsing, transformation, I/O, orchestration) instead of by convenience.
- Maximum 3 responsibilities per file/module; if a 4th appears, split the file.
- When a file grows mixed responsibilities, split it into focused modules with explicit interfaces.
