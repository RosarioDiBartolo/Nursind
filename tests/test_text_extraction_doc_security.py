from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cartellino_parser.drive_service.text_extraction_csv import resolve_doc_json_path  # noqa: E402


def test_resolve_doc_json_path_rejects_escape_attempts(tmp_path: Path):
    base_dir = tmp_path / "documents"
    docs_dir = base_dir / "docs"
    docs_dir.mkdir(parents=True)
    allowed = docs_dir / "allowed.json"
    allowed.write_text("{}", encoding="utf-8")
    secret = tmp_path / "secret.json"
    secret.write_text("{}", encoding="utf-8")

    assert resolve_doc_json_path(base_dir, "docs/allowed.json") == allowed.resolve()
    assert resolve_doc_json_path(base_dir, "../secret.json") is None
    assert resolve_doc_json_path(base_dir, str(secret.resolve())) is None

