from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from drive_scanner.names import normalize_term  # noqa: E402
from drive_scanner.scan_service import file_excluded, folder_excluded  # noqa: E402


def test_folder_excluded_normalizes_name():
    terms = [normalize_term("Busta Paga"), normalize_term("Cedolino")]
    assert folder_excluded("Busta    Paga", terms) == "busta paga"
    assert folder_excluded("Altro", terms) is None


def test_file_excluded_matches_terms():
    terms = [normalize_term("busta paga"), normalize_term("cedolino")]
    assert file_excluded("Busta Paga gennaio.pdf", terms) == "busta paga"
    assert file_excluded("report.pdf", terms) is None
