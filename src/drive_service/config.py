import os
from pathlib import Path

from dotenv import load_dotenv

from .names import normalize_term

try:
    from .output_paths import build_pipelines_paths
except Exception:  # pragma: no cover - defensive fallback for unrelated path module failures
    build_pipelines_paths = None

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
 
CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "token.json")
DRIVE_ROOT_FOLDER_ID = os.getenv("DRIVE_ROOT_FOLDER_ID", "")
if build_pipelines_paths is not None:
    _DEFAULT_OUTPUTS = build_pipelines_paths(root_id=DRIVE_ROOT_FOLDER_ID)
    _DEFAULT_SCAN_OUT = str(_DEFAULT_OUTPUTS.scan_output)
else:
    _DEFAULT_OUTPUTS = None
    _DEFAULT_SCAN_OUT = str(Path("output") / "scan")
SCAN_OUT_DIR = os.getenv("SCAN_OUT_DIR", _DEFAULT_SCAN_OUT)
SCAN_INCLUDED_NAME = os.getenv("SCAN_INCLUDED_NAME", "included_index.json")
SCAN_FILTERED_NAME = os.getenv("SCAN_FILTERED_NAME", "filtered_index.json")

def validate_env():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET in env")
