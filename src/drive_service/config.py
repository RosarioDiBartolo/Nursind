import os

from dotenv import load_dotenv

from .names import normalize_term
from .output_paths import build_output_paths

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
 
CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "token.json")
DRIVE_ROOT_FOLDER_ID = os.getenv("DRIVE_ROOT_FOLDER_ID", "")
_DEFAULT_OUTPUTS = build_output_paths(root_id=DRIVE_ROOT_FOLDER_ID)
SCAN_OUT_DIR = os.getenv("SCAN_OUT_DIR", str(_DEFAULT_OUTPUTS.scan_output))
SCAN_INCLUDED_NAME = os.getenv("SCAN_INCLUDED_NAME", "included_index.json")
SCAN_FILTERED_NAME = os.getenv("SCAN_FILTERED_NAME", "filtered_index.json")

def validate_env():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET in env")
