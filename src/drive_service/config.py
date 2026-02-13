import os

from dotenv import load_dotenv

from drive_service.names import normalize_term

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
 
CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "token.json")
DRIVE_ROOT_FOLDER_ID = os.getenv("DRIVE_ROOT_FOLDER_ID", "")
SCAN_OUT_DIR = os.getenv("SCAN_OUT_DIR", "scan")
SCAN_INCLUDED_NAME = os.getenv("SCAN_INCLUDED_NAME", "included.index.json")
SCAN_FILTERED_NAME = os.getenv("SCAN_FILTERED_NAME", "filtered.index.json")

def validate_env():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET in env")
