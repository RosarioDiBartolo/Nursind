import os

from dotenv import load_dotenv

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
EXCLUDE_TERMS = [
    "cedolino",
    "cedolini",
    "busta",
    "buste",
    "paga",
    "busta paga",
    "buste paga",
]

CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "token.json")
DRIVE_ROOT_FOLDER_ID = os.getenv("DRIVE_ROOT_FOLDER_ID", "")
SCAN_REPORT_PATH = os.getenv("SCAN_REPORT_PATH", "./scan.json")

def validate_env():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET in env")
