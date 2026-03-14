import os
from pathlib import Path

from dotenv import load_dotenv

 


load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
 
CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "token.json")
 

def validate_env():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET in env")
