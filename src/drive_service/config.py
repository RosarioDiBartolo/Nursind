from __future__ import annotations

import os
from dataclasses import dataclass

from cartellino_parser.exceptions import ConfigurationError, OptionalDependencyError

DEFAULT_SCOPES = ("https://www.googleapis.com/auth/drive.readonly",)


@dataclass(frozen=True, slots=True)
class GoogleDriveSettings:
    client_id: str
    client_secret: str
    token_path: str = "token.json"
    scopes: tuple[str, ...] = DEFAULT_SCOPES


def load_env_file() -> None:
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError as exc:
        raise OptionalDependencyError(
            "python-dotenv is required to load Google Drive settings from .env files. "
            "Install the 'google' extra or pass explicit drive credentials."
        ) from exc

    load_dotenv()


def load_google_drive_settings(*, load_env: bool = True) -> GoogleDriveSettings:
    if load_env:
        load_env_file()

    client_id = str(os.getenv("GOOGLE_CLIENT_ID") or "").strip()
    client_secret = str(os.getenv("GOOGLE_CLIENT_SECRET") or "").strip()
    token_path = str(os.getenv("GOOGLE_TOKEN_PATH") or "token.json").strip() or "token.json"

    if not client_id or not client_secret:
        raise ConfigurationError("Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET in env")

    return GoogleDriveSettings(
        client_id=client_id,
        client_secret=client_secret,
        token_path=token_path,
    )


def validate_env(*, load_env: bool = True) -> GoogleDriveSettings:
    return load_google_drive_settings(load_env=load_env)
