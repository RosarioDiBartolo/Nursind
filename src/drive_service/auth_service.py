import os

from cartellino_parser.exceptions import CredentialsError, OptionalDependencyError

from . import config


def load_creds(
    *,
    settings: config.GoogleDriveSettings | None = None,
    load_env: bool = True,
):
    try:
        from google.auth.exceptions import RefreshError
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ModuleNotFoundError as exc:
        raise OptionalDependencyError(
            "Google Drive support requires optional Google client dependencies. "
            "Install the 'google' extra to enable Drive-backed steps."
        ) from exc

    resolved_settings = settings or config.load_google_drive_settings(load_env=load_env)
    if not resolved_settings.client_id or not resolved_settings.client_secret:
        raise CredentialsError("Google Drive client_id and client_secret are required.")
    creds = None

    if os.path.exists(resolved_settings.token_path):
        creds = Credentials.from_authorized_user_file(
            resolved_settings.token_path,
            resolved_settings.scopes,
        )

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                creds = None

        if not creds or not creds.valid:
            flow = InstalledAppFlow.from_client_config(
                {
                    "installed": {
                        "client_id": resolved_settings.client_id,
                        "client_secret": resolved_settings.client_secret,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                    }
                },
                resolved_settings.scopes,
            )
            creds = flow.run_local_server(port=0)

        if creds:
            with open(resolved_settings.token_path, "w", encoding="utf-8") as f:
                f.write(creds.to_json())

    if creds is None:
        raise CredentialsError("Unable to load Google Drive credentials.")

    return creds
