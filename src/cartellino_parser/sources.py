from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


DEFAULT_DRIVE_SCOPES = ("https://www.googleapis.com/auth/drive.readonly",)


class SourceModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class LocalSourceSpec(SourceModel):
    kind: str = "local"
    pipeline_dir: Path | str | None = None
    documents_dir: Path | str | None = None
    index_path: Path | str | None = None


class DriveSourceSpec(SourceModel):
    kind: str = "drive"
    root_id: str


class DriveAuthConfig(SourceModel):
    client_id: str | None = None
    client_secret: str | None = None
    token_path: Path | str = Field(default="token.json")
    scopes: tuple[str, ...] = Field(default_factory=lambda: DEFAULT_DRIVE_SCOPES)
    load_env: bool = True

