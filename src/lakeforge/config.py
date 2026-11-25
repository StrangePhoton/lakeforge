"""Runtime configuration loaded from the process environment."""

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
ProfileName = Literal["tiny", "dev", "large"]


def _local_dummy_password() -> SecretStr:
    """Return the local dummy password for the synthetic source database."""
    return SecretStr("lakeforge")


class Settings(BaseSettings):
    """Typed settings for the local source simulator.

    Values come from ``LAKEFORGE_*`` environment variables, then a local ``.env``
    file, then the defaults below. The database password is a local dummy for
    synthetic data and is kept out of ``repr``.
    """

    model_config = SettingsConfigDict(
        env_prefix="LAKEFORGE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    postgres_host: str = "localhost"
    postgres_port: int = Field(default=54329, ge=1, le=65535)
    postgres_db: str = "lakeforge"
    postgres_user: str = "lakeforge"
    postgres_password: SecretStr = Field(default_factory=_local_dummy_password)
    landing_dir: Path = Path("data/landing")
    bronze_dir: Path = Path("data/bronze")
    silver_dir: Path = Path("data/silver")
    gold_dir: Path = Path("data/gold")
    quality_dir: Path = Path("data/quality")
    log_level: LogLevel = "INFO"
    seed: int = Field(default=42, ge=0)
    profile: ProfileName = "dev"
    anomalies: bool = True
    vehicle_count: int | None = Field(default=None, ge=1)
    hours: int | None = Field(default=None, ge=1)
    interval_minutes: int | None = Field(default=None, ge=1)

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_log_level(cls, value: object) -> object:
        if isinstance(value, str):
            return value.upper()
        return value

    @field_validator("profile", mode="before")
    @classmethod
    def normalize_profile(cls, value: object) -> object:
        if isinstance(value, str):
            return value.lower()
        return value


def load_settings() -> Settings:
    """Load settings from the environment and an optional ``.env`` file."""
    return Settings()
