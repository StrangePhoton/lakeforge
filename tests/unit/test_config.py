import pytest
from pydantic import SecretStr, ValidationError

from lakeforge.config import Settings

_ENV_KEYS = (
    "LAKEFORGE_POSTGRES_HOST",
    "LAKEFORGE_POSTGRES_PORT",
    "LAKEFORGE_POSTGRES_DB",
    "LAKEFORGE_POSTGRES_USER",
    "LAKEFORGE_POSTGRES_PASSWORD",
    "LAKEFORGE_LANDING_DIR",
    "LAKEFORGE_BRONZE_DIR",
    "LAKEFORGE_LOG_LEVEL",
    "LAKEFORGE_SEED",
    "LAKEFORGE_PROFILE",
    "LAKEFORGE_ANOMALIES",
    "LAKEFORGE_VEHICLE_COUNT",
    "LAKEFORGE_HOURS",
    "LAKEFORGE_INTERVAL_MINUTES",
)


@pytest.fixture
def isolated_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_defaults(isolated_env: None) -> None:
    settings = Settings(_env_file=None)

    assert settings.postgres_host == "localhost"
    assert settings.postgres_port == 54329
    assert settings.postgres_db == "lakeforge"
    assert settings.postgres_user == "lakeforge"
    assert settings.postgres_password.get_secret_value() == "lakeforge"
    assert settings.landing_dir.as_posix() == "data/landing"
    assert settings.bronze_dir.as_posix() == "data/bronze"
    assert settings.log_level == "INFO"
    assert settings.seed == 42
    assert settings.profile == "dev"
    assert settings.anomalies is True
    assert settings.vehicle_count is None
    assert settings.hours is None
    assert settings.interval_minutes is None


def test_environment_overrides(isolated_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAKEFORGE_POSTGRES_HOST", "db.internal")
    monkeypatch.setenv("LAKEFORGE_POSTGRES_PORT", "5432")
    monkeypatch.setenv("LAKEFORGE_LOG_LEVEL", "debug")
    monkeypatch.setenv("LAKEFORGE_SEED", "7")

    settings = Settings(_env_file=None)

    assert settings.postgres_host == "db.internal"
    assert settings.postgres_port == 5432
    assert settings.log_level == "DEBUG"
    assert settings.seed == 7


def test_password_is_hidden_from_repr(isolated_env: None) -> None:
    settings = Settings(_env_file=None, postgres_password=SecretStr("super-secret-value"))

    assert "super-secret-value" not in repr(settings)
    assert "super-secret-value" not in repr(settings.postgres_password)


def test_invalid_port_is_rejected(isolated_env: None) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, postgres_port=0)


def test_invalid_vehicle_count_is_rejected(isolated_env: None) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, vehicle_count=0)
