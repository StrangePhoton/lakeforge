import pytest

from lakeforge.cli import main


def test_cli_rejects_invalid_vehicle_count(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["generate", "--vehicles", "0"])

    assert code == 1
    assert "vehicle_count" in capsys.readouterr().err
