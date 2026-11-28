import lakeforge


def test_package_version() -> None:
    assert lakeforge.__version__ == "0.8.0"
