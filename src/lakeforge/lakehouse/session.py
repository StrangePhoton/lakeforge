"""One local Spark session for the Bronze load."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def default_warehouse_dir() -> Path:
    """Scratch directory for Derby and Spark local files, outside any Bronze snapshot."""
    if sys.platform == "win32":
        return _windows_hadoop_home().parent / "spark-warehouse"
    cache_home = os.environ.get("XDG_CACHE_HOME")
    root = Path(cache_home) if cache_home else Path.home() / ".cache"
    return root / "lakeforge" / "spark-warehouse"


def build_session(*, warehouse_dir: Path) -> SparkSession:
    """Start a local Spark session with the Delta catalog enabled."""
    _prepare_process(warehouse_dir)
    warehouse = warehouse_dir.resolve()
    if sys.platform == "win32":
        warehouse = _windows_hadoop_home().parent / "spark-warehouse"
    derby_home = warehouse / "derby"
    local_dir = warehouse / "local"
    derby_home.mkdir(parents=True, exist_ok=True)
    local_dir.mkdir(parents=True, exist_ok=True)
    java_options = [
        f'-Dderby.system.home="{derby_home.as_posix()}"',
        "-Dfile.encoding=UTF-8",
        "-Duser.timezone=UTC",
    ]
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("lakeforge")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse.as_posix())
        .config("spark.local.dir", local_dir.as_posix())
        .config("spark.driver.extraJavaOptions", " ".join(java_options))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    if sys.platform == "win32":
        classes = _compile_bare_filesystem()
        builder = (
            builder.config("spark.driver.extraClassPath", classes.as_posix())
            .config("spark.executor.extraClassPath", classes.as_posix())
            .config("spark.hadoop.fs.file.impl", "lakeforge.hadoop.BareLocalFileSystem")
        )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _prepare_process(warehouse_dir: Path) -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    if sys.platform == "win32":
        _prepare_windows_hadoop()
    if os.environ.get("JAVA_HOME"):
        return
    completed = subprocess.run(
        ["java", "-XshowSettings:properties", "-version"],
        check=False,
        capture_output=True,
        text=True,
    )
    for line in completed.stderr.splitlines():
        if "java.home" not in line or "=" not in line:
            continue
        os.environ["JAVA_HOME"] = line.split("=", 1)[1].strip()
        return
    raise ValueError("Java 17 or 21 is required to start the local Spark session")


def _prepare_windows_hadoop() -> None:
    """Give Hadoop a winutils executable so local Spark can chmod files.

    Hadoop on Windows refuses to start unless ``HADOOP_HOME/bin/winutils.exe``
    exists. The commands LakeForge needs are permission checks, so the stub
    reports a fixed mode and accepts chmod. A preinstalled Hadoop home is left
    unchanged.
    """
    existing = os.environ.get("HADOOP_HOME")
    if existing and (Path(existing) / "bin" / "winutils.exe").is_file():
        return
    home = _windows_hadoop_home()
    executable = home / "bin" / "winutils.exe"
    if not executable.is_file():
        _compile_winutils(executable)
    os.environ["HADOOP_HOME"] = str(home)


def _windows_hadoop_home() -> Path:
    local_app = os.environ.get("LOCALAPPDATA")
    if local_app and local_app.isascii():
        return Path(local_app) / "lakeforge" / "hadoop"
    return Path.home() / "lakeforge" / "hadoop"


def _compile_winutils(executable: Path) -> None:
    compiler = _csc_compiler()
    executable.parent.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).with_name("winutils_stub.cs")
    completed = subprocess.run(
        [str(compiler), "/nologo", f"/out:{executable}", str(source)],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 or not executable.is_file():
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ValueError(
            "Windows Spark requires winutils.exe. Set HADOOP_HOME to a directory "
            f"that contains bin\\winutils.exe. Compiler output: {detail}"
        )


def _compile_bare_filesystem() -> Path:
    classes = _windows_hadoop_home().parent / "bare-fs"
    marker = classes / "lakeforge" / "hadoop" / "BareLocalFileSystem.class"
    if marker.is_file():
        return classes
    source = Path(__file__).parent / "hadoop"
    jars = Path(pyspark.__file__).parent / "jars"
    api_jars = sorted(jars.glob("hadoop-client-api-*.jar"))
    if not api_jars:
        raise ValueError("PySpark is missing hadoop-client-api")
    classes.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        [
            str(_javac()),
            "--release",
            "17",
            "-classpath",
            str(api_jars[0]),
            "-d",
            str(classes),
            str(source / "BareRawLocalFileSystem.java"),
            str(source / "BareLocalFileSystem.java"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 or not marker.is_file():
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ValueError(f"Could not compile the Windows Spark filesystem shim. {detail}")
    return classes


def _javac() -> Path:
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / "javac.exe"
        if candidate.is_file():
            return candidate
    java = shutil.which("java")
    if java is not None:
        candidate = Path(java).resolve().parent / "javac.exe"
        if candidate.is_file():
            return candidate
    raise ValueError("javac is required to prepare the Windows Spark filesystem")


def _csc_compiler() -> Path:
    found = shutil.which("csc")
    if found is not None:
        return Path(found)
    windows = Path(os.environ.get("WINDIR", r"C:\Windows"))
    for name in ("Framework64", "Framework"):
        candidate = windows / "Microsoft.NET" / name / "v4.0.30319" / "csc.exe"
        if candidate.is_file():
            return candidate
    raise ValueError(
        "Windows Spark requires winutils.exe. Set HADOOP_HOME to a directory "
        "that contains bin\\winutils.exe, or install the .NET Framework compiler."
    )
