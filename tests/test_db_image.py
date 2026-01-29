"""
Tests and fixtures for building and running the custom DB image.

This module builds the db/Dockerfile once, validates the image, and
starts a DockerContainer so downstream DB tests share the same runtime.
"""

from __future__ import annotations

import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator

import psycopg
import pytest


from testcontainers.core.container import DockerContainer

pytestmark = pytest.mark.database

PROJECT_ROOT = Path(__file__).parent.parent
DB_CONTEXT = PROJECT_ROOT / "db"


@dataclass(frozen=True)
class BuiltImage:
    tag: str
    stdout: str
    stderr: str
    ok: bool = True


@dataclass(frozen=True)
class InitResult:
    ok: bool
    logs: str = ""


def _wait_for_db_ready(host: str, port: int, user: str, password: str, db: str) -> None:
    """Poll until PostgreSQL accepts connections or timeout expires."""
    deadline = time.time() + 60
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with psycopg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=db,
                connect_timeout=5,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                return
        except Exception as exc:  # pragma: no cover - best effort polling
            last_error = exc
            time.sleep(1)
    raise TimeoutError(f"Postgres did not become ready: {last_error}")


def _assert_db_dockerfile_exists() -> None:
    dockerfile = DB_CONTEXT / "Dockerfile"
    assert dockerfile.exists(), "db/Dockerfile does not exist"


def _assert_db_dockerfile_has_healthcheck() -> None:
    dockerfile = DB_CONTEXT / "Dockerfile"
    content = dockerfile.read_text()
    assert "HEALTHCHECK" in content, "Dockerfile missing HEALTHCHECK instruction"
    assert "pg_isready" in content, "HEALTHCHECK should use pg_isready command"


def _assert_db_dockerfile_copies_initdb_scripts() -> None:
    dockerfile = DB_CONTEXT / "Dockerfile"
    content = dockerfile.read_text()
    assert "COPY initdb /docker-entrypoint-initdb.d/" in content, (
        "Dockerfile should copy initdb scripts to /docker-entrypoint-initdb.d/"
    )


def _assert_db_initdb_scripts_exist() -> None:
    initdb_dir = DB_CONTEXT / "initdb"
    assert initdb_dir.exists(), "db/initdb directory does not exist"
    assert initdb_dir.is_dir(), "db/initdb is not a directory"
    scripts = list(initdb_dir.glob("*.sh"))
    assert len(scripts) > 0, "No init scripts found in db/initdb/"
    assert (initdb_dir / "01_create_databases.sh").exists(), "Missing 01_create_databases.sh"
    assert (initdb_dir / "02_users.sh").exists(), "Missing 02_users.sh"


def _assert_db_version_file_exists() -> None:
    version_file = DB_CONTEXT / "VERSION"
    assert version_file.exists(), "db/VERSION does not exist"
    version = version_file.read_text().strip()
    assert version, "db/VERSION is empty"
    assert version.count(".") >= 1, "Version should follow semantic versioning"


def _build_container(image: str, test_env: dict[str, str]) -> DockerContainer:
    container = DockerContainer(image).with_exposed_ports("5432/tcp")
    for key, value in test_env.items():
        if value is not None:
            container = container.with_env(key, value)
    return container


@pytest.fixture(scope="session")
def db_image_tag() -> str:
    version_file = DB_CONTEXT / "VERSION"
    assert version_file.exists(), "db/VERSION must exist"
    try:
        version = version_file.read_text().strip()
    except Exception as exc:  # pragma: no cover - fail fast on unreadable version
        raise AssertionError(f"Failed to read db/VERSION: {exc}") from exc

    assert version, "db/VERSION must not be empty"
    assert version.count(".") == 2 and all(part.isdigit() for part in version.split(".")), (
        "db/VERSION must be in x.y.z format (digits only)"
    )

    suffix = uuid.uuid4().hex[:8]
    return f"kivoll-db-test:{version}-{suffix}"


@pytest.fixture(scope="session")
def custom_db_image_validated() -> None:
    _assert_db_dockerfile_exists()
    _assert_db_dockerfile_has_healthcheck()
    _assert_db_dockerfile_copies_initdb_scripts()
    _assert_db_initdb_scripts_exist()
    _assert_db_version_file_exists()


@pytest.fixture(scope="session")
def built_db_image(db_image_tag: str, custom_db_image_validated: None) -> Generator[BuiltImage, Any, None]:
    result = subprocess.run(
        ["docker", "build", "-t", db_image_tag, str(DB_CONTEXT)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        yield BuiltImage(tag="", stdout=result.stdout, stderr=result.stderr, ok=False)
        return
    yield BuiltImage(tag=db_image_tag, stdout=result.stdout, stderr=result.stderr, ok=True)
    subprocess.run(["docker", "rmi", "-f", db_image_tag], capture_output=True, text=True)


@pytest.fixture(scope="session")
def init_result(built_db_image: BuiltImage, test_env: dict[str, str]) -> InitResult:
    if not built_db_image.ok:
        return InitResult(ok=False, logs=f"Image build failed "
                                         f"\nSTDOUT:{built_db_image.stdout}"
                                         f"\nSTDERR:{built_db_image.stderr}")
    container = _build_container(built_db_image.tag, test_env)
    logs = ""
    try:
        container.start()
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(5432))
        _wait_for_db_ready(
            host,
            port,
            test_env["POSTGRES_USER"],
            test_env["POSTGRES_PASSWORD"],
            test_env["POSTGRES_DB"],
        )
        return InitResult(ok=True, logs="")
    except Exception:
        try:
            raw_logs = container.get_logs()
            logs = raw_logs.decode("utf-8", errors="ignore") if isinstance(raw_logs, (bytes, bytearray)) else str(raw_logs)
        except Exception:
            logs = ""
        return InitResult(ok=False, logs=logs)
    finally:
        try:
            container.stop()
        except Exception:
            pass


@pytest.fixture(scope="session")
def postgres_container(
    built_db_image: BuiltImage, test_env: dict[str, str], init_result: InitResult
):
    if not built_db_image.ok:
        pytest.skip("Image build failed; skipping postgres_container.")
    if not init_result.ok:
        pytest.skip(
            "Container failed to initialise; skipping tests that require postgres_container."
        )

    container = _build_container(built_db_image.tag, test_env)
    with container:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(5432))
        _wait_for_db_ready(
            host,
            port,
            test_env["POSTGRES_USER"],
            test_env["POSTGRES_PASSWORD"],
            test_env["POSTGRES_DB"],
        )
        yield container


class TestCustomDBImage:
    """Validate the custom DB Dockerfile contents."""

    def test_db_dockerfile_exists(self):
        _assert_db_dockerfile_exists()

    def test_db_dockerfile_has_healthcheck(self):
        _assert_db_dockerfile_has_healthcheck()

    def test_db_dockerfile_copies_initdb_scripts(self):
        _assert_db_dockerfile_copies_initdb_scripts()

    def test_db_initdb_scripts_exist(self):
        _assert_db_initdb_scripts_exist()

    def test_db_version_file_exists(self):
        _assert_db_version_file_exists()


def test_build_db_dockerfile(built_db_image: BuiltImage):
    if not built_db_image.ok:
        pytest.fail("Image build failed; skipping build validation.")
    assert built_db_image.tag, "Built image tag should not be empty"
    inspect = subprocess.run(
        ["docker", "image", "inspect", built_db_image.tag],
        capture_output=True,
        text=True,
    )
    assert inspect.returncode == 0, (
        f"Built image {built_db_image.tag} not found.\n"
        f"stdout:\n{inspect.stdout}\n"
        f"stderr:\n{inspect.stderr}\n"
    )
    assert built_db_image.stdout or built_db_image.stderr, "docker build output missing"


def test_initialisation_is_valid(init_result: InitResult):
    assert init_result.ok, (
        f"postgres db failed to initialise after timeout! Logs: {init_result.logs}"
    )
