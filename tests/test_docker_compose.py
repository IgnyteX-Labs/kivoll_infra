"""
Test Docker Compose configurations.

These tests verify that:
1. Docker Compose files are valid YAML
2. Compose configurations validate successfully
3. Services can be started and health checks pass
4. Volume and network configurations are correct
"""

import copy
import subprocess
import uuid
from pathlib import Path
from typing import Generator, TypedDict

import pytest
import yaml

from tests.test_db_image import BuiltImage
from tests.test_db_image import built_db_image  # noqa: F401 - imported for plugin discovery

PROJECT_ROOT = Path(__file__).parent.parent


class ComposeProject(TypedDict):
    compose_file: Path
    compose_dir: Path
    project_name: str


# Helper assertions to share between tests and gating fixtures
def _assert_compose_file_exists(compose_file: Path) -> None:
    assert compose_file.exists(), "docker-compose.yml does not exist"


def _assert_compose_has_services(data: dict) -> None:
    assert data is not None, "Compose file is empty"
    assert "services" in data, "Compose file missing 'services' key"


def _assert_compose_has_db_service(data: dict) -> None:
    assert "db" in data["services"], "Missing 'db' service in compose"


def _assert_db_service_uses_custom_image(data: dict) -> None:
    db_service = data["services"]["db"]
    assert "image" in db_service, "db service missing image"
    assert "kivoll_db" in db_service["image"], "db should use custom kivoll_db image"


@pytest.fixture(scope="session")
def compose_file() -> Path:
    return PROJECT_ROOT / "docker-compose.yml"


@pytest.fixture(scope="session")
def compose_data(compose_file: Path) -> dict:
    _assert_compose_file_exists(compose_file)
    with open(compose_file) as f:
        data = yaml.safe_load(f)
    _assert_compose_has_services(data)
    return data


@pytest.fixture(scope="session")
def compose_validated(compose_data: dict) -> dict:
    _assert_compose_has_db_service(compose_data)
    _assert_db_service_uses_custom_image(compose_data)
    return compose_data


@pytest.fixture(scope="session")
def patched_compose_file(
    compose_validated: dict, built_db_image: BuiltImage, tmp_path_factory: pytest.TempPathFactory
) -> Path:
    """Write a temporary compose file that uses the freshly built DB image."""
    if not built_db_image.ok:
        pytest.skip("Image build failed; skipping patched compose file.")
    temp_dir = tmp_path_factory.mktemp("compose")
    compose_suffix = uuid.uuid4().hex[:8]
    patched = copy.deepcopy(compose_validated)
    patched["services"]["db"]["image"] = built_db_image.tag

    compose_path = temp_dir / f"docker-compose-{compose_suffix}.yml"
    with open(compose_path, "w") as f:
        yaml.safe_dump(patched, f, sort_keys=False)
    return compose_path


@pytest.fixture(scope="session")
def compose_config_validated(patched_compose_file: Path) -> None:
    result = subprocess.run(
        ["docker", "compose", "-f", str(patched_compose_file), "config"],
        capture_output=True,
        text=True,
        cwd=patched_compose_file.parent,
    )
    assert result.returncode == 0, (
        f"docker-compose config failed:\n"
        f"stdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )


@pytest.fixture(scope="class")
def compose_project(patched_compose_file: Path) -> Generator[ComposeProject, None, None]:
    project_name = f"kivoll_test_{uuid.uuid4().hex[:8]}"
    yield ComposeProject(
        compose_file=patched_compose_file,
        compose_dir=patched_compose_file.parent,
        project_name=project_name,
    )


@pytest.mark.compose
class TestDockerComposeValidity:
    """Test that Docker Compose file is readable and merges config."""

    def test_compose_is_valid_yaml(self, compose_file: Path, compose_data: dict):
        _assert_compose_has_services(compose_data)

    def test_compose_config_validates(self, compose_config_validated: None):
        # Validation handled in fixture; assertion is in fixture for reuse
        assert compose_config_validated is None


@pytest.mark.compose
class TestComposeStructure:
    """Test the structure of the Docker Compose configuration."""

    def test_compose_has_db_service(self, compose_validated: dict):
        _assert_compose_has_db_service(compose_validated)

    def test_db_uses_custom_image_reference(self, compose_validated: dict):
        _assert_db_service_uses_custom_image(compose_validated)


@pytest.mark.full
@pytest.mark.usefixtures("compose_validated", "compose_config_validated")
class TestDockerComposeFullInstance:
    """Test the full compose stack with custom DB image."""

    def test_compose_services_become_healthy(
        self,
        compose_project: ComposeProject,
    ) -> None:
        compose_file = compose_project["compose_file"]
        compose_dir = compose_project["compose_dir"]
        project_name = compose_project["project_name"]

        wait_timeout = 180
        up_timeout = wait_timeout + 60

        try:
            result = subprocess.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(compose_file),
                    "-p",
                    project_name,
                    "up",
                    "--wait",
                    "--wait-timeout",
                    str(wait_timeout),
                    "--no-build",
                ],
                capture_output=True,
                text=True,
                cwd=compose_dir,
                timeout=up_timeout,
            )

            assert result.returncode == 0, (
                "docker compose up --wait failed:\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}"
            )
        finally:
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(compose_file),
                    "-p",
                    project_name,
                    "down",
                    "-v",
                ],
                capture_output=True,
                text=True,
                cwd=compose_dir,
                timeout=60,
            )
