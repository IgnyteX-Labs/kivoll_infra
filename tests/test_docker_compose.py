"""
Test Docker Compose configurations.

These tests verify that:
1. Docker Compose files are valid YAML
2. Compose configurations validate successfully
3. Services can be started and health checks pass
4. Volume and network configurations are correct
"""

import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Generator, TypedDict

import pytest
import yaml


PROJECT_ROOT = Path(__file__).parent.parent


class ComposeProject(TypedDict):
    compose_file: Path
    compose_dir: Path
    project_name: str


class BuiltComposeProject(ComposeProject):
    build_result: subprocess.CompletedProcess[str]


def create_temp_env_files(temp_dir: Path) -> None:
    """
    Create temporary .env files from .env.example templates.

    This is needed for docker-compose config validation since .env files
    are not committed to the repository.

    Args:
        temp_dir: Temporary directory where .env files will be created
    """
    # Map of .env files to their .env.example sources in project root
    env_mappings = {
        ".env.admin": PROJECT_ROOT / ".env.admin.example",
        ".env.worker": PROJECT_ROOT / ".env.worker.example",
        ".env.api": PROJECT_ROOT / ".env.api.example",
        ".env.predict": PROJECT_ROOT / ".env.predict.example",
    }

    for env_file, example_file in env_mappings.items():
        if example_file.exists():
            # Copy example file to temp directory with the expected name
            shutil.copy(example_file, temp_dir / env_file)


def create_temp_compose_project(
    compose_file: Path,
    temp_root: Path,
    build_context: Path | None = None,
) -> Path:
    """
    Create a temporary compose project layout without touching the repo root.

    This copies the compose file, generates .env files from examples in temp_root,
    and mirrors common/initdb so volume paths resolve correctly.
    """
    temp_compose_dir = temp_root / compose_file.parent.name
    temp_compose_dir.mkdir(parents=True, exist_ok=True)

    create_temp_env_files(temp_root)

    temp_compose = temp_compose_dir / compose_file.name
    if build_context is None:
        shutil.copy(compose_file, temp_compose)
    else:
        with open(compose_file) as f:
            data = yaml.safe_load(f)

        for service in data.get("services", {}).values():
            build = service.get("build")
            if isinstance(build, dict):
                build["context"] = str(build_context)
            elif isinstance(build, str):
                service["build"] = str(build_context)

        with open(temp_compose, "w") as f:
            yaml.safe_dump(data, f, sort_keys=False)

    temp_initdb = temp_root / "common" / "initdb"
    temp_initdb.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(PROJECT_ROOT / "common" / "initdb", temp_initdb)

    return temp_compose


@pytest.fixture(scope="class")
def full_compose_project() -> Generator[ComposeProject, None, None]:
    """Create a temporary compose project for integration-style tests."""
    compose_file = PROJECT_ROOT / "prod" / "docker-compose.yml"
    project_name = f"kivoll_test_{uuid.uuid4().hex[:8]}"

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_root = Path(tmpdir)
        temp_compose = create_temp_compose_project(
            compose_file,
            temp_root,
            build_context=PROJECT_ROOT,
        )

        yield ComposeProject(
            compose_file=temp_compose, compose_dir=temp_root, project_name=project_name
        )


@pytest.fixture(scope="class")
def built_worker_image(
    full_compose_project: ComposeProject,
) -> BuiltComposeProject:
    """Build the worker image once for the full-compose integration test."""
    compose_file = full_compose_project["compose_file"]
    compose_dir = full_compose_project["compose_dir"]
    project_name = full_compose_project["project_name"]

    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "-p",
            project_name,
            "build",
            "worker",
            "--no-cache",
        ],
        capture_output=True,
        text=True,
        cwd=compose_dir,
        timeout=900,
    )

    return {
        "compose_file": compose_file,
        "compose_dir": compose_dir,
        "project_name": project_name,
        "build_result": result,
    }


@pytest.mark.integration
class TestDockerComposeValidity:
    """Test that Docker Compose files are valid."""

    def test_local_compose_is_valid_yaml(self):
        """Verify local/docker-compose.yml is valid YAML."""
        compose_file = PROJECT_ROOT / "local" / "docker-compose.yml"
        assert compose_file.exists(), "local/docker-compose.yml does not exist"

        with open(compose_file) as f:
            data = yaml.safe_load(f)

        assert data is not None, "Compose file is empty"
        assert "services" in data, "Compose file missing 'services' key"

    def test_prod_compose_is_valid_yaml(self):
        """Verify prod/docker-compose.yml is valid YAML."""
        compose_file = PROJECT_ROOT / "prod" / "docker-compose.yml"
        assert compose_file.exists(), "prod/docker-compose.yml does not exist"

        with open(compose_file) as f:
            data = yaml.safe_load(f)

        assert data is not None, "Compose file is empty"
        assert "services" in data, "Compose file missing 'services' key"

    def test_local_compose_config_validates(self):
        """Verify docker-compose config command succeeds for local."""
        compose_file = PROJECT_ROOT / "local" / "docker-compose.yml"

        # Create temporary directory with .env files from examples
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_root = Path(tmpdir)
            temp_local = temp_root / "local"
            temp_local.mkdir()
            create_temp_env_files(temp_root)

            # Copy docker-compose.yml to temp directory to use temp .env files
            temp_compose = temp_local / "docker-compose.yml"
            shutil.copy(compose_file, temp_compose)

            result = subprocess.run(
                ["docker", "compose", "-f", str(temp_compose), "config"],
                capture_output=True,
                text=True,
                cwd=temp_local,
            )

            # docker-compose config should succeed (exit code 0)
            assert result.returncode == 0, (
                f"docker-compose config failed:\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}"
            )

    def test_prod_compose_config_validates(self):
        """Verify docker-compose config command succeeds for prod."""
        compose_file = PROJECT_ROOT / "prod" / "docker-compose.yml"

        # Create temporary directory structure mimicking the project layout
        # prod/docker-compose.yml references ../.env.* files
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_root = Path(tmpdir)
            temp_prod = temp_root / "prod"
            temp_prod.mkdir()

            # Create .env files in the temp root (parent of prod/)
            create_temp_env_files(temp_root)

            # Copy docker-compose.yml to temp prod directory
            temp_compose = temp_prod / "docker-compose.yml"
            shutil.copy(compose_file, temp_compose)

            result = subprocess.run(
                ["docker", "compose", "-f", str(temp_compose), "config"],
                capture_output=True,
                text=True,
                cwd=temp_prod,
            )

            # docker-compose config should succeed (exit code 0)
            assert result.returncode == 0, (
                f"docker-compose config failed:\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}"
            )


@pytest.mark.integration
class TestLocalComposeStructure:
    """Test the structure of the local Docker Compose configuration."""

    def test_local_compose_has_db_service(self):
        """Verify local compose defines a db service."""
        compose_file = PROJECT_ROOT / "local" / "docker-compose.yml"
        with open(compose_file) as f:
            data = yaml.safe_load(f)

        assert "db" in data["services"], "Missing 'db' service"

    def test_local_db_service_has_healthcheck(self):
        """Verify db service has a healthcheck configured."""
        compose_file = PROJECT_ROOT / "local" / "docker-compose.yml"
        with open(compose_file) as f:
            data = yaml.safe_load(f)

        db_service = data["services"]["db"]
        assert "healthcheck" in db_service, "db service missing healthcheck"
        assert "test" in db_service["healthcheck"], "healthcheck missing test command"
        assert "interval" in db_service["healthcheck"], "healthcheck missing interval"
        assert "timeout" in db_service["healthcheck"], "healthcheck missing timeout"
        assert "retries" in db_service["healthcheck"], "healthcheck missing retries"

    def test_local_db_mounts_init_scripts(self):
        """Verify db service mounts the init scripts directory."""
        compose_file = PROJECT_ROOT / "local" / "docker-compose.yml"
        with open(compose_file) as f:
            data = yaml.safe_load(f)

        db_service = data["services"]["db"]
        assert "volumes" in db_service, "db service has no volumes"

        # Check for init scripts mount
        init_mount = None
        for volume in db_service["volumes"]:
            if isinstance(volume, str) and "/docker-entrypoint-initdb.d" in volume:
                init_mount = volume
                break

        assert init_mount is not None, "db service does not mount init scripts"
        assert "../common/initdb" in init_mount, "Init scripts mount path incorrect"
        assert ":ro" in init_mount, "Init scripts should be mounted read-only"

    def test_local_db_uses_correct_image(self):
        """Verify db service uses the correct PostgreSQL image."""
        compose_file = PROJECT_ROOT / "local" / "docker-compose.yml"
        with open(compose_file) as f:
            data = yaml.safe_load(f)

        db_service = data["services"]["db"]
        assert "image" in db_service, "db service missing image"
        assert db_service["image"].startswith("postgres:"), (
            "db should use postgres image"
        )

    def test_local_worker_depends_on_db_health(self):
        """Verify worker service has proper depends_on with health condition."""
        compose_file = PROJECT_ROOT / "local" / "docker-compose.yml"
        with open(compose_file) as f:
            data = yaml.safe_load(f)

        if "worker" in data["services"]:
            worker_service = data["services"]["worker"]
            assert "depends_on" in worker_service, "worker service should depend on db"

            # Check if depends_on uses long syntax with condition
            depends_on = worker_service["depends_on"]
            if isinstance(depends_on, dict):
                assert "db" in depends_on, "worker should depend on db service"
                db_dep = depends_on["db"]
                if isinstance(db_dep, dict):
                    assert "condition" in db_dep, "db dependency should have condition"
                    assert db_dep["condition"] == "service_healthy", (
                        "worker should wait for db to be healthy"
                    )


@pytest.mark.integration
class TestProdComposeStructure:
    """Test the structure of the prod Docker Compose configuration."""

    def test_prod_compose_has_db_service(self):
        """Verify prod compose defines a db service."""
        compose_file = PROJECT_ROOT / "prod" / "docker-compose.yml"
        with open(compose_file) as f:
            data = yaml.safe_load(f)

        assert "db" in data["services"], "Missing 'db' service in prod compose"

    def test_prod_db_service_has_healthcheck(self):
        """Verify prod db service has a healthcheck configured."""
        compose_file = PROJECT_ROOT / "prod" / "docker-compose.yml"
        with open(compose_file) as f:
            data = yaml.safe_load(f)

        db_service = data["services"]["db"]
        assert "healthcheck" in db_service, "prod db service missing healthcheck"

    def test_prod_db_mounts_init_scripts(self):
        """Verify prod db service mounts the init scripts directory."""
        compose_file = PROJECT_ROOT / "prod" / "docker-compose.yml"
        with open(compose_file) as f:
            data = yaml.safe_load(f)

        db_service = data["services"]["db"]
        assert "volumes" in db_service, "prod db service has no volumes"

        # Check for init scripts mount
        init_mount = None
        for volume in db_service["volumes"]:
            if isinstance(volume, str) and "/docker-entrypoint-initdb.d" in volume:
                init_mount = volume
                break

        assert init_mount is not None, "prod db service does not mount init scripts"


@pytest.mark.integration
@pytest.mark.slow
class TestDockerComposeFullInstance:
    """Build the worker image and start the full compose stack."""

    def test_worker_dockerfile_builds(
        self,
        built_worker_image: BuiltComposeProject,
    ) -> None:
        """Build the worker Dockerfile via compose."""
        result = built_worker_image["build_result"]

        assert result.returncode == 0, (
            "docker compose build worker failed:\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

    def test_compose_services_become_healthy(
        self,
        built_worker_image: BuiltComposeProject,
    ) -> None:
        """Start the full compose stack and wait for health checks."""
        result = built_worker_image["build_result"]
        assert result.returncode == 0, (
            "docker compose build worker failed:\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

        compose_file = built_worker_image["compose_file"]
        compose_dir = built_worker_image["compose_dir"]
        project_name = built_worker_image["project_name"]

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
