"""
Pytest fixtures for database integration tests.
"""

pytest_plugins = ["tests.test_db_image"]

import os

os.environ["TC_MAX_TRIES"] = "10"
# This sets the timeout for starting containers to 10 seconds
# (10 tries * 1 second interval)
# db service will usually not take longer than that

from pathlib import Path
import pytest
from dotenv import load_dotenv
from testcontainers.core.container import DockerContainer
import psycopg

# Load test environment variables
TEST_ENV_PATH = Path(__file__).parent / ".env.test"
load_dotenv(TEST_ENV_PATH)


@pytest.fixture(scope="session")
def test_env() -> dict[str, str]:
    """Load and return test environment variables."""
    load_dotenv(TEST_ENV_PATH)
    return {
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "testadmin"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "testadminpass"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB", "postgres"),
        "WORKER_APP_PASSWORD": os.getenv("WORKER_APP_PASSWORD"),
        "WORKER_MIGRATOR_PASSWORD": os.getenv("WORKER_MIGRATOR_PASSWORD"),
        "API_APP_PASSWORD": os.getenv("API_APP_PASSWORD"),
        "API_MIGRATOR_PASSWORD": os.getenv("API_MIGRATOR_PASSWORD"),
        "PREDICT_APP_PASSWORD": os.getenv("PREDICT_APP_PASSWORD"),
        "PREDICT_MIGRATOR_PASSWORD": os.getenv("PREDICT_MIGRATOR_PASSWORD"),
        "SCHEDULER_DB_PASSWORD": os.getenv("SCHEDULER_DB_PASSWORD"),
    }


@pytest.fixture(scope="session")
def db_admin_connection(
    postgres_container: DockerContainer, test_env: dict[str, str]
):
    """Provide an admin database connection."""
    # Get connection parameters directly instead of using the URL
    # which may have driver-specific formatting
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)

    conn = psycopg.connect(
        host=host,
        port=port,
        user=test_env["POSTGRES_USER"],
        password=test_env["POSTGRES_PASSWORD"],
        dbname=test_env["POSTGRES_DB"],
        autocommit=True,
    )
    yield conn
    conn.close()


@pytest.fixture
def db_connection_factory(postgres_container: DockerContainer):
    """
    Factory fixture to create database connections for specific users/databases.

    Usage:
        conn = db_connection_factory(username="worker_app", password="...", database="worker_db")
    """
    connections = []

    def _create_connection(user: str, password: str, database: str = "postgres"):
        """Create a connection for a specific user and database."""
        host = postgres_container.get_container_host_ip()
        port = postgres_container.get_exposed_port(5432)

        conn = psycopg.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database,
            autocommit=True,
        )
        connections.append(conn)
        return conn

    yield _create_connection

    # Cleanup: close all connections
    for conn in connections:
        try:
            conn.close()
        except Exception:
            pass


@pytest.fixture
def test_credentials(test_env: dict[str, str]) -> dict[str, dict[str, str]]:
    """Provide test credentials for all roles."""
    return {
        "admin": {
            "user": test_env["POSTGRES_USER"],
            "password": test_env["POSTGRES_PASSWORD"],
        },
        "worker_app": {
            "user": "worker_app",
            "password": test_env["WORKER_APP_PASSWORD"],
        },
        "worker_migrator": {
            "user": "worker_migrator",
            "password": test_env["WORKER_MIGRATOR_PASSWORD"],
        },
        "api_app": {
            "user": "api_app",
            "password": test_env["API_APP_PASSWORD"],
        },
        "api_migrator": {
            "user": "api_migrator",
            "password": test_env["API_MIGRATOR_PASSWORD"],
        },
        "predict_app": {
            "user": "predict_app",
            "password": test_env["PREDICT_APP_PASSWORD"],
        },
        "predict_migrator": {
            "user": "predict_migrator",
            "password": test_env["PREDICT_MIGRATOR_PASSWORD"],
        },
        "scheduler": {
            "user": "scheduler",
            "password": test_env["SCHEDULER_DB_PASSWORD"],
        },
    }
