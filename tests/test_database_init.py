"""
Test database initialization scripts.

These tests verify that:
1. All required databases are created
2. All required roles exist with correct properties (LOGIN/NOLOGIN)
3. Database ownership is correctly assigned
"""

import pytest
import psycopg


@pytest.mark.integration
class TestDatabaseCreation:
    """Test that all required databases are created by 01_create_databases.sh"""

    def test_all_databases_exist(self, db_admin_connection):
        """Verify all 5 databases are created."""
        expected_databases = {
            "worker_db",
            "scheduler_db",
            "jobs_db",
            "predictions_db",
            "userdata_db",
        }

        cursor = db_admin_connection.cursor()
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = {row[0] for row in cursor.fetchall()}

        # Check that all expected databases exist
        assert expected_databases.issubset(databases), (
            f"Missing databases: {expected_databases - databases}"
        )


@pytest.mark.integration
class TestRoleCreation:
    """Test that all required roles are created by 02_users.sh"""

    def test_all_roles_exist(self, db_admin_connection):
        """Verify all required roles exist."""
        expected_roles = {
            "worker_owner",
            "worker_app",
            "worker_migrator",
            "api_owner",
            "api_app",
            "api_migrator",
            "predict_owner",
            "predict_app",
            "predict_migrator",
            "scheduler",
        }

        cursor = db_admin_connection.cursor()
        cursor.execute("SELECT rolname FROM pg_roles;")
        roles = {row[0] for row in cursor.fetchall()}

        # Check that all expected roles exist
        assert expected_roles.issubset(roles), (
            f"Missing roles: {expected_roles - roles}"
        )

    def test_nologin_roles_cannot_login(self, db_admin_connection):
        """Verify that owner roles have NOLOGIN set."""
        nologin_roles = ["worker_owner", "api_owner", "predict_owner"]

        cursor = db_admin_connection.cursor()
        for role in nologin_roles:
            cursor.execute(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s;", (role,)
            )
            result = cursor.fetchone()
            assert result is not None, f"Role {role} does not exist"
            can_login = result[0]
            assert not can_login, f"Role {role} should have NOLOGIN but can login"

    def test_login_roles_can_login(self, db_admin_connection):
        """Verify that app and migrator roles have LOGIN set."""
        login_roles = [
            "worker_app",
            "worker_migrator",
            "api_app",
            "api_migrator",
            "predict_app",
            "predict_migrator",
            "scheduler",
        ]

        cursor = db_admin_connection.cursor()
        for role in login_roles:
            cursor.execute(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s;", (role,)
            )
            result = cursor.fetchone()
            assert result is not None, f"Role {role} does not exist"
            can_login = result[0]
            assert can_login, f"Role {role} should have LOGIN but cannot login"

    def test_nologin_roles_cannot_authenticate(
        self, postgres_container, test_credentials
    ):
        """Verify that NOLOGIN roles cannot actually authenticate."""
        nologin_roles = ["worker_owner", "api_owner", "predict_owner"]
        host = postgres_container.get_container_host_ip()
        port = postgres_container.get_exposed_port(5432)

        for role in nologin_roles:
            # Try to connect with a NOLOGIN role (should fail)
            with pytest.raises(psycopg.OperationalError):
                psycopg.connect(
                    host=host,
                    port=port,
                    user=role,
                    password="anypassword",  # Password doesn't matter
                    dbname="postgres",
                    connect_timeout=3,
                )


@pytest.mark.integration
class TestDatabaseOwnership:
    """Test that databases have correct ownership."""

    def test_database_owners(self, db_admin_connection):
        """Verify database ownership is correctly assigned."""
        expected_ownership = {
            "worker_db": "worker_owner",
            "scheduler_db": "scheduler",
            "jobs_db": "api_owner",
            "userdata_db": "api_owner",
            "predictions_db": "predict_owner",
        }

        cursor = db_admin_connection.cursor()
        for database, expected_owner in expected_ownership.items():
            cursor.execute(
                """
                SELECT pg_catalog.pg_get_userbyid(d.datdba) as owner
                FROM pg_catalog.pg_database d
                WHERE d.datname = %s;
                """,
                (database,),
            )
            result = cursor.fetchone()
            assert result is not None, f"Database {database} does not exist"
            actual_owner = result[0]
            assert actual_owner == expected_owner, (
                f"Database {database} should be owned by {expected_owner}, "
                f"but is owned by {actual_owner}"
            )


@pytest.mark.integration
class TestRoleMembership:
    """Test that migrator roles are members of owner roles."""

    def test_migrator_role_membership(self, db_admin_connection):
        """Verify that migrator roles have membership in owner roles."""
        memberships = [
            ("worker_migrator", "worker_owner"),
            ("api_migrator", "api_owner"),
            ("predict_migrator", "predict_owner"),
        ]

        cursor = db_admin_connection.cursor()
        for member, owner in memberships:
            cursor.execute(
                """
                SELECT 1
                FROM pg_auth_members m
                JOIN pg_roles member ON m.member = member.oid
                JOIN pg_roles owner ON m.roleid = owner.oid
                WHERE member.rolname = %s AND owner.rolname = %s;
                """,
                (member, owner),
            )
            result = cursor.fetchone()
            assert result is not None, f"Role {member} should be a member of {owner}"
