"""
Test database permission model.

These tests verify that each role has the correct permissions according to the README.md:
- DDL operations (CREATE TABLE, DROP TABLE, etc.)
- DML operations (INSERT, UPDATE, DELETE, SELECT)
- Restricted operations (e.g., SELECT+UPDATE only for workers on jobs_db)
"""

import pytest
import psycopg


@pytest.mark.integration
class TestWorkerDBPermissions:
    """Test permissions on worker_db according to the permission model."""

    def test_worker_migrator_can_perform_ddl(
        self, db_connection_factory, test_credentials
    ):
        """worker_migrator should be able to create and drop tables (DDL)."""
        creds = test_credentials["worker_migrator"]
        conn = db_connection_factory(
            user=creds["user"], password=creds["password"], database="worker_db"
        )
        cursor = conn.cursor()

        # Create table
        cursor.execute(
            "CREATE TABLE test_migrator_table (id SERIAL PRIMARY KEY, data TEXT);"
        )

        # Verify table exists
        cursor.execute(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'test_migrator_table');"
        )
        assert cursor.fetchone()[0] is True

        # Drop table
        cursor.execute("DROP TABLE test_migrator_table;")

    def test_worker_migrator_can_perform_dml(
        self, db_connection_factory, test_credentials
    ):
        """worker_migrator should have full DML access (INSERT, UPDATE, DELETE, SELECT)."""
        creds = test_credentials["worker_migrator"]
        conn = db_connection_factory(
            user=creds["user"], password=creds["password"], database="worker_db"
        )
        cursor = conn.cursor()

        # Create table for testing
        cursor.execute("CREATE TABLE test_dml (id SERIAL PRIMARY KEY, value TEXT);")

        # INSERT
        cursor.execute("INSERT INTO test_dml (value) VALUES ('test');")

        # SELECT
        cursor.execute("SELECT value FROM test_dml;")
        assert cursor.fetchone()[0] == "test"

        # UPDATE
        cursor.execute("UPDATE test_dml SET value = 'updated';")

        # DELETE
        cursor.execute("DELETE FROM test_dml;")

        # Cleanup
        cursor.execute("DROP TABLE test_dml;")

    def test_worker_app_can_perform_dml(self, db_connection_factory, test_credentials):
        """worker_app should have full DML access but no DDL."""
        migrator_creds = test_credentials["worker_migrator"]
        app_creds = test_credentials["worker_app"]

        # Setup: Create table as migrator
        migrator_conn = db_connection_factory(
            user=migrator_creds["user"],
            password=migrator_creds["password"],
            database="worker_db",
        )
        migrator_cursor = migrator_conn.cursor()
        migrator_cursor.execute(
            "CREATE TABLE test_worker_app (id SERIAL PRIMARY KEY, value TEXT);"
        )

        # Test: worker_app can do DML
        app_conn = db_connection_factory(
            user=app_creds["user"], password=app_creds["password"], database="worker_db"
        )
        app_cursor = app_conn.cursor()

        # INSERT
        app_cursor.execute("INSERT INTO test_worker_app (value) VALUES ('test');")

        # SELECT
        app_cursor.execute("SELECT value FROM test_worker_app;")
        assert app_cursor.fetchone()[0] == "test"

        # UPDATE
        app_cursor.execute("UPDATE test_worker_app SET value = 'updated';")

        # DELETE
        app_cursor.execute("DELETE FROM test_worker_app;")

        # Cleanup
        migrator_cursor.execute("DROP TABLE test_worker_app;")

    def test_worker_app_cannot_perform_ddl(
        self, db_connection_factory, test_credentials
    ):
        """worker_app should NOT be able to create tables."""
        creds = test_credentials["worker_app"]
        conn = db_connection_factory(
            user=creds["user"], password=creds["password"], database="worker_db"
        )
        cursor = conn.cursor()

        # Try to create table (should fail)
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            cursor.execute("CREATE TABLE test_forbidden (id SERIAL PRIMARY KEY);")

    def test_api_app_has_readonly_access_to_worker_db(
        self, db_connection_factory, test_credentials
    ):
        """api_app should have read-only access to worker_db."""
        migrator_creds = test_credentials["worker_migrator"]
        api_creds = test_credentials["api_app"]

        # Setup: Create and populate table as migrator
        migrator_conn = db_connection_factory(
            user=migrator_creds["user"],
            password=migrator_creds["password"],
            database="worker_db",
        )
        migrator_cursor = migrator_conn.cursor()
        migrator_cursor.execute(
            "CREATE TABLE test_readonly (id SERIAL PRIMARY KEY, value TEXT);"
        )
        migrator_cursor.execute("INSERT INTO test_readonly (value) VALUES ('data');")

        # Test: api_app can SELECT
        api_conn = db_connection_factory(
            user=api_creds["user"], password=api_creds["password"], database="worker_db"
        )
        api_cursor = api_conn.cursor()
        api_cursor.execute("SELECT value FROM test_readonly;")
        assert api_cursor.fetchone()[0] == "data"

        # Test: api_app cannot INSERT
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            api_cursor.execute(
                "INSERT INTO test_readonly (value) VALUES ('forbidden');"
            )

        # Test: api_app cannot UPDATE
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            api_cursor.execute("UPDATE test_readonly SET value = 'forbidden';")

        # Test: api_app cannot DELETE
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            api_cursor.execute("DELETE FROM test_readonly;")

        # Cleanup
        migrator_cursor.execute("DROP TABLE test_readonly;")


@pytest.mark.integration
class TestJobsDBPermissions:
    """Test permissions on jobs_db according to the permission model."""

    def test_api_migrator_can_perform_ddl(
        self, db_connection_factory, test_credentials
    ):
        """api_migrator should be able to create and drop tables."""
        creds = test_credentials["api_migrator"]
        conn = db_connection_factory(
            user=creds["user"], password=creds["password"], database="jobs_db"
        )
        cursor = conn.cursor()

        cursor.execute(
            "CREATE TABLE test_jobs_table (id SERIAL PRIMARY KEY, status TEXT);"
        )
        cursor.execute("DROP TABLE test_jobs_table;")

    def test_api_app_has_full_dml_access(self, db_connection_factory, test_credentials):
        """api_app should have full DML access to jobs_db."""
        migrator_creds = test_credentials["api_migrator"]
        app_creds = test_credentials["api_app"]

        # Setup
        migrator_conn = db_connection_factory(
            user=migrator_creds["user"],
            password=migrator_creds["password"],
            database="jobs_db",
        )
        migrator_cursor = migrator_conn.cursor()
        migrator_cursor.execute(
            "CREATE TABLE test_jobs (id SERIAL PRIMARY KEY, status TEXT);"
        )

        # Test
        app_conn = db_connection_factory(
            user=app_creds["user"], password=app_creds["password"], database="jobs_db"
        )
        app_cursor = app_conn.cursor()

        app_cursor.execute("INSERT INTO test_jobs (status) VALUES ('pending');")
        app_cursor.execute("SELECT status FROM test_jobs;")
        assert app_cursor.fetchone()[0] == "pending"
        app_cursor.execute("UPDATE test_jobs SET status = 'completed';")
        app_cursor.execute("DELETE FROM test_jobs;")

        # Cleanup
        migrator_cursor.execute("DROP TABLE test_jobs;")

    def test_worker_app_has_select_update_only(
        self, db_connection_factory, test_credentials
    ):
        """worker_app should have SELECT and UPDATE but not INSERT or DELETE on jobs_db."""
        migrator_creds = test_credentials["api_migrator"]
        worker_creds = test_credentials["worker_app"]

        # Setup
        migrator_conn = db_connection_factory(
            user=migrator_creds["user"],
            password=migrator_creds["password"],
            database="jobs_db",
        )
        migrator_cursor = migrator_conn.cursor()
        migrator_cursor.execute(
            "CREATE TABLE test_worker_jobs (id SERIAL PRIMARY KEY, status TEXT);"
        )
        migrator_cursor.execute(
            "INSERT INTO test_worker_jobs (status) VALUES ('pending');"
        )

        # Test
        worker_conn = db_connection_factory(
            user=worker_creds["user"],
            password=worker_creds["password"],
            database="jobs_db",
        )
        worker_cursor = worker_conn.cursor()

        # SELECT should work
        worker_cursor.execute("SELECT status FROM test_worker_jobs;")
        assert worker_cursor.fetchone()[0] == "pending"

        # UPDATE should work
        worker_cursor.execute("UPDATE test_worker_jobs SET status = 'processing';")

        # INSERT should fail
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            worker_cursor.execute(
                "INSERT INTO test_worker_jobs (status) VALUES ('new');"
            )

        # DELETE should fail
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            worker_cursor.execute("DELETE FROM test_worker_jobs;")

        # Cleanup
        migrator_cursor.execute("DROP TABLE test_worker_jobs;")

    def test_predict_app_has_select_update_only(
        self, db_connection_factory, test_credentials
    ):
        """predict_app should have SELECT and UPDATE but not INSERT or DELETE on jobs_db."""
        migrator_creds = test_credentials["api_migrator"]
        predict_creds = test_credentials["predict_app"]

        # Setup
        migrator_conn = db_connection_factory(
            user=migrator_creds["user"],
            password=migrator_creds["password"],
            database="jobs_db",
        )
        migrator_cursor = migrator_conn.cursor()
        migrator_cursor.execute(
            "CREATE TABLE test_predict_jobs (id SERIAL PRIMARY KEY, status TEXT);"
        )
        migrator_cursor.execute(
            "INSERT INTO test_predict_jobs (status) VALUES ('pending');"
        )

        # Test
        predict_conn = db_connection_factory(
            user=predict_creds["user"],
            password=predict_creds["password"],
            database="jobs_db",
        )
        predict_cursor = predict_conn.cursor()

        # SELECT should work
        predict_cursor.execute("SELECT status FROM test_predict_jobs;")
        assert predict_cursor.fetchone()[0] == "pending"

        # UPDATE should work
        predict_cursor.execute("UPDATE test_predict_jobs SET status = 'processing';")

        # INSERT should fail
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            predict_cursor.execute(
                "INSERT INTO test_predict_jobs (status) VALUES ('new');"
            )

        # DELETE should fail
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            predict_cursor.execute("DELETE FROM test_predict_jobs;")

        # Cleanup
        migrator_cursor.execute("DROP TABLE test_predict_jobs;")


@pytest.mark.integration
class TestSchedulerDBPermissions:
    """Test permissions on scheduler_db (single-role model)."""

    def test_scheduler_has_tablemaster_access(
        self, db_connection_factory, test_credentials
    ):
        """scheduler should have full DDL and DML access to scheduler_db."""
        creds = test_credentials["scheduler"]
        conn = db_connection_factory(
            user=creds["user"], password=creds["password"], database="scheduler_db"
        )
        cursor = conn.cursor()

        # DDL
        cursor.execute(
            "CREATE TABLE test_scheduler (id SERIAL PRIMARY KEY, job_name TEXT);"
        )

        # DML
        cursor.execute("INSERT INTO test_scheduler (job_name) VALUES ('test_job');")
        cursor.execute("SELECT job_name FROM test_scheduler;")
        assert cursor.fetchone()[0] == "test_job"
        cursor.execute("UPDATE test_scheduler SET job_name = 'updated_job';")
        cursor.execute("DELETE FROM test_scheduler;")

        # Cleanup
        cursor.execute("DROP TABLE test_scheduler;")


@pytest.mark.integration
class TestPredictionsDBPermissions:
    """Test permissions on predictions_db."""

    def test_predict_migrator_can_perform_ddl(
        self, db_connection_factory, test_credentials
    ):
        """predict_migrator should be able to create and drop tables."""
        creds = test_credentials["predict_migrator"]
        conn = db_connection_factory(
            user=creds["user"], password=creds["password"], database="predictions_db"
        )
        cursor = conn.cursor()

        cursor.execute(
            "CREATE TABLE test_predictions (id SERIAL PRIMARY KEY, result FLOAT);"
        )
        cursor.execute("DROP TABLE test_predictions;")

    def test_predict_app_has_full_dml_access(
        self, db_connection_factory, test_credentials
    ):
        """predict_app should have full DML access to predictions_db."""
        migrator_creds = test_credentials["predict_migrator"]
        app_creds = test_credentials["predict_app"]

        # Setup
        migrator_conn = db_connection_factory(
            user=migrator_creds["user"],
            password=migrator_creds["password"],
            database="predictions_db",
        )
        migrator_cursor = migrator_conn.cursor()
        migrator_cursor.execute(
            "CREATE TABLE test_pred (id SERIAL PRIMARY KEY, value FLOAT);"
        )

        # Test
        app_conn = db_connection_factory(
            user=app_creds["user"],
            password=app_creds["password"],
            database="predictions_db",
        )
        app_cursor = app_conn.cursor()

        app_cursor.execute("INSERT INTO test_pred (value) VALUES (0.95);")
        app_cursor.execute("SELECT value FROM test_pred;")
        assert app_cursor.fetchone()[0] == 0.95
        app_cursor.execute("UPDATE test_pred SET value = 0.99;")
        app_cursor.execute("DELETE FROM test_pred;")

        # Cleanup
        migrator_cursor.execute("DROP TABLE test_pred;")

    def test_api_app_has_readonly_access_to_predictions_db(
        self, db_connection_factory, test_credentials
    ):
        """api_app should have read-only access to predictions_db."""
        migrator_creds = test_credentials["predict_migrator"]
        api_creds = test_credentials["api_app"]

        # Setup
        migrator_conn = db_connection_factory(
            user=migrator_creds["user"],
            password=migrator_creds["password"],
            database="predictions_db",
        )
        migrator_cursor = migrator_conn.cursor()
        migrator_cursor.execute(
            "CREATE TABLE test_pred_ro (id SERIAL PRIMARY KEY, value FLOAT);"
        )
        migrator_cursor.execute("INSERT INTO test_pred_ro (value) VALUES (0.85);")

        # Test
        api_conn = db_connection_factory(
            user=api_creds["user"],
            password=api_creds["password"],
            database="predictions_db",
        )
        api_cursor = api_conn.cursor()

        # SELECT should work
        api_cursor.execute("SELECT value FROM test_pred_ro;")
        assert api_cursor.fetchone()[0] == 0.85

        # INSERT should fail
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            api_cursor.execute("INSERT INTO test_pred_ro (value) VALUES (0.90);")

        # Cleanup
        migrator_cursor.execute("DROP TABLE test_pred_ro;")


@pytest.mark.integration
class TestUserdataDBPermissions:
    """Test permissions on userdata_db."""

    def test_api_migrator_can_perform_ddl(
        self, db_connection_factory, test_credentials
    ):
        """api_migrator should be able to create and drop tables in userdata_db."""
        creds = test_credentials["api_migrator"]
        conn = db_connection_factory(
            user=creds["user"], password=creds["password"], database="userdata_db"
        )
        cursor = conn.cursor()

        cursor.execute(
            "CREATE TABLE test_userdata (id SERIAL PRIMARY KEY, username TEXT);"
        )
        cursor.execute("DROP TABLE test_userdata;")

    def test_api_app_has_full_dml_access(self, db_connection_factory, test_credentials):
        """api_app should have full DML access to userdata_db."""
        migrator_creds = test_credentials["api_migrator"]
        app_creds = test_credentials["api_app"]

        # Setup
        migrator_conn = db_connection_factory(
            user=migrator_creds["user"],
            password=migrator_creds["password"],
            database="userdata_db",
        )
        migrator_cursor = migrator_conn.cursor()
        migrator_cursor.execute(
            "CREATE TABLE test_users (id SERIAL PRIMARY KEY, username TEXT);"
        )

        # Test
        app_conn = db_connection_factory(
            user=app_creds["user"],
            password=app_creds["password"],
            database="userdata_db",
        )
        app_cursor = app_conn.cursor()

        app_cursor.execute("INSERT INTO test_users (username) VALUES ('alice');")
        app_cursor.execute("SELECT username FROM test_users;")
        assert app_cursor.fetchone()[0] == "alice"
        app_cursor.execute("UPDATE test_users SET username = 'bob';")
        app_cursor.execute("DELETE FROM test_users;")

        # Cleanup
        migrator_cursor.execute("DROP TABLE test_users;")
