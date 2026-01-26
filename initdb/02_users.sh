#!/bin/bash
set -euo pipefail

# Runs during first container init (Postgres official image).
#
# Goal: create *roles* and grant *least privilege* per database.
#
# Naming conventions used in this script:
#   - *_owner:   owns the database objects (tables, sequences, etc.). Full DDL/DML.
#                This is the "tablemaster" role. Cant login.
#   - *_migrator: connects to run schema migrations. Typically needs DDL + DML.
#   - *_app:     application runtime role. Usually needs DML (and sometimes read-only).
#

# Verify required environment variables are set.
: "${WORKER_APP_PASSWORD:?WORKER_APP_PASSWORD is required}"
: "${WORKER_MIGRATOR_PASSWORD:?WORKER_MIGRATOR_PASSWORD is required}"
: "${API_APP_PASSWORD:?API_APP_PASSWORD is required}"
: "${API_MIGRATOR_PASSWORD:?API_MIGRATOR_PASSWORD is required}"
: "${PREDICT_APP_PASSWORD:?PREDICT_APP_PASSWORD is required}"
: "${PREDICT_MIGRATOR_PASSWORD:?PREDICT_MIGRATOR_PASSWORD is required}"
: "${SCHEDULER_DB_PASSWORD:?SCHEDULER_DB_PASSWORD is required}"

########################################
# Helpers
########################################

create_role_if_not_exists() {
  local role="$1"
  local can_login="$2"   # "LOGIN" or "NOLOGIN"
  local password="$3"

  # Check if role exists
  local role_exists
  role_exists=$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -tAc \
    "SELECT 1 FROM pg_roles WHERE rolname = '$role'")

  if [ -z "$role_exists" ]; then
    # Role doesn't exist, create it
    if [ "$can_login" = "LOGIN" ]; then
      psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE ROLE "$role" LOGIN PASSWORD '$password';
SQL
    else
      psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE ROLE "$role" NOLOGIN;
SQL
    fi
  fi
}

# grant_tablemaster_access
#
# Grants a role the ability to fully manage the schema and all objects in it:
#   - DDL (schema CREATE)
#   - DML (ALL PRIVILEGES on tables/sequences)
#   - default privileges for future tables/sequences
#
# Use this for *_owner roles.
grant_tablemaster_access() {
  local db="$1"
  local role="$2"

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" \
    --set=role_name="$role" --set=db_name="$db" <<'SQL'
-- In this session, :"db_name"^ is the target DB set via --dbname.
GRANT CONNECT ON DATABASE :"db_name" TO :"role_name";

-- public schema is used in this project today.
GRANT USAGE, CREATE ON SCHEMA public TO :"role_name";

-- Existing objects
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO :"role_name";
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO :"role_name";

-- Future objects (created by whatever role executes this ALTER DEFAULT PRIVILEGES).
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL PRIVILEGES ON TABLES TO :"role_name";

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL PRIVILEGES ON SEQUENCES TO :"role_name";
SQL
}

# grant_write_access
#
# Grants a role full read/write access to data (DML) but not schema changes (no CREATE).
# This is the typical application runtime permission set.
# Note: default privileges here are only for objects created by the role running this
# statement (the init user), so we add per-creator defaults separately below.
grant_write_access() {
  local db="$1"
  local role="$2"

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" \
    --set=role_name="$role" --set=db_name="$db" <<'SQL'
GRANT CONNECT ON DATABASE :"db_name" TO :"role_name";
GRANT USAGE ON SCHEMA public TO :"role_name";

-- Existing objects
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO :"role_name";
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO :"role_name";

-- Future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"role_name";

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO :"role_name";
SQL
}

# grant_read_access
#
# Read-only access to data.
# Note: default privileges here are only for objects created by the role running this
# statement (the init user), so we add per-creator defaults separately below.
grant_read_access() {
  local db="$1"
  local role="$2"

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" \
    --set=role_name="$role" --set=db_name="$db" <<'SQL'
GRANT CONNECT ON DATABASE :"db_name" TO :"role_name";
GRANT USAGE ON SCHEMA public TO :"role_name";

-- Existing objects
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"role_name";

-- Future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO :"role_name";
SQL
}

# grant_jobs_worker_access
#
# Worker access pattern for jobs_db: workers *process* jobs created by the API.
# They typically need to:
#   - dequeue/claim a job (SELECT + row locking, then UPDATE status)
#   - acknowledge completion/failure (UPDATE)
#
# So we grant SELECT+UPDATE on tables, and minimal sequence privileges.
grant_select_update_access() {
  local db="$1"
  local role="$2"

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" \
    --set=role_name="$role" --set=db_name="$db" <<'SQL'
GRANT CONNECT ON DATABASE :"db_name" TO :"role_name";
GRANT USAGE ON SCHEMA public TO :"role_name";

-- Existing objects
GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA public TO :"role_name";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO :"role_name";

-- Future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, UPDATE ON TABLES TO :"role_name";

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO :"role_name";
SQL
}

# grant_default_access_for_creator
#
# Grants default privileges for objects created by a specific role.
# This is required because default privileges are scoped to the creating role.
grant_default_access_for_creator() {
  local db="$1"
  local creator_role="$2"
  local role="$3"
  local access="$4"

  case "$access" in
    write)
      psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" \
        --set=creator_role="$creator_role" --set=role_name="$role" <<'SQL'
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"role_name";

ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO :"role_name";
SQL
      ;;
    read)
      psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" \
        --set=creator_role="$creator_role" --set=role_name="$role" <<'SQL'
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
  GRANT SELECT ON TABLES TO :"role_name";
SQL
      ;;
    select_update)
      psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" \
        --set=creator_role="$creator_role" --set=role_name="$role" <<'SQL'
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
  GRANT SELECT, UPDATE ON TABLES TO :"role_name";

ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO :"role_name";
SQL
      ;;
    *)
      echo "Unknown default access level: $access" >&2
      exit 1
      ;;
  esac
}

########################################
# Role creation
########################################

# Worker
# - worker_owner: tablemaster (owns worker_db)
# - worker_app:   runtime role (no-login here; typically used via SET ROLE from worker DB user)
# - worker_migrator: login role that runs migrations
create_role_if_not_exists worker_owner NOLOGIN ""
create_role_if_not_exists worker_app LOGIN "$WORKER_APP_PASSWORD"
create_role_if_not_exists worker_migrator LOGIN "$WORKER_MIGRATOR_PASSWORD"

# Migrator pattern:
# Keep the "owner" role as NOLOGIN (safer), and have a dedicated LOGIN role for migrations.
# The migrator becomes a member of the owner role so migrations can perform DDL/DML as needed.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
GRANT worker_owner TO worker_migrator;
SQL

# API
  create_role_if_not_exists api_owner NOLOGIN ""
create_role_if_not_exists api_app LOGIN "$API_APP_PASSWORD"
create_role_if_not_exists api_migrator LOGIN "$API_MIGRATOR_PASSWORD"

# Grant migrator membership to owner role.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
GRANT api_owner TO api_migrator;
SQL

# Scheduler (single-role model) (managed by apscheduler in worker)
create_role_if_not_exists scheduler LOGIN "$SCHEDULER_DB_PASSWORD"

# Predictor (ML executor)
create_role_if_not_exists predict_owner NOLOGIN ""
create_role_if_not_exists predict_app LOGIN "$PREDICT_APP_PASSWORD"
create_role_if_not_exists predict_migrator LOGIN "$PREDICT_MIGRATOR_PASSWORD"

# Grant migrator membership to owner role.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
GRANT predict_owner TO predict_migrator;
SQL


########################################
# Database ownership & privileges
########################################

# --------------------------------
# Worker DB
# --------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
ALTER DATABASE worker_db OWNER TO worker_owner;
SQL

grant_tablemaster_access "worker_db" "worker_owner"

grant_write_access "worker_db" "worker_app"
grant_default_access_for_creator "worker_db" "worker_owner" "worker_app" "write"
grant_default_access_for_creator "worker_db" "worker_migrator" "worker_app" "write"

grant_read_access "worker_db" "api_app"
grant_default_access_for_creator "worker_db" "worker_owner" "api_app" "read"
grant_default_access_for_creator "worker_db" "worker_migrator" "api_app" "read"

# --------------------------------
# Scheduler DB
# --------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
ALTER DATABASE scheduler_db OWNER TO scheduler;
SQL

grant_tablemaster_access "scheduler_db" "scheduler"

# --------------------------------
# Jobs DB (API-owned)
# --------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
ALTER DATABASE jobs_db OWNER TO api_owner;
SQL

grant_tablemaster_access "jobs_db" "api_owner"

# API writes jobs.
grant_write_access "jobs_db" "api_app"
grant_default_access_for_creator "jobs_db" "api_owner" "api_app" "write"
grant_default_access_for_creator "jobs_db" "api_migrator" "api_app" "write"

# Worker can read/update jobs to process them.
grant_select_update_access "jobs_db" "worker_app"
grant_default_access_for_creator "jobs_db" "api_owner" "worker_app" "select_update"
grant_default_access_for_creator "jobs_db" "api_migrator" "worker_app" "select_update"

# Predictor can read/update jobs to process them.
grant_select_update_access "jobs_db" "predict_app"
grant_default_access_for_creator "jobs_db" "api_owner" "predict_app" "select_update"
grant_default_access_for_creator "jobs_db" "api_migrator" "predict_app" "select_update"

# --------------------------------
# Userdata DB (API-owned)
# --------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
ALTER DATABASE userdata_db OWNER TO api_owner;
SQL

grant_tablemaster_access "userdata_db" "api_owner"

# API app needs runtime read/write access.
grant_write_access "userdata_db" "api_app"
grant_default_access_for_creator "userdata_db" "api_owner" "api_app" "write"
grant_default_access_for_creator "userdata_db" "api_migrator" "api_app" "write"

# --------------------------------
# Predictor DB
# --------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
ALTER DATABASE predictions_db OWNER TO predict_owner;
SQL

grant_tablemaster_access "predictions_db" "predict_owner"

grant_write_access "predictions_db" "predict_app"
grant_default_access_for_creator "predictions_db" "predict_owner" "predict_app" "write"
grant_default_access_for_creator "predictions_db" "predict_migrator" "predict_app" "write"
grant_read_access "predictions_db" "api_app"
grant_default_access_for_creator "predictions_db" "predict_owner" "api_app" "read"
grant_default_access_for_creator "predictions_db" "predict_migrator" "api_app" "read"
