#!/bin/bash
set -euo pipefail

# Runs during first container init (Postgres official image).
# Creates roles/users using env vars and grants permissions.

: "${WORKER_DB_PASSWORD:?WORKER_DB_PASSWORD is required}"
: "${SCHEDULER_DB_PASSWORD:?SCHEDULER_DB_PASSWORD is required}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
BEGIN;

CREATE USER worker WITH PASSWORD '${WORKER_DB_PASSWORD}';
CREATE USER scheduler WITH PASSWORD '${SCHEDULER_DB_PASSWORD}';

-- If you don't want a password at all, consider removing this role entirely.
-- CREATE USER api;

GRANT ALL PRIVILEGES ON DATABASE worker_db TO worker;
GRANT ALL PRIVILEGES ON DATABASE scheduler_db TO scheduler;

COMMIT;
SQL

# Grant schema/table privileges in each target DB
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "scheduler_db" <<SQL
GRANT USAGE, CREATE ON SCHEMA public TO scheduler;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO scheduler;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO scheduler;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL PRIVILEGES ON TABLES TO scheduler;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL PRIVILEGES ON SEQUENCES TO scheduler;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "worker_db" <<SQL
GRANT USAGE, CREATE ON SCHEMA public TO worker;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO worker;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO worker;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL PRIVILEGES ON TABLES TO worker;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL PRIVILEGES ON SEQUENCES TO worker;
SQL

# Optional: grant read-only access for api on worker_db's public schema.
# This needs to run while connected to the target database.
# Uncomment if you actually use the 'api' role.
# psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "worker_db" <<'SQL'
# GRANT CONNECT ON DATABASE worker_db TO api;
# GRANT USAGE ON SCHEMA public TO api;
# GRANT SELECT ON ALL TABLES IN SCHEMA public TO api;
# ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO api;
# SQL
