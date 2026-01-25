#!/bin/bash
set -euo pipefail

# Runs during first container init (Postgres official image).
# Creates the application databases.

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<'SQL'
CREATE DATABASE worker_db;
CREATE DATABASE scheduler_db;
CREATE DATABASE jobs_db;
CREATE DATABASE predictions_db;
CREATE DATABASE userdata_db;
SQL

# worker_db - managed by kivoll_worker
# scheduler_db - managed by kivoll_worker (apscheduler)
# jobs_db - managed by api
# predictions_db - managed by predict
# userdata_db - managed by api