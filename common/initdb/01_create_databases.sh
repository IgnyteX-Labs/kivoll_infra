#!/bin/bash
set -euo pipefail

# Runs during first container init (Postgres official image).
# Creates the application databases.

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<'SQL'
CREATE DATABASE worker_db;
CREATE DATABASE scheduler_db;
SQL
