FROM postgres:18

LABEL authors="frader"

COPY initdb /docker-entrypoint-initdb.d/

HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=3 \
    CMD pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" || exit 1
