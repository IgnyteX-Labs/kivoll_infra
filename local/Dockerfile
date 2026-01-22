# ---- build kivoll_worker ----

# ---- Builder ----
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ARG GIT_REPO
ARG GIT_BRANCH=main
RUN git clone --depth 1 --branch ${GIT_BRANCH} ${GIT_REPO} .

RUN uv sync --no-dev --frozen


# ---- Runtime ----
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY --from=builder /app /app
COPY deploy/common/healthcheck.sh /usr/local/bin/healthcheck.sh
RUN chmod +x /usr/local/bin/healthcheck_kivoll_worker.sh

HEALTHCHECK --interval=1m --timeout=10s --start-period=2m --retries=3 CMD /usr/local/bin/healthcheck.sh

CMD [ "uv", "run", "kivoll-schedule", "--verbose" ]
