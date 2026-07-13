# syntax=docker/dockerfile:1.7

ARG EMBYX_RUNTIME_IMAGE=ghcr.io/cyrahs/embyx@sha256:766b574ba953b1a01b8e48ee4fab155cc1c3e2afde5b4a83f6ac4a985cb3d6c8

FROM node:22-bookworm-slim AS frontend-build

WORKDIR /build

COPY frontend/package.json frontend/package-lock.json ./frontend/
RUN --mount=type=cache,target=/root/.npm \
    npm --prefix frontend ci

COPY frontend ./frontend
RUN npm --prefix frontend run build


FROM ghcr.io/astral-sh/uv:0.9.13-python3.13-trixie-slim AS python-build

WORKDIR /build

COPY pyproject.toml uv.lock README.md ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv export --quiet \
        --locked \
        --no-dev \
        --no-emit-project \
        --format requirements.txt \
        --output-file /tmp/runtime-requirements.txt && \
    uv pip install --quiet --link-mode copy \
        --target /opt/embyx-web \
        --require-hashes \
        --requirements /tmp/runtime-requirements.txt

COPY src ./src
COPY --from=frontend-build /build/src/embyx_web/static ./src/embyx_web/static

RUN --mount=type=cache,target=/root/.cache/uv \
    uv build --wheel --out-dir /wheels && \
    uv pip install --quiet --link-mode copy \
        --target /opt/embyx-web \
        --no-deps \
        /wheels/embyx_web-*.whl


FROM ${EMBYX_RUNTIME_IMAGE} AS runtime

COPY --from=python-build /opt/embyx-web /opt/embyx-web

RUN mkdir -p /var/lib/embyx-web/log

ENV PYTHONPATH=/opt/embyx-web:/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    EMBYX_WEB_DATABASE_PATH=/var/lib/embyx-web/embyx-web.sqlite3 \
    EMBYX_RUNTIME_LOG_DIR=/var/lib/embyx-web/log \
    EMBYX_WEB_RUNTIME_ROOT=/app \
    EMBYX_WEB_RUNTIME_MODULE=src.embyx_runtime.fill_actor_api

WORKDIR /app

EXPOSE 8000

ENTRYPOINT ["/app/.venv/bin/python", "-m", "embyx_web"]
