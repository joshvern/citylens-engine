# syntax=docker/dockerfile:1

# Pin both build and runtime to the same Python/Alpine manifest. Dependabot
# updates this digest deliberately instead of allowing rebuilds to drift.
ARG PYTHON_IMAGE=python:3.11.15-alpine3.23@sha256:f73754c398b259dfbbe482361dca8b464dea57da74efe5214966ca2ee767ee12

# ---- Builder ----------------------------------------------------------
FROM ${PYTHON_IMAGE} AS builder

# uv is also version- and digest-pinned. It creates the runtime environment
# directly from uv.lock without installing pip/setuptools/wheel into that venv.
COPY --from=ghcr.io/astral-sh/uv:0.10.9@sha256:10902f58a1606787602f303954cea099626a4adb02acbac4c69920fe9d278f82 /uv /uvx /bin/

RUN apk add --no-cache \
    build-base \
    ca-certificates \
    gdal-dev \
    geos-dev \
    git \
    proj-dev

WORKDIR /build

# uv needs every workspace member manifest to validate the root workspace, but
# only the API dependency graph is installed below.
COPY pyproject.toml uv.lock ./
COPY api/pyproject.toml api/README.md ./api/
COPY worker/pyproject.toml worker/README.md ./worker/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

RUN uv sync \
    --package citylens-engine-api \
    --no-dev \
    --frozen \
    --no-install-project

# Fail the image build at the dependency boundary rather than at Cloud Run
# startup. These imports cover the cloud clients and native geospatial stack.
RUN /opt/venv/bin/python -c "import fastapi, google.cloud.firestore, google.cloud.storage, pyproj, rasterio, shapely"

# ---- Runtime ----------------------------------------------------------
FROM ${PYTHON_IMAGE} AS runtime

WORKDIR /app

# The base Python image ships package-management tooling for developer
# convenience. Production does not need it; removing it eliminates the
# vulnerable wheel/setuptools surface and prevents runtime package mutation.
RUN python -m pip uninstall -y pip setuptools wheel \
    && apk add --no-cache \
        ca-certificates \
        gdal \
        geos \
        libstdc++ \
        proj \
    && addgroup -S -g 10001 citylens \
    && adduser -S -D -H -u 10001 -G citylens citylens

COPY --from=builder --chown=citylens:citylens /opt/venv /opt/venv
COPY --chown=citylens:citylens api/app /app/app
COPY --chown=citylens:citylens deploy/demo_runs.json /app/deploy/demo_runs.json

ENV HOME=/tmp \
    PATH="/opt/venv/bin:$PATH" \
    PORT=8080 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 10001:10001
EXPOSE 8080

CMD ["/opt/venv/bin/python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
