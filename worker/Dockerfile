# syntax=docker/dockerfile:1

# Torch CPU wheels require glibc, so the worker remains Debian-based. The
# manifest is pinned and the final stage removes the vulnerable Perl/tooling
# surface inherited by the old single-stage image.
ARG PYTHON_IMAGE=python:3.11.15-slim@sha256:db3ff2e1800a8581e2c48a27c3995339d47bdf046da21c7627accd3d51053a93

# ---- Builder ----------------------------------------------------------
FROM ${PYTHON_IMAGE} AS builder

COPY --from=ghcr.io/astral-sh/uv:0.10.9@sha256:10902f58a1606787602f303954cea099626a4adb02acbac4c69920fe9d278f82 /uv /uvx /bin/

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY pyproject.toml uv.lock ./
COPY api/pyproject.toml api/README.md ./api/
COPY worker/pyproject.toml worker/README.md ./worker/

ENV UV_COMPILE_BYTECODE=1 \
    UV_HTTP_TIMEOUT=300 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

RUN uv sync \
    --package citylens-engine-worker \
    --no-dev \
    --frozen \
    --no-install-project

ARG CITYLENS_SAM2_ASSETS_SIZE=small
ENV CITYLENS_ASSETS_ROOT=/opt/citylens-assets

RUN /opt/venv/bin/python -c "import citylens_core, laspy, pyproj, rasterio, sam2, shapely, torch, torchvision"
RUN /opt/venv/bin/python -c "from citylens_core.sam.assets import download_sam2_assets; download_sam2_assets('${CITYLENS_SAM2_ASSETS_SIZE}')"

# ---- Runtime ----------------------------------------------------------
FROM ${PYTHON_IMAGE} AS runtime

WORKDIR /app

# The old worker installed Git in the final image, which pulled the complete
# Perl runtime and multiplied critical CVEs. Production needs neither. The
# uv-created venv intentionally retains its locked setuptools dependency
# because Torch imports it at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libexpat1 \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip uninstall -y pip setuptools wheel \
    && dpkg --purge --force-depends --force-remove-essential perl-base \
    && groupadd --gid 10001 citylens \
    && useradd --uid 10001 --gid 10001 --system --no-create-home citylens

COPY --from=builder --chown=citylens:citylens /opt/venv /opt/venv
COPY --from=builder --chown=citylens:citylens /opt/citylens-assets /opt/citylens-assets
COPY --chown=citylens:citylens worker/worker.py /app/worker.py
COPY --chown=citylens:citylens worker/services /app/services

ENV CITYLENS_ASSETS_ROOT=/opt/citylens-assets \
    HOME=/tmp \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 10001:10001

CMD ["/opt/venv/bin/python", "-u", "/app/worker.py"]
