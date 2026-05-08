# Multi-stage build: builder has git + build-essential for installing
# deps from source/git, runtime is a clean slim image with only the
# venv and the app source. Cuts ~50 MB of build tooling from the
# runtime image, so Cloud Run pulls a smaller image on every cold
# start (~500 ms-1 s saved on phase 1 of cold-start latency).

# ---- Builder ----------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update \
  && apt-get install -y --no-install-recommends git ca-certificates build-essential \
  && rm -rf /var/lib/apt/lists/*

# Build into a self-contained venv so the runtime stage can copy it
# whole without worrying about pip's prefix layout. Activating the venv
# is just prepending /opt/venv/bin to PATH.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir --upgrade pip wheel

# Install the API's runtime deps. Pinned in api/pyproject.toml; we list
# them inline here so the builder doesn't need to consume the source
# tree just to resolve the dep graph.
RUN pip install --no-cache-dir \
      "fastapi>=0.110" \
      "uvicorn[standard]>=0.27" \
      "pydantic>=2.0" \
      "google-cloud-firestore>=2.11" \
      "google-cloud-storage>=2.14" \
      "google-auth>=2.25" \
      "requests>=2.31" \
      "httpx>=0.25" \
      "pyjwt[crypto]>=2.8"

# Pin citylens-core via build arg.
ARG CITYLENS_CORE_GIT_URL=git+https://github.com/joshvern/citylens-core.git@v0.3.23
RUN pip install --no-cache-dir "citylens-core @ ${CITYLENS_CORE_GIT_URL}"

# ---- Runtime ----------------------------------------------------------
FROM python:3.11-slim AS runtime

WORKDIR /app

# Runtime system libs:
# - ca-certificates: TLS for outbound HTTPS (Firestore, GCS, citylens-core fetches)
# - libexpat1: required by rasterio (transitive citylens-core dep). The
#   single-stage Dockerfile got this for free via git's transitive deps;
#   we install it explicitly now since git is builder-only.
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates libexpat1 \
  && rm -rf /var/lib/apt/lists/*

# Bring over the prepared venv from the builder.
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# App source. `app.main:app` resolves because Python's sys.path
# includes WORKDIR (/app) and `app/` is a package directly under it.
COPY api/app /app/app
COPY deploy /app/deploy

ENV PORT=8080 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
EXPOSE 8080

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
