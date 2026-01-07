FROM python:3.10-slim

WORKDIR /app

# Needed for `pip install "pkg @ git+https://..."`
RUN apt-get update \
  && apt-get install -y --no-install-recommends git ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Python deps
RUN pip install --no-cache-dir --upgrade pip

COPY api/pyproject.toml /app/pyproject.toml
COPY api/app /app/app

# Demo allowlist JSON lives here (used by /v1/demo/* endpoints)
COPY deploy /app/deploy

# Install API deps
RUN pip install --no-cache-dir -e .

# Install citylens-core from git at build time.
# Provide build arg CITYLENS_CORE_GIT_URL, e.g.
#   --build-arg CITYLENS_CORE_GIT_URL="git+https://github.com/<ORG>/citylens-core.git@<REF>"
ARG CITYLENS_CORE_GIT_URL=
RUN if [ -z "$CITYLENS_CORE_GIT_URL" ]; then \
      echo "ERROR: CITYLENS_CORE_GIT_URL build-arg is required (citylens-core is the canonical contract)." && exit 1; \
    fi && \
    pip install --no-cache-dir "citylens-core @ ${CITYLENS_CORE_GIT_URL}"

ENV PORT=8080
EXPOSE 8080

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
