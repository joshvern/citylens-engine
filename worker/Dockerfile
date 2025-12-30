FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip

COPY pyproject.toml /app/pyproject.toml
COPY worker.py /app/worker.py
COPY services /app/services

RUN pip install --no-cache-dir -e .

# Install citylens-core from git at build time.
ARG CITYLENS_CORE_GIT_URL=
RUN if [ -z "$CITYLENS_CORE_GIT_URL" ]; then \
      echo "ERROR: CITYLENS_CORE_GIT_URL build-arg is required (citylens-core is the canonical contract)." && exit 1; \
    fi && \
    pip install --no-cache-dir "citylens-core @ ${CITYLENS_CORE_GIT_URL}"

CMD ["python", "-u", "/app/worker.py"]
