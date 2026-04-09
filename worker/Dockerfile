FROM python:3.11-slim

WORKDIR /app

# Needed for `pip install "pkg @ git+https://..."`
RUN apt-get update \
  && apt-get install -y --no-install-recommends git ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip

COPY pyproject.toml /app/pyproject.toml
COPY worker.py /app/worker.py
COPY services /app/services

RUN pip install --no-cache-dir -e .

# Install citylens-core from git at build time.
ARG CITYLENS_CORE_GIT_URL=git+https://github.com/joshvern/citylens-core.git@v0.3.0
ARG CITYLENS_SAM2_ASSETS_SIZE=small

# Where citylens-core looks for runtime assets like SAM2 checkpoints.
ENV CITYLENS_ASSETS_ROOT=/opt/citylens-assets
RUN mkdir -p "${CITYLENS_ASSETS_ROOT}"

# SAM2 requires PyTorch; install a CPU build suitable for Cloud Run.
# (CUDA is not available in the default Cloud Run environment.)
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu torch torchvision

RUN pip install --no-cache-dir "citylens-core[sam2] @ ${CITYLENS_CORE_GIT_URL}"

# Download SAM2 config/weights into CITYLENS_ASSETS_ROOT at build time.
RUN python -c "from citylens_core.sam.assets import download_sam2_assets; download_sam2_assets('${CITYLENS_SAM2_ASSETS_SIZE}')"

CMD ["python", "-u", "/app/worker.py"]
