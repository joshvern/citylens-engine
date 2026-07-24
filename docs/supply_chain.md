# Runtime supply-chain policy

CityLens treats each dependency graph and final container filesystem as a
separate release boundary. A clean Python audit does not prove the operating
system layer is clean, and an image scan does not prove the lockfile is
reproducible.

## API build contract

- Python is pinned to the `python:3.11.15-alpine3.23` multi-architecture
  manifest by digest.
- uv is pinned by version and image digest.
- `citylens-core@v0.3.25` is recorded in `api/pyproject.toml`; `uv.lock`
  resolves that tag to its immutable Git commit.
- The builder installs only the API production graph with `uv sync --frozen`.
- The runtime contains no compiler, Git, pip, setuptools, or wheel.
- The runtime executes as UID/GID `10001`.
- `.dockerignore` excludes local environments, Git history, caches, and
  `.env` material from the build context.

Updating Python, Alpine, uv, `citylens-core`, or any Python dependency requires
an ordinary reviewed pull request with a regenerated lockfile and passing image
scan.

## Worker build contract

- Python is pinned to the `python:3.11.15-slim` manifest by digest.
- CPU Torch `2.13.0`, Torchvision `0.28.0`, `citylens-core@v0.3.25`,
  SAM2's immutable commit, and LiDAR dependencies are all in `uv.lock`.
- Git and the full Perl runtime exist only in the builder. The final worker
  retains only the explicit `libexpat1` native dependency required by Rasterio.
- SAM2 assets are downloaded in the builder and copied into the final image.
- The runtime contains no Git, compiler, pip, wheel, or Perl executable and
  executes as UID/GID `10001`.

## CI release gates

The `api-supply-chain` and `worker-supply-chain` jobs each:

1. verifies `uv.lock` is current;
2. audit all locked public runtime dependencies with
   `pip-audit==2.10.1`;
3. generate a CycloneDX 1.5 dependency SBOM;
4. build the exact production image;
5. record a Trivy high/critical vulnerability report;
6. fail on any critical finding, fixed or unfixed;
7. fail on any fixable high or critical finding.

Unfixed high findings are retained in the uploaded report for review. They do
not automatically block a release because upstream distributions can publish a
finding before a fix exists. Critical findings always block.

## Local verification

From the repository root:

```bash
uv lock --check

uv export \
  --package citylens-engine-api \
  --no-dev \
  --no-emit-project \
  --no-emit-package citylens-core \
  --frozen \
  --format requirements.txt \
  --output-file /tmp/citylens-api-audit.txt \
  >/dev/null

uvx --from pip-audit==2.10.1 pip-audit \
  --strict \
  --progress-spinner off \
  --disable-pip \
  --requirement /tmp/citylens-api-audit.txt

docker build --tag citylens-api:audit --file api/Dockerfile .

trivy image \
  --scanners vuln \
  --severity HIGH,CRITICAL \
  --exit-code 1 \
  citylens-api:audit
```

The final all-high/critical local command is intentionally stricter than the
split CI policy. It is the preferred release result whenever upstream packages
permit it.

Repeat the dependency and image checks for the worker with:

```bash
uv export \
  --package citylens-engine-worker \
  --no-dev \
  --no-emit-project \
  --no-emit-package citylens-core \
  --no-emit-package sam-2 \
  --no-emit-package torch \
  --no-emit-package torchvision \
  --frozen \
  --format requirements.txt \
  --output-file /tmp/citylens-worker-audit.txt \
  >/dev/null

uvx --from pip-audit==2.10.1 pip-audit \
  --strict \
  --progress-spinner off \
  --disable-pip \
  --requirement /tmp/citylens-worker-audit.txt

docker build --tag citylens-worker:audit --file worker/Dockerfile .

trivy image \
  --scanners vuln \
  --severity CRITICAL \
  --exit-code 1 \
  citylens-worker:audit

trivy image \
  --scanners vuln \
  --severity HIGH,CRITICAL \
  --ignore-unfixed \
  --exit-code 1 \
  citylens-worker:audit
```

## Current verified baseline

On July 24, 2026:

- the previously deployed Debian-slim API digest had 31 unique high/critical
  findings: 27 high and 4 critical;
- 29 were inherited Debian findings with no available fix;
- two fixable Python build-tool findings were present in the runtime;
- the replacement Alpine runtime built from this repository had zero high or
  critical findings in Trivy 0.72.0.
- the previously deployed worker digest had 62 unique high/critical findings:
  46 high and 16 critical;
- the replacement locked multi-stage worker had 21 unfixed Debian highs, zero
  critical findings, and zero fixable high/critical findings.

This baseline is evidence for this build, not a permanent security claim. Every
pull request rebuilds and rescans the image.
