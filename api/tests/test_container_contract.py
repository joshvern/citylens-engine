from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_api_image_is_reproducible_and_non_root() -> None:
    dockerfile = (REPO_ROOT / "api" / "Dockerfile").read_text(encoding="utf-8")

    assert "python:3.11.15-alpine3.23@sha256:" in dockerfile
    assert "ghcr.io/astral-sh/uv:0.10.9@sha256:" in dockerfile
    assert "--package citylens-engine-api" in dockerfile
    assert "--no-dev" in dockerfile
    assert "--frozen" in dockerfile
    assert "apk add --no-cache" in dockerfile
    assert "gdal" in dockerfile
    assert "USER 10001:10001" in dockerfile
    assert "pip uninstall -y pip setuptools wheel" in dockerfile
    assert "deploy/demo_runs.json /app/deploy/demo_runs.json" in dockerfile
    assert "COPY --chown=citylens:citylens deploy /app/deploy" not in dockerfile


def test_api_core_release_is_locked_in_the_manifest() -> None:
    manifest = (REPO_ROOT / "api" / "pyproject.toml").read_text(encoding="utf-8")
    cloudbuild = (REPO_ROOT / "api" / "cloudbuild.yaml").read_text(encoding="utf-8")

    assert (
        "citylens-core @ git+https://github.com/joshvern/citylens-core.git@v0.3.25"
        in manifest
    )
    assert "CITYLENS_CORE_GIT_URL" not in cloudbuild


def test_worker_image_is_reproducible_and_non_root() -> None:
    dockerfile = (REPO_ROOT / "worker" / "Dockerfile").read_text(encoding="utf-8")
    manifest = (REPO_ROOT / "worker" / "pyproject.toml").read_text(encoding="utf-8")
    cloudbuild = (REPO_ROOT / "worker" / "cloudbuild.yaml").read_text(encoding="utf-8")

    assert "python:3.11.15-slim@sha256:" in dockerfile
    assert "--package citylens-engine-worker" in dockerfile
    assert "--no-dev" in dockerfile
    assert "--frozen" in dockerfile
    assert "apt-get install -y --no-install-recommends libexpat1" in dockerfile
    assert "dpkg --purge --force-depends --force-remove-essential perl-base" in dockerfile
    assert "USER 10001:10001" in dockerfile
    assert "torch==2.13.0" in manifest
    assert "torchvision==0.28.0" in manifest
    assert "citylens-core[sam2,lidar]" in manifest
    assert "CITYLENS_CORE_GIT_URL" not in cloudbuild


def test_docker_context_excludes_local_environments_and_secrets() -> None:
    dockerignore = (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    gcloudignore = (REPO_ROOT / ".gcloudignore").read_text(
        encoding="utf-8"
    ).splitlines()

    assert ".venv" in dockerignore
    assert ".env" in dockerignore
    assert ".env.*" in dockerignore
    assert ".git" in dockerignore
    assert "deploy/*.sh" in dockerignore
    assert "!.dockerignore" in gcloudignore
    assert "!pyproject.toml" in gcloudignore
    assert "!uv.lock" in gcloudignore


def test_versioned_deploy_scripts_keep_worker_runtime_contract() -> None:
    deploy_all = (REPO_ROOT / "deploy" / "deploy_all.sh").read_text(encoding="utf-8")
    deploy_worker = (REPO_ROOT / "deploy" / "deploy_worker.sh").read_text(
        encoding="utf-8"
    )

    for script in (deploy_all, deploy_worker):
        assert "--config \"worker/cloudbuild.yaml\"" in script
        assert "--substitutions _IMAGE=${WORKER_IMAGE}" in script
        assert "_CITYLENS_CORE_GIT_URL" not in script
        assert "--task-timeout" in script
        assert "--max-retries" in script
        assert "CITYLENS_REFERENCE_DATA_DIR=/tmp/reference-data" in script

    assert 'WORKER_CPU="${WORKER_CPU:-4}"' in deploy_all
    assert 'WORKER_MEMORY="${WORKER_MEMORY:-8Gi}"' in deploy_all
