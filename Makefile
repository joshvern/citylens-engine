.PHONY: dev test fmt

PYTHON ?= ./.venv/bin/python
UV ?= uv
CITYLENS_CORE_REF ?= v0.3.0
CITYLENS_CORE_GIT_URL ?= git+https://github.com/joshvern/citylens-core.git@$(CITYLENS_CORE_REF)

dev:
	cd api && $(PYTHON) -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload

test:
	cd api && $(PYTHON) -m pytest
	cd worker && $(PYTHON) -m pytest

fmt:
	cd api && $(PYTHON) -m ruff format . && $(PYTHON) -m ruff check . --fix
	cd worker && $(PYTHON) -m ruff format . && $(PYTHON) -m ruff check . --fix

sync:
	$(UV) sync --all-packages --all-extras
	@if [ -d ../citylens-core ]; then \
		$(UV) pip install --python $(PYTHON) -e ../citylens-core; \
	else \
		$(UV) pip install --python $(PYTHON) --no-cache-dir "citylens-core[sam2] @ $(CITYLENS_CORE_GIT_URL)"; \
	fi
