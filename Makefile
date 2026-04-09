.PHONY: dev test fmt

PYTHON ?= ./.venv/bin/python
UV ?= uv

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
	elif [ -n "$$CITYLENS_CORE_GIT_URL" ]; then \
		$(UV) pip install --python $(PYTHON) --no-cache-dir "citylens-core[sam2] @ $${CITYLENS_CORE_GIT_URL}"; \
	else \
		echo "citylens-core is not installed; place ../citylens-core next to this repo or set CITYLENS_CORE_GIT_URL."; \
		exit 1; \
	fi
