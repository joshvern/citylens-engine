.PHONY: dev test fmt

PYTHON ?= python3.10

dev:
	cd api && $(PYTHON) -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload

test:
	cd api && $(PYTHON) -m pytest
	cd worker && $(PYTHON) -m pytest

fmt:
	cd api && $(PYTHON) -m ruff format . && $(PYTHON) -m ruff check . --fix
	cd worker && $(PYTHON) -m ruff format . && $(PYTHON) -m ruff check . --fix
