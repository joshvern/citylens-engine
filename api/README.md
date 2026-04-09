# citylens-engine API

This subproject is the FastAPI service deployed to Cloud Run.

See the repo-level README at `../README.md` and docs in `../docs/`.

Local install:

```bash
cd ..
uv sync --all-packages --all-extras
uv pip install --python ./.venv/bin/python -e ../citylens-core
source .venv/bin/activate
```
