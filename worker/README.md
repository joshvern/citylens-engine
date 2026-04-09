# citylens-engine Worker

This subproject is the Cloud Run Job worker that executes `citylens-core`.

See the repo-level README at `../README.md` and docs in `../docs/`.

Local install:

```bash
cd ..
uv sync --all-packages --all-extras
uv pip install --python ./.venv/bin/python -e ../citylens-core
source .venv/bin/activate
```
