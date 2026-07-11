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

The worker stages current NYC building footprints from OpenData before invoking
core. `CITYLENS_CURRENT_FOOTPRINTS_QUERY_PAD_M` controls the Socrata query
padding in the orthophoto CRS and defaults to 250 metres. Padding prevents
boundary-crossing buildings from being omitted by `within_box`; it does not
expand the analysis area, because core still rasterizes against the original
orthophoto bounds. The value must be finite and nonnegative; set it to `0` to
disable padding.
