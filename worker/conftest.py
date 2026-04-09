from __future__ import annotations

import sys
from pathlib import Path


def pytest_configure() -> None:
    worker_root = Path(__file__).resolve().parent
    if str(worker_root) not in sys.path:
        sys.path.insert(0, str(worker_root))
    repo_root = worker_root.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
