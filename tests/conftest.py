"""Test fixtures for the multiplayer plugin.

Spins up a minimal FastAPI app, loads the plugin's setup() with a stub
context that points to a tmp config dir, and exposes a TestClient for
HTTP + WebSocket interactions. The plugin's module-level state (`_rooms`,
`_cleanup_tasks`) is reset between tests to avoid cross-test bleed.
"""

import importlib
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture
def routes_module():
    # Re-import so each test starts with empty `_rooms` / `_cleanup_tasks`.
    if "routes" in sys.modules:
        importlib.reload(sys.modules["routes"])
    import routes  # noqa: F401
    return sys.modules["routes"]


@pytest.fixture
def app(routes_module, tmp_path):
    fastapi_app = FastAPI()
    context = {
        "config_dir": tmp_path,
        "get_dlc_dir": lambda: tmp_path / "dlc",
        "extract_meta": None,
        "meta_db": None,
        "get_sloppak_cache_dir": lambda: tmp_path / "sloppak_cache",
    }
    routes_module.setup(fastapi_app, context)
    return fastapi_app


@pytest.fixture
def client(app):
    with TestClient(app) as c:
        yield c
