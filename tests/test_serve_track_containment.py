"""Path-containment tests for serve_track.

`serve_track` serves audio for the mixer preview from an attacker-controlled
`dir` query param + `filename`. It must only serve files under known roots
(the room recording dir, the DLC folder, or the sloppak unpack cache) so a
crafted request can't read arbitrary files off disk.
"""

from pathlib import Path


def _create_room(client, name="Alice"):
    r = client.post("/api/plugins/multiplayer/rooms", json={"name": name})
    r.raise_for_status()
    return r.json()["code"]


def test_serves_file_under_room_dir(client, routes_module):
    code = _create_room(client)
    room_dir = routes_module._MP_DIR / code.upper()
    room_dir.mkdir(parents=True, exist_ok=True)
    (room_dir / "take.wav").write_bytes(b"RIFFfake")

    r = client.get(f"/api/plugins/multiplayer/rooms/{code}/track/take.wav")
    assert r.status_code == 200, r.text
    assert r.content == b"RIFFfake"
    assert r.headers["content-type"].startswith("audio/wav")


def test_serves_stem_under_dlc_root(client, routes_module, tmp_path):
    code = _create_room(client)
    # get_dlc_dir() -> tmp_path/dlc per conftest; put a stem under it.
    stem_dir = tmp_path / "dlc" / "Song.sloppak" / "stems"
    stem_dir.mkdir(parents=True, exist_ok=True)
    (stem_dir / "bass.ogg").write_bytes(b"OggSstem")

    r = client.get(
        f"/api/plugins/multiplayer/rooms/{code}/track/bass.ogg",
        params={"dir": str(stem_dir)},
    )
    assert r.status_code == 200, r.text
    assert r.content == b"OggSstem"
    assert r.headers["content-type"].startswith("audio/ogg")


def test_absolute_dir_outside_roots_is_rejected(client, routes_module, tmp_path):
    """The original bug: `?dir=<arbitrary absolute>` read any file."""
    code = _create_room(client)
    secret = tmp_path / "outside" / "secret.ogg"
    secret.parent.mkdir(parents=True, exist_ok=True)
    secret.write_bytes(b"TOPSECRET")

    r = client.get(
        f"/api/plugins/multiplayer/rooms/{code}/track/secret.ogg",
        params={"dir": str(secret.parent)},
    )
    assert r.status_code == 404, r.text
    assert b"TOPSECRET" not in r.content


def test_dotdot_dir_traversal_is_rejected(client, routes_module, tmp_path):
    code = _create_room(client)
    secret = tmp_path / "secret.ogg"
    secret.write_bytes(b"TOPSECRET")
    # Try to climb out of an allowed root (the sloppak cache) via `..`.
    cache = tmp_path / "sloppak_cache"
    cache.mkdir(parents=True, exist_ok=True)

    r = client.get(
        f"/api/plugins/multiplayer/rooms/{code}/track/secret.ogg",
        params={"dir": str(cache / "..")},
    )
    assert r.status_code == 404, r.text
    assert b"TOPSECRET" not in r.content


def test_unknown_room_is_404(client):
    r = client.get("/api/plugins/multiplayer/rooms/ZZZZ/track/anything.ogg")
    assert r.status_code == 404, r.text
