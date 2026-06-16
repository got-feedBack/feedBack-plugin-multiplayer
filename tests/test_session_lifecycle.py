"""Phase 1a tests: highway WS session lifecycle per PROTOCOL.md.

Each test goes through the public REST + WS surface. The plugin's
module-level state is reset between tests by the conftest fixture
(re-importing the routes module).

PROTOCOL.md sections covered:
  - Endpoints / "session_id REQUIRED" (rejection on missing/malformed → 4401)
  - v1 server policy / Connection-arrival rules 1, 2, 3
  - Lifecycle close codes 4409 (superseded), 4410 (replaced)
  - Grace-window finalization: emits player_disconnected + cleans up the
    session record. (4408 itself is reserved for the audio slot in Phase 1b
    — Phase 1a does not emit it on the highway WS, so this suite does not
    assert a 4408 close event.)
"""

import time

import pytest
from starlette.websockets import WebSocketDisconnect


# Speed up grace tests by patching the constant.
@pytest.fixture
def fast_grace(routes_module):
    routes_module.SESSION_GRACE_SEC = 0.5
    yield 0.5


def _create_room(client, name="Alice"):
    r = client.post("/api/plugins/multiplayer/rooms", json={"name": name})
    r.raise_for_status()
    body = r.json()
    return body["code"], body["player_id"]


def _join_room(client, code, name="Bob"):
    r = client.post(f"/api/plugins/multiplayer/rooms/{code}/join", json={"name": name})
    r.raise_for_status()
    return r.json()["player_id"]


def _ws_url(code, player_id, session_id=None):
    base = f"/ws/plugins/multiplayer/{code}?player_id={player_id}"
    if session_id is not None:
        base += f"&session_id={session_id}"
    return base


# ── session_id validation ────────────────────────────────────────────────

def test_missing_session_id_rejected(client):
    code, pid = _create_room(client)
    with client.websocket_connect(_ws_url(code, pid)) as ws:
        msg = ws.receive_json()
        assert msg["type"] == "error"
        assert "session_id" in msg["message"].lower()
        with pytest.raises(WebSocketDisconnect) as excinfo:
            ws.receive_json()
        assert excinfo.value.code == 4401, (
            "missing session_id must close with 4401 per PROTOCOL.md"
        )


def test_malformed_session_id_rejected(client):
    code, pid = _create_room(client)
    # Values that should fail the [A-Za-z0-9_-]{1,128} pattern AFTER URL parsing.
    # Using URL-encoded chars so the literal sneaks through to the server.
    bad_values = [
        "",                # empty (rule: missing → 4401)
        "has%20space",     # decoded space — not in allowed charset
        "has%2Fslash",     # decoded slash — not in allowed charset
        "x" * 129,         # too long
        "has%21bang",      # decoded "!" — not in allowed charset
    ]
    for bad in bad_values:
        with client.websocket_connect(_ws_url(code, pid, bad)) as ws:
            msg = ws.receive_json()
            assert msg["type"] == "error", f"expected reject for session_id={bad!r}"
            with pytest.raises(WebSocketDisconnect) as excinfo:
                ws.receive_json()
            assert excinfo.value.code == 4401, (
                f"malformed session_id={bad!r} must close with 4401 per PROTOCOL.md"
            )


def test_well_formed_session_id_accepted(client):
    code, pid = _create_room(client)
    # UUIDv4 form
    sid = "11111111-2222-4333-8444-555555555555"
    with client.websocket_connect(_ws_url(code, pid, sid)) as ws:
        msg = ws.receive_json()
        assert msg["type"] == "connected"
        assert msg["room"]["code"] == code


def test_alphanumeric_session_id_accepted(client):
    code, pid = _create_room(client)
    sid = "tab_xyz-123"
    with client.websocket_connect(_ws_url(code, pid, sid)) as ws:
        msg = ws.receive_json()
        assert msg["type"] == "connected"


# ── Rule 1: new session ───────────────────────────────────────────────────

def test_first_connect_creates_session_record(client, routes_module):
    code, pid = _create_room(client)
    sid = "session-aaa"
    with client.websocket_connect(_ws_url(code, pid, sid)):
        room = routes_module._rooms[code]
        assert pid in room["sessions"]
        rec = room["sessions"][pid]
        assert rec["session_id"] == sid
        assert rec["highway_ws"] is not None
        assert rec["audio_ws"] is None  # Phase 1b hooks this up


def test_first_connect_broadcasts_player_connected_to_others(client):
    code, host_pid = _create_room(client)
    other_pid = _join_room(client, code, "Bob")

    # Host connects first.
    with client.websocket_connect(_ws_url(code, host_pid, "host-sid")) as host_ws:
        host_ws.receive_json()  # 'connected'

        # Bob joining should fire player_connected to host.
        with client.websocket_connect(_ws_url(code, other_pid, "bob-sid")) as bob_ws:
            bob_ws.receive_json()  # 'connected'
            event = host_ws.receive_json()
            assert event["type"] == "player_connected"
            assert event["player_id"] == other_pid


# ── Rule 2: same-session reconnect (4410) ─────────────────────────────────

def test_same_session_reconnect_closes_old_with_4410(client):
    code, pid = _create_room(client)
    sid = "same-session-id"

    with client.websocket_connect(_ws_url(code, pid, sid)) as old_ws:
        old_ws.receive_json()  # 'connected'

        # Open a SECOND connection with the SAME session_id.
        with client.websocket_connect(_ws_url(code, pid, sid)) as new_ws:
            new_ws.receive_json()  # 'connected'

            # The old socket should now be closed by the server with code 4410.
            with pytest.raises(WebSocketDisconnect) as exc_info:
                old_ws.receive_json()
            assert exc_info.value.code == 4410


def test_same_session_reconnect_does_not_emit_player_connected(client):
    code, host_pid = _create_room(client)
    other_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_ws_url(code, host_pid, "host-sid")) as host_ws:
        host_ws.receive_json()  # 'connected'

        with client.websocket_connect(_ws_url(code, other_pid, "bob-sid")) as bob_ws_1:
            bob_ws_1.receive_json()
            evt = host_ws.receive_json()
            assert evt["type"] == "player_connected"

            # Bob reconnects with the SAME session_id.
            with client.websocket_connect(_ws_url(code, other_pid, "bob-sid")) as bob_ws_2:
                bob_ws_2.receive_json()  # 'connected'

                # Drive a deterministic event from bob_ws_2 that the server
                # WILL forward to host: set_arrangement → arrangement_changed.
                # If Rule 2 was violated, host's queue would contain a duplicate
                # player_connected BEFORE the arrangement_changed.
                bob_ws_2.send_json({"type": "set_arrangement", "arrangement": "Bass"})
                evt2 = host_ws.receive_json()
                assert evt2["type"] == "arrangement_changed", (
                    f"Rule 2 reconnect must not emit a fresh player_connected; "
                    f"host received {evt2['type']!r} first"
                )
                assert evt2["player_id"] == other_pid
                assert evt2["arrangement"] == "Bass"


# ── Rule 3: takeover by different session_id (4409) ───────────────────────

def test_different_session_id_takeover_closes_old_with_4409(client):
    code, pid = _create_room(client)

    with client.websocket_connect(_ws_url(code, pid, "tab-A-sid")) as old_ws:
        old_ws.receive_json()

        # Tab B connects with a DIFFERENT session_id for the same player_id.
        with client.websocket_connect(_ws_url(code, pid, "tab-B-sid")) as new_ws:
            new_ws.receive_json()

            with pytest.raises(WebSocketDisconnect) as exc_info:
                old_ws.receive_json()
            assert exc_info.value.code == 4409


def test_takeover_replaces_session_record(client, routes_module):
    code, pid = _create_room(client)

    with client.websocket_connect(_ws_url(code, pid, "tab-A-sid")) as ws_a:
        ws_a.receive_json()
        with client.websocket_connect(_ws_url(code, pid, "tab-B-sid")) as ws_b:
            ws_b.receive_json()
            rec = routes_module._rooms[code]["sessions"][pid]
            assert rec["session_id"] == "tab-B-sid"


# ── Per-endpoint grace + 4408 finalization ────────────────────────────────

def test_disconnect_within_grace_does_not_finalize(client, routes_module, fast_grace):
    code, pid = _create_room(client)
    sid = "grace-sid"

    with client.websocket_connect(_ws_url(code, pid, sid)) as ws_a:
        ws_a.receive_json()

    # Client closed the socket; reattach within grace.
    time.sleep(fast_grace * 0.4)
    with client.websocket_connect(_ws_url(code, pid, sid)) as ws_b:
        msg = ws_b.receive_json()
        assert msg["type"] == "connected"
        rec = routes_module._rooms[code]["sessions"].get(pid)
        assert rec is not None, "Session should have been preserved across grace reattach"
        assert rec["session_id"] == sid


def test_disconnect_past_grace_finalizes_session(client, routes_module, fast_grace):
    code, host_pid = _create_room(client)
    other_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_ws_url(code, host_pid, "host-sid")) as host_ws:
        host_ws.receive_json()

        with client.websocket_connect(_ws_url(code, other_pid, "bob-sid")) as bob_ws:
            bob_ws.receive_json()
            host_ws.receive_json()  # player_connected for bob

        # Bob disconnects (context manager closed). Sleep past the grace window
        # so the server's _grace_then_finalize timer has time to fire.
        time.sleep(fast_grace * 2.5)

        # Host should now have a player_disconnected message ready. The first
        # blocking receive yields control to the server's event loop and pulls
        # whatever was broadcast during the sleep.
        msg = host_ws.receive_json()
        assert msg["type"] == "player_disconnected"
        assert msg["player_id"] == other_pid

    # Session record for bob should be gone.
    rec = routes_module._rooms[code]["sessions"].get(other_pid)
    assert rec is None
