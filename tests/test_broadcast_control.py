"""Phase 1c tests: broadcast control plane on the highway WS.

Covers PROTOCOL.md "Audio control messages":
  - broadcast_start with valid v1 params → broadcaster_changed to all peers
  - broadcast_start while another broadcaster is active → broadcaster_busy
  - broadcast_start with invalid params → broadcast_start_invalid error
  - broadcast_stop by the active broadcaster → broadcaster_changed null
  - broadcast_stop by a non-broadcaster → silently ignored
  - audio_quality forwarded to the broadcaster only
  - broadcaster_id surfaced in the connected snapshot for late joiners
  - broadcaster_changed null on session-end paths (grace expiry, leave, takeover)
"""

import time

import pytest
from starlette.websockets import WebSocketDisconnect


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


def _highway_url(code, player_id, session_id):
    return (
        f"/ws/plugins/multiplayer/{code}"
        f"?player_id={player_id}&session_id={session_id}"
    )


def _audio_url(code, player_id, session_id):
    return (
        f"/ws/plugins/multiplayer/{code}/audio"
        f"?player_id={player_id}&session_id={session_id}"
    )


_VALID_PARAMS = {
    "interval_beats": 4,
    "sample_rate": 48000,
    "channel_count": 1,
    "codec": "opus",
    "bitrate": 96000,
}


def _drain_until(ws, predicate, max_msgs=20):
    """Read messages until one satisfies `predicate(msg)`. Returns the
    matching message. Pytest-timeout (configured in pyproject.toml)
    bounds the worst-case wall time; this loop bound prevents infinite
    iteration on simple bugs."""
    for _ in range(max_msgs):
        msg = ws.receive_json()
        if predicate(msg):
            return msg
    raise AssertionError(
        f"no matching message within {max_msgs} reads; predicate not satisfied"
    )


# ── broadcast_start happy path ────────────────────────────────────────────

def test_broadcast_start_emits_broadcaster_changed_to_peers(client, routes_module):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
        host_hw.receive_json()  # 'connected'
        bob_hw.receive_json()
        host_hw.receive_json()  # player_connected for bob

        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})

        # Bob receives broadcaster_changed.
        evt = _drain_until(bob_hw, lambda m: m.get("type") == "broadcaster_changed")
        assert evt["broadcaster_id"] == host_pid
        assert evt["interval_beats"] == 4
        assert evt["sample_rate"] == 48000
        assert evt["channel_count"] == 1
        assert evt["codec"] == "opus"
        assert evt["bitrate"] == 96000

        # Host also receives broadcaster_changed (they're a peer in the room).
        evt2 = _drain_until(host_hw, lambda m: m.get("type") == "broadcaster_changed")
        assert evt2["broadcaster_id"] == host_pid

        # Server state.
        room = routes_module._rooms[code]
        assert room["broadcaster_id"] == host_pid
        assert room["broadcast_params"] == {
            "interval_beats": 4,
            "sample_rate": 48000,
            "channel_count": 1,
            "codec": "opus",
            "bitrate": 96000,
        }


def test_broadcaster_id_surfaced_to_late_joiner_in_connected_snapshot(client):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw:
        host_hw.receive_json()  # 'connected'
        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        host_hw.receive_json()  # broadcaster_changed

        # Bob joins LATE — broadcaster is already active. The 'connected'
        # snapshot must surface broadcaster_id so bob's UI activates the
        # listener pipeline immediately, without waiting for a fresh
        # broadcaster_changed event (per PROTOCOL.md Lifecycle §4).
        with client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
            snapshot = bob_hw.receive_json()
            assert snapshot["type"] == "connected"
            assert snapshot["room"]["broadcaster_id"] == host_pid
            assert snapshot["room"]["broadcast_params"]["bitrate"] == 96000


# ── broadcaster_busy ───────────────────────────────────────────────────────

def test_second_broadcaster_rejected_with_broadcaster_busy(client, routes_module):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_audio_url(code, bob_pid, "bob-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
        host_hw.receive_json()
        bob_hw.receive_json()
        host_hw.receive_json()  # player_connected for bob

        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        host_hw.receive_json()  # broadcaster_changed (host)
        bob_hw.receive_json()   # broadcaster_changed (bob)

        # Bob tries to also broadcast.
        bob_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        evt = _drain_until(bob_hw, lambda m: m.get("type") == "error")
        assert evt["message"] == "broadcaster_busy"

        # Server state unchanged.
        assert routes_module._rooms[code]["broadcaster_id"] == host_pid


def test_active_broadcaster_can_re_send_broadcast_start(client, routes_module):
    """The same player re-issuing broadcast_start should NOT be treated as
    busy — that would lock them out after a transient disconnect-reconnect
    where they want to resume their broadcast. v1 server policy: only a
    DIFFERENT player_id triggers broadcaster_busy."""
    code, host_pid = _create_room(client)

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw:
        host_hw.receive_json()
        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        host_hw.receive_json()  # broadcaster_changed

        # Same player_id sends broadcast_start again with new params.
        new_params = {**_VALID_PARAMS, "bitrate": 64000}
        host_hw.send_json({"type": "broadcast_start", **new_params})
        evt = _drain_until(host_hw, lambda m: m.get("type") == "broadcaster_changed")
        assert evt["broadcaster_id"] == host_pid
        assert evt["bitrate"] == 64000
        assert routes_module._rooms[code]["broadcast_params"]["bitrate"] == 64000


# ── audio_ws_not_open rejection ───────────────────────────────────────────

def test_broadcast_start_rejected_when_audio_ws_not_open(client, routes_module):
    """Codex round 1 P1 (Phase 1c): broadcast_start MUST be rejected with
    audio_ws_not_open if the host's session has no live /audio socket.
    Otherwise peers (and late joiners) would see an active broadcaster
    even though no audio frames can ever arrive."""
    code, host_pid = _create_room(client)
    with client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw:
        host_hw.receive_json()  # 'connected'
        # Audio NOT opened. broadcast_start must reject.
        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        evt = _drain_until(host_hw, lambda m: m.get("type") == "error")
        assert evt["message"] == "audio_ws_not_open"
        assert routes_module._rooms[code]["broadcaster_id"] is None


# ── broadcast_start invalid params ────────────────────────────────────────

@pytest.mark.parametrize("bad_overrides,expected_field", [
    ({"sample_rate": 44100}, "sample_rate"),
    ({"channel_count": 2}, "channel_count"),
    ({"codec": "vorbis"}, "codec"),
    ({"bitrate": 1000}, "bitrate"),       # below min
    ({"bitrate": 999_999}, "bitrate"),    # above max
    ({"bitrate": "96000"}, "bitrate"),    # string, not int
    ({"interval_beats": -1}, "interval_beats"),
    ({"interval_beats": "four"}, "interval_beats"),
])
def test_broadcast_start_with_invalid_params_returns_error(
    client, routes_module, bad_overrides, expected_field,
):
    code, host_pid = _create_room(client)
    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw:
        host_hw.receive_json()
        params = {**_VALID_PARAMS, **bad_overrides}
        host_hw.send_json({"type": "broadcast_start", **params})
        evt = _drain_until(host_hw, lambda m: m.get("type") == "error")
        assert evt["message"].startswith("broadcast_start_invalid:")
        assert expected_field in evt["message"]
        # No broadcast became active.
        assert routes_module._rooms[code]["broadcaster_id"] is None


# ── broadcast_stop ─────────────────────────────────────────────────────────

def test_broadcaster_can_stop_their_own_broadcast(client, routes_module):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
        host_hw.receive_json()
        bob_hw.receive_json()
        host_hw.receive_json()  # player_connected for bob

        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        host_hw.receive_json()
        bob_hw.receive_json()  # broadcaster_changed

        host_hw.send_json({"type": "broadcast_stop"})
        evt = _drain_until(bob_hw, lambda m: m.get("type") == "broadcaster_changed")
        assert evt["broadcaster_id"] is None
        assert routes_module._rooms[code]["broadcaster_id"] is None
        assert routes_module._rooms[code]["broadcast_params"] is None


def test_broadcast_stop_from_non_broadcaster_is_silently_ignored(client, routes_module):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
        host_hw.receive_json()
        bob_hw.receive_json()
        host_hw.receive_json()  # player_connected for bob

        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        host_hw.receive_json()
        bob_hw.receive_json()

        # Bob (NOT the broadcaster) sends broadcast_stop. Server must
        # silently ignore — host's broadcast continues. We verify by
        # driving a deterministic exchange and checking no
        # broadcaster_changed event sneaks in.
        bob_hw.send_json({"type": "broadcast_stop"})
        bob_hw.send_json({"type": "set_arrangement", "arrangement": "Bass"})
        evt = _drain_until(host_hw, lambda m: m.get("type") == "arrangement_changed")
        assert evt["player_id"] == bob_pid
        # Broadcast still active.
        assert routes_module._rooms[code]["broadcaster_id"] == host_pid


# ── audio_quality ─────────────────────────────────────────────────────────

def test_audio_quality_forwarded_only_to_broadcaster(client):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")
    carol_pid = _join_room(client, code, "Carol")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw, \
         client.websocket_connect(_highway_url(code, carol_pid, "carol-sid")) as carol_hw:
        host_hw.receive_json()
        bob_hw.receive_json()
        carol_hw.receive_json()
        host_hw.receive_json()
        host_hw.receive_json()  # player_connected for bob, carol

        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        host_hw.receive_json()
        bob_hw.receive_json()
        carol_hw.receive_json()

        bob_hw.send_json({
            "type": "audio_quality",
            "broadcaster_id": host_pid,
            "intervals_received": 100,
            "intervals_late": 1,
            "intervals_dropped": 0,
            "decoder_underruns": 0,
            "report_period_ms": 30000,
        })

        # Host (the broadcaster) receives the report.
        evt = _drain_until(host_hw, lambda m: m.get("type") == "audio_quality")
        assert evt["from_player_id"] == bob_pid
        assert evt["broadcaster_id"] == host_pid
        assert evt["intervals_received"] == 100

        # Carol (a non-broadcaster peer) does NOT see it. Verify by
        # driving a deterministic event from carol that the host WILL
        # forward and asserting host's next message is THAT, not another
        # audio_quality.
        carol_hw.send_json({"type": "set_arrangement", "arrangement": "Lead"})
        next_evt = _drain_until(host_hw, lambda m: m.get("type") in {"audio_quality", "arrangement_changed"})
        assert next_evt["type"] == "arrangement_changed"


def test_audio_quality_with_stale_broadcaster_id_dropped(client):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
        host_hw.receive_json()
        bob_hw.receive_json()
        host_hw.receive_json()  # player_connected for bob

        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        host_hw.receive_json()
        bob_hw.receive_json()

        # Bob sends a quality report claiming a different broadcaster_id.
        # Server must drop it (stale telemetry).
        bob_hw.send_json({
            "type": "audio_quality",
            "broadcaster_id": "stale-id-not-host",
            "intervals_received": 100,
        })
        # Then bob sends a deterministic event so we can assert host's
        # NEXT message is that one (not the dropped audio_quality).
        bob_hw.send_json({"type": "set_arrangement", "arrangement": "Rhythm"})
        evt = _drain_until(host_hw, lambda m: m.get("type") in {"audio_quality", "arrangement_changed"})
        assert evt["type"] == "arrangement_changed"


# ── broadcaster_changed null on session-end paths ─────────────────────────

def test_broadcast_ends_on_grace_expiry(client, routes_module, fast_grace):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
        host_hw.receive_json()
        bob_hw.receive_json()
        host_hw.receive_json()  # player_connected for bob

        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        host_hw.receive_json()
        bob_hw.receive_json()

        # Host disconnects (context exit). Wait past grace.
    time.sleep(fast_grace * 2.5)

    # Bob's WS is now closed too (the with-block exit), so re-open to
    # check final state via a fresh connection.
    with client.websocket_connect(_highway_url(code, bob_pid, "bob-sid-2")) as bob_hw2:
        snapshot = bob_hw2.receive_json()
        assert snapshot["room"]["broadcaster_id"] is None
    assert routes_module._rooms[code]["broadcaster_id"] is None


def test_broadcast_ends_on_leave_room(client, routes_module):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
        host_hw.receive_json()
        bob_hw.receive_json()
        host_hw.receive_json()  # player_connected for bob

        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        host_hw.receive_json()
        bob_hw.receive_json()

        # Host explicitly leaves via REST.
        r = client.post(
            f"/api/plugins/multiplayer/rooms/{code}/leave",
            json={"player_id": host_pid},
        )
        r.raise_for_status()

        evt = _drain_until(bob_hw, lambda m: m.get("type") == "broadcaster_changed")
        assert evt["broadcaster_id"] is None
        assert routes_module._rooms[code]["broadcaster_id"] is None


def test_broadcast_ends_on_takeover_by_different_session(client, routes_module):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
        host_hw.receive_json()
        bob_hw.receive_json()
        host_hw.receive_json()  # player_connected for bob

        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        host_hw.receive_json()
        bob_hw.receive_json()

        # A different session_id for the SAME host_pid arrives — Rule 3
        # takeover. The new tab must explicitly re-issue broadcast_start
        # to resume; the old broadcast is ended.
        with client.websocket_connect(_highway_url(code, host_pid, "host-sid-NEW")) as host_hw_new:
            # Old socket was closed with 4409; ignore.
            try:
                host_hw.receive_json()
            except WebSocketDisconnect:
                pass

            host_hw_new.receive_json()  # 'connected'
            evt = _drain_until(bob_hw, lambda m: m.get("type") == "broadcaster_changed")
            assert evt["broadcaster_id"] is None
            assert routes_module._rooms[code]["broadcaster_id"] is None


# ── Phase 2c: A→B handoff (purge + worker rebuild) ────────────────────────

def test_a_to_b_handoff_purges_old_broadcaster_workers(client, routes_module):
    """A broadcasts → A stops → B starts: B's broadcast_start MUST trigger
    _purge_audio_for_handoff so per-listener queues + workers are
    cancelled and rebuilt before B's first frame fans out. Without this,
    the previous broadcaster's tail frames in slow-listener queues would
    be misattributed to B on the listener side.

    Verifies:
      - Each peer has fresh audio_send_queue + audio_send_worker after
        the handoff (different identities than before).
      - room["broadcaster_id"] correctly reports B; broadcast_handoff_in_progress
        returns to 0 after the purge completes.
    """
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")
    carol_pid = _join_room(client, code, "Carol")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_audio_url(code, bob_pid, "bob-sid")), \
         client.websocket_connect(_audio_url(code, carol_pid, "carol-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw, \
         client.websocket_connect(_highway_url(code, carol_pid, "carol-sid")) as carol_hw:
        # Drain initial connected + player_connected events.
        for hw in (host_hw, bob_hw, carol_hw):
            _drain_until(hw, lambda m: m.get("type") == "connected")

        # A (host) starts broadcasting.
        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        _drain_until(bob_hw, lambda m: m.get("type") == "broadcaster_changed" and m.get("broadcaster_id") == host_pid)
        _drain_until(carol_hw, lambda m: m.get("type") == "broadcaster_changed" and m.get("broadcaster_id") == host_pid)
        assert routes_module._rooms[code]["broadcaster_id"] == host_pid

        # Snapshot bob and carol's worker/queue identities so we can
        # confirm the handoff actually replaced them.
        room = routes_module._rooms[code]
        bob_sess = room["sessions"][bob_pid]
        carol_sess = room["sessions"][carol_pid]
        bob_worker_before = bob_sess["audio_send_worker"]
        bob_queue_before = bob_sess["audio_send_queue"]
        carol_worker_before = carol_sess["audio_send_worker"]
        carol_queue_before = carol_sess["audio_send_queue"]
        assert bob_worker_before is not None
        assert carol_worker_before is not None

        # A stops; Bob and Carol see broadcaster_changed: null.
        host_hw.send_json({"type": "broadcast_stop"})
        _drain_until(bob_hw, lambda m: m.get("type") == "broadcaster_changed" and m.get("broadcaster_id") is None)
        _drain_until(carol_hw, lambda m: m.get("type") == "broadcaster_changed" and m.get("broadcaster_id") is None)
        assert room["broadcaster_id"] is None

        # B (Bob) starts broadcasting. This must trigger purge.
        bob_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        _drain_until(host_hw, lambda m: m.get("type") == "broadcaster_changed" and m.get("broadcaster_id") == bob_pid)
        _drain_until(carol_hw, lambda m: m.get("type") == "broadcaster_changed" and m.get("broadcaster_id") == bob_pid)

        assert room["broadcaster_id"] == bob_pid
        assert room.get("broadcast_handoff_in_progress", 0) == 0

        # Each NON-broadcaster peer's worker + queue must have been
        # replaced by the purge. Bob (the new broadcaster) is excluded
        # from purge so their worker can be either the same instance
        # (if the purge skipped them) — we don't assert on Bob.
        carol_sess_after = room["sessions"][carol_pid]
        host_sess_after = room["sessions"][host_pid]
        assert carol_sess_after["audio_send_worker"] is not carol_worker_before
        assert carol_sess_after["audio_send_queue"] is not carol_queue_before
        assert host_sess_after["audio_send_worker"] is not None


def test_handoff_in_progress_blocks_same_player_reentrant_start(client, routes_module):
    """A SAME-player rapid retry of broadcast_start while purge is
    running should be silently dropped — otherwise the second call
    would emit broadcaster_changed early, before the first call's
    purge of listener workers has completed.

    Simulates the race by flipping broadcast_handoff_in_progress to a
    positive value before the second broadcast_start arrives, then
    verifies no second broadcaster_changed is emitted to peers.
    """
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
        for hw in (host_hw, bob_hw):
            _drain_until(hw, lambda m: m.get("type") == "connected")

        # First broadcast_start completes normally. On the initial
        # None → host_pid transition, the purge path runs (because
        # current != player_id when current is None), but with no
        # prior broadcaster's queue to drain it is effectively a
        # no-op. Reworded after Copilot review on PR #7 round 4.
        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        _drain_until(bob_hw, lambda m: m.get("type") == "broadcaster_changed" and m.get("broadcaster_id") == host_pid)

        room = routes_module._rooms[code]
        # Simulate a same-player retry arriving while a purge is in
        # progress: manually set the counter to mimic the in-flight
        # state. (In production, this state exists for ~0–500 ms.)
        room["broadcast_handoff_in_progress"] = 1
        try:
            host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
            # Bob should NOT see a second broadcaster_changed — the
            # re-entrant retry was dropped silently. Send something
            # else to give the dispatch a chance to drain, then assert
            # the next non-internal msg isn't a duplicate broadcaster_changed.
            host_hw.send_json({
                "type": "set_arrangement",
                "arrangement": "Lead",
            })
            evt = _drain_until(bob_hw, lambda m: m.get("type") in {"broadcaster_changed", "arrangement_changed"})
            assert evt["type"] == "arrangement_changed"
        finally:
            room["broadcast_handoff_in_progress"] = 0


def test_audio_frame_dropped_during_handoff_in_progress(client, routes_module):
    """The /audio fan-out gate must drop frames from the broadcaster
    while broadcast_handoff_in_progress > 0. PROTOCOL.md requires the
    broadcaster to wait for the broadcaster_changed ack before sending,
    so a well-behaved client won't hit this — the gate is belt-and-
    suspenders against a buggy or malicious broadcaster.
    """
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")) as host_audio, \
         client.websocket_connect(_audio_url(code, bob_pid, "bob-sid")) as bob_audio, \
         client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
        for hw in (host_hw, bob_hw):
            _drain_until(hw, lambda m: m.get("type") == "connected")

        host_hw.send_json({"type": "broadcast_start", **_VALID_PARAMS})
        _drain_until(bob_hw, lambda m: m.get("type") == "broadcaster_changed" and m.get("broadcaster_id") == host_pid)

        room = routes_module._rooms[code]
        room["broadcast_handoff_in_progress"] = 1
        try:
            # Send the FIRST frame while the gate is active — this MUST
            # be dropped server-side, not just delayed.
            first_frame = _make_minimal_smau_frame()
            host_audio.send_bytes(first_frame)

            # Control-plane round-trip proves the session is responsive
            # while the data-plane gate is active.
            host_hw.send_json({
                "type": "set_arrangement",
                "arrangement": "Lead",
            })
            _drain_until(bob_hw, lambda m: m.get("type") == "arrangement_changed")

            # Now clear the gate and send a SECOND, distinct frame.
            # If the first frame had been buffered rather than dropped,
            # Bob's first receive_bytes would return that first frame,
            # not this one — so this assertion proves the drop. Suggested
            # by Copilot review on PR #7 round 3.
            room["broadcast_handoff_in_progress"] = 0
            second_frame = _make_minimal_smau_frame()[:-1] + b"\x01"
            host_audio.send_bytes(second_frame)
            assert bob_audio.receive_bytes() == second_frame
        finally:
            room["broadcast_handoff_in_progress"] = 0


def _make_minimal_smau_frame():
    """Build a minimal valid SMAU frame: 40-byte header + 1-byte payload."""
    import struct
    header = bytearray(40)
    header[0:4] = b"SMAU"
    struct.pack_into("<H", header, 4, 1)              # version
    struct.pack_into("<H", header, 6, 0)              # flags
    struct.pack_into("<Q", header, 8, 0)              # interval_index
    struct.pack_into("<d", header, 16, 0.0)           # chart_time_start
    struct.pack_into("<d", header, 24, 1.0)           # chart_time_end
    struct.pack_into("<I", header, 32, 1)             # sample_count
    struct.pack_into("<I", header, 36, 1)             # opus_size
    return bytes(header) + b"\x00"
