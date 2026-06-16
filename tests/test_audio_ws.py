"""Phase 1b tests: audio WS endpoint and binary fan-out.

Covers:
  - Endpoints / Audio WS — auth rejection via 4401 (accept-then-close)
  - Audio frame format — 40-byte SMAU header, opus_size length match,
    MAX_FRAME_BYTES (256 KB) hard limit, magic / version validation
  - Server's "binary fan-out, no payload inspection" relay loop:
    sender's frame is forwarded byte-for-byte to other audio subscribers,
    sender does NOT receive their own frame back
  - Session integration: 4410 reconnect on the audio slot leaves the highway
    slot alive; 4408 grace expiry on the audio slot ends the session and
    closes the highway slot with 4408 too; 4409 takeover via the audio
    endpoint closes BOTH old endpoints
"""

import struct
import time

import pytest
from starlette.websockets import WebSocketDisconnect


_MAGIC = b"SMAU"
_VERSION = 1


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


def _audio_url(code, player_id, session_id=None):
    base = f"/ws/plugins/multiplayer/{code}/audio?player_id={player_id}"
    if session_id is not None:
        base += f"&session_id={session_id}"
    return base


def _highway_url(code, player_id, session_id):
    return f"/ws/plugins/multiplayer/{code}?player_id={player_id}&session_id={session_id}"


def _force_broadcaster(routes_module, code, player_id):
    """Set room["broadcaster_id"] directly without going through the
    full broadcast_start handshake.

    Most tests in this file exercise the audio data plane (header
    validation, fan-out, queue overflow, lifecycle) in isolation from
    the control-plane handshake covered in test_broadcast_control.py.
    Phase 1c gates fan-out on `room["broadcaster_id"] == sender_pid`,
    so without this helper every relay-side test would silently drop
    its frames. Tests that DO want to exercise the handshake should
    not use this helper — they're in test_broadcast_control.py.
    """
    routes_module._rooms[code]["broadcaster_id"] = player_id
    routes_module._rooms[code]["broadcast_params"] = {
        "interval_beats": 4,
        "sample_rate": 48000,
        "channel_count": 1,
        "codec": "opus",
        "bitrate": 96000,
    }


def _build_audio_frame(opus_payload: bytes, *, magic=_MAGIC, version=_VERSION,
                       interval_index=1, chart_time_start=0.0,
                       chart_time_end=2.0, sample_count=None,
                       opus_size_override=None):
    """Construct a SMAU audio frame for tests.

    Header layout (40 bytes, little-endian):
      0..4    magic       (4 bytes)
      4..6    version     (u16)
      6..8    flags       (u16)
      8..16   interval_idx (u64)
     16..24   chart_time_start (f64)
     24..32   chart_time_end   (f64)
     32..36   sample_count (u32)
     36..40   opus_size    (u32)
    """
    if sample_count is None:
        sample_count = max(1, len(opus_payload))
    opus_size = (
        opus_size_override
        if opus_size_override is not None
        else len(opus_payload)
    )
    header = (
        magic
        + struct.pack("<H", version)
        + struct.pack("<H", 0)        # flags
        + struct.pack("<Q", interval_index)
        + struct.pack("<d", chart_time_start)
        + struct.pack("<d", chart_time_end)
        + struct.pack("<I", sample_count)
        + struct.pack("<I", opus_size)
    )
    assert len(header) == 40, f"header is {len(header)} bytes, expected 40"
    return header + opus_payload


# ── Auth rejection (4401) ────────────────────────────────────────────────

def test_audio_ws_missing_session_id_rejected(client):
    code, pid = _create_room(client)
    with client.websocket_connect(_audio_url(code, pid)) as ws:
        with pytest.raises(WebSocketDisconnect) as excinfo:
            ws.receive_bytes()
        assert excinfo.value.code == 4401


def test_audio_ws_unknown_player_id_rejected(client):
    code, _ = _create_room(client)
    with client.websocket_connect(_audio_url(code, "ghost", "any-sid")) as ws:
        with pytest.raises(WebSocketDisconnect) as excinfo:
            ws.receive_bytes()
        assert excinfo.value.code == 4401


def test_audio_ws_unknown_room_rejected(client):
    code, pid = _create_room(client)
    # Use a code containing characters outside _CODE_ALPHABET (no 0/O/1/I/L)
    # so _gen_code() could never produce it — guaranteed unknown regardless
    # of which room the test fixture happened to mint.
    with client.websocket_connect(_audio_url("000000", pid, "sid")) as ws:
        with pytest.raises(WebSocketDisconnect) as excinfo:
            ws.receive_bytes()
        assert excinfo.value.code == 4401


def test_audio_ws_malformed_session_id_rejected(client):
    code, pid = _create_room(client)
    with client.websocket_connect(_audio_url(code, pid, "x" * 200)) as ws:
        with pytest.raises(WebSocketDisconnect) as excinfo:
            ws.receive_bytes()
        assert excinfo.value.code == 4401


def test_audio_ws_valid_session_id_accepts(client, routes_module):
    code, pid = _create_room(client)
    sid = "audio-sid-1"
    with client.websocket_connect(_audio_url(code, pid, sid)):
        rec = routes_module._rooms[code]["sessions"][pid]
        assert rec["session_id"] == sid
        assert rec["audio_ws"] is not None
        # Highway slot is independent and not yet open.
        assert rec["highway_ws"] is None


# ── Binary fan-out ────────────────────────────────────────────────────────

def test_binary_frame_forwarded_byte_identical_to_other_peer(client, routes_module):
    code, host_pid = _create_room(client)
    other_pid = _join_room(client, code, "Bob")
    _force_broadcaster(routes_module, code, host_pid)

    # Host opens audio WS, listener opens audio WS.
    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")) as host_audio, \
         client.websocket_connect(_audio_url(code, other_pid, "bob-sid")) as bob_audio:

        payload = b"\x12\x34\x56" * 100  # arbitrary opus-shaped bytes
        frame = _build_audio_frame(payload)
        host_audio.send_bytes(frame)

        received = bob_audio.receive_bytes()
        assert received == frame, "Listener must receive the host's frame byte-identical"


def test_sender_does_not_receive_own_frame(client, routes_module):
    """Verify the relay never echoes a frame back to its sender. Uses a second
    player as a canary: if the sender were getting echoes, their FIRST inbound
    frame after sending frame_X would be frame_X. We instead expect frame_Y
    (sent by the other player AFTER) — proving sender's queue is empty until
    the other player speaks. NOTE: in this isolated relay test, both host
    AND bob are forced as the active broadcaster sequentially, since the
    Phase 1c single-broadcaster gate only allows the active broadcaster's
    frames through (test_broadcast_control covers the spec-correct flow)."""
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")) as host_audio, \
         client.websocket_connect(_audio_url(code, bob_pid, "bob-sid")) as bob_audio:

        frame_x = _build_audio_frame(b"\xAA" * 16, interval_index=1)
        frame_y = _build_audio_frame(b"\xBB" * 16, interval_index=2)

        # Phase 1: host is the broadcaster. Send frame_x → bob receives.
        _force_broadcaster(routes_module, code, host_pid)
        host_audio.send_bytes(frame_x)
        assert bob_audio.receive_bytes() == frame_x

        # Phase 2: bob takes over as broadcaster. Send frame_y → host
        # receives. If host had been getting echoes earlier, host's queue
        # would already contain frame_x and that would be the FIRST receive.
        _force_broadcaster(routes_module, code, bob_pid)
        bob_audio.send_bytes(frame_y)
        first_received_by_host = host_audio.receive_bytes()
        assert first_received_by_host == frame_y, (
            "Sender's first inbound frame must be the canary from the OTHER peer, "
            "not an echo of its own send. Got bytes that match frame_x echo."
        )


def test_audio_worker_cleaned_up_on_leave_room(client, routes_module):
    """Codex round 6 P2: leave_room pops the session before closing the
    audio_ws, which makes the handler's finally block see sess=None and
    skip its worker cancel. Without explicit cleanup in leave_room the
    worker stays parked on queue.get() forever."""
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")
    room = routes_module._rooms[code]

    bob_ws_holder = {}
    with client.websocket_connect(_audio_url(code, bob_pid, "bob-sid")) as bob_audio:
        bob_sess = room["sessions"][bob_pid]
        worker = bob_sess.get("audio_send_worker")
        assert worker is not None and not worker.done()
        bob_ws_holder["worker"] = worker

        # Explicit user-leave path.
        leave_resp = client.post(
            f"/api/plugins/multiplayer/rooms/{code}/leave",
            json={"player_id": bob_pid},
        )
        leave_resp.raise_for_status()

    # After leave_room the session is gone and the worker must be cancelled.
    assert bob_pid not in room["sessions"]
    leaked = bob_ws_holder["worker"]
    assert leaked.done() or leaked.cancelled(), (
        "leave_room must cancel the audio worker, otherwise it leaks "
        "parked on queue.get() forever per departed listener"
    )


def test_audio_worker_cleaned_up_on_grace_expiry(client, routes_module, fast_grace):
    """Codex round 6 P2: _grace_then_finalize_endpoint pops the session and
    closes the OTHER endpoint, but the now-dropped audio worker is also
    parked on queue.get(). It must be cancelled before pop, not relied on
    via the audio handler's finally (which early-returns on sess=None)."""
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")
    room = routes_module._rooms[code]

    # Open audio for bob, then ALSO open highway, then drop highway only.
    # Highway grace expires, finalization closes audio_ws with 4408. We need
    # the worker to be gone by the time finalization completes.
    with client.websocket_connect(_audio_url(code, bob_pid, "bob-sid")):
        bob_sess = room["sessions"][bob_pid]
        worker = bob_sess.get("audio_send_worker")
        assert worker is not None and not worker.done()

        with client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
            bob_hw.receive_json()
        # Highway closed; per-endpoint grace starts. Wait past the grace
        # window so _grace_then_finalize_endpoint runs.
        time.sleep(fast_grace * 2.5)

    # Session is gone, worker must be cancelled.
    assert bob_pid not in room["sessions"]
    assert worker.done() or worker.cancelled()


async def test_enqueue_audio_frame_bounded_queue(routes_module):
    """Direct unit test of `_enqueue_audio_frame`'s drop-oldest-on-overflow.

    The previous version of this test went through TestClient sockets and
    relied on `bob_audio.receive_bytes(timeout=...)` — but Starlette's
    WebSocketTestSession.receive_bytes() doesn't accept a timeout kwarg,
    so the call raised TypeError immediately and the broad `except` made
    the test pass vacuously regardless of bound enforcement. Codex
    correctly flagged that as a false positive.

    This unit test exercises the helper directly without any sockets:
    construct a bounded asyncio.Queue, push 3× capacity into it via
    `_enqueue_audio_frame`, and assert the queue retained exactly the
    most recent _AUDIO_SEND_QUEUE_MAX frames (drop-oldest semantics).
    """
    import asyncio

    sess = {
        "audio_send_queue": asyncio.Queue(maxsize=routes_module._AUDIO_SEND_QUEUE_MAX),
        "audio_send_worker": None,  # no worker draining; pure-bound test
    }
    max_frames = routes_module._AUDIO_SEND_QUEUE_MAX
    n_sent = max_frames * 3

    for i in range(n_sent):
        routes_module._enqueue_audio_frame(sess, f"frame-{i}".encode())

    queue = sess["audio_send_queue"]
    assert queue.qsize() == max_frames, (
        f"queue size {queue.qsize()} != _AUDIO_SEND_QUEUE_MAX={max_frames}; "
        f"_enqueue_audio_frame must cap the queue at the configured maximum"
    )

    # Drain and verify the retained frames are the MOST RECENT max_frames
    # sent (drop-OLDEST policy, not drop-newest).
    retained = []
    while not queue.empty():
        retained.append(queue.get_nowait())
    expected = [f"frame-{i}".encode() for i in range(n_sent - max_frames, n_sent)]
    assert retained == expected, (
        "drop-oldest policy violated: queue retained the wrong frames"
    )


@pytest.mark.timeout(5)
def test_host_read_loop_alive_under_load(client, routes_module):
    """Smoke test that HOST's /audio read loop stays alive when overflowing
    its peer's bounded queue. The previous version drove a marker via Bob's
    socket, but Bob's marker is forwarded by Bob's loop, so it would still
    arrive at host even if host's own loop were wedged in fan-out. This
    version sends a unique FINAL frame from HOST after the burst — bob
    can only see it if host's read loop is still pulling and dispatching.

    Bounded by @pytest.mark.timeout(5) so a real deadlock fails the test
    with a TimeoutError instead of hanging the entire pytest run."""
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")
    _force_broadcaster(routes_module, code, host_pid)

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")) as host_audio, \
         client.websocket_connect(_audio_url(code, bob_pid, "bob-sid")) as bob_audio:

        max_frames = routes_module._AUDIO_SEND_QUEUE_MAX
        n_sent = max_frames * 3

        # Burst — most of these will be overflowed and dropped from the queue.
        for i in range(n_sent):
            payload = f"FRAME-{i:04d}".encode().ljust(40, b".")
            host_audio.send_bytes(_build_audio_frame(payload, interval_index=i))

        # Final unique frame from HOST. Drop-OLDEST policy means this
        # newest frame DOES survive even if the queue overflowed.
        final = _build_audio_frame(b"FINAL!", interval_index=99999)
        host_audio.send_bytes(final)

        # Drain bob's queue until we see the FINAL frame. If host's read
        # loop wedged at any point, FINAL was never enqueued and this
        # blocks; pytest-timeout fires after 5s.
        for _ in range(n_sent + 5):
            frame = bob_audio.receive_bytes()
            if frame == final:
                return
        pytest.fail(
            f"FINAL frame from host never reached bob within {n_sent + 5} "
            f"reads; host's /audio read loop may be wedged"
        )


def test_non_broadcaster_audio_frames_are_dropped(client, routes_module):
    """Codex round 1 P2 (Phase 1c): the audio data plane MUST drop frames
    from any sender other than the active broadcaster. Without this
    enforcement a listener or buggy client could inject audio that other
    peers play, even though no broadcast_start was accepted for them."""
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")
    carol_pid = _join_room(client, code, "Carol")
    # Host is the active broadcaster.
    _force_broadcaster(routes_module, code, host_pid)

    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")) as host_audio, \
         client.websocket_connect(_audio_url(code, bob_pid, "bob-sid")) as bob_audio, \
         client.websocket_connect(_audio_url(code, carol_pid, "carol-sid")) as carol_audio:

        # Bob (NOT the broadcaster) sends an otherwise-valid SMAU frame.
        # Server must drop it — neither host nor carol should receive.
        rogue = _build_audio_frame(b"ROGUE!", interval_index=1)
        bob_audio.send_bytes(rogue)

        # Now host (the active broadcaster) sends a legit frame. We use it
        # as a deterministic terminator: host's frame MUST reach both
        # listeners, and it MUST be the FIRST audio they see (no rogue
        # frame ahead of it).
        legit = _build_audio_frame(b"LEGIT!", interval_index=2)
        host_audio.send_bytes(legit)

        first_carol = carol_audio.receive_bytes()
        assert first_carol == legit, (
            "Carol's first audio frame must be the broadcaster's legit "
            "frame, not bob's dropped rogue frame"
        )
        # Host doesn't receive their own frame (no echo).


def test_two_listeners_both_receive_host_frame(client, routes_module):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")
    carol_pid = _join_room(client, code, "Carol")
    _force_broadcaster(routes_module, code, host_pid)

    with client.websocket_connect(_audio_url(code, host_pid, "host")) as host_audio, \
         client.websocket_connect(_audio_url(code, bob_pid, "bob")) as bob_audio, \
         client.websocket_connect(_audio_url(code, carol_pid, "carol")) as carol_audio:

        payload = b"\xde\xad\xbe\xef" * 50
        frame = _build_audio_frame(payload, interval_index=42)
        host_audio.send_bytes(frame)

        assert bob_audio.receive_bytes() == frame
        assert carol_audio.receive_bytes() == frame


# ── Header validation ─────────────────────────────────────────────────────

def test_oversized_frame_closes_with_1009(client):
    code, pid = _create_room(client)
    with client.websocket_connect(_audio_url(code, pid, "sid")) as ws:
        # Build a frame whose total length > MAX_FRAME_BYTES (256 KB).
        big_payload = b"\x00" * (262145 - 40)  # header + payload = 262145
        frame = _build_audio_frame(big_payload)
        assert len(frame) > 262144
        ws.send_bytes(frame)
        with pytest.raises(WebSocketDisconnect) as excinfo:
            ws.receive_bytes()
        assert excinfo.value.code == 1009, (
            f"expected 1009 (Frame too big), got {excinfo.value.code}"
        )


def test_truncated_header_closes_with_1009(client):
    code, pid = _create_room(client)
    with client.websocket_connect(_audio_url(code, pid, "sid")) as ws:
        ws.send_bytes(b"\x00" * 39)  # one byte shy of the 40-byte header
        with pytest.raises(WebSocketDisconnect) as excinfo:
            ws.receive_bytes()
        assert excinfo.value.code == 1009


def test_header_body_mismatch_closes_with_1009(client):
    code, pid = _create_room(client)
    with client.websocket_connect(_audio_url(code, pid, "sid")) as ws:
        # Header claims opus_size=100 but we attach only 10 bytes.
        frame = _build_audio_frame(b"\x00" * 10, opus_size_override=100)
        ws.send_bytes(frame)
        with pytest.raises(WebSocketDisconnect) as excinfo:
            ws.receive_bytes()
        assert excinfo.value.code == 1009


def test_bad_magic_dropped_silently(client, routes_module):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")
    _force_broadcaster(routes_module, code, host_pid)

    with client.websocket_connect(_audio_url(code, host_pid, "host")) as host_audio, \
         client.websocket_connect(_audio_url(code, bob_pid, "bob")) as bob_audio:

        bad = _build_audio_frame(b"hello", magic=b"XXXX")
        good = _build_audio_frame(b"world")

        host_audio.send_bytes(bad)
        host_audio.send_bytes(good)

        # Bob should ONLY receive the good frame; the bad one was dropped.
        received = bob_audio.receive_bytes()
        assert received == good


def test_bad_version_dropped_silently(client, routes_module):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")
    _force_broadcaster(routes_module, code, host_pid)

    with client.websocket_connect(_audio_url(code, host_pid, "host")) as host_audio, \
         client.websocket_connect(_audio_url(code, bob_pid, "bob")) as bob_audio:

        bad = _build_audio_frame(b"hello", version=99)
        good = _build_audio_frame(b"world")

        host_audio.send_bytes(bad)
        host_audio.send_bytes(good)

        received = bob_audio.receive_bytes()
        assert received == good


def test_non_binary_frame_dropped_silently(client, routes_module):
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")
    _force_broadcaster(routes_module, code, host_pid)

    with client.websocket_connect(_audio_url(code, host_pid, "host")) as host_audio, \
         client.websocket_connect(_audio_url(code, bob_pid, "bob")) as bob_audio:

        # Send a TEXT frame on the audio WS — server must drop, not forward.
        host_audio.send_text('{"hello": "world"}')

        # Now send a valid binary frame; bob should receive only that.
        good = _build_audio_frame(b"opus")
        host_audio.send_bytes(good)

        received = bob_audio.receive_bytes()
        assert received == good


# ── Session integration with the audio slot ──────────────────────────────

def test_4410_on_audio_does_not_close_highway(client, routes_module):
    code, pid = _create_room(client)
    sid = "shared-sid"

    with client.websocket_connect(_highway_url(code, pid, sid)) as highway, \
         client.websocket_connect(_audio_url(code, pid, sid)) as audio_a:
        highway.receive_json()  # 'connected'

        # Open a SECOND audio WS with the same session_id → Rule 2 closes
        # the old audio slot with 4410, leaves the highway slot untouched.
        with client.websocket_connect(_audio_url(code, pid, sid)) as audio_b:
            with pytest.raises(WebSocketDisconnect) as excinfo:
                audio_a.receive_bytes()
            assert excinfo.value.code == 4410

            # Highway is still alive — exercise it.
            highway.send_json({"type": "set_arrangement", "arrangement": "Lead"})
            evt = highway.receive_json()
            assert evt["type"] == "arrangement_changed"

            rec = routes_module._rooms[code]["sessions"][pid]
            assert rec["highway_ws"] is not None
            assert rec["audio_ws"] is not None
            assert rec["session_id"] == sid


def test_4409_on_audio_closes_both_endpoints(client):
    code, pid = _create_room(client)

    with client.websocket_connect(_highway_url(code, pid, "tab-A")) as highway_a, \
         client.websocket_connect(_audio_url(code, pid, "tab-A")) as audio_a:
        highway_a.receive_json()  # 'connected'

        # New session_id arrives on the audio endpoint → Rule 3 takeover
        # closes BOTH the prior highway_ws and audio_ws with 4409.
        with client.websocket_connect(_audio_url(code, pid, "tab-B")):
            with pytest.raises(WebSocketDisconnect) as exc_h:
                highway_a.receive_json()
            assert exc_h.value.code == 4409

            with pytest.raises(WebSocketDisconnect) as exc_a:
                audio_a.receive_bytes()
            assert exc_a.value.code == 4409


def test_audio_first_then_highway_emits_player_connected(client):
    """Codex round 1 P2: audio-first connection ordering still has to fire
    player_connected to peers when the highway opens, otherwise the player
    is invisible on the control plane until a full room refresh."""
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw:
        host_hw.receive_json()  # 'connected'

        # Bob opens AUDIO first (no highway yet) — host should see no event
        # because the audio handler doesn't broadcast player_connected.
        with client.websocket_connect(_audio_url(code, bob_pid, "bob-sid")):
            # Bob then opens highway with the SAME session_id. This is now a
            # 'first_attach' on the highway slot, NOT a quiet 'reconnect',
            # so peers MUST see player_connected for the first time.
            with client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
                bob_hw.receive_json()  # 'connected'
                evt = host_hw.receive_json()
                assert evt["type"] == "player_connected"
                assert evt["player_id"] == bob_pid


def test_audio_only_session_keeps_room_alive(client, routes_module, fast_grace):
    """Codex round 1 P2: audio-only attach (no highway open) must cancel any
    pending 60s room cleanup — otherwise an /audio reconnect that happens
    before /ws during recovery would silently lose the room."""
    code, pid = _create_room(client)
    sid = "audio-only-sid"

    # Set up the cleanup-pending state: connect highway, then disconnect AND
    # wait past the per-endpoint grace so the session ends and _start_cleanup
    # queues the 60-second room destruction.
    with client.websocket_connect(_highway_url(code, pid, sid)) as hw:
        hw.receive_json()
    time.sleep(fast_grace * 2.5)
    # _grace_then_finalize ran: connected=False, cleanup queued.
    assert code in routes_module._cleanup_tasks
    assert not routes_module._rooms[code]["players"][pid]["connected"]

    # Now reconnect audio-only with a fresh session_id (the prior session
    # ended). The audio handler must cancel the pending cleanup so the room
    # survives.
    with client.websocket_connect(_audio_url(code, pid, "fresh-sid")):
        assert code not in routes_module._cleanup_tasks, (
            "audio attach must cancel pending room cleanup"
        )
        assert routes_module._rooms[code]["players"][pid]["connected"], (
            "audio attach must mark the player as connected so _connected_count "
            "keeps the room alive"
        )


def test_highway_reconnect_during_grace_does_not_emit_player_connected(client, fast_grace):
    """Codex round 2 P2: a same-session_id highway reconnect that lands
    while the OLD highway is in its grace window (audio still alive) is a
    'reconnect', NOT a 'first_attach'. Peers must NOT see a fresh
    player_connected — the player never went offline."""
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    with client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw:
        host_hw.receive_json()  # 'connected'

        # Bob attaches BOTH highway and audio; host sees player_connected once.
        with client.websocket_connect(_audio_url(code, bob_pid, "bob-sid")):
            with client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw_1:
                bob_hw_1.receive_json()
                evt = host_hw.receive_json()
                assert evt["type"] == "player_connected"
                assert evt["player_id"] == bob_pid
            # Bob's highway closed (context exit). Audio still open.
            # Within grace, bob's highway reconnects with the SAME session_id.
            with client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw_2:
                bob_hw_2.receive_json()
                # Drive a deterministic event from bob_hw_2 the server forwards
                # to host: set_arrangement → arrangement_changed. If this branch
                # mistakenly fires another player_connected, host would see it
                # FIRST, before arrangement_changed.
                bob_hw_2.send_json({"type": "set_arrangement", "arrangement": "Bass"})
                evt2 = host_hw.receive_json()
                assert evt2["type"] == "arrangement_changed", (
                    f"highway reconnect during grace must not emit a fresh "
                    f"player_connected; got {evt2['type']!r}"
                )


def test_promote_host_skips_audio_only_players(client, routes_module, fast_grace):
    """Codex round 2 P2: _promote_host must skip players whose highway WS
    isn't currently active — they cannot receive host_changed or send
    host-only commands. Otherwise leaving the host strands the room."""
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")
    carol_pid = _join_room(client, code, "Carol")
    room = routes_module._rooms[code]

    with client.websocket_connect(_highway_url(code, carol_pid, "carol-sid")) as carol_hw, \
         client.websocket_connect(_audio_url(code, bob_pid, "bob-sid")):
        carol_hw.receive_json()  # 'connected'

        # Bob attaches highway then drops it → highway slot None, audio alive.
        # During the grace window, bob is "connected" for room-liveness purposes
        # but his player["ws"] is None.
        with client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
            bob_hw.receive_json()
            carol_hw.receive_json()  # player_connected for bob (drain)
        # bob_hw closed; audio still open. Verify the audio-only state.
        assert room["players"][bob_pid]["connected"] is True
        assert room["players"][bob_pid]["ws"] is None

        # Simulate the host being removed so _promote_host has to pick from
        # {bob (audio-only), carol (highway-active)}. Drop the host's record
        # directly to avoid the leave_room HTTP path's other side effects.
        del room["players"][host_pid]
        room["sessions"].pop(host_pid, None)

        new_host = routes_module._promote_host(room)
        assert new_host == carol_pid, (
            f"_promote_host must skip audio-only players "
            f"(bob_pid={bob_pid}, carol_pid={carol_pid}); got {new_host}"
        )


def test_host_self_heal_when_audio_only_player_reattaches_highway(client, routes_module):
    """Codex round 3 P1: when leave_room runs while every remaining player is
    audio-only (highway grace), _promote_host returns None and room["host"]
    stays pointing at the departed player. The next highway attach must
    self-heal by re-running the election and broadcasting host_changed."""
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")
    room = routes_module._rooms[code]

    # Host opens highway; Bob opens audio only (no highway → audio-only state).
    with client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw, \
         client.websocket_connect(_audio_url(code, bob_pid, "bob-sid")):
        host_hw.receive_json()  # 'connected'

        # Host leaves while Bob is still audio-only. _promote_host returns None
        # because no remaining player has an active highway WS.
        leave_resp = client.post(
            f"/api/plugins/multiplayer/rooms/{code}/leave",
            json={"player_id": host_pid},
        )
        leave_resp.raise_for_status()
        # Stale host id remains; this is the bug condition.
        assert room["host"] == host_pid
        assert host_pid not in room["players"]

    # Now Bob opens highway. The handler must detect the stale host and
    # promote Bob, broadcasting host_changed (Bob is the only listener at this
    # point; he sees his own elevation via the 'connected' room snapshot OR
    # via host_changed).
    with client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
        # Drain initial 'connected'.
        first = bob_hw.receive_json()
        assert first["type"] == "connected"
        # The 'connected' room snapshot already reflects the new host.
        assert first["room"]["host"] == bob_pid, (
            f"highway attach must self-heal a stranded host slot; "
            f"snapshot still says {first['room']['host']!r}"
        )
        assert room["host"] == bob_pid


@pytest.fixture
def fast_creator_grace(routes_module):
    """Shrink the creator-grace window so abandoned-creator tests run quickly."""
    routes_module.HOST_CREATOR_GRACE_SEC = 0.3
    yield 0.3


def test_audio_only_creator_does_not_set_ever_attached(client, routes_module, fast_creator_grace):
    """Codex round 4 P2: a creator who opens ONLY /audio (never /ws) must NOT
    flip ever_attached. The host self-heal logic uses ever_attached to mean
    "showed up on the control plane"; audio alone is irrelevant for host
    duties. Without this gate, an audio-only creator would be considered
    'attached' and the abandoned-creator timer wouldn't fire."""
    code, host_pid = _create_room(client)
    other_pid = _join_room(client, code, "Bob")
    room = routes_module._rooms[code]

    # Creator opens ONLY /audio, never /ws.
    with client.websocket_connect(_audio_url(code, host_pid, "host-sid")), \
         client.websocket_connect(_highway_url(code, other_pid, "bob-sid")) as bob_hw:
        bob_hw.receive_json()
        # Despite the audio attach, ever_attached must still be False on the
        # host record — the abandoned-creator semantics only care about
        # control-plane presence.
        assert room["players"][host_pid]["ever_attached"] is False, (
            "audio-only attach must NOT flip ever_attached; that flag is "
            "reserved for highway WS attach (control-plane presence)"
        )

        # Wait past the (shrunk) creator-grace window. The timer must fire
        # because the creator is still considered abandoned (no control-plane
        # WS), and bob is the eligible guest.
        time.sleep(fast_creator_grace * 2.5)
        evt = bob_hw.receive_json()
        assert evt["type"] == "host_changed"
        assert evt["new_host_id"] == other_pid


def test_abandoned_creator_grace_fires_when_guest_already_connected(client, routes_module, fast_creator_grace):
    """Codex round 6 P1: abandoned-creator self-heal must also fire on a
    timer, not just on a subsequent highway attach. A guest who connected
    BEFORE creator-grace expired and stays connected would otherwise be
    stuck without host privileges forever."""
    code, host_pid = _create_room(client)
    bob_pid = _join_room(client, code, "Bob")

    # Bob connects highway BEFORE creator-grace expires.
    with client.websocket_connect(_highway_url(code, bob_pid, "bob-sid")) as bob_hw:
        first = bob_hw.receive_json()
        assert first["type"] == "connected"
        # At this point creator hasn't connected and grace hasn't fired yet.
        assert first["room"]["host"] == host_pid

        # Wait past the (shrunk) creator-grace window. Timer fires, _promote_host
        # picks Bob (highway-active), broadcasts host_changed.
        time.sleep(fast_creator_grace * 2.5)
        evt = bob_hw.receive_json()
        assert evt["type"] == "host_changed"
        assert evt["new_host_id"] == bob_pid


def test_abandoned_creator_releases_host_after_creator_grace(client, routes_module, fast_creator_grace):
    """Codex round 5 P1: a creator who never opens any WS must eventually
    release the host slot to a connected guest. Without a time-bound on
    the never-attached protection, the room stays permanently hostless
    when the creator closes the tab without connecting."""
    code, host_pid = _create_room(client)
    other_pid = _join_room(client, code, "Bob")
    room = routes_module._rooms[code]

    # Wait past the (shrunk) creator-grace window.
    time.sleep(fast_creator_grace * 2.0)

    # Bob attaches highway. With creator-grace expired, self-heal must fire.
    with client.websocket_connect(_highway_url(code, other_pid, "bob-sid")) as bob_hw:
        snapshot = bob_hw.receive_json()
        assert snapshot["type"] == "connected"
        assert snapshot["room"]["host"] == other_pid, (
            f"abandoned creator (never attached, room older than grace) must "
            f"release host to the connected guest; got {snapshot['room']['host']!r}, "
            f"expected {other_pid!r}"
        )
        assert room["host"] == other_pid


def test_never_connected_creator_keeps_host_when_guest_attaches_first(client, routes_module):
    """Codex round 4 P2: a brand-new room has the creator as host with no
    session yet — they haven't opened /ws. If a guest beats them to a
    highway connect, the self-heal must NOT promote the guest. Only a
    host who has ever attached and then expired qualifies as 'truly gone'."""
    code, host_pid = _create_room(client)
    other_pid = _join_room(client, code, "Bob")
    room = routes_module._rooms[code]

    # Host hasn't connected. ever_attached is False.
    assert room["players"][host_pid]["ever_attached"] is False
    assert host_pid not in room["sessions"]
    assert room["host"] == host_pid

    # Bob (guest) attaches highway BEFORE the creator. Self-heal must NOT
    # fire — host is a never-attached creator, not a stale tombstone.
    with client.websocket_connect(_highway_url(code, other_pid, "bob-sid")) as bob_hw:
        snapshot = bob_hw.receive_json()
        assert snapshot["type"] == "connected"
        assert snapshot["room"]["host"] == host_pid, (
            f"a never-connected creator must keep the host slot when a guest "
            f"attaches first; snapshot says {snapshot['room']['host']!r}, "
            f"expected {host_pid!r}"
        )
        assert room["host"] == host_pid


def test_host_self_heal_after_session_grace_expiry(client, routes_module, fast_grace):
    """Codex round 4 P1: when the host fully disconnects long enough for the
    grace timer to fire, _grace_then_finalize_endpoint pops their session
    and flips connected=False but leaves the player record in
    room["players"]. The next other-player highway attach must self-heal
    the host slot — checking only "is the player record present" would
    leave the room stuck with a tombstoned host forever."""
    code, host_pid = _create_room(client)
    other_pid = _join_room(client, code, "Bob")
    room = routes_module._rooms[code]

    # Host attaches highway, then drops it. Audio never opens.
    with client.websocket_connect(_highway_url(code, host_pid, "host-sid")) as host_hw:
        host_hw.receive_json()
    # Wait past per-endpoint grace so the session is finalized.
    time.sleep(fast_grace * 2.5)

    # Post-grace state: player record still there, session gone, connected=False.
    assert host_pid in room["players"]
    assert host_pid not in room["sessions"]
    assert room["players"][host_pid]["connected"] is False
    # Tombstoned room["host"] still points at the gone player.
    assert room["host"] == host_pid

    # Bob attaches highway → self-heal must promote Bob.
    with client.websocket_connect(_highway_url(code, other_pid, "bob-sid")) as bob_hw:
        snapshot = bob_hw.receive_json()
        assert snapshot["type"] == "connected"
        assert snapshot["room"]["host"] == other_pid, (
            f"highway attach must self-heal a stranded host slot after the "
            f"prior host's session expired; snapshot says host is "
            f"{snapshot['room']['host']!r} (expected {other_pid!r})"
        )
        assert room["host"] == other_pid


def test_host_grace_window_does_not_steal_host_to_other_player(client, routes_module):
    """Codex round 3 P1: while the host is in a per-endpoint grace window
    (highway dropped, audio still alive), another player attaching their
    highway MUST NOT steal host privileges. The host's player record is
    still present — they're transiently disconnected, not gone."""
    code, host_pid = _create_room(client)
    other_pid = _join_room(client, code, "Bob")
    room = routes_module._rooms[code]

    sid = "host-sid"
    # Host opens highway+audio. Then drops highway only (audio still alive).
    with client.websocket_connect(_audio_url(code, host_pid, sid)):
        with client.websocket_connect(_highway_url(code, host_pid, sid)) as host_hw:
            host_hw.receive_json()  # 'connected'
        # Host's highway closed inside the with block. Audio still alive.
        # Host is in grace window: player record present, ws=None, connected=True.
        host_record = room["players"][host_pid]
        assert host_record["connected"] is True
        assert host_record["ws"] is None
        assert room["host"] == host_pid

        # Bob attaches highway. The self-heal logic MUST notice that the host
        # is still in the players dict (just transiently in grace) and NOT
        # promote bob.
        with client.websocket_connect(_highway_url(code, other_pid, "bob-sid")) as bob_hw:
            snapshot = bob_hw.receive_json()
            assert snapshot["type"] == "connected"
            assert snapshot["room"]["host"] == host_pid, (
                f"host must NOT be transferred away during a grace window; "
                f"snapshot says new host is {snapshot['room']['host']!r} but "
                f"original host {host_pid!r} is still in players"
            )
            assert room["host"] == host_pid


def test_audio_grace_expiry_closes_highway_with_4408(client, routes_module, fast_grace):
    code, pid = _create_room(client)
    sid = "joint-sid"

    with client.websocket_connect(_highway_url(code, pid, sid)) as highway:
        highway.receive_json()  # 'connected'

        with client.websocket_connect(_audio_url(code, pid, sid)):
            pass  # audio WS opens then closes immediately when context exits

        # Audio slot grace expires → entire session ends, highway closed with 4408.
        time.sleep(fast_grace * 2.5)

        with pytest.raises(WebSocketDisconnect) as excinfo:
            highway.receive_json()
        assert excinfo.value.code == 4408

    # Session record gone.
    assert pid not in routes_module._rooms[code]["sessions"]
