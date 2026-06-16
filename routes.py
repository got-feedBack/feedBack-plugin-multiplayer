"""Multiplayer plugin — synced rooms, shared queue, optional mixdown."""

import asyncio
import logging
import math
import re
import secrets
import shutil
import subprocess
import time
from pathlib import Path

from fastapi import WebSocket, WebSocketDisconnect, UploadFile, File, Form
from fastapi.responses import FileResponse, JSONResponse


_CODE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"  # 30 chars, no 0/O/1/I/L

_rooms = {}           # code -> room dict
_cleanup_tasks = {}   # code -> asyncio.Task
_MP_DIR = None        # set by setup(); used by module-level _cleanup_after_grace

_log = logging.getLogger("slopsmith.plugin.multiplayer")

# Session lifecycle constants — see PROTOCOL.md "v1 server policy".

SESSION_GRACE_SEC = 5.0
# Cap per-peer cleanup time during a broadcaster handoff. A backpressured
# or stuck ws.send_bytes on one slow listener should not gate the whole
# room's broadcast_start; if cleanup takes longer than this we move on
# and let the client-side handoff_suppress window catch any stragglers.
_PURGE_TIMEOUT_SEC = 0.5
# How long a creator who has never opened any WS keeps the host slot before
# self-heal will transfer it to the first connected guest. Long enough to
# absorb a normal page-load + WS-handshake (typically 1–3s); short enough that
# an abandoned room (creator closed the tab without connecting) doesn't stay
# permanently hostless once a guest arrives.
HOST_CREATOR_GRACE_SEC = 30.0
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
# Lifecycle close codes; values track PROTOCOL.md.
_CLOSE_AUTH_FAIL = 4401
_CLOSE_GRACE_EXPIRED = 4408
_CLOSE_SUPERSEDED = 4409
_CLOSE_REPLACED = 4410
_CLOSE_FRAME_TOO_BIG = 1009
_CLOSE_NORMAL = 1000
_CLOSE_INTERNAL_ERROR = 1011

# Audio WS frame format constants — see PROTOCOL.md "Audio frame format".
_AUDIO_FRAME_MAGIC = b"SMAU"
_AUDIO_FRAME_VERSION = 1
_AUDIO_FRAME_HEADER_LEN = 40
_AUDIO_FRAME_MAX_BYTES = 262144  # 256 KB hard limit (header + payload)
# Maximum pending audio frames per listener. New frames overwrite the oldest
# when full, so a chronically slow listener stays bounded in memory and a
# resumed listener can only fall a fixed number of frames behind. At a
# typical interval of ~2 s and 96 kbps Opus, 8 frames ≈ 16 s of headroom.
_AUDIO_SEND_QUEUE_MAX = 8

# v1 broadcast control-plane validation (PROTOCOL.md "Receiver validation /
# Control-plane validation"). broadcast_start parameters MUST conform to
# these or the server rejects the request before flipping broadcaster_id.
_V1_SAMPLE_RATE = 48000
_V1_CHANNEL_COUNT = 1
_V1_CODEC = "opus"
_V1_BITRATE_MIN = 16000
_V1_BITRATE_MAX = 256000


def _gen_code():
    for _ in range(100):
        code = "".join(secrets.choice(_CODE_ALPHABET) for _ in range(6))
        if code not in _rooms:
            return code
    raise RuntimeError("Failed to generate unique room code")


def _gen_player_id():
    return secrets.token_hex(8)


def _serialize_room(room):
    """Return room state safe for JSON (no WebSocket refs)."""
    players = {}
    for pid, p in room["players"].items():
        players[pid] = {
            "name": p["name"],
            "arrangement": p["arrangement"],
            "connected": p["connected"],
        }
    return {
        "code": room["code"],
        "host": room["host"],
        "players": players,
        "queue": room["queue"],
        "now_playing": room["now_playing"],
        "state": room["state"],
        "time": room["time"],
        "speed": room["speed"],
        "recording": room["recording"],
        "recordings_received": list(room["recordings"].keys()),
        # Broadcast control plane (PROTOCOL.md). Late joiners see the
        # active broadcaster in their initial 'connected' snapshot rather
        # than waiting for a fresh broadcaster_changed event.
        "broadcaster_id": room.get("broadcaster_id"),
        "broadcast_params": room.get("broadcast_params"),
    }


async def _broadcast(room, msg, exclude=None):
    """Send JSON message to all connected players in a room."""
    for pid, p in room["players"].items():
        if pid == exclude:
            continue
        ws = p.get("ws")
        if ws and p["connected"]:
            try:
                await ws.send_json(msg)
            except Exception:
                pass


def _connected_count(room):
    return sum(1 for p in room["players"].values() if p["connected"])


def _promote_host(room):
    """Promote the first player with an active highway WS to host.

    Audio-only "connected" players (player["connected"] is True for room
    liveness, but player["ws"] is None because their highway WS hasn't
    reattached yet) are NOT eligible — the host needs a control plane to
    receive `host_changed` and to send host-only commands. Promoting an
    audio-only player would leave the room without a usable host until
    that client's highway socket comes back. Falling back to "any
    connected player" is a no-op vs. returning None: the room genuinely
    has no usable host until at least one highway WS exists.
    """
    for pid, p in room["players"].items():
        if p["connected"] and p.get("ws") is not None:
            room["host"] = pid
            return pid
    return None


# ── Session lifecycle (PROTOCOL.md "v1 server policy") ─────────────────

def _is_valid_session_id(sid):
    return bool(sid) and bool(_SESSION_ID_RE.match(sid))


def _validate_broadcast_params(data):
    """Apply PROTOCOL.md "Control-plane validation" to a broadcast_start
    payload. Returns a normalized params dict on success, or a string
    describing the failure reason. v1 only accepts opus / 48 kHz / mono /
    bitrate in [16_000, 256_000] bits-per-second (i.e. 16–256 kbps);
    future versions may relax these."""
    sample_rate = data.get("sample_rate")
    if sample_rate != _V1_SAMPLE_RATE:
        return f"sample_rate must be {_V1_SAMPLE_RATE} (got {sample_rate!r})"
    channel_count = data.get("channel_count")
    if channel_count != _V1_CHANNEL_COUNT:
        return f"channel_count must be {_V1_CHANNEL_COUNT} (got {channel_count!r})"
    codec = data.get("codec")
    if codec != _V1_CODEC:
        return f"codec must be {_V1_CODEC!r} (got {codec!r})"
    bitrate = data.get("bitrate")
    if (
        not isinstance(bitrate, int)
        or isinstance(bitrate, bool)
        or bitrate < _V1_BITRATE_MIN
        or bitrate > _V1_BITRATE_MAX
    ):
        return (
            f"bitrate must be an int in [{_V1_BITRATE_MIN}, {_V1_BITRATE_MAX}] "
            f"(got {bitrate!r})"
        )
    interval_beats = data.get("interval_beats")
    if not isinstance(interval_beats, int) or isinstance(interval_beats, bool) or interval_beats <= 0:
        return f"interval_beats must be a positive int (got {interval_beats!r})"
    return {
        "interval_beats": interval_beats,
        "sample_rate": sample_rate,
        "channel_count": channel_count,
        "codec": codec,
        "bitrate": bitrate,
    }


async def _clear_broadcaster_if_player(room, player_id):
    """If `player_id` is the current room broadcaster, clear the slot and
    broadcast `broadcaster_changed: null` to all peers. No-op otherwise.

    Called from every session-end path that could leave a tombstoned
    broadcaster_id while peers remain in the room: grace expiry,
    Rule 3 takeover, leave_room. Not called from `_cleanup_after_grace`
    (room destroy) because that path only runs when `_connected_count`
    is zero — the room dict is dropped along with broadcaster_id and
    there are no peers left to notify, so the broadcast event would be
    a no-op. Idempotent and safe to call when broadcaster_id is None.
    """
    if room.get("broadcaster_id") != player_id:
        return
    room["broadcaster_id"] = None
    room["broadcast_params"] = None
    await _broadcast(room, {
        "type": "broadcaster_changed",
        "broadcaster_id": None,
    })
    # Deliberately DO NOT flush per-listener audio queues here. PROTOCOL.md
    # "Lifecycle §5 Stop" says listeners should drain pending intervals on
    # stop — slow listeners may still have a few buffered frames behind
    # broadcaster_changed:null and we want those to play out. Mis-
    # attribution on rapid handoff is handled in the broadcast_start
    # path (which does flush + worker restart) instead.


async def _safe_close(ws, code):
    """Close a WebSocket without raising. Used for server-initiated session closes."""
    if ws is None:
        return
    try:
        await ws.close(code=code)
    except Exception:
        pass


def _new_session_record(session_id, highway_ws=None, audio_ws=None):
    return {
        "session_id": session_id,
        "highway_ws": highway_ws,
        "audio_ws": audio_ws,
        "highway_grace_task": None,
        "audio_grace_task": None,
        # Sticky flags: True the first time the slot is ever filled within
        # this session, never reset. Used to distinguish "first_attach"
        # (slot never opened) from "reconnect" (slot was open before, just
        # temporarily None during a grace window).
        "highway_ever_opened": highway_ws is not None,
        "audio_ever_opened": audio_ws is not None,
        # Per-peer audio relay state. The queue is bounded
        # (_AUDIO_SEND_QUEUE_MAX) so a slow listener can't accumulate
        # unbounded memory. The worker is a single asyncio.Task that drains
        # the queue and serializes send_bytes (ASGI's "one sender at a time"
        # rule). Both are bound to a specific audio_ws — same-session
        # reconnects cancel the prior worker + drop the prior queue and
        # spawn a fresh pair, so stale sends never block a new attached
        # socket. Both are None whenever audio_ws is None (no slot, or
        # currently in per-endpoint grace).
        "audio_send_queue": None,
        "audio_send_worker": None,
    }


def _cancel_grace_tasks(sess):
    for k in ("highway_grace_task", "audio_grace_task"):
        t = sess.get(k)
        if t and not t.done():
            t.cancel()
        sess[k] = None


_HIGHWAY = "highway"
_AUDIO = "audio"


def _other_endpoint(endpoint):
    return _AUDIO if endpoint == _HIGHWAY else _HIGHWAY


def _slot_key(endpoint):
    return f"{endpoint}_ws"


def _grace_key(endpoint):
    return f"{endpoint}_grace_task"


async def _take_session_slot(websocket, room, player_id, session_id, endpoint):
    """Apply PROTOCOL.md connection-arrival rules for the given endpoint
    (`_HIGHWAY` or `_AUDIO`). Returns one of:

      'new_session'  — no prior session existed; this endpoint started it.
      'first_attach' — prior session existed (other endpoint already open) but
                       THIS slot was empty until now. Distinct from 'reconnect'
                       so callers (the highway handler in particular) can fire
                       a peer-visible event when the player only becomes
                       visible via this endpoint for the first time.
      'reconnect'    — THIS slot had an existing socket that the new connection
                       supersedes (4410 emitted to the old socket).
      'takeover'     — prior session with a DIFFERENT session_id; closes BOTH
                       slots of the old session with 4409.

    The new websocket is registered in the room's session record on success.
    """
    sessions = room["sessions"]
    existing = sessions.get(player_id)
    slot = _slot_key(endpoint)
    grace = _grace_key(endpoint)

    if existing is None:
        rec = _new_session_record(session_id)
        rec[slot] = websocket
        rec[f"{endpoint}_ever_opened"] = True
        sessions[player_id] = rec
        if endpoint == _AUDIO:
            _setup_audio_worker(rec)
        return "new_session"

    if existing["session_id"] == session_id:
        # Rule 2: matching session_id. Three sub-cases distinguished by the
        # sticky `<endpoint>_ever_opened` flag plus the current slot value:
        #   * never opened before → 'first_attach' (peers should see the
        #     player come online for the first time on the highway side).
        #   * opened before, slot currently filled → 'reconnect' with 4410
        #     to the old socket (slot was live; we replaced it).
        #   * opened before, slot currently None → 'reconnect' silent
        #     (slot dropped during a grace window and is reattaching).
        ever_key = f"{endpoint}_ever_opened"
        ever_opened_before = existing.get(ever_key, False)
        old_ws = existing[slot]
        # Snapshot the prior audio worker BEFORE _setup_audio_worker
        # overwrites the slot, so the audio-side close below can defer
        # until the cancelled worker has actually exited (avoids the
        # close-vs-send_bytes race on the same WebSocket).
        prev_audio_worker = (
            existing.get("audio_send_worker") if endpoint == _AUDIO else None
        )
        existing[slot] = websocket
        existing[ever_key] = True
        # Reattach cancels any in-flight grace timer for this endpoint only.
        t = existing.get(grace)
        if t and not t.done():
            t.cancel()
        existing[grace] = None
        # Audio reconnect/first_attach gets a fresh worker + queue bound to
        # the new ws so any tasks left over from the old ws can't block the
        # newly-attached socket. _setup_audio_worker cancels prev_audio_worker
        # but does NOT await its exit (cancel only requests; the worker may
        # still be inside ws.send_bytes on old_ws).
        if endpoint == _AUDIO:
            _setup_audio_worker(existing)
        if not ever_opened_before:
            return "first_attach"
        if old_ws is not None and old_ws is not websocket:
            if endpoint == _AUDIO and prev_audio_worker is not None:
                # Fire-and-forget the close: _close_audio_after_worker waits
                # for prev_audio_worker to actually exit, then closes old_ws
                # with 4410. This is the sole sender on old_ws by the time
                # close fires.
                asyncio.create_task(
                    _close_audio_after_worker(
                        old_ws, prev_audio_worker, _CLOSE_REPLACED,
                    )
                )
            else:
                await _safe_close(old_ws, _CLOSE_REPLACED)
        return "reconnect"

    # Rule 3: takeover by a different session_id — close BOTH old endpoints.
    # Replace the session record FIRST, then close the old sockets. Each
    # `await _safe_close(...)` yields to the event loop, which lets the OLD
    # endpoint handlers' finally blocks run and call _on_endpoint_disconnect.
    # Those helpers check `sess.get(slot) is websocket` against the CURRENT
    # session record — so by the time they run, sessions[player_id] is the
    # new record and they early-return instead of scheduling a grace task on
    # the orphaned `existing` dict.
    _cancel_grace_tasks(existing)
    old_highway_ws = existing.get("highway_ws")
    old_audio_ws = existing.get("audio_ws")
    # Swap the session record FIRST, BEFORE awaiting the old worker's
    # shutdown. _cleanup_audio_worker below yields to the event loop; if
    # sessions[player_id] still pointed at the OLD record during that yield,
    # an old endpoint handler's finally block could schedule a grace task
    # on the soon-to-be-replaced record, and that timer would later resolve
    # sessions[player_id] to the NEW record and close it with 4408 before
    # its other endpoint attached. Swapping first makes any concurrent
    # finally see sessions[player_id] = rec, fail its `is websocket (old)`
    # check, and early-return cleanly.
    old_existing = existing
    rec = _new_session_record(session_id)
    rec[slot] = websocket
    rec[f"{endpoint}_ever_opened"] = True
    sessions[player_id] = rec
    if endpoint == _AUDIO:
        _setup_audio_worker(rec)
    # Clear the player's stale highway-WS reference SYNCHRONOUSLY (before any
    # await below). After rule 3 the new session's highway_ws is None until
    # the new tab's highway WS connects; leaving player["ws"] pointing at the
    # just-superseded old highway during the awaits below would create a
    # split-routing window: audio fan-out uses sessions[player_id] = rec
    # (new audio_ws) while _broadcast still targets the OLD highway. The
    # highway-side caller of _take_session_slot immediately reassigns
    # player["ws"] = websocket after this returns, so for the highway
    # endpoint this is a no-op; for the audio endpoint it closes the gap.
    player = room["players"].get(player_id)
    if player is not None:
        player["ws"] = None
    # Cancel the OLD session's worker. Operates on a detached old_existing
    # reference (sessions[player_id] no longer points at it), so the
    # _cleanup_audio_worker reference-check at the end of its body simply
    # confirms old_existing.audio_send_worker is still its own captured
    # value and clears the slot on the OLD dict — never touches the new.
    await _cleanup_audio_worker(old_existing)
    await _safe_close(old_highway_ws, _CLOSE_SUPERSEDED)
    await _safe_close(old_audio_ws, _CLOSE_SUPERSEDED)
    # If the OLD session was the active broadcaster, end the broadcast.
    # The new tab is conceptually the same player_id but a different
    # session, so it must explicitly re-issue broadcast_start to resume —
    # otherwise listeners would receive frames from a session that
    # never announced itself as the broadcaster.
    await _clear_broadcaster_if_player(room, player_id)
    return "takeover"


async def _on_endpoint_disconnect(websocket, room, player_id, endpoint):
    """Called from a WS handler's finally block.

    Distinguishes "this socket is still the active slot for `endpoint` on
    this session" (start grace timer) from "the server already replaced this
    socket via 4409/4410" (no further action).
    """
    sess = room.get("sessions", {}).get(player_id)
    if sess is None or sess.get(_slot_key(endpoint)) is not websocket:
        # Server-initiated close already swapped this slot; nothing to do.
        return
    sess[_slot_key(endpoint)] = None
    # If the highway slot just emptied, also clear the player's WS reference so
    # _broadcast skips the dead socket during the grace window. The audio slot
    # has no equivalent on the player object, so nothing to clear there — but
    # the audio send worker IS bound to the now-closed ws, so cancel it and
    # drop the queue. A future reattach of the same session will spawn a
    # fresh worker + queue via _setup_audio_worker.
    if endpoint == _HIGHWAY:
        player = room["players"].get(player_id)
        if player is not None:
            player["ws"] = None
    elif endpoint == _AUDIO:
        await _cleanup_audio_worker(sess, room, player_id)
    # Schedule per-endpoint grace expiry. Either endpoint expiring ends the
    # whole session per PROTOCOL.md "Per-endpoint grace".
    sess[_grace_key(endpoint)] = asyncio.create_task(
        _grace_then_finalize_endpoint(room, player_id, endpoint)
    )


# Backward-compat alias retained for any in-flight callers; new call sites
# should use _on_endpoint_disconnect directly.
async def _on_highway_disconnect(websocket, room, player_id):
    await _on_endpoint_disconnect(websocket, room, player_id, _HIGHWAY)


async def _close_audio_after_worker(ws, worker, code):
    """Wait for `worker` to actually exit, THEN close `ws` with `code`.

    Used by paths that need to close a WebSocket whose worker has been
    cancelled but may not have fully exited yet (e.g. the Rule 2 audio
    reconnect path closing old_ws with 4410). The cancelled worker may
    still be inside an in-flight ws.send_bytes when we abandon it;
    closing the same ws concurrently would race with that send (ASGI
    single-sender rule). This helper runs as a fire-and-forget task: it
    awaits the worker to completion (best effort) and only then closes,
    so the close is the sole sender.
    """
    if worker is not None:
        try:
            await worker
        except (asyncio.CancelledError, Exception):
            pass
    await _safe_close(ws, code)


async def _restart_audio_worker_after_old(room, player_id, expected_sess, old_worker):
    """Wait for `old_worker` to exit, then spawn a fresh worker on
    `expected_sess` IFF the room still references that exact session
    dict for `player_id`. Used by _cleanup_audio_worker's cancel-
    propagation branch.

    The room/player_id re-lookup matters: existing teardown paths
    (Rule 3 takeover, _grace_then_finalize_endpoint, leave_room) drop
    the session dict from `room["sessions"]` without nulling
    `sess["audio_ws"]` first. If we restarted blindly on the detached
    dict, we'd call _setup_audio_worker on a closed/orphaned audio_ws,
    leaking the new task and the audio_ws+session refs along with it.
    Comparing `room["sessions"][player_id] is expected_sess` confirms
    the session is still authoritative before we touch it.
    """
    if old_worker is not None:
        try:
            await old_worker
        except (asyncio.CancelledError, Exception):
            pass
    if room.get("sessions", {}).get(player_id) is not expected_sess:
        return
    if expected_sess.get("audio_ws") is None:
        return
    # _cleanup_audio_worker's cancel-propagation branch deliberately
    # leaves audio_send_worker pointing at the cancelled-but-now-exited
    # old_worker so concurrent rule-2 audio reconnects could snapshot it
    # for their deferred close. By the time we get here that snapshot
    # window is over (we just awaited the worker to completion), so it
    # is safe — and necessary — to replace the slot with a fresh worker.
    # If a concurrent reattach via _setup_audio_worker has already
    # installed a different worker, leave it alone (the new worker is
    # already running on the same audio_ws).
    current_worker = expected_sess.get("audio_send_worker")
    if current_worker is None or current_worker is old_worker:
        _setup_audio_worker(expected_sess)


async def _audio_send_worker(ws, queue):
    """Drain a per-peer audio send queue, calling ws.send_bytes for each frame.

    Exits cleanly on:
      - asyncio.CancelledError (e.g. socket replaced via 4410, or session
        teardown), or
      - any send_bytes exception (broken socket; the peer's own handler
        cleans up the rest).
    Each worker is bound to the exact ws passed in here, so a same-session
    reconnect that spawns a fresh worker + queue pair never has the old
    worker contending with the new socket.
    """
    try:
        while True:
            payload = await queue.get()
            try:
                await ws.send_bytes(payload)
            except Exception:
                return
    except asyncio.CancelledError:
        return


async def _cleanup_audio_worker(sess, room=None, player_id=None):
    """Cancel the audio worker (if any), wait for it to actually exit, and
    clear the queue — but only if the slot still references the worker we
    captured. Safe to call on a session that has no worker (no-op).

    `room` and `player_id` are optional. They are used only on the cancel-
    propagation branch (external cancel of our caller) to safely schedule
    a background restart of the audio worker if the session is still
    authoritative. Callers who can't provide them (or for which a restart
    isn't desired) may omit them — the cancel-propagation branch then
    falls through without scheduling a restart.

    Why async / awaits the cancelled task: cancel() only REQUESTS
    cancellation. If the worker is currently inside `await ws.send_bytes(...)`
    when we cancel it, the next caller of `audio_ws.close(...)` would race
    against the still-running send and the close can fail silently (ASGI
    forbids concurrent senders on the same WebSocket). Awaiting the
    cancelled task here guarantees the worker has fully exited before we
    return.

    Why we capture the worker reference and re-check before clobbering:
    the `await` below yields to the event loop. During that yield, a
    reattach via `_take_session_slot` can install a FRESH worker into
    `sess["audio_send_worker"]`. If we unconditionally cleared
    `sess["audio_send_worker"] = None` on resume, we'd silently drop the
    new worker and the reattached client would be muted. Re-checking that
    the slot still points at our captured worker preserves a fresh worker
    installed concurrently.

    Why we re-raise CancelledError when worker.cancelled() is False: if
    OUR task is being cancelled externally (e.g. `_grace_then_finalize_endpoint`
    is cancelled by a same-session reconnect while we're awaiting worker
    teardown), `await worker` raises CancelledError but the worker may
    have completed normally. We must propagate that cancellation so the
    grace timer doesn't continue popping the session.
    """
    worker = sess.get("audio_send_worker")
    if worker is None:
        return
    if worker.done():
        if sess.get("audio_send_worker") is worker:
            sess["audio_send_worker"] = None
            sess["audio_send_queue"] = None
        return
    worker.cancel()
    # Distinguish "worker cancelled" from "caller cancelled" cleanly:
    # asyncio.gather(worker, return_exceptions=True) captures the worker's
    # CancelledError as a return value rather than re-raising it. The
    # only way `await asyncio.gather(...)` itself raises CancelledError
    # is if OUR task is being cancelled externally — at which point we
    # MUST propagate. This avoids the Py-version-specific
    # Task.cancelling() check entirely and works correctly on Python
    # 3.8+. (Suggested by Copilot during PR #3 round 12 review.)
    try:
        await asyncio.gather(worker, return_exceptions=True)
    except asyncio.CancelledError:
        # OUR caller was cancelled. We MUST NOT restart the worker here
        # even if audio_ws is still alive: the old worker may not have
        # fully exited from ws.send_bytes yet (we abandoned the gather),
        # and starting a fresh worker on the same ws would have two
        # concurrent senders, violating ASGI's one-sender-at-a-time rule
        # and corrupting the relay.
        #
        # Synchronously clear the queue so fan-out immediately skips
        # this peer, but leave the existing /audio WebSocket open. If
        # the cancelled worker is still the active one, we only schedule
        # a deferred worker restart that waits for that worker to
        # actually exit before replacing it; we do not close the same
        # ws here. Closing would emit a misleading close code (the
        # session is typically still alive — our cleanup was interrupted
        # by an external cancel like a same-session reconnect of the
        # OTHER endpoint, not by a real teardown), so we let the relay
        # self-heal silently.
        #
        # All slot mutations (worker, queue) are gated on our captured
        # worker still being the active one. If a same-session reattach
        # has run during our await, _setup_audio_worker synchronously
        # installed a fresh worker AND a fresh audio_ws in the same
        # atomic block — `is worker` returns False, and we leave the
        # new state untouched (otherwise we'd orphan its new worker).
        if sess.get("audio_send_worker") is worker:
            # Clear the QUEUE so fan-out skips this peer (no pushes to a
            # queue with no live consumer), but leave audio_send_worker
            # pointing at the cancelled worker so concurrent rule-2
            # audio reconnects in _take_session_slot can still snapshot
            # `prev_audio_worker = sess["audio_send_worker"]` and route
            # their old-ws close through _close_audio_after_worker —
            # which awaits the cancelled worker before close, avoiding
            # the ASGI single-sender race. The audio_send_worker slot
            # itself will be replaced by either:
            #   (a) the deferred restart helper below, after the
            #       cancelled worker has actually exited, or
            #   (b) a concurrent reattach via _setup_audio_worker.
            sess["audio_send_queue"] = None
            if (
                room is not None
                and player_id is not None
                and sess.get("audio_ws") is not None
            ):
                asyncio.create_task(
                    _restart_audio_worker_after_old(
                        room, player_id, sess, worker,
                    )
                )
        raise
    # Normal completion path: gather returned (worker exited, possibly
    # via the cancellation we requested). Clear the slots if our worker
    # is still what's there.
    if sess.get("audio_send_worker") is worker:
        sess["audio_send_worker"] = None
        sess["audio_send_queue"] = None


def _setup_audio_worker(sess):
    """Spawn a fresh queue + worker bound to sess['audio_ws']. Cancels and
    drops any prior worker/queue so stale sends on a replaced socket can't
    block the new socket.

    Caller must have already set sess['audio_ws'] to the new WebSocket.
    """
    prev_worker = sess.get("audio_send_worker")
    if prev_worker is not None and not prev_worker.done():
        prev_worker.cancel()
    queue = asyncio.Queue(maxsize=_AUDIO_SEND_QUEUE_MAX)
    sess["audio_send_queue"] = queue
    sess["audio_send_worker"] = asyncio.create_task(
        _audio_send_worker(sess["audio_ws"], queue)
    )


async def _force_close_stale_audio_ws(ws):
    """Close an audio WebSocket whose worker is stuck on a non-cancellable
    `ws.send_bytes`. Used by the broadcaster-handoff purge timeout path:
    waiting for the stuck worker before closing (via
    `_close_audio_after_worker`) would just block on the same send_bytes.

    ASGI normally forbids concurrent senders on a WebSocket. In this case
    the existing sender is permanently stuck (otherwise we wouldn't be
    here), so the violation is moot — close() either races and unsticks
    the send (best case) or raises and we ignore it (the peer's
    connection is already broken; their reconnect logic will recover).
    """
    try:
        await ws.close(code=1011)
    except Exception:
        pass


async def _purge_audio_for_handoff(room, except_player_id=None):
    """Cancel in-flight audio sends + restart per-listener workers on a
    broadcaster handoff so the next broadcaster's frames cannot be tail-
    mixed with the previous broadcaster's still-buffered or in-flight
    frames.

    Called from the `broadcast_start` success path when a broadcaster
    transition happens (None → new, or — after `_clear_broadcaster_if_player`
    has just cleared the slot — null → new). The per-peer queue + worker
    pair is replaced atomically:

      1. `_cleanup_audio_worker` cancels the running worker and AWAITS
         its actual exit (so any `await ws.send_bytes(payload)` already
         in flight either completes or raises CancelledError before we
         continue — see the worker's docstring).
      2. `_setup_audio_worker` then spawns a fresh empty queue + new
         worker bound to the same audio_ws.

    Step 1 drops anything still in the old queue and stops the next
    iteration; step 2 gives fan-out a clean slate. A frame that was
    already handed off to the OS TCP buffer via send_bytes between the
    cancel request and the worker's await resolution can still arrive
    at the listener — the listener's epoch invalidation (bumped on
    control_set) silences it post-decode.

    `except_player_id` skips the broadcaster's own queue (no self-loop;
    the server's single-broadcaster gate already drops their frames
    upstream of fan-out).
    """
    # Snapshot to a list BEFORE awaiting: _cleanup_audio_worker yields
    # to the event loop, and another task (player reconnect, leave,
    # grace expiry) could mutate room["sessions"] mid-iteration. A live
    # dict iterator would then raise RuntimeError("dictionary changed
    # size during iteration") and abort broadcast_start mid-handoff,
    # leaving the room without an active broadcaster.
    sessions_map = room.get("sessions") or {}
    snapshot = [(pid, sess) for pid, sess in sessions_map.items() if pid != except_player_id]
    if not snapshot:
        return

    async def _purge_one(pid, sess):
        # Capture the original audio_ws BEFORE awaiting cleanup so the
        # timeout branch closes the SAME socket that timed out, not
        # whatever the listener may have reattached during our 500 ms
        # wait. A same-session reconnect via _take_session_slot can
        # replace sess["audio_ws"] with a fresh socket while we're
        # awaiting; we must not tear that fresh socket down.
        old_audio_ws = sess.get("audio_ws")
        if old_audio_ws is None:
            return
        try:
            # Bound per-peer cleanup so a single backpressured / stuck
            # ws.send_bytes can't block the entire handoff. Deliberately
            # do NOT pass room/player_id here. When wait_for() cancels
            # _cleanup_audio_worker, its cancel-propagation branch only
            # schedules a deferred worker restart when given those
            # arguments. We don't want a restart in the timeout case —
            # the timeout branch below force-closes the stale ws, and
            # a concurrent restart would race against that close
            # (recreating a worker on a socket about to be torn down,
            # or fanning out new-broadcaster frames over the stale
            # connection). The slow peer's clean recovery path is
            # reconnect, not restart.
            await asyncio.wait_for(
                _cleanup_audio_worker(sess),
                timeout=_PURGE_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            # Slow peer's send_bytes is wedged. Force-close the
            # CAPTURED ws WITHOUT waiting for the stuck worker to
            # exit (using _close_audio_after_worker would just block
            # on the same stuck send_bytes the cleanup did). The
            # captured `old_audio_ws` reference is the stuck socket
            # specifically — even if the listener has since reattached
            # a replacement (sess["audio_ws"] != old_audio_ws), the
            # OLD ws still needs to be closed: a same-session
            # reattach in _take_session_slot already scheduled a
            # _close_audio_after_worker(old_audio_ws, prev_worker,
            # 4410) that's blocked on the same stuck send, so the
            # old ws/worker would be leaked indefinitely. Closing
            # `old_audio_ws` here unblocks that path. We never
            # touch sess["audio_ws"] (which may be a fresh
            # replacement we'd corrupt by closing).
            asyncio.create_task(_force_close_stale_audio_ws(old_audio_ws))
            # If this session slot still points at the stale ws, detach
            # it instead of spawning a replacement worker on the same
            # socket. wait_for() only cancelled cleanup; it did NOT
            # prove the old worker exited (send_bytes is the thing
            # blocking it). Restarting here could create two concurrent
            # senders on one WebSocket — an ASGI single-sender
            # violation. With audio_ws cleared the fan-out skips this
            # peer cleanly; the peer recovers via reconnect (the
            # _force_close_stale_audio_ws task will deliver the close
            # event whenever the stuck send finally yields). Spotted
            # by Copilot review on PR #7.
            if sess.get("audio_ws") is old_audio_ws:
                sess["audio_ws"] = None
            return
        # Re-verify that this session is still the authoritative one
        # for `pid` before restarting. Between cleanup awaits, a
        # Rule-3 takeover or a leave could have replaced or removed
        # the session record, leaving our captured `sess` orphaned
        # with a detached audio_ws. Restarting a worker on the
        # orphan would leak a task + ws ref.
        current_sess = (room.get("sessions") or {}).get(pid)
        if current_sess is not sess:
            return
        if (
            sess.get("audio_ws") is not None
            and sess.get("audio_send_worker") is None
            and sess.get("audio_send_queue") is None
        ):
            _setup_audio_worker(sess)

    # Parallelize so handoff latency is bounded by the slowest single
    # peer (capped by _PURGE_TIMEOUT_SEC), not the SUM of all peers'
    # cleanup latencies. Surface unexpected per-peer failures via
    # logging — return_exceptions=True swallows them by design, but
    # silent failure here can leave that listener on a stale worker
    # without anyone noticing. Spotted by Copilot review on PR #7.
    results = await asyncio.gather(
        *(_purge_one(pid, sess) for pid, sess in snapshot),
        return_exceptions=True,
    )
    for (pid, _sess), result in zip(snapshot, results):
        if isinstance(result, BaseException):
            _log.warning(
                "audio handoff purge raised for player %s: %s: %s",
                pid, type(result).__name__, result,
            )


def _enqueue_audio_frame(sess, payload):
    """Push a frame onto a peer's bounded send queue. On overflow the OLDEST
    frame is dropped — newer audio is more useful to a recovering listener
    than stale audio. Caller must have already verified audio_ws is non-None.
    """
    queue = sess.get("audio_send_queue")
    if queue is None:
        return
    while True:
        try:
            queue.put_nowait(payload)
            return
        except asyncio.QueueFull:
            try:
                queue.get_nowait()  # drop oldest
            except asyncio.QueueEmpty:
                # Race with a worker that drained between full and get_nowait.
                # Retry put.
                continue


def _classify_audio_frame(data):
    """Validate the 40-byte SMAU header on an audio WS binary frame.

    Returns one of:
      "ok"            — frame passes server-side safety checks; safe to fan out.
      "size_violation"  — frame_length < 40, > MAX, or header/body mismatch.
                          Caller MUST drop and SHOULD close the sender's
                          audio WS with 1009 per PROTOCOL.md.
      "drop"          — magic mismatch or version mismatch. Caller MUST drop
                          (no close — this is a benign protocol-version skew
                          that the spec lets the receiver handle silently).
    """
    if data is None:
        return "drop"
    n = len(data)
    if n > _AUDIO_FRAME_MAX_BYTES or n < _AUDIO_FRAME_HEADER_LEN:
        return "size_violation"
    if data[:4] != _AUDIO_FRAME_MAGIC:
        return "drop"
    # Bytes 4-5: u16 little-endian version.
    version = data[4] | (data[5] << 8)
    if version != _AUDIO_FRAME_VERSION:
        return "drop"
    # Bytes 36-39: u32 little-endian opus_size; total frame must equal header + payload.
    opus_size = data[36] | (data[37] << 8) | (data[38] << 16) | (data[39] << 24)
    if _AUDIO_FRAME_HEADER_LEN + opus_size != n:
        return "size_violation"
    return "ok"


def _start_cleanup(code):
    """Start the 60-second grace period before destroying a room.

    Module-level (not inside setup()) so module-level coroutines like
    `_grace_then_finalize_endpoint` can reach it without a NameError when the
    only-player-just-disconnected branch fires.
    """
    # `_rooms` is canonically keyed by uppercase code (see _gen_code() and the
    # `code = code.upper()` normalization at every WS handler entry). Normalize
    # here too so a stray lower-case caller can't park the cleanup task under a
    # case-mismatched key, which would later look up _rooms[lowercase] → None
    # and leak the room dict + room-dir files.
    code = code.upper()
    if code in _cleanup_tasks:
        return
    _cleanup_tasks[code] = asyncio.ensure_future(_cleanup_after_grace(code))


async def _cleanup_after_grace(code, seconds=60):
    code = code.upper()
    await asyncio.sleep(seconds)
    room = _rooms.get(code)
    if room and _connected_count(room) == 0:
        # Cancel any session-grace tasks AND audio workers before discarding
        # the room so they don't fire / stay parked against a deleted room
        # dict. Snapshot the values list FIRST: _cleanup_audio_worker awaits
        # the worker's exit, which yields to the event loop. Other tasks
        # (grace finalizers, leave paths) can pop entries from
        # room["sessions"] during that yield, which would raise
        # RuntimeError: dictionary changed size during iteration if we were
        # iterating the live view.
        sessions_snapshot = list(room.get("sessions", {}).values())
        for sess in sessions_snapshot:
            _cancel_grace_tasks(sess)
            await _cleanup_audio_worker(sess)
        # Same for the one-shot creator-grace timer.
        cgt = room.get("creator_grace_task")
        if cgt and not cgt.done():
            cgt.cancel()
        del _rooms[code]
        if _MP_DIR is not None:
            room_dir = _MP_DIR / code
            if room_dir.exists():
                shutil.rmtree(str(room_dir), ignore_errors=True)
        _log.info("Room %s destroyed after grace period", code)
    _cleanup_tasks.pop(code, None)


async def _creator_grace_timer(code, creator_pid):
    """Fires HOST_CREATOR_GRACE_SEC after a room is created. If the creator
    has not opened the highway / control-plane WebSocket by then (their
    `ever_attached` flag stays False — audio-only attaches intentionally
    do not flip it because audio alone can't perform host duties) AND
    another player is connected, transfer the host slot to the first
    eligible connected guest. This is the time-driven half of the
    abandoned-creator self-heal — the highway-attach branch in
    multiplayer_ws covers the case where the guest arrives AFTER grace,
    this timer covers the case where the guest is already connected
    when grace expires."""
    try:
        await asyncio.sleep(HOST_CREATOR_GRACE_SEC)
    except asyncio.CancelledError:
        return
    room = _rooms.get(code)
    if room is None:
        return
    if room.get("host") != creator_pid:
        return  # Host changed by some other path; nothing to do.
    creator = room["players"].get(creator_pid)
    if creator is None or creator.get("ever_attached"):
        return  # Creator left or actually showed up.
    new_host = _promote_host(room)
    if new_host is not None and new_host != creator_pid:
        await _broadcast(room, {
            "type": "host_changed",
            "new_host_id": new_host,
        })


async def _grace_then_finalize_endpoint(room, player_id, endpoint):
    """Wait SESSION_GRACE_SEC; if `endpoint`'s slot is still empty, end the
    entire session (close the OTHER endpoint with 4408 if it's still alive,
    drop the session record, fire player_disconnected, run room cleanup if
    appropriate)."""
    try:
        await asyncio.sleep(SESSION_GRACE_SEC)
    except asyncio.CancelledError:
        return

    sessions = room.get("sessions", {})
    sess = sessions.get(player_id)
    if sess is None:
        return
    if sess.get(_slot_key(endpoint)) is not None:
        # Reattached during the grace window.
        return

    # Grace expired with no reattach: end the session. Cancel + AWAIT the
    # audio worker BEFORE the close below so the worker isn't still mid-
    # send_bytes when we close the same ws (ASGI requires one sender at a
    # time; otherwise the close races with an in-flight send and may fail
    # silently). This also prevents the worker leak the audio handler's
    # finally would skip on sess-is-None early return.
    # Pass room/player_id so a cancel-propagation (a same-session
    # endpoint reconnect cancelling this grace task) can schedule a
    # background restart of the audio worker — the session is alive in
    # that case and audio shouldn't go silently mute.
    await _cleanup_audio_worker(sess, room, player_id)
    other = _slot_key(_other_endpoint(endpoint))
    other_ws = sess.get(other)
    sessions.pop(player_id, None)
    await _safe_close(other_ws, _CLOSE_GRACE_EXPIRED)

    player = room["players"].get(player_id)
    if player is not None:
        player["connected"] = False
        player["ws"] = None

    await _broadcast(room, {"type": "player_disconnected", "player_id": player_id})

    # If this player was the active broadcaster, the session ending means
    # the broadcast also ended. Tell peers so they can tear down their
    # listener pipelines.
    await _clear_broadcaster_if_player(room, player_id)

    if _connected_count(room) == 0:
        _start_cleanup(room["code"])


def setup(app, context):
    config_dir = context["config_dir"]
    STATIC_DIR = Path(__file__).resolve().parent.parent.parent / "static"
    MP_DIR = config_dir / "multiplayer"
    # Make MP_DIR reachable from module-level coroutines (e.g.
    # _cleanup_after_grace runs the room-destroy path, which deletes files
    # from this dir). Stored at module level so it survives across the
    # closures here.
    global _MP_DIR
    _MP_DIR = MP_DIR
    _get_dlc_dir = context.get("get_dlc_dir")

    # ── Room CRUD ──────────────────────────────────────────────────────

    @app.post("/api/plugins/multiplayer/rooms")
    async def create_room(data: dict):
        name = (data.get("name") or "").strip() or "Player"
        code = _gen_code()
        player_id = _gen_player_id()
        room = {
            "code": code,
            "host": player_id,
            "created_at": time.monotonic(),
            "players": {
                player_id: {
                    "name": name,
                    "arrangement": "Lead",
                    "connected": False,
                    "ws": None,
                    "last_seen": time.monotonic(),
                    # Sticky flag: True the first time this player opens the
                    # highway/control-plane WS. Audio-only WS attachment
                    # does NOT set this — the host self-heal cares about
                    # control-plane presence (sending host commands /
                    # receiving host_changed), which only the highway WS
                    # provides. Used to distinguish "creator hasn't
                    # connected yet" (never_attached, keep host) from
                    # "host connected once and then expired"
                    # (attached_then_gone, self-heal).
                    "ever_attached": False,
                }
            },
            "queue": [],
            "now_playing": -1,
            "state": "stopped",
            "time": 0.0,
            "speed": 1.0,
            "recording": False,
            "recordings": {},
            "mixdown_path": None,
            "skip_votes": set(),
            "sessions": {},
            "creator_grace_task": None,
            # Broadcast control plane: who is currently broadcasting audio
            # (player_id) and the codec parameters they advertised in
            # broadcast_start. Both None when no one is broadcasting.
            "broadcaster_id": None,
            "broadcast_params": None,
        }
        _rooms[code] = room
        # Schedule the one-shot creator-grace timer. If the creator never
        # opens any WS within HOST_CREATOR_GRACE_SEC, this transfers host
        # to the first connected guest. See _creator_grace_timer.
        room["creator_grace_task"] = asyncio.create_task(
            _creator_grace_timer(code, player_id)
        )
        return {"code": code, "player_id": player_id, "is_host": True}

    @app.post("/api/plugins/multiplayer/rooms/{code}/join")
    async def join_room(code: str, data: dict):
        room = _rooms.get(code.upper())
        if not room:
            return JSONResponse({"error": "Room not found"}, 404)
        name = (data.get("name") or "").strip() or "Player"
        player_id = _gen_player_id()
        room["players"][player_id] = {
            "name": name,
            "arrangement": "Lead",
            "connected": False,
            "ws": None,
            "last_seen": time.monotonic(),
            "ever_attached": False,
        }
        await _broadcast(room, {
            "type": "player_joined",
            "player_id": player_id,
            "name": name,
            "arrangement": "Lead",
        })
        return {
            "code": room["code"],
            "player_id": player_id,
            "is_host": False,
            "room": _serialize_room(room),
        }

    @app.get("/api/plugins/multiplayer/rooms/{code}")
    async def get_room(code: str):
        room = _rooms.get(code.upper())
        if not room:
            return JSONResponse({"error": "Room not found"}, 404)
        return _serialize_room(room)

    @app.post("/api/plugins/multiplayer/rooms/{code}/leave")
    async def leave_room(code: str, data: dict):
        room = _rooms.get(code.upper())
        if not room:
            return JSONResponse({"error": "Room not found"}, 404)
        player_id = data.get("player_id", "")
        if player_id not in room["players"]:
            return JSONResponse({"error": "Player not in room"}, 404)

        # Close WebSocket if connected
        ws = room["players"][player_id].get("ws")
        if ws:
            try:
                await ws.close()
            except Exception:
                pass

        # Drop the session record + cancel any in-flight grace timers for this player.
        # User-initiated leave uses RFC 6455 1000 (normal closure) on the audio
        # slot — 4408 means "grace expired / timed out" and would mislead a
        # client into auto-reconnecting with a fresh session_id when in fact
        # the user has explicitly left. Also tear down the audio worker
        # explicitly: closing the audio_ws makes the handler's finally block
        # see `sess is None` (we just popped) and early-return without
        # cancelling the worker, leaking it.
        sess = room.get("sessions", {}).pop(player_id, None)
        if sess is not None:
            _cancel_grace_tasks(sess)
            # Await worker shutdown BEFORE close to avoid the race where the
            # worker's in-flight send_bytes overlaps with our close.
            await _cleanup_audio_worker(sess)
            await _safe_close(sess.get("audio_ws"), _CLOSE_NORMAL)

        del room["players"][player_id]
        await _broadcast(room, {"type": "player_left", "player_id": player_id})

        # If the leaving player was the broadcaster, end the broadcast so
        # peers tear down their listener pipelines.
        await _clear_broadcaster_if_player(room, player_id)

        # Promote host if needed
        if room["host"] == player_id and room["players"]:
            new_host = _promote_host(room)
            if new_host:
                await _broadcast(room, {"type": "host_changed", "new_host_id": new_host})

        # Check if room is empty
        if not room["players"]:
            _start_cleanup(code)

        return {"ok": True}

    # ── Queue ──────────────────────────────────────────────────────────

    @app.post("/api/plugins/multiplayer/rooms/{code}/queue")
    async def add_to_queue(code: str, data: dict):
        room = _rooms.get(code.upper())
        if not room:
            return JSONResponse({"error": "Room not found"}, 404)
        item = {
            "filename": data.get("filename", ""),
            "title": data.get("title", ""),
            "artist": data.get("artist", ""),
            "added_by": data.get("player_id", ""),
            "arrangements": data.get("arrangements", []),
        }
        room["queue"].append(item)
        await _broadcast(room, {
            "type": "queue_updated",
            "queue": room["queue"],
            "now_playing": room["now_playing"],
        })
        return {"ok": True}

    @app.delete("/api/plugins/multiplayer/rooms/{code}/queue/{index}")
    async def remove_from_queue(code: str, index: int, player_id: str = ""):
        room = _rooms.get(code.upper())
        if not room:
            return JSONResponse({"error": "Room not found"}, 404)
        if index < 0 or index >= len(room["queue"]):
            return JSONResponse({"error": "Invalid index"}, 400)
        room["queue"].pop(index)
        # Adjust now_playing
        if room["now_playing"] >= len(room["queue"]):
            room["now_playing"] = len(room["queue"]) - 1
        elif index < room["now_playing"]:
            room["now_playing"] -= 1
        await _broadcast(room, {
            "type": "queue_updated",
            "queue": room["queue"],
            "now_playing": room["now_playing"],
        })
        return {"ok": True}

    @app.post("/api/plugins/multiplayer/rooms/{code}/queue/reorder")
    async def reorder_queue(code: str, data: dict):
        room = _rooms.get(code.upper())
        if not room:
            return JSONResponse({"error": "Room not found"}, 404)
        indices = data.get("indices", [])
        if sorted(indices) != list(range(len(room["queue"]))):
            return JSONResponse({"error": "Invalid indices"}, 400)
        room["queue"] = [room["queue"][i] for i in indices]
        await _broadcast(room, {
            "type": "queue_updated",
            "queue": room["queue"],
            "now_playing": room["now_playing"],
        })
        return {"ok": True}

    @app.post("/api/plugins/multiplayer/rooms/{code}/vote-skip")
    async def vote_skip(code: str, data: dict):
        room = _rooms.get(code.upper())
        if not room:
            return JSONResponse({"error": "Room not found"}, 404)
        player_id = data.get("player_id", "")
        room["skip_votes"].add(player_id)
        connected = _connected_count(room)
        needed = max(1, math.ceil(connected / 2))
        votes = len(room["skip_votes"])

        if votes >= needed:
            # Skip to next song
            room["skip_votes"] = set()
            await _advance_song(room)
        else:
            await _broadcast(room, {
                "type": "vote_skip",
                "votes": votes,
                "needed": needed,
                "voter": player_id,
            })
        return {"ok": True, "votes": votes, "needed": needed}

    # ── Recording Upload + Mixdown ─────────────────────────────────────

    @app.post("/api/plugins/multiplayer/rooms/{code}/upload")
    async def upload_recording(
        code: str,
        file: UploadFile = File(...),
        player_id: str = Form(""),
        start_server_time: str = Form("0"),
    ):
        room = _rooms.get(code.upper())
        if not room:
            return JSONResponse({"error": "Room not found"}, 404)
        if player_id not in room["players"]:
            return JSONResponse({"error": "Player not in room"}, 404)

        try:
            rec_start_ms = float(start_server_time)
        except (ValueError, TypeError):
            rec_start_ms = 0.0

        # Save raw upload
        room_dir = MP_DIR / code
        room_dir.mkdir(parents=True, exist_ok=True)

        ext = Path(file.filename).suffix if file.filename else ".webm"
        if ext not in (".wav", ".webm", ".ogg", ".mp3"):
            ext = ".webm"
        raw_path = room_dir / f"{player_id}_raw{ext}"
        content = await file.read()
        raw_path.write_bytes(content)

        # Convert to WAV
        wav_path = room_dir / f"{player_id}.wav"
        try:
            result = subprocess.run(
                ["ffmpeg", "-y", "-i", str(raw_path), "-ar", "48000", str(wav_path)],
                capture_output=True, text=True, timeout=120,
            )
            if result.returncode == 0 and wav_path.exists():
                raw_path.unlink(missing_ok=True)
                _log.debug("Converted recording for %s: %s", player_id, wav_path)
            else:
                _log.warning("WAV conversion failed: %s", result.stderr[-300:])
                wav_path = raw_path
        except Exception as e:
            _log.warning("WAV conversion error: %s", e)
            wav_path = raw_path

        room["recordings"][player_id] = {
            "path": str(wav_path),
            "start_ms": rec_start_ms,
        }

        await _broadcast(room, {
            "type": "recording_uploaded",
            "player_id": player_id,
            "total_uploads": len(room["recordings"]),
            "total_players": _connected_count(room),
        })
        return {"ok": True}

    @app.post("/api/plugins/multiplayer/rooms/{code}/mixdown")
    async def trigger_mixdown(code: str, data: dict):
        room = _rooms.get(code.upper())
        if not room:
            return JSONResponse({"error": "Room not found"}, 404)
        if data.get("player_id") != room["host"]:
            return JSONResponse({"error": "Only the host can trigger mixdown"}, 403)
        if not room["recordings"]:
            return JSONResponse({"error": "No recordings to mix"}, 400)

        include_stems = data.get("include_stems")       # list of stem IDs, or None for all
        include_recordings = data.get("include_recordings")  # list of player IDs, or None for all
        track_offsets = data.get("track_offsets", {})  # {track_id: offset_ms} from mixer
        track_volumes = data.get("track_volumes", {})  # {track_id: 0.0-1.0} from mixer
        track_mutes = data.get("track_mutes", [])      # [track_id, ...] muted tracks

        # Find the song audio files from the DLC directory
        song_audio_files = []  # list of (stem_id, path) tuples
        if 0 <= room["now_playing"] < len(room["queue"]):
            queue_item = room["queue"][room["now_playing"]]
            filename = queue_item.get("filename", "")
            if filename and callable(_get_dlc_dir):
                dlc = _get_dlc_dir()
                if dlc:
                    song_path = dlc / filename
                    if song_path.exists():
                        try:
                            import sloppak as sloppak_mod
                            if sloppak_mod.is_sloppak(song_path):
                                cache_dir = context.get("get_sloppak_cache_dir", lambda: None)()
                                source_dir = sloppak_mod.resolve_source_dir(filename, dlc, cache_dir)
                                manifest = sloppak_mod.load_manifest(song_path)
                                # Collect all stems as separate inputs
                                stems = manifest.get("stems", []) or []
                                for s in stems:
                                    if isinstance(s, dict) and s.get("file"):
                                        sid = str(s.get("id", ""))
                                        if include_stems is not None and sid not in include_stems:
                                            continue
                                        stem_path = source_dir / s["file"]
                                        if stem_path.exists():
                                            song_audio_files.append((sid, str(stem_path)))
                                # Fall back to main audio if no stems
                                if not song_audio_files:
                                    audio_file = manifest.get("audio", "")
                                    if audio_file:
                                        audio_path = source_dir / audio_file
                                        if audio_path.exists():
                                            song_audio_files.append(("mix", str(audio_path)))
                        except Exception:
                            pass
                        # Non-sloppak: check audio cache
                        if not song_audio_files:
                            cached = STATIC_DIR / "audio_cache"
                            if cached.is_dir():
                                stem_name = song_path.stem
                                for ext in (".ogg", ".mp3", ".wav"):
                                    cached_file = cached / f"{stem_name}{ext}"
                                    if cached_file.exists():
                                        song_audio_files.append(("mix", str(cached_file)))
                                        break

        room_dir = MP_DIR / code
        room_dir.mkdir(parents=True, exist_ok=True)

        # Build FFmpeg command
        inputs = []
        filter_parts = []
        idx = 0

        # Add selected song audio tracks as inputs (with mixer offsets/volumes)
        for sid, audio_path in song_audio_files:
            if sid in track_mutes:
                continue
            offset_ms = max(0, round(track_offsets.get(sid, 0)))
            vol = track_volumes.get(sid, 1.0)
            inputs.extend(["-i", audio_path])
            filt = f"[{idx}:a]aformat=sample_rates=48000:channel_layouts=stereo"
            if offset_ms > 0:
                filt += f",adelay={offset_ms}|{offset_ms}"
            if abs(vol - 1.0) > 0.01:
                filt += f",volume={vol:.3f}"
            filt += f"[a{idx}]"
            filter_parts.append(filt)
            idx += 1

        # Collect player recordings
        rec_entries = []
        for pid, rec_info in room["recordings"].items():
            if include_recordings is not None and pid not in include_recordings:
                continue
            rec_path = rec_info["path"] if isinstance(rec_info, dict) else rec_info
            start_ms = rec_info.get("start_ms", 0) if isinstance(rec_info, dict) else 0
            if Path(rec_path).exists():
                rec_entries.append((pid, rec_path, start_ms))

        # Add player recordings with mixer offsets (falls back to auto start_ms)
        for pid, rec_path, _start_ms in rec_entries:
            track_id = f"rec_{pid}"
            if track_id in track_mutes:
                continue
            offset_ms = max(0, round(track_offsets.get(track_id, _start_ms)))
            vol = track_volumes.get(track_id, 1.0)
            inputs.extend(["-i", rec_path])
            filt = f"[{idx}:a]aformat=sample_rates=48000:channel_layouts=stereo"
            if offset_ms > 0:
                filt += f",adelay={offset_ms}|{offset_ms}"
            if abs(vol - 1.0) > 0.01:
                filt += f",volume={vol:.3f}"
            filt += f"[a{idx}]"
            filter_parts.append(filt)
            idx += 1

        if idx == 0:
            return JSONResponse({"error": "No audio tracks available"}, 400)

        # Build amix filter — loudnorm each input to -16 LUFS so stems
        # and recordings sit at comparable levels before mixing.
        norm_parts = []
        for i in range(idx):
            norm_parts.append(
                f"[a{i}]loudnorm=I=-16:TP=-1:LRA=11[n{i}]"
            )
        mix_inputs = "".join(f"[n{i}]" for i in range(idx))
        filter_complex = "; ".join(filter_parts + norm_parts)
        filter_complex += (
            f"; {mix_inputs}amix=inputs={idx}:duration=longest:normalize=1"
            f",alimiter=limit=1:attack=3:release=50[out]"
        )

        output_path = room_dir / "mixdown.mp3"
        cmd = ["ffmpeg", "-y"] + inputs + [
            "-filter_complex", filter_complex,
            "-map", "[out]",
            "-b:a", "192k",
            str(output_path),
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                _log.warning("Mixdown failed: %s", result.stderr[-500:])
                return JSONResponse({"error": "Mixdown failed"}, 500)
        except Exception as e:
            _log.warning("Mixdown error: %s", e)
            return JSONResponse({"error": str(e)}, 500)

        room["mixdown_path"] = str(output_path)
        await _broadcast(room, {
            "type": "mixdown_ready",
            "url": f"/api/plugins/multiplayer/rooms/{code}/mixdown",
        })
        return {"ok": True, "url": f"/api/plugins/multiplayer/rooms/{code}/mixdown"}

    @app.get("/api/plugins/multiplayer/rooms/{code}/mixdown")
    async def download_mixdown(code: str):
        room = _rooms.get(code.upper())
        if not room or not room.get("mixdown_path"):
            return JSONResponse({"error": "No mixdown available"}, 404)
        path = Path(room["mixdown_path"])
        if not path.exists():
            return JSONResponse({"error": "Mixdown file not found"}, 404)
        return FileResponse(str(path), media_type="audio/mpeg", filename="mixdown.mp3")

    # ── Mixer: list tracks + serve individual audio files ───────────────

    @app.get("/api/plugins/multiplayer/rooms/{code}/tracks")
    async def list_tracks(code: str):
        """Return all available audio tracks (stems + recordings) for the mixer."""
        room = _rooms.get(code.upper())
        if not room:
            return JSONResponse({"error": "Room not found"}, 404)

        tracks = []

        # Song stems
        if 0 <= room["now_playing"] < len(room["queue"]):
            queue_item = room["queue"][room["now_playing"]]
            filename = queue_item.get("filename", "")
            if filename and callable(_get_dlc_dir):
                dlc = _get_dlc_dir()
                if dlc:
                    song_path = dlc / filename
                    if song_path.exists():
                        try:
                            import sloppak as sloppak_mod
                            if sloppak_mod.is_sloppak(song_path):
                                cache_dir = context.get("get_sloppak_cache_dir", lambda: None)()
                                source_dir = sloppak_mod.resolve_source_dir(filename, dlc, cache_dir)
                                manifest = sloppak_mod.load_manifest(song_path)
                                stems = manifest.get("stems", []) or []
                                for s in stems:
                                    if isinstance(s, dict) and s.get("file"):
                                        stem_path = source_dir / s["file"]
                                        if stem_path.exists():
                                            tracks.append({
                                                "id": s.get("id", ""),
                                                "label": s.get("id", "stem"),
                                                "type": "stem",
                                                "url": f"/api/plugins/multiplayer/rooms/{code}/track/{stem_path.name}?dir={stem_path.parent}",
                                                "path": str(stem_path),
                                            })
                        except Exception:
                            pass

        # Player recordings
        for pid, rec_info in room["recordings"].items():
            rec_path = rec_info["path"] if isinstance(rec_info, dict) else rec_info
            start_ms = rec_info.get("start_ms", 0) if isinstance(rec_info, dict) else 0
            if Path(rec_path).exists():
                player_name = room["players"].get(pid, {}).get("name", pid[:8])
                tracks.append({
                    "id": f"rec_{pid}",
                    "label": f"{player_name} (recording)",
                    "type": "recording",
                    "player_id": pid,
                    "start_ms": start_ms,
                    "url": f"/api/plugins/multiplayer/rooms/{code}/track/{Path(rec_path).name}",
                    "path": str(rec_path),
                })

        return {"tracks": tracks}

    @app.get("/api/plugins/multiplayer/rooms/{code}/track/{filename}")
    async def serve_track(code: str, filename: str, dir: str = ""):
        """Serve an individual audio file for the mixer preview."""
        room = _rooms.get(code.upper())
        if not room:
            return JSONResponse({"error": "Room not found"}, 404)

        room_dir = MP_DIR / code.upper()

        # `dir` is an attacker-controlled query param (it round-trips from the
        # tracks listing, but a client can supply any value). Constrain every
        # served path to a known root so it can't read arbitrary files: the
        # room's recording dir, the DLC folder, or the sloppak unpack cache
        # (where stems live). `filename` is a single path segment from the
        # route, but resolve+containment also neutralises a `..` segment.
        allowed_roots = [room_dir]
        if callable(_get_dlc_dir):
            _dlc = _get_dlc_dir()
            if _dlc:
                allowed_roots.append(Path(_dlc))
        _cache = context.get("get_sloppak_cache_dir", lambda: None)()
        if _cache:
            allowed_roots.append(Path(_cache))

        def _contained(p: Path) -> bool:
            try:
                rp = p.resolve()
            except (OSError, ValueError, RuntimeError):
                return False
            for root in allowed_roots:
                try:
                    rp.relative_to(root.resolve())
                    return True
                except ValueError:
                    continue
            return False

        def _audio_mime(name: str) -> str:
            # Recordings can be .webm/.mp3/.ogg/.wav (see the recording
            # upload handler) and stems are .ogg — map each to the right
            # type instead of guessing wav-vs-ogg.
            return {
                ".wav": "audio/wav",
                ".ogg": "audio/ogg",
                ".opus": "audio/ogg",
                ".mp3": "audio/mpeg",
                ".webm": "audio/webm",
                ".flac": "audio/flac",
                ".m4a": "audio/mp4",
            }.get(Path(name).suffix.lower(), "application/octet-stream")

        # Check recordings in room dir
        track_path = room_dir / filename
        if track_path.is_file() and _contained(track_path):
            return FileResponse(str(track_path), media_type=_audio_mime(filename))

        # Check stem dir
        if dir:
            stem_path = Path(dir) / filename
            if stem_path.is_file() and _contained(stem_path):
                return FileResponse(str(stem_path), media_type=_audio_mime(filename))

        return JSONResponse({"error": "Track not found"}, 404)

    # ── WebSocket ──────────────────────────────────────────────────────

    @app.websocket("/ws/plugins/multiplayer/{code}")
    async def multiplayer_ws(
        websocket: WebSocket,
        code: str,
        player_id: str = "",
        session_id: str = "",
    ):
        await websocket.accept()

        code = code.upper()
        room = _rooms.get(code)
        if not room or player_id not in room["players"]:
            # PROTOCOL.md "Rejection on auth failure": send the JSON error frame
            # for backward compatibility with already-shipped clients, then close
            # with the typed 4401 so spec-aware clients can branch on event.code.
            await websocket.send_json({"type": "error", "message": "Invalid room or player"})
            await websocket.close(code=_CLOSE_AUTH_FAIL)
            return

        if not _is_valid_session_id(session_id):
            await websocket.send_json(
                {"type": "error", "message": "Missing or malformed session_id"}
            )
            await websocket.close(code=_CLOSE_AUTH_FAIL)
            return

        # Apply PROTOCOL.md connection-arrival rules. May close prior sockets
        # for this player_id with 4410 (same-session reconnect) or 4409
        # (different-session takeover) before this socket's "connected" frame
        # is sent. The WebSocket upgrade itself was already accepted above.
        transition = await _take_session_slot(
            websocket, room, player_id, session_id, _HIGHWAY,
        )

        player = room["players"][player_id]
        player["ws"] = websocket
        player["connected"] = True
        player["last_seen"] = time.monotonic()
        player["ever_attached"] = True

        # Cancel pending room-empty cleanup if any (e.g. last player just rejoined).
        task = _cleanup_tasks.pop(code, None)
        if task:
            task.cancel()

        # Self-heal the host slot. _promote_host requires an active highway WS
        # (not just connected=True), so the room can end up with `room["host"]`
        # pointing at a player who can no longer hold host duties — either
        # because they left (leave_room removed their player record) OR
        # because their session expired beyond grace and was finalized
        # (room["sessions"][host_id] was popped, player["connected"]=False,
        # player["ws"]=None). Re-run the election now that this highway
        # attach made the candidate set non-empty.
        #
        # Four conditions trigger self-heal away from the current host:
        #   1. host_id missing.
        #   2. host's player record is gone (left via leave_room).
        #   3. host has ever attached a WS in this room AND no longer has an
        #      active session (they connected, dropped, grace expired).
        #   4. host has NEVER attached a WS AND the creator-grace window has
        #      already passed (room is older than HOST_CREATOR_GRACE_SEC).
        #
        # Cases (3) and (4) together catch every "host can't fulfill duties"
        # state without stealing host from a slow-but-arriving creator:
        #   - Slow creator (within HOST_CREATOR_GRACE_SEC): never_attached
        #     but room is young → no self-heal, creator keeps host slot.
        #   - Abandoned creator (past HOST_CREATOR_GRACE_SEC): never_attached
        #     and room is old → self-heal, guest takes over.
        #   - Connected-then-dropped: ever_attached + no session → self-heal
        #     immediately on next guest highway attach.
        #
        # Transient-grace protection: during a per-endpoint grace window the
        # session record IS present (only the endpoint slot is None), so
        # condition (3) doesn't fire and the host keeps their slot.
        #
        # We update room["host"] BEFORE sending the 'connected' snapshot so
        # the new client sees the corrected host_id immediately, then
        # broadcast host_changed to OTHER peers AFTER sending 'connected'
        # so messages stay properly ordered.
        host_id = room.get("host")
        host_player = room["players"].get(host_id) if host_id else None
        host_session = room.get("sessions", {}).get(host_id) if host_id else None
        room_age = time.monotonic() - room.get("created_at", time.monotonic())
        host_truly_gone = (
            host_id is None
            or host_player is None
            or (host_player.get("ever_attached") and host_session is None)
            or (
                host_player is not None
                and not host_player.get("ever_attached")
                and room_age > HOST_CREATOR_GRACE_SEC
            )
        )
        host_was_self_healed = False
        if host_truly_gone:
            new_host = _promote_host(room)
            if new_host is not None and new_host != host_id:
                host_was_self_healed = True

        # Once the current host has a live highway WS, the one-shot
        # creator-grace timer is no longer load-bearing: it would either
        # find ever_attached=True (slow-creator path resolved) or find
        # the host has changed (self-heal already happened). Cancelling
        # here avoids leaving an idle 30s sleep task in flight for the
        # lifetime of every healthy room. The cgt slot is also cleared so
        # _cleanup_after_grace doesn't try to cancel it again.
        if player_id == room.get("host"):
            cgt = room.pop("creator_grace_task", None)
            if cgt is not None and not cgt.done():
                cgt.cancel()

        await websocket.send_json({
            "type": "connected",
            "room": _serialize_room(room),
            "server_time": time.monotonic() * 1000,
        })

        if host_was_self_healed:
            await _broadcast(room, {
                "type": "host_changed",
                "new_host_id": room["host"],
            }, exclude=player_id)

        # Peers see player_connected when this is the FIRST highway attach for
        # this player_id — whether the session is brand-new ('new_session') or
        # the audio endpoint already opened the session before highway joined
        # ('first_attach'). On 'reconnect' (replacing a still-live highway slot)
        # and 'takeover' (different session_id, but same player_id was already
        # visible to peers), no new player_connected event is needed.
        if transition in ("new_session", "first_attach"):
            await _broadcast(room, {
                "type": "player_connected",
                "player_id": player_id,
                "name": player["name"],
            }, exclude=player_id)

        try:
            while True:
                data = await websocket.receive_json()
                await _handle_message(room, player_id, data)
        except WebSocketDisconnect:
            pass
        except Exception as e:
            _log.warning("WS error for %s: %s", player_id, e, exc_info=True)
        finally:
            await _on_endpoint_disconnect(websocket, room, player_id, _HIGHWAY)

    @app.websocket("/ws/plugins/multiplayer/{code}/audio")
    async def multiplayer_audio_ws(
        websocket: WebSocket,
        code: str,
        player_id: str = "",
        session_id: str = "",
    ):
        # Accept-then-close pattern (PROTOCOL.md "Implementation note (server
        # side)"): always accept the upgrade so the close-code reaches the
        # browser as a `close` event with `event.code === 4401`. Closing
        # before accept yields HTTP 403 in Starlette and the browser only
        # sees a generic WebSocket error.
        await websocket.accept()

        code = code.upper()
        room = _rooms.get(code)
        if not room or player_id not in room["players"]:
            await websocket.close(code=_CLOSE_AUTH_FAIL)
            return
        if not _is_valid_session_id(session_id):
            await websocket.close(code=_CLOSE_AUTH_FAIL)
            return

        # Apply rules 1/2/3 for the audio slot. Same close-code semantics as
        # the highway slot (4410 / 4409 on takeover/replace). The bounded
        # send queue + worker for THIS audio_ws is set up inside
        # _take_session_slot before the rule returns, so by the time other
        # peers' read loops can see this audio_ws on the room's session map,
        # its queue/worker are already in place — no race window where a
        # new attach drops the first frames.
        await _take_session_slot(websocket, room, player_id, session_id, _AUDIO)

        # An audio attach keeps the room alive even when no highway WS is
        # currently open — for example during a recovery where /audio reconnects
        # before /ws. Without this, _connected_count would stay 0 and a pending
        # 60-second room cleanup would delete the room while audio is still
        # flowing, leaving listeners stranded with a 404 on their next HTTP
        # action. The legacy `connected` flag tracks "any live endpoint exists";
        # `player["ws"]` stays whatever the highway handler last set (None when
        # highway is closed) so _broadcast still skips this player on JSON
        # control-plane messages it can't deliver.
        #
        # Importantly, audio alone must NOT mark the player as "ever attached"
        # for host self-heal purposes. That sticky flag is reserved for the
        # highway WS — the control plane is what actually lets a player do
        # host duties (receive host_changed, send play/pause/etc). A creator
        # who only opens /audio without /ws should still be considered
        # abandoned-from-the-control-plane and lose the host slot to a
        # connected highway peer once the creator-grace window expires.
        player = room["players"][player_id]
        player["connected"] = True
        player["last_seen"] = time.monotonic()
        task = _cleanup_tasks.pop(code, None)
        if task:
            task.cancel()

        try:
            while True:
                message = await websocket.receive()
                if message.get("type") == "websocket.disconnect":
                    break

                # Self-check: if this socket has been superseded by a same-
                # session reconnect (Rule 2) or different-session takeover
                # (Rule 3) since we entered the loop, our slot in the session
                # record now points at a NEW ws. Continuing to forward frames
                # from THIS (old) ws would put two senders on the same
                # player_id and violate the single-active-slot invariant
                # PROTOCOL.md guarantees. Exit the loop; the deferred close
                # already scheduled by Rule 2 / takeover will close us
                # cleanly with 4410 / 4409.
                live_sess = room.get("sessions", {}).get(player_id)
                if live_sess is None or live_sess.get("audio_ws") is not websocket:
                    break

                payload = message.get("bytes")
                if payload is None:
                    # Non-binary frame on the audio WS — drop silently per
                    # PROTOCOL.md "binary frames only". Servers MAY also close;
                    # v1 implementation chooses to drop and keep the connection.
                    continue

                verdict = _classify_audio_frame(payload)
                if verdict == "size_violation":
                    # Frame too big / truncated header / body length mismatch:
                    # drop and close with 1009 per PROTOCOL.md "Frame size
                    # budget and bounds".
                    await _safe_close(websocket, _CLOSE_FRAME_TOO_BIG)
                    break
                if verdict == "drop":
                    # Bad magic or version mismatch: drop, keep connection.
                    continue

                # Single-broadcaster gate (PROTOCOL.md "v1 enforces a single
                # broadcaster per room"). The control plane published an
                # active broadcaster_id via broadcast_start; the data plane
                # MUST drop frames from anyone else, otherwise a listener
                # or buggy client could inject audio that peers play even
                # though no broadcast_start was accepted for them.
                if room.get("broadcaster_id") != player_id:
                    continue

                # Also drop while a handoff purge is in flight: the
                # broadcast_start handler claims broadcaster_id BEFORE
                # awaiting _purge_audio_for_handoff (so concurrent
                # broadcast_starts serialize correctly), but accepting
                # broadcaster frames during that window would feed
                # them into about-to-be-cancelled listener workers
                # and cause clipped first intervals. PROTOCOL.md
                # also requires broadcasters to wait for the
                # broadcaster_changed ack before sending, so a
                # well-behaved client won't hit this; the gate is
                # belt-and-suspenders against buggy/malicious
                # broadcasters. Spotted by Copilot review on PR #7.
                if room.get("broadcast_handoff_in_progress", 0) > 0:
                    continue

                # Verdict "ok" + sender is the active broadcaster: fan out
                # byte-for-byte to every other audio subscriber in the same
                # room. The Opus payload is never touched — only header
                # validation happened above.
                #
                # Each peer has its own bounded send queue drained by a single
                # worker bound to that peer's audio_ws. _enqueue_audio_frame
                # is non-blocking: on overflow it drops the OLDEST queued
                # frame so a slow peer stays bounded in memory and a recovering
                # peer falls at most _AUDIO_SEND_QUEUE_MAX frames behind. The
                # sender's read loop never awaits any send, so a slow peer
                # cannot head-of-line-block forwarding to others OR stall the
                # next inbound frame.
                for pid, peer_sess in list(room.get("sessions", {}).items()):
                    if pid == player_id:
                        continue
                    if peer_sess.get("audio_ws") is None:
                        continue
                    _enqueue_audio_frame(peer_sess, payload)
        except WebSocketDisconnect:
            pass
        except Exception as e:
            _log.warning("Audio WS error for %s: %s", player_id, e, exc_info=True)
        finally:
            await _on_endpoint_disconnect(websocket, room, player_id, _AUDIO)

    async def _handle_message(room, player_id, data):
        msg_type = data.get("type", "")
        is_host = room["host"] == player_id
        server_time = time.monotonic() * 1000

        if msg_type == "clock_sync_request":
            ws = room["players"][player_id].get("ws")
            if ws:
                await ws.send_json({
                    "type": "clock_sync_response",
                    "client_t1": data.get("client_t1", 0),
                    "server_t2": server_time,
                    "server_t3": time.monotonic() * 1000,
                })

        elif msg_type == "set_arrangement":
            player = room["players"].get(player_id)
            if player:
                player["arrangement"] = data.get("arrangement", "Lead")
                await _broadcast(room, {
                    "type": "arrangement_changed",
                    "player_id": player_id,
                    "arrangement": player["arrangement"],
                })

        elif msg_type == "play" and is_host:
            room["state"] = "playing"
            room["time"] = data.get("time", 0.0)
            room["speed"] = data.get("speed", 1.0)
            await _broadcast(room, {
                "type": "playback_state",
                "state": "playing",
                "time": room["time"],
                "speed": room["speed"],
                "server_time": server_time,
            }, exclude=player_id)

        elif msg_type == "pause" and is_host:
            room["state"] = "paused"
            room["time"] = data.get("time", 0.0)
            await _broadcast(room, {
                "type": "playback_state",
                "state": "paused",
                "time": room["time"],
                "speed": room["speed"],
                "server_time": server_time,
            }, exclude=player_id)

        elif msg_type == "seek" and is_host:
            room["time"] = data.get("time", 0.0)
            await _broadcast(room, {
                "type": "playback_state",
                "state": room["state"],
                "time": room["time"],
                "speed": room["speed"],
                "server_time": server_time,
            }, exclude=player_id)

        elif msg_type == "set_speed" and is_host:
            room["speed"] = data.get("speed", 1.0)
            await _broadcast(room, {
                "type": "playback_state",
                "state": room["state"],
                "time": room["time"],
                "speed": room["speed"],
                "server_time": server_time,
            }, exclude=player_id)

        elif msg_type == "heartbeat" and is_host:
            room["time"] = data.get("time", room["time"])
            await _broadcast(room, {
                "type": "heartbeat",
                "time": room["time"],
                "state": room["state"],
                "server_time": server_time,
            }, exclude=player_id)

        elif msg_type == "start_recording" and is_host:
            room["recording"] = True
            room["recordings"] = {}
            await _broadcast(room, {"type": "recording_state", "recording": True})

        elif msg_type == "stop_recording" and is_host:
            room["recording"] = False
            await _broadcast(room, {"type": "recording_state", "recording": False})

        # ── Broadcast control plane (PROTOCOL.md "Audio control messages") ──
        elif msg_type == "broadcast_start":
            current = room.get("broadcaster_id")
            ws = room["players"][player_id].get("ws")
            sender_session = room.get("sessions", {}).get(player_id)
            sender_audio_ws = sender_session.get("audio_ws") if sender_session else None
            if current is not None and current != player_id:
                # v1: single broadcaster per room. Reject the new attempt.
                if ws is not None:
                    try:
                        await ws.send_json({
                            "type": "error",
                            "message": "broadcaster_busy",
                        })
                    except Exception:
                        pass
            elif sender_audio_ws is None:
                # PROTOCOL.md "Implementation note (client side)": the host
                # MUST NOT issue broadcast_start before /audio is attached.
                # If the audio WS was rejected with 4401 or simply not opened
                # yet, accepting the broadcast would have peers waiting for
                # frames that can never arrive. Reject with audio_ws_not_open
                # so the client can retry after opening /audio.
                if ws is not None:
                    try:
                        await ws.send_json({
                            "type": "error",
                            "message": "audio_ws_not_open",
                        })
                    except Exception:
                        pass
            else:
                params_or_err = _validate_broadcast_params(data)
                if isinstance(params_or_err, str):
                    if ws is not None:
                        try:
                            await ws.send_json({
                                "type": "error",
                                "message": f"broadcast_start_invalid: {params_or_err}",
                            })
                        except Exception:
                            pass
                else:
                    # ORDER MATTERS — codex multi-round, then refined
                    # in Copilot round 4 of PR #7:
                    # 1. Atomically claim the broadcaster slot BEFORE
                    #    awaiting anything. The check + assignment is
                    #    synchronous (no await between them), so two
                    #    concurrent broadcast_start handlers that both
                    #    saw current == None resolve here: only one
                    #    wins; the other's re-check fails and it bails
                    #    with broadcaster_busy. This prevents the loser
                    #    from running _purge_audio_for_handoff against
                    #    the winner's listener workers and clipping the
                    #    winner's first intervals.
                    # 2. THEN purge old listener workers (drains
                    #    leftover queue entries and cancels in-flight
                    #    sends from the previous broadcaster). Skipped
                    #    when the same broadcaster is re-issuing
                    #    broadcast_start (reconnect / param refresh).
                    #    Frames sent by the new broadcaster during this
                    #    purge window are dropped at the /audio fan-out
                    #    gate (which checks broadcast_handoff_in_progress
                    #    > 0); a well-behaved broadcaster won't send
                    #    them anyway because PROTOCOL.md requires
                    #    waiting for the broadcaster_changed ack.
                    # 3. Emit broadcaster_changed so listeners know to
                    #    treat subsequent /audio frames as belonging
                    #    to the new broadcaster. Frames listeners
                    #    receive AFTER this event are guaranteed to
                    #    come from the new broadcaster.
                    pre_claim = room.get("broadcaster_id")
                    if pre_claim is not None and pre_claim != player_id:
                        # Lost an idle-race (concurrent broadcast_start
                        # already claimed the slot during our message
                        # processing).
                        if ws is not None:
                            try:
                                await ws.send_json({
                                    "type": "error",
                                    "message": "broadcaster_busy",
                                })
                            except Exception:
                                pass
                    elif (
                        pre_claim == player_id
                        and room.get("broadcast_handoff_in_progress", 0) > 0
                    ):
                        # A SAME-player rapid retry of broadcast_start
                        # (reconnect / param refresh) is still inside
                        # _purge_audio_for_handoff. Without this guard,
                        # this re-entrant request would emit
                        # broadcaster_changed early — letting the
                        # broadcaster send frames before the original
                        # handler's purge completes, so the first
                        # intervals get clipped against the not-yet-
                        # cancelled stale workers. Drop silently: the
                        # original broadcast_start will emit the ack
                        # when its purge finishes.
                        # Different-player broadcast_starts are NOT
                        # blocked here — if pre_claim is None (the
                        # provisional broadcaster left mid-purge), a
                        # new player's start should still succeed
                        # rather than be silently discarded.
                        # Counter-not-bool semantics: overlapping
                        # purges (A leaves mid-purge, B starts) are
                        # tracked independently — A's exit decrements
                        # without zeroing B's counter.
                        pass
                    else:
                        room["broadcaster_id"] = player_id
                        room["broadcast_params"] = params_or_err
                        if current != player_id:
                            # Counter (not boolean) so overlapping
                            # purges don't clobber each other's
                            # active state. A's `finally` decrements
                            # to whatever B's still-running purge
                            # incremented to; the in-progress check
                            # reads `> 0` so the flag stays "true"
                            # until ALL active purges have completed.
                            room["broadcast_handoff_in_progress"] = (
                                room.get("broadcast_handoff_in_progress", 0) + 1
                            )
                            try:
                                await _purge_audio_for_handoff(room, except_player_id=player_id)
                            finally:
                                room["broadcast_handoff_in_progress"] = (
                                    room.get("broadcast_handoff_in_progress", 1) - 1
                                )
                        # Re-check ownership before announcing. While
                        # we awaited the purge, any of broadcast_stop /
                        # leave_room / grace expiry / takeover could
                        # have cleared broadcaster_id (or set it to a
                        # different player). Without this, we'd emit
                        # a stale broadcaster_changed against a slot
                        # that's no longer ours, telling listeners a
                        # broadcast is active when the room state
                        # disagrees.
                        if room.get("broadcaster_id") == player_id:
                            await _broadcast(room, {
                                "type": "broadcaster_changed",
                                "broadcaster_id": player_id,
                                **params_or_err,
                            })

        elif msg_type == "broadcast_stop":
            # Only the active broadcaster can stop their own broadcast.
            # A stop from anyone else is silently ignored to avoid
            # exposing broadcast state to unrelated peers.
            if room.get("broadcaster_id") == player_id:
                await _clear_broadcaster_if_player(room, player_id)

        elif msg_type == "audio_quality":
            # Forward listener-side quality telemetry to the broadcaster
            # (and only the broadcaster — peers don't need each other's
            # stats). No-op if no one is broadcasting or the report
            # references a different broadcaster_id (stale telemetry).
            current = room.get("broadcaster_id")
            if current is not None and data.get("broadcaster_id") == current:
                broadcaster = room["players"].get(current)
                if broadcaster is not None and broadcaster.get("ws") is not None:
                    try:
                        await broadcaster["ws"].send_json({
                            "type": "audio_quality",
                            "from_player_id": player_id,
                            "broadcaster_id": current,
                            "intervals_received": data.get("intervals_received"),
                            "intervals_late": data.get("intervals_late"),
                            "intervals_dropped": data.get("intervals_dropped"),
                            "decoder_underruns": data.get("decoder_underruns"),
                            "report_period_ms": data.get("report_period_ms"),
                        })
                    except Exception:
                        pass

        elif msg_type == "song_ended" and is_host:
            room["skip_votes"] = set()
            await _advance_song(room)

        elif msg_type == "load_song" and is_host:
            # Host explicitly starts a song from queue
            index = data.get("index", 0)
            if 0 <= index < len(room["queue"]):
                room["now_playing"] = index
                room["state"] = "stopped"
                room["time"] = 0.0
                room["recording"] = False
                room["recordings"] = {}
                room["mixdown_path"] = None
                room["skip_votes"] = set()
                await _broadcast(room, {
                    "type": "song_changed",
                    "now_playing": room["now_playing"],
                    "queue_item": room["queue"][room["now_playing"]],
                }, exclude=player_id)

    async def _advance_song(room):
        """Move to next song in queue."""
        room["state"] = "stopped"
        room["time"] = 0.0
        room["skip_votes"] = set()
        room["recordings"] = {}
        room["mixdown_path"] = None

        if room["now_playing"] < len(room["queue"]) - 1:
            room["now_playing"] += 1
            await _broadcast(room, {
                "type": "song_changed",
                "now_playing": room["now_playing"],
                "queue_item": room["queue"][room["now_playing"]],
            })
        else:
            room["now_playing"] = -1
            await _broadcast(room, {
                "type": "queue_finished",
                "now_playing": -1,
            })

    # _start_cleanup and _cleanup_after_grace live at module level (above)
    # so module-level coroutines can call them — see comment there.
