# Multiplayer Plugin — Wire Protocol

This document describes the wire protocol used by the Slopsmith Multiplayer plugin. Two WebSocket endpoints are exposed:

- **Highway WS** — `/ws/plugins/multiplayer/{code}` — JSON control plane (already implemented; see `routes.py`).
- **Audio WS** — `/ws/plugins/multiplayer/{code}/audio` — binary peer-audio relay. Planned for the interval-quantized peer-audio feature; not live in any released plugin version yet. `plugin.json` stays at the current version until the endpoint actually ships.

Both endpoints share the same room registry and authentication state: the connecting client must pass `player_id` as a query parameter, the room must exist, and the player must already be a member of the room (joined via the REST API). The two endpoints use different rejection mechanisms because their framing rules differ — see each endpoint section below.

---

## Endpoints

### Highway WS — JSON control plane

```
ws://<host>/ws/plugins/multiplayer/{code}?player_id={id}&session_id={sid}
```

`session_id` is a client-generated random identifier (UUIDv4 recommended; e.g. `crypto.randomUUID()`) that uniquely identifies a single browser tab / app instance for the lifetime of the session. **Wire-format requirement:** any non-empty string matching `[A-Za-z0-9_-]{1,128}` is a valid session_id. Servers MUST accept any value matching this pattern (UUIDv4 satisfies it) and MUST reject only missing or non-conformant values with `4401`. The client mints it when it starts trying to connect and keeps it in `sessionStorage` (or equivalent) so reconnects after a network blip carry the same `session_id`. A different tab gets a different `session_id`.

**Status (as of Phase 2d).** Server-side: `session_id` + per-session takeover/grace lifecycle (`4401` / `4408` / `4409` / `4410`) + `/audio` endpoint with binary fan-out and `SMAU` header validation (`1009`) + broadcast control plane (`broadcast_start` / `broadcast_stop` / `broadcaster_changed` / `broadcaster_busy` / `audio_quality`) + `broadcaster_id` / `broadcast_params` room state. Exercised by `tests/test_session_lifecycle.py` + `tests/test_audio_ws.py` + `tests/test_broadcast_control.py`. Client-side: `screen.js` opens `/audio` alongside the highway WS using the same `session_id`, handles all lifecycle close codes (4401 / 4408 / 4409 / 4410 / 1009 / 1011) with independent per-endpoint reconnect cadence, parses the 40-byte SMAU header on every inbound binary frame, decodes the Opus payload via WebCodecs `AudioDecoder` and schedules an `AudioBufferSourceNode` per interval to start at `chartTimeStart + intervalDuration` translated into `AudioContext.currentTime` via the listener's chart audio element. Listener pipeline activates from either the `connected` snapshot's `broadcaster_id` (late joiners) or a fresh `broadcaster_changed` event, drops scheduled sources on chart pause / hard seek / 4408 grace recovery, and refuses frames when chart is paused or `playbackRate != 1.0` (counted under `listener_paused` / `listener_speed`). v1 limitation: peer audio assumes both ends are at 1.0x; speed changes warp the chart→AudioContext mapping. Source of truth `audio/smau-frame.js`; inlined into `screen.js` because the plugin loader serves only the single `screen.js` entry point. Broadcast capture pipeline now live: `getUserMedia` (no AGC/NS/AEC, 48 kHz mono), inlined `AudioWorkletProcessor` (loaded via Blob URL because the plugin loader serves only one entry point), level meter via `AnalyserNode`, WebCodecs `AudioEncoder` configured for Opus 96 kbps, interval slicing keyed on the chart's BPM via the inlined `selectInterval` helper, SMAU frame build via the inlined encoder (mirror of `audio/smau-frame.js`), and frame send over the audio WS. Frame send is gated on a `broadcaster_changed` ack with the broadcaster's own `player_id` (so frames never leave the page before the server-side single-broadcaster check has accepted us). UI: "Broadcast my sound" toggle + input device dropdown + level meter + status pill, with a `beforeunload` handler that tears down capture + sends `broadcast_stop` on tab close. Phase 2e is polish (pause/seek interaction, late-join from connected snapshot, tempo-change handling, error states).

Carries all control messages. The message types used by the audio feature are documented under "Audio control messages" below; the existing playback/queue/recording messages are unchanged.

**Rejection on auth failure:** the highway WS sends a JSON `{"type": "error", "message": "..."}` text frame and then closes the connection (existing behavior; preserves backward compatibility with already-shipped clients).

### Audio WS — binary peer-audio relay (NEW)

```
ws://<host>/ws/plugins/multiplayer/{code}/audio?player_id={id}&session_id={sid}
```

`session_id` is REQUIRED on the audio WS and MUST be the same value the client used (or will use) on its highway WS. This binds the two endpoints into a single session and is the mechanism that lets the server distinguish "same tab opening its second socket" from "different tab taking over."

Carries **binary frames only**. Every frame is one full Ninjam-style interval of Opus-encoded audio.

The server's forwarding path is intentionally trivial: on receipt of a binary frame from a subscriber, it forwards the frame **byte-for-byte** to every other audio subscriber in the same room — no re-encoding, no payload inspection, no rewriting. The server DOES, however, parse the fixed-size 40-byte header before forwarding in order to enforce the safety checks in "Frame size budget and bounds" below (magic, version, frame-length / `opus_size` consistency, and the `MAX_FRAME_BYTES` hard limit). Those are O(1) integrity checks on the header only; the Opus payload is never touched.

**The audio WS is binary-only in BOTH directions in v1.** Neither client nor server may send text/JSON frames over it. Specifically:

- Clients MUST NOT send text frames. If the server receives a non-binary frame, it MUST drop it and MUST NOT forward it to any peer. (Servers MAY additionally close the connection on receipt of a non-binary frame; v1 implementations are not required to.)
- Servers MUST NOT send text frames either. **Connection-rejection on auth failure (room not found, player not in room) is signalled via the WebSocket close handshake only**, with one of the close codes below — no JSON error payload as on the highway WS. This preserves the binary-only invariant and lets clients implement a single uniform reader.

| Close code | Meaning                                                             |
|-----------:|---------------------------------------------------------------------|
| `4401`     | Auth failure (room does not exist, `player_id` not in the room, or `session_id` missing). See `session_id` syntax under "Endpoints" — any non-empty string of `[A-Za-z0-9_-]{1,128}` is accepted; servers MUST NOT reject a session_id that conforms to that pattern even if it is not specifically a UUIDv4. |
| `4408`     | Session timed out — one of this session's endpoints failed to reattach within `ENDPOINT_GRACE_PERIOD_SEC`, ending the session. The other endpoint is also being closed with `4408`. |
| `4409`     | Superseded — a DIFFERENT session (different `session_id`) for the same `player_id` took over. The OTHER endpoint of THIS session is also being closed with `4409`. |
| `4410`     | Replaced — the SAME session (matching `session_id`) reconnected this endpoint. The OTHER endpoint of this session is unaffected and remains live. |
| `1009`     | Frame too big — see Frame size budget and bounds below.             |
| `1011`     | Server error (unexpected; rare).                                    |

**v1 server policy: at most one active *session* per `(room_code, player_id)`, where a session is identified by the client-supplied `session_id` and owns BOTH the highway WS and the audio WS atomically.** Session tracking is **per-room** — `player_id` values are scoped to a room (see existing `room["players"][player_id]` in `routes.py`), so the session registry lives on the room object alongside that map. The server keeps state of the form:

```
room["sessions"][player_id] = {
    session_id,
    highway_ws,   # may be None if not yet opened or already closed
    audio_ws,     # may be None if not yet opened or already closed
}
```

Two rooms with the same `player_id` value (which is theoretically possible since `player_id`s are minted per-room) keep independent sessions. Implementations MUST NOT build a global `player_id → session` map.

Connection-arrival rules (apply identically to a highway-WS open and an audio-WS open; the only difference is which slot the new socket fills; lookup is always against `room["sessions"][player_id]` for the room identified by the URL `code`):

1. **No existing session for this `(room_code, player_id)`** → create a new session with the supplied `session_id`, register the new socket in the matching slot, accept normally.
2. **Existing session and `session_id` matches** → this is the same tab opening its second endpoint, OR a reconnect of the same endpoint after a network blip. Close any existing socket already in the matching slot with code `4410`, register the new socket in that slot, accept normally. The OTHER endpoint's socket is **left untouched** — it remains a live, healthy member of the same session.
3. **Existing session and `session_id` differs** → this is a different tab taking over. Close BOTH the existing highway and audio sockets in the existing session with code `4409`, discard the old session, create a new session with the supplied `session_id`, register the new socket, accept normally.

The lifecycle close codes (`4408`, `4409`, `4410`) carry the entire required signal — clients MUST branch on `event.code` alone, not on `event.reason`, since some WebSocket stacks normalize or drop the reason string. Each code maps to a distinct required client behavior; see "Client behavior on close" below for the full branching rules.

**Client behavior on close:**
- **`4408` (session timed out):** this session's other endpoint failed to reattach within the grace window, so the server ended the whole session. Both endpoints are or will be closed with `4408`. Clients MUST tear down BOTH WS states cleanly. UI SHOULD show a connection-lost notice ("Reconnecting…" or similar) and may attempt to start a fresh session with a NEW `session_id`.
- **`4409` (superseded by another tab):** the user's own action elsewhere has invalidated this session. Both endpoints are or will be closed by the server. Clients MUST tear down BOTH the highway-WS and audio-WS state cleanly. UI SHOULD show a non-error notice ("Your session moved to another tab") rather than an error. The closed-out tab MUST NOT auto-reconnect (it would just steal back from the new tab).
- **`4410` (replaced by your own reconnect):** the OTHER endpoint of the same session is still live. Clients MUST tear down ONLY the closed endpoint's local state and rebuild it (typically the new socket has just been opened by the same tab and is already replacing it). Clients MUST NOT touch the other endpoint's WS.

Why a `session_id` is needed at all: without it, a single tab opening its second endpoint is indistinguishable from a different tab taking over, since both arrive as "a new connection for the same `player_id`." `session_id` lets the server make that distinction deterministically without having to time-window or guess. The two close codes (`4409` vs `4410`) carry the disambiguation back to the closed-out client.

**Connections without `session_id`** are rejected with close code `4401` (using the accept-then-close pattern documented above on the audio WS, or the existing JSON-error-then-close pattern on the highway WS). Phase 1 ships the server and client changes together, so no transition path or livelock-avoidance is needed — every conformant client of v1.1+ sends `session_id`.

Phase 1 will tighten the existing highway WS handler in `routes.py` to enforce these rules (today it overwrites `player["ws"]` without closing the old socket and accepts duplicate sessions silently; that becomes a bug under the new audio feature).

As a corollary of this policy, `broadcaster_id` (= `player_id`) uniquely identifies the host's session — both endpoints — at any moment in time, and `broadcast_start` is acknowledged against whichever session currently holds both sockets for that `player_id`. The `4401` close code remains for auth failures (room/player_id invalid, or missing/malformed `session_id`) and uses the accept-then-close pattern documented above. v1 servers MUST NOT use any close code outside `{4401, 4408, 4409, 4410, 1009, 1011}` on the audio WS.

Codes in the `4000–4999` range are protocol-reserved per RFC 6455 and visible to the client via `event.code` on the `close` event. The optional `event.reason` string MAY include a short ASCII hint (≤ 123 bytes per RFC) for human/debug consumption only — **clients MUST branch behavior on `event.code` alone**, never on the reason string. Some WebSocket stacks normalize or drop the reason; only `event.code` is reliably interoperable.

**Implementation note (server side).** To make the typed close code actually reach the browser, the server MUST accept the WebSocket upgrade first and only then close with the custom code. On Starlette / FastAPI, `await websocket.close(code=4401)` *before* `await websocket.accept()` causes the HTTP upgrade to fail with `403 Forbidden`, and browser clients receive a generic `WebSocket` error rather than a `close` event whose `event.code === 4401`. The required pattern is:

```python
@app.websocket("/ws/plugins/multiplayer/{code}/audio")
async def audio_ws(websocket: WebSocket, code: str, player_id: str = ""):
    await websocket.accept()                 # accept first
    if not _is_authorised(code, player_id):
        await websocket.close(code=4401)     # then close with typed code
        return
    # ... normal session ...
```

**Implementation note (client side).** A direct consequence of accept-then-close is that **the audio WS `open` event is NOT proof that authentication succeeded.** A rejected socket fires `open` first and only then `close` with code `4401`. Treating `open` alone as "ready" — for example, enabling broadcaster UI or starting a send loop in the open handler — will mis-handle rejected connections, and there is no fixed timeout that closes this race deterministically (close delivery is not bounded under network stall).

The deterministic readiness signal for the audio WS comes from the **control plane on the highway WS**, not from the audio WS itself:

- **Host (sender).** Treat the audio WS as ready for outbound audio only after the server has acknowledged `broadcast_start` by emitting `broadcaster_changed` on the highway WS with the host's own `broadcaster_id`. If the audio WS was rejected with `4401`, the server's broadcaster registry will not contain the host's audio subscriber, and the server MUST refuse `broadcast_start` from that host (returning `{"type":"error","message":"audio_ws_not_open"}` on the highway WS). Combined with the one-audio-WS-per-player_id server policy above, the `broadcaster_changed` ack uniquely identifies the host's own audio WS. UI that begins capture or sending MUST be gated on the `broadcaster_changed` ack, not on the audio WS `open` event.
- **Listener.** The user-visible "broadcast active" state comes from EITHER the initial `connected` room snapshot's `broadcaster_id` (for late joiners — clients that join while a broadcast is already in progress) OR a fresh `broadcaster_changed` event (for clients in the room when the broadcast starts). Both are valid signals; UI MUST treat them equivalently. The user-visible "audio is actually flowing" state is the **first valid binary frame** arriving on the audio WS; the listener's audio graph (decoder, gain node, scheduling) is constructed on receipt of that first frame. **Listeners MUST NOT use the audio WS `open` event for any UI or readiness purpose** — including clearing a "connecting…" indicator — because under the accept-then-close pattern, a rejected socket fires `open` first and `close(4401)` may be delivered arbitrarily later.

This anchors readiness on the already-authenticated highway WS (which uses the existing JSON-error-then-close rejection path) plus deterministic data-plane events (first frame received) — never on audio WS `open`, which is racy for both auth-failure (`4401`) and is the canonical race source codex flagged.

---

## Audio control messages (Highway WS)

These flow over the existing highway WebSocket.

### `broadcast_start` — host announces it is broadcasting

Sent by a client to the server when its user enables the broadcast toggle.

```json
{
  "type": "broadcast_start",
  "interval_beats": 4,
  "sample_rate": 48000,
  "channel_count": 1,
  "codec": "opus",
  "bitrate": 96000
}
```

Server response — broadcast to all peers in the room:

```json
{
  "type": "broadcaster_changed",
  "broadcaster_id": "ab12cd34",
  "interval_beats": 4,
  "sample_rate": 48000,
  "channel_count": 1,
  "codec": "opus",
  "bitrate": 96000
}
```

Server also stores `broadcaster_id` (and the codec parameters) in the room dict; subsequent `_serialize_room` snapshots include them so late-joiners learn about the active broadcast immediately on `connected`.

v1 enforces a single broadcaster per room. A second `broadcast_start` from a different player while one is already active is rejected with:

```json
{ "type": "error", "message": "broadcaster_busy" }
```

### `broadcast_stop` — host stops broadcasting

```json
{ "type": "broadcast_stop" }
```

Server response — broadcast to all peers:

```json
{ "type": "broadcaster_changed", "broadcaster_id": null }
```

The server also clears `broadcaster_id` (and emits the `broadcaster_changed` null event above) when the broadcaster's **session ends**. The session-end decision is intentionally deferred so that transparent reconnects (network blips, brief Wi-Fi drops, mobile-device backgrounding) do not produce a spurious stop/start blip in listener UIs:

- **Per-endpoint grace.** Each endpoint (highway and audio) tracks its own `alive_until` deadline. While the endpoint is connected, `alive_until = ∞`. When the endpoint disconnects (any close code: `4410`, `1006`, `1001`, etc.), `alive_until = now + ENDPOINT_GRACE_PERIOD_SEC` (RECOMMENDED: `5` seconds). When a new connection arrives with the **same** `session_id` and `player_id` for that endpoint, it reattaches to the held session record and resets `alive_until = ∞`.
- **Session liveness.** The session is alive while BOTH endpoints' `alive_until` is in the future. If EITHER endpoint's grace expires without reattach, the session ends. (Both endpoints must remain healthy — a permanently-dead audio endpoint with a still-connected highway endpoint is NOT a valid broadcasting session, since no audio can flow.)
- **Session-end finalization.** Two distinct paths, with distinct close codes:
  - **Grace expiry** (one endpoint died and its `alive_until` has passed without reattach): server closes any still-connected endpoint with `4408`, clears `broadcaster_id`, emits `broadcaster_changed: null` to all peers, discards the held record.
  - **Takeover** (a connection arrives with same `player_id` but different `session_id`): server closes any still-connected endpoint(s) of the OLD session with `4409`, clears `broadcaster_id`, emits `broadcaster_changed: null` to all peers, discards the held record, then accepts the new connection and creates a new session.
  Clients receive a typed close code that distinguishes "your session timed out, you may reconnect" from "another tab took over, do not reconnect," matching the "Client behavior on close" rules above. `1011` is reserved exclusively for unexpected server errors and is NEVER emitted for ordinary session lifecycle events.
- **No event during grace.** During an endpoint's grace window, the server emits NO peer-visible event. Listeners may briefly see a gap in audio frames (jitter buffer absorbs it) but `broadcaster_changed: null` is NOT emitted unless and until the grace period expires.
- **Room teardown.** Independent of the above, the existing 60-second room-empty grace period applies; if the room is torn down, any held session is also discarded with `broadcaster_changed: null`.

In short: server emits `broadcaster_changed: null` exactly once per real broadcast end. Transient disconnect-then-reattach on either endpoint produces no event. Only (a) either endpoint going dead beyond `ENDPOINT_GRACE_PERIOD_SEC` without reattach, or (b) takeover by a different `session_id`, ends the session.

### `audio_quality` — listener-reported telemetry (optional)

Periodic listener-side report so the broadcaster's UI can display "N late frames in last 30s" etc. Each listener with an active broadcast subscription emits at most one report per `report_period_ms` window (current implementation: 30 s). Counters are **deltas over the window**, not cumulative-since-page-load — empty windows are not sent at all to save WS bandwidth, but the listener still rolls its internal window pointer forward each tick so consecutive deltas always describe a single window.

**Implementation note — first-tick skip after broadcaster change.** The current `screen.js` listener deliberately skips sending on the first reporting tick after initially binding (or rebinding) to a `broadcaster_id`; that tick only reseeds the snapshot for the new broadcaster. As a result, after a broadcaster change the first `audio_quality` report may be delayed up to one full `report_period_ms` window past the change, rather than appearing on the next 30 s boundary. This is a deliberate trade for correct attribution — the listener's cumulative RX counters carry frames from previous broadcasts in the same session, so reseeding at the current cumulative values is the only way to keep deltas scoped to the active broadcast.

Field semantics:

- `intervals_received` — frames that were successfully scheduled for playback in the window.
- `intervals_late` — frames whose target chart time had already passed at decode time. These are dropped before scheduling.
- `intervals_dropped` — frames dropped due to **decoder or validation failures** only (Opus payload malformed, decoder threw, mismatched channel count, etc.). Drops caused by local listener state (paused, non-1.0 playback speed, AudioContext suspended, song still loading, no broadcaster yet bound) are NOT counted here — they aren't quality issues the broadcaster can act on.
- `decoder_underruns` — subset of `intervals_dropped` attributable specifically to decoder-thrown errors (kept separate so the broadcaster can distinguish "your stream is malformed" from "the listener's hardware can't keep up").

**Listener → server:**

```json
{
  "type": "audio_quality",
  "broadcaster_id": "a1b2c3d4e5f60718",
  "intervals_received": 120,
  "intervals_late": 3,
  "intervals_dropped": 0,
  "decoder_underruns": 0,
  "report_period_ms": 30000
}
```

**Server behavior:** the server forwards each report **only to the active broadcaster** — peers don't need each other's stats. Reports whose `broadcaster_id` doesn't match the current `room.broadcaster_id` are dropped (stale telemetry from a since-replaced broadcaster).

**Server → broadcaster:** identical fields plus a `from_player_id` field naming the listener that produced the report:

```json
{
  "type": "audio_quality",
  "from_player_id": "9f8e7d6c5b4a3928",
  "broadcaster_id": "a1b2c3d4e5f60718",
  "intervals_received": 120,
  "intervals_late": 3,
  "intervals_dropped": 0,
  "decoder_underruns": 0,
  "report_period_ms": 30000
}
```

The broadcaster aggregates the most recent report per `from_player_id`; stale entries are evicted using the broadcaster's fixed local TTL (currently 3 × the broadcaster's reporting cadence, ≈ 90 s for the 30 s default) so a listener that disconnected without a clean `broadcast_stop` isn't double-counted forever. `report_period_ms` is telemetry metadata describing the listener's cadence; it is not currently used to compute per-entry eviction deadlines.

---

## Audio frame format (Audio WS)

Every audio WS binary frame consists of a fixed-size header followed by the Opus payload. All multi-byte integer fields are **little-endian**. All floating-point fields are IEEE 754 little-endian.

### Header layout (40 bytes)

| Offset | Size | Type    | Field               | Description                                                                                  |
|-------:|-----:|---------|---------------------|----------------------------------------------------------------------------------------------|
|   0    |   4  | char[4] | `magic`             | ASCII `"SMAU"` (Slopsmith Multiplayer AUdio). Frame is rejected if mismatched.               |
|   4    |   2  | u16     | `version`           | Protocol version. v1 = `1`. v1 receivers MUST drop frames where `version != 1`. (Forward-compat handling for specific known-newer versions can be defined in later revisions of this spec.) |
|   6    |   2  | u16     | `flags`             | Bit field. Bit 0 = `tempo_change_at_end` (this interval ends on a tempo change). Other bits reserved; receivers MUST treat them as 0. |
|   8    |   8  | u64     | `interval_index`    | Monotonically increasing, scoped per broadcaster per session. Wraps after `2^64 − 1` (rolls over to `0`) — never in practice. JavaScript receivers MUST parse this as `BigInt` (e.g. `DataView.getBigUint64`) rather than `Number`, since `Number` loses precision above `2^53 − 1`. |
|  16    |   8  | f64     | `chart_time_start`  | Chart playback time (seconds) at which this interval's first sample was captured.            |
|  24    |   8  | f64     | `chart_time_end`    | Chart playback time **immediately after** this interval's last sample (end-exclusive). `chart_time_end - chart_time_start` is the interval's authoritative wall-clock duration in seconds; `chart_time_end` of frame N equals `chart_time_start` of frame N+1 when intervals are contiguous. |
|  32    |   4  | u32     | `sample_count`      | Number of PCM samples encoded into the Opus payload (per channel). **Hint only** — receivers MUST validate against the duration-derived cap (see "Receiver validation" below) before using this value to size any allocation, and SHOULD treat the decoder's actual output as authoritative. |
|  36    |   4  | u32     | `opus_size`         | Size of the Opus payload in bytes. Receiver must verify `frame_length == 40 + opus_size`.    |

### Payload

The byte range `[40, 40 + opus_size)` (half-open — offset 40 inclusive, `40 + opus_size` exclusive) is a **length-prefixed sequence of Opus packets** produced by the WebCodecs `AudioEncoder` configured as:

```js
{
  codec: "opus",
  sampleRate: 48000,
  numberOfChannels: 1,    // v1: mono only
  bitrate: 96000,
  opus: { application: "audio" }
}
```

Per RFC 6716, an individual Opus packet covers **at most 120 ms of audio** — a multi-second interval cannot be represented as a single packet. The encoder is `flush()`ed at each interval boundary and emits one or more `EncodedAudioChunk`s; each becomes one length-prefixed packet record in the SMAU opus payload:

```
[u32 LE packet_count]
( [u32 LE packet_size] [packet_size bytes] ){packet_count}
```

All multi-byte integers are little-endian. Receivers MUST decode each packet independently (one `EncodedAudioChunk` per record fed to `AudioDecoder`) and accumulate the resulting `AudioData` outputs into a single playback buffer for the interval.

**Receiver validation of records.** Receivers MUST drop the entire SMAU frame when:

- `opus_size < 4` (no room for the packet count prefix);
- `packet_count == 0` or `packet_count > 4096` (a 32 s interval at typical 20 ms Opus packetization is ~1,600 packets; even 32 s at 5 ms packetization is ~6,400 — but Opus emits 5 ms frames only when explicitly configured, and v1 broadcasters use the default 20 ms. 4096 is a defensive ceiling that comfortably covers v1 interval bands without conflicting with `MAX_INTERVAL_SEC`);
- any `packet_size == 0` or `packet_size > 8192` (the spec's typical Opus packet is well under 1.5 KB; 8 KB is a defensive ceiling against adversarial inputs);
- the record sequence runs past `40 + opus_size` (truncation).

### Frame size budget and bounds

A 2-second interval at 96 kbps Opus ≈ 24 KB of audio + 40 bytes of header = ~24 KB per frame. Even worst-case slow ballads (30 BPM × 4 beats = 8 s interval) cap out around ~96 KB.

**Hard limit (DoS bound): `MAX_FRAME_BYTES = 262144` (256 KB), inclusive of header.** This is roughly 2× the worst-case legitimate frame, so well-behaved clients will never approach it.

- Senders MUST NOT emit a frame larger than `MAX_FRAME_BYTES`.
- Servers MUST drop any frame larger than `MAX_FRAME_BYTES` (no fan-out) and SHOULD close the offending audio WS with status code `1009` (Message Too Big).
- Servers MUST also reject frames where `frame_length < 40` (header truncation) or where `40 + opus_size != frame_length` (header/body mismatch), with the same drop-and-close behavior.

This bound is the primary backpressure / abuse defense for the relay, since the server otherwise does no payload inspection.

### Receiver validation (defense against malicious peers)

The server forwards binary frames byte-for-byte from one peer to another, so receivers MUST treat header fields — and the broadcaster-asserted parameters in `broadcast_start` — as untrusted input. All checks below are O(1) and MUST be performed before any allocation or decoder call.

**v1 hard limits (constant, not derived from peer-supplied data):**
- `V1_SAMPLE_RATE = 48000` — the only sample rate v1 supports.
- `V1_CHANNEL_COUNT = 1` — mono only in v1.
- `MAX_INTERVAL_SEC = 32` — duration cap. (Generous; even 30 BPM × 8 beats is only 16 seconds.)
- `MAX_SLACK_SAMPLES = 480` — ~10 ms at 48 kHz, to account for Opus packetization padding when bounding `sample_count`.

**Control-plane validation (`broadcast_start` JSON).** Receivers MUST reject (and SHOULD disconnect the audio WS from) any `broadcast_start` where:
- `sample_rate != V1_SAMPLE_RATE`, or
- `channel_count != V1_CHANNEL_COUNT`, or
- `codec != "opus"`, or
- `bitrate` is missing, non-finite, or outside `[16_000, 256_000]`.

Future protocol versions may relax these constraints; v1 receivers reject everything else so the allocation bounds below can use trusted constants.

**Per-frame validation (binary header).** Receivers MUST drop frames where any of the following holds:
- `magic != "SMAU"` or `version != 1` (already covered above).
- `chart_time_start` is not finite (i.e. `NaN` or `±Infinity`).
- `chart_time_end` is not finite.
- `(chart_time_end - chart_time_start)` is not finite, is `≤ 0`, or exceeds `MAX_INTERVAL_SEC`.
- `sample_count > ceil(V1_SAMPLE_RATE * (chart_time_end - chart_time_start)) + MAX_SLACK_SAMPLES`. This bound uses the v1 constant `V1_SAMPLE_RATE`, not any peer-supplied sample rate, so the limit cannot be enlarged by a malicious broadcaster.
- `opus_size + 40 != frame_length` (header/body mismatch — should already have been caught by the server, but receivers SHOULD re-validate independently).

**Note:** `sample_count` is a *hint* for pre-allocating the decoder's output `AudioBuffer`. The canonical sample count for actual playback is whatever the decoder produces; receivers SHOULD treat the decoder output as authoritative and reserve `sample_count` purely for buffer pre-sizing.

---

## Interval-selection algorithm

The host picks the interval length from the chart's BPM, deterministically, and advertises it in `broadcast_start.interval_beats`. This value is **metadata only** — it tells listeners the broadcaster's chosen unit so the UI can label intervals, but listeners **MUST NOT** use it to compute frame-by-frame playback duration. Each frame's authoritative duration is `chart_time_end − chart_time_start` (see Lifecycle §4 below). This is what survives mid-song tempo changes correctly: a single frame that straddles a tempo change has a duration the broadcaster already measured at capture time, which differs from a naive `interval_beats × 60 / bpm` calculation.

The algorithm only ever returns a **whole-measure** value (4 beats in 4/4) or a **two-measure** value (8 beats). It never returns sub-measure intervals — half-measure boundaries do not align with the natural musical pulse a player feels, which makes the previous-interval recording feel jagged rather than locked-in.

```
function selectInterval(bpm):
    if bpm <= 0 or not finite:
        return 4                         # safety default
    secondsPerBeat = 60 / bpm
    if 4 * secondsPerBeat < 1.0:         # very fast: one measure < MIN
        return 8
    return 4                             # default: one measure
```

Decision table:

| BPM range     | Interval (beats) | Duration         | Notes                                              |
|---------------|------------------|------------------|----------------------------------------------------|
| ≤ 240         | 4                | ≥ 1.0 s          | Default. One measure of 4/4.                       |
| > 240         | 8                | depends on BPM   | One measure shrinks below 1.0 s; bump to 2 measures. |
| invalid (≤ 0) | 4                | n/a              | Safety default.                                    |

For very slow songs (e.g. 60 BPM → 4.0 s/measure, 30 BPM → 8.0 s/measure) the interval exceeds the soft 3.0 s target. v1 accepts that — a longer-than-target whole measure is musical; a half-measure is not. v2 may add 2-measure-default behavior at slow tempos if the longer "previous interval" delay turns out to feel sluggish in practice.

The constants `MIN_DURATION_SEC = 1.0`, `DEFAULT_BEATS = 4`, and `FAST_TEMPO_BEATS = 8` live in `audio/select-interval.js`. A pure-Node test file `audio/select-interval.test.js` validates the algorithm with no test-framework dependency: `node audio/select-interval.test.js`.

The helper is currently CommonJS-only because slopsmith core's plugin loader serves only `/api/plugins/{id}/screen.js` (one entry point per plugin) — no generic asset path. Phase 2 will inline the helper into `screen.js` when the broadcast/listen pipelines need it browser-side.

---

## Lifecycle

1. **Room join (existing).** Client `POST`s to `/api/plugins/multiplayer/rooms/{code}/join`, then opens the highway WS.
2. **Audio WS open.** Any time after the highway WS connects, the client opens the audio WS. v1: clients open it eagerly on room join so that broadcasts can start instantly.
3. **Broadcast.** A user toggles "broadcast my sound." The client sends `broadcast_start` on the highway WS. Server validates, stores `broadcaster_id`, broadcasts `broadcaster_changed` to all peers. The client begins streaming binary frames on the audio WS.
4. **Listen.** Every other peer's listener pipeline activates when the client first observes a non-null `broadcaster_id` for the room — either via the initial `connected` room snapshot's `broadcaster_id` field (for clients joining mid-broadcast — late joiners) or via a fresh `broadcaster_changed` event with non-null id (for clients in the room when the broadcast starts). Both signals are treated equivalently. The listener reads binary frames on the audio WS, decodes, and schedules playback using the frame's authoritative interval duration: the next-interval start is `chart_time_end` (and the playable duration is `chart_time_end − chart_time_start`). Listeners MUST NOT compute the interval boundary from `interval_beats × 60 / bpm`, because that calculation breaks when a tempo change occurs inside or at the boundary of an interval; the frame header is the single source of truth.
5. **Stop.** Broadcaster sends `broadcast_stop`, OR the broadcaster's session ends per the deferred session-end rules in `broadcast_stop` above (grace-period expiry without reattach, or takeover by a different `session_id`). Server clears `broadcaster_id`, broadcasts `broadcaster_changed: null`. Listeners drain pending intervals and tear down their audio graph. Transient disconnect-and-reattach within the grace window is NOT a stop and produces no listener-visible event.
6. **Pause/seek (existing host control).** Existing playback semantics apply: when the host pauses, all clients pause. Listeners must `.stop()` any in-flight `AudioBufferSourceNode`s when the chart pauses, and rebuild the playback queue against the new chart time when playback resumes (since pending buffers may now schedule into the past or future incorrectly).
7. **Room teardown.** Existing 60-second grace period applies to both WS endpoints. Audio subscribers are dropped on player disconnect; if the dropped player was the broadcaster, the room is updated and peers notified.

---

## Versioning

The header `version` field exists so future revisions (e.g. stereo support, per-frame Opus packets instead of one-chunk-per-interval) can extend the format. v1 receiver requirements:

- MUST reject frames with `magic != "SMAU"`.
- MUST drop frames where `version != 1`. (Forward-compat handling for specific known-newer versions can be defined in later revisions of this spec; v1 takes the conservative drop-everything-unknown stance.)
- MUST treat unknown bits in `flags` as 0.

The control-message protocol uses the existing `"type"` discriminator and is forward-compatible: unknown types are logged-and-ignored by both ends.
