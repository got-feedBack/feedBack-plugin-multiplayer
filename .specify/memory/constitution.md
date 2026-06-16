# Multiplayer Plugin Constitution

By far the largest plugin in the Slopsmith ecosystem (≈2,100 backend
LoC, ≈5,100 frontend LoC, ≈230 LoC of tests, plus a 367-line wire
PROTOCOL.md). It owns synced rooms, shared queue, recording,
post-song mixdown, and a Ninjam-style interval-quantized peer-audio
broadcast feature.

## Principles

### 1. Two WebSocket Endpoints, One Session

`/ws/plugins/multiplayer/{code}` (highway, JSON) and
`/ws/plugins/multiplayer/{code}/audio` (binary peer audio) share a
single client-supplied `session_id` (`[A-Za-z0-9_-]{1,128}`).
Lifecycle close codes (4401 / 4408 / 4409 / 4410 / 1009 / 1011) carry
all session-state transitions; `event.reason` is unreliable across
WebSocket stacks. Clients MUST branch on `event.code` alone.

### 2. PROTOCOL.md Is the Source of Truth

Wire format, close codes, session lifecycle, frame budgets, and
control-plane validation are normatively specified in
[PROTOCOL.md](../../PROTOCOL.md). Implementation MUST track that
document; if behaviour and PROTOCOL.md disagree, the document is the
spec and the code is the bug.

### 3. Audio Frames Are Binary-Only

The audio WS is binary in both directions. Server connection
rejections use close codes only — no JSON error frames. Frame format
is the 40-byte SMAU header + Opus payload, capped at 256 KB
(`_AUDIO_FRAME_MAX_BYTES`).

### 4. Listener Backpressure Is Bounded

Each listener has a max-size send queue (`_AUDIO_SEND_QUEUE_MAX = 8`).
A chronically slow listener stays bounded in memory; new frames
overwrite the oldest. A resumed listener can fall at most 8 frames
behind.

### 5. Beat-Grid Quantization, Not Wall-Clock

Peer audio intervals are anchored to the **chart's** beat grid (BPM
at broadcast start), not the broadcaster's local clock. This is what
makes the experience "one measure delayed" rather than "lagged" — the
chart audio is the shared anchor across all clients.

### 6. v1 = Single Broadcaster

`broadcaster_id` is single-valued in room state; `broadcast_start`
preempts the previous broadcaster cleanly via
`broadcaster_changed`. Multi-broadcaster mixing is explicitly out of
scope for v1.

### 7. Ephemeral Rooms

Rooms are destroyed 60 s after the last player leaves. There is no
persistence layer for rooms / queues / recordings beyond the
recording artefacts on disk. Restarting Slopsmith resets every
multiplayer state.

### 8. Tests Cover the Lifecycle Edges

`tests/test_session_lifecycle.py`,
`tests/test_audio_ws.py`,
`tests/test_broadcast_control.py` are the smoke tests for the
session FSM. Any change to session lifecycle / close codes /
broadcast control MUST update or extend them.

## Inherits from Slopsmith Core Constitution

- `setup(app, context)` contract.
- WebSocket routes under `/ws/plugins/multiplayer/...`.
- REST routes under `/api/plugins/multiplayer/...`.
- The plugin loader serves only the files referenced by
  `plugin.json` (`screen.html`, `screen.js`, `routes.py`).
  `audio/*.js` and tests are dev / packaging only — `audio/*.js`
  modules are inlined into `screen.js`.

Where this plugin's principles disagree with the core constitution,
the core wins.
