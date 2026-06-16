// Multiplayer plugin — synced rooms, shared queue, optional mixdown

(function () {
'use strict';

// ── State ──────────────────────────────────────────────────────────────
let _ws = null;
let _roomCode = null;
let _playerId = null;
let _sessionId = null;
let _playerName = '';
let _isHost = false;
let _room = null;

// Clock sync
let _clockOffset = 0;       // local_time - server_time in ms
let _pendingSyncResolve = null;

// Playback sync
let _heartbeatInterval = null;
let _driftResetTimer = null;
let _songLoading = false;

// Recording
let _mediaStream = null;
let _mediaRecorder = null;
let _recordedChunks = [];
let _isRecording = false;
let _recStartServerTime = 0;  // server time (ms) when recording started

// Reconnection
let _reconnectAttempts = 0;
let _reconnectTimer = null;
let _intentionalClose = false;

// Audio WebSocket (Phase 2a — connection lifecycle only; the listener +
// broadcast pipelines that actually consume / produce audio frames land
// in Phase 2b). The audio WS runs in parallel to the highway WS, sharing
// the same session_id, with its own independent reconnect timer because
// PROTOCOL.md "Per-endpoint grace" treats the two endpoints' liveness
// independently.
let _audioWs = null;
let _audioReconnectAttempts = 0;
let _audioReconnectTimer = null;

// Tracks (filename, arrangement) of the chart most recently loaded via
// _loadSong. Used by _bootstrapOnConnected to avoid a redundant reload
// on transient WS reconnects when the local <audio> already has the
// right song AND arrangement: _loadSong sets _songLoading = true for
// ~2s, and both _onHeartbeat and _audioListenerHandleFrame drop work
// during that window. Both fields must match — switching arrangement
// (Lead ↔ Rhythm ↔ Bass) calls a different playSong() variant, so a
// filename-only cache would skip a needed reload after a mid-room
// arrangement change. Reset in _cleanup.
let _loadedFilename = null;
let _loadedArrangement = null;
// Promise of an in-flight _loadSong, or null. _bootstrapOnConnected
// checks this so a second _loadSong call (e.g. a reconnect arriving
// mid-song-change) doesn't overlap the first — _loadSong's tail
// pauses non-host audio, and a stale tail firing after the bootstrap
// has resumed playback would leave the guest stuck stopped.
let _loadingPromise = null;
// Generation counter bumped in _cleanup. _doLoadSong captures it at
// entry and refuses to persist _loadedFilename / _loadedArrangement
// if the value has changed — without this, a song load that started
// in room A could finish after the user joined room B and poison
// the cache (a later bootstrap into room B would then skip a needed
// reload thinking room A's song was already loaded).
let _loadGen = 0;
// In-flight bootstrap counter. Incremented at every bootstrap
// entry, decremented in finally. _maybeReissueBroadcastAfterRecovery
// gates on counter == 0 (no in-flight bootstrap mucking with chart
// state). A counter handles the case where multiple bootstraps run
// concurrently without latching: even if the older's finally fires
// while a newer is still in-flight, the counter only hits 0 when
// the LAST bootstrap finishes.
let _bootstrapInFlight = 0;
// Set true by _onSessionEnded when capture is up — the server
// has cleared our broadcaster_id slot during 4408 recovery and we
// need to re-claim it. _maybeReissueBroadcastAfterRecovery only
// fires the actual re-send when this is true, so resends DON'T
// happen for unrelated paths (e.g. user toggling broadcast on
// during the initial connected bootstrap, where the standard
// _broadcastStart path already sends broadcast_start). Cleared
// after a successful re-send. Spotted by Copilot review on PR #9.
let _captureNeedsReissueAfterReset = false;
// Result of the most recently COMPLETED bootstrap (only written
// when _bootstrapInFlight returns to 0 AND the inner returned a
// definitive true/false — supersede returns null and preserves
// the previous value). Initially true so a fresh non-recovery
// toggle of broadcast doesn't gate on a recovery that never ran.
// Preserved across _onSessionEnded — only completed bootstrap
// attempts update this flag, so a non-bootstrap supersede during
// 4408 recovery doesn't latch the gate to false. Codex multi-
// round on PR #8 / PR #9.
let _lastBootstrapSucceeded = true;

// Idempotency guard for _onSessionEnded: when a 4408 (grace expired) close
// fires, BOTH endpoint handlers see it. Resetting session_id twice would
// produce two different new ids, and the two reconnects would then collide
// (one would arrive as session A, the other as session B → server treats
// it as a Rule 3 takeover loop). _resetMarker remembers the new id we just
// minted; the second handler sees _sessionId == _resetMarker and skips.
//
// _resetMarker stays armed until BOTH endpoints have committed to the
// new session_id (highway received its `connected` message AND audio's
// onopen fired). Clearing on the first endpoint's commit alone reopens
// the race: a late 4408 from the still-stale OTHER endpoint would see
// _resetMarker == null and mint a SECOND fresh session_id, invalidating
// the recovery.
let _resetMarker = null;
let _highwayAuthedAfterReset = true;  // initially true — no reset has happened
let _audioOpenedAfterReset = true;

// Original functions (saved for restore on leave)
let _origTogglePlay = null;
let _origSeekBy = null;
let _origSetSpeed = null;

const STORAGE_KEY = 'slopsmith_mp';
const SESSION_STORAGE_KEY = 'mp_session';
const SYNC_ROUNDS = 5;
const HEARTBEAT_HZ = 100;  // ms between heartbeats

// Close codes from PROTOCOL.md "Endpoints" / "v1 server policy".
const CLOSE_GRACE_EXPIRED = 4408;
const CLOSE_SUPERSEDED = 4409;
const CLOSE_REPLACED = 4410;

// ── SMAU audio frame codec (inlined) ─────────────────────────────────────────
//
// Source of truth: audio/smau-frame.js (CommonJS, exercised by
// audio/smau-frame.test.js — `node audio/smau-frame.test.js`). Inlined here
// because slopsmith core's plugin loader only serves the single screen.js
// entry point per plugin (see plugins/__init__.py). When changing this
// codec, change BOTH copies and re-run the Node tests.
//
// Wire format defined by PROTOCOL.md "Audio frame format (Audio WS)":
// 40-byte little-endian header + Opus payload, validated against v1 hard
// constants (NOT peer-supplied broadcast_start params) so a malicious
// broadcaster cannot enlarge the bounds.
const SMAU_HEADER_LEN = 40;
const SMAU_VERSION = 1;
const SMAU_MAX_FRAME_BYTES = 262144;   // 256 KB
const SMAU_V1_SAMPLE_RATE = 48000;
const SMAU_V1_CHANNEL_COUNT = 1;
const SMAU_MAX_INTERVAL_SEC = 32;
const SMAU_MAX_SLACK_SAMPLES = 480;    // ~10 ms at 48 kHz
const SMAU_FLAG_TEMPO_CHANGE_AT_END = 0x0001;
const SMAU_MAGIC_0 = 0x53; // 'S'
const SMAU_MAGIC_1 = 0x4d; // 'M'
const SMAU_MAGIC_2 = 0x41; // 'A'
const SMAU_MAGIC_3 = 0x55; // 'U'

function _smauDecodeFrame(buf) {
    let bytes;
    if (buf instanceof Uint8Array) {
        bytes = buf;
    } else if (buf instanceof ArrayBuffer) {
        bytes = new Uint8Array(buf);
    } else if (buf && ArrayBuffer.isView(buf)) {
        bytes = new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
    } else {
        return { ok: false, reason: 'invalid_buffer' };
    }
    const len = bytes.byteLength;
    if (len < SMAU_HEADER_LEN) return { ok: false, reason: 'too_small' };
    if (len > SMAU_MAX_FRAME_BYTES) return { ok: false, reason: 'frame_too_big' };
    if (bytes[0] !== SMAU_MAGIC_0 || bytes[1] !== SMAU_MAGIC_1
        || bytes[2] !== SMAU_MAGIC_2 || bytes[3] !== SMAU_MAGIC_3) {
        return { ok: false, reason: 'magic_mismatch' };
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, len);
    const version = view.getUint16(4, true);
    if (version !== SMAU_VERSION) return { ok: false, reason: 'version_mismatch' };
    const flags = view.getUint16(6, true);
    const intervalIndex = view.getBigUint64(8, true);
    const chartTimeStart = view.getFloat64(16, true);
    const chartTimeEnd = view.getFloat64(24, true);
    const sampleCount = view.getUint32(32, true);
    const opusSize = view.getUint32(36, true);
    if (SMAU_HEADER_LEN + opusSize !== len) return { ok: false, reason: 'size_mismatch' };
    if (!Number.isFinite(chartTimeStart) || !Number.isFinite(chartTimeEnd)) {
        return { ok: false, reason: 'invalid_chart_time' };
    }
    const duration = chartTimeEnd - chartTimeStart;
    if (!Number.isFinite(duration) || duration <= 0 || duration > SMAU_MAX_INTERVAL_SEC) {
        return { ok: false, reason: 'invalid_duration' };
    }
    const maxSamples = Math.ceil(SMAU_V1_SAMPLE_RATE * duration) + SMAU_MAX_SLACK_SAMPLES;
    if (sampleCount > maxSamples) return { ok: false, reason: 'sample_count_too_high' };

    const opus = new Uint8Array(bytes.buffer, bytes.byteOffset + SMAU_HEADER_LEN, opusSize);
    return {
        ok: true,
        header: {
            version: version,
            flags: flags,
            intervalIndex: intervalIndex,
            chartTimeStart: chartTimeStart,
            chartTimeEnd: chartTimeEnd,
            sampleCount: sampleCount,
            opusSize: opusSize,
            frameLength: len,
            tempoChangeAtEnd: (flags & SMAU_FLAG_TEMPO_CHANGE_AT_END) !== 0,
        },
        opus: opus,
    };
}

// SMAU encoder, mirror of audio/smau-frame.js. Used by the broadcast
// path (Phase 2d) to build outbound binary frames. Throws on invalid
// input — strict at the source so we never put a frame on the wire
// that _smauDecodeFrame above (or peer receivers) would discard. When
// changing this codec, update both this inlined copy AND
// audio/smau-frame.js + audio/smau-frame.test.js.
const SMAU_U64_MAX = (1n << 64n) - 1n;
const SMAU_U32_MAX = 0xffffffff;

function _smauToBigUint64(value, fieldName) {
    let bi;
    if (typeof value === 'bigint') {
        bi = value;
    } else if (typeof value === 'number') {
        if (!Number.isInteger(value) || value < 0 || value > Number.MAX_SAFE_INTEGER) {
            throw new RangeError(fieldName + ' must be a non-negative integer ≤ Number.MAX_SAFE_INTEGER, or a BigInt');
        }
        bi = BigInt(value);
    } else {
        throw new TypeError(fieldName + ' must be a BigInt or non-negative integer Number');
    }
    if (bi < 0n || bi > SMAU_U64_MAX) {
        throw new RangeError(fieldName + ' out of u64 range');
    }
    return bi;
}

function _smauEncodeFrame(fields) {
    if (!fields || typeof fields !== 'object') {
        throw new TypeError('_smauEncodeFrame: fields object required');
    }
    const intervalIndex = _smauToBigUint64(fields.intervalIndex, 'intervalIndex');
    const chartTimeStart = fields.chartTimeStart;
    const chartTimeEnd = fields.chartTimeEnd;
    const sampleCount = fields.sampleCount;
    const flags = fields.flags == null ? 0 : fields.flags;
    const opus = fields.opus;

    if (!(typeof chartTimeStart === 'number' && Number.isFinite(chartTimeStart))) {
        throw new RangeError('chartTimeStart must be a finite number');
    }
    if (!(typeof chartTimeEnd === 'number' && Number.isFinite(chartTimeEnd))) {
        throw new RangeError('chartTimeEnd must be a finite number');
    }
    const duration = chartTimeEnd - chartTimeStart;
    if (!Number.isFinite(duration) || duration <= 0) {
        throw new RangeError('chartTimeEnd must be strictly greater than chartTimeStart');
    }
    if (duration > SMAU_MAX_INTERVAL_SEC) {
        throw new RangeError('interval duration ' + duration + 's exceeds SMAU_MAX_INTERVAL_SEC');
    }
    if (!(Number.isInteger(sampleCount) && sampleCount >= 0 && sampleCount <= SMAU_U32_MAX)) {
        throw new RangeError('sampleCount must be a u32 integer');
    }
    // Mirror the receiver's sample_count cap so encode/decode round-
    // trips never produce a frame the receiver would discard with
    // sample_count_too_high.
    const maxSamples = Math.ceil(SMAU_V1_SAMPLE_RATE * duration) + SMAU_MAX_SLACK_SAMPLES;
    if (sampleCount > maxSamples) {
        throw new RangeError('sampleCount ' + sampleCount + ' exceeds receiver cap ' + maxSamples);
    }
    if (!(Number.isInteger(flags) && flags >= 0 && flags <= 0xffff)) {
        throw new RangeError('flags must be a u16 integer');
    }
    if (!(opus instanceof Uint8Array)) {
        throw new TypeError('opus must be a Uint8Array');
    }
    if (opus.byteLength > SMAU_U32_MAX) {
        throw new RangeError('opus payload size out of u32 range');
    }
    const frameLen = SMAU_HEADER_LEN + opus.byteLength;
    if (frameLen > SMAU_MAX_FRAME_BYTES) {
        throw new RangeError('frame length ' + frameLen + ' exceeds SMAU_MAX_FRAME_BYTES');
    }

    const buf = new ArrayBuffer(frameLen);
    const bytes = new Uint8Array(buf);
    const view = new DataView(buf);
    bytes[0] = SMAU_MAGIC_0;
    bytes[1] = SMAU_MAGIC_1;
    bytes[2] = SMAU_MAGIC_2;
    bytes[3] = SMAU_MAGIC_3;
    view.setUint16(4, SMAU_VERSION, true);
    view.setUint16(6, flags, true);
    view.setBigUint64(8, intervalIndex, true);
    view.setFloat64(16, chartTimeStart, true);
    view.setFloat64(24, chartTimeEnd, true);
    view.setUint32(32, sampleCount, true);
    view.setUint32(36, opus.byteLength, true);
    bytes.set(opus, SMAU_HEADER_LEN);
    return buf;
}

// Per-session inbound-frame counters. Cleared in _cleanup. The Phase 5
// listener-side `audio_quality` highway message will read these so the
// broadcaster can see "N frames dropped due to <reason>" in its UI.
// Exposed on window for debugging via _audioGetRxStats(); not a public API.
let _audioRxFramesValid = 0;
let _audioRxFramesDropped = 0;
let _audioRxFramesLate = 0;        // Phase 2c: incremented in listener pipeline
let _audioRxFramesScheduled = 0;   // Phase 2c: incremented in listener pipeline
const _audioRxDropReasons = Object.create(null);

function _audioRxRecordDrop(reason) {
    _audioRxFramesDropped++;
    _audioRxDropReasons[reason] = (_audioRxDropReasons[reason] || 0) + 1;
}

function _audioRxResetStats() {
    _audioRxFramesValid = 0;
    _audioRxFramesDropped = 0;
    _audioRxFramesLate = 0;
    _audioRxFramesScheduled = 0;
    for (const key of Object.keys(_audioRxDropReasons)) {
        delete _audioRxDropReasons[key];
    }
    // The audio_quality snapshot baselines off the cumulative
    // counters — when those reset (4408 session recovery, room
    // change), the snapshot must reset too or the next delta
    // would be negative or roll the previous session into the
    // new one. Spotted by codex review on Phase 2e.
    _audioQualitySnapshot = {
        scheduled: 0, dropped: 0, late: 0, decoderUnderruns: 0, broadcasterId: null,
    };
}

function _audioGetRxStats() {
    return {
        valid: _audioRxFramesValid,
        dropped: _audioRxFramesDropped,
        dropReasons: Object.assign({}, _audioRxDropReasons),
        late: _audioRxFramesLate,
        scheduled: _audioRxFramesScheduled,
    };
}

// ── Listener-side audio_quality telemetry (Phase 2e) ─────────────────────────
//
// Periodic report sent to the broadcaster (via the highway WS, server
// relays only to the active broadcaster). Reports DELTAS over a
// fixed window so the broadcaster sees "N late frames in last 30s",
// not cumulative-since-page-load. Snapshots roll forward EVERY tick
// (regardless of whether we sent), so a quiet window followed by an
// active one doesn't cause a multi-window pile-up that would make the
// broadcaster's stats misrepresent "the last 30 s".
//
// `intervals_dropped` counts only "real" failure reasons (decoder /
// validation errors). Local-state drops (listener_paused,
// listener_speed, song_loading, context_suspended, no_broadcaster,
// handoff_suppress) are excluded — they don't represent quality
// problems the broadcaster can do anything about, and bundling them
// in would systematically overstate issues. `late` is excluded from
// the dropped tally and tracked separately in `intervals_late`.

const AUDIO_QUALITY_REPORT_PERIOD_MS = 30000;
// Drop-reason keys that count as a real decoder/validation failure.
// Any reason NOT in this set is excluded from the dropped-delta
// (e.g. listener_paused doesn't reflect a problem with the
// broadcaster's stream). Includes both SMAU header-validation drops
// (emitted by _smauDecodeFrame) and downstream decode/validation
// failures from the listener pipeline. Spotted by codex review on
// Phase 2e.
const AUDIO_QUALITY_REAL_DROP_REASONS = new Set([
    // SMAU frame-validation reasons (parse failures before decode)
    'invalid_buffer',
    'too_small',
    'frame_too_big',
    'magic_mismatch',
    'version_mismatch',
    'size_mismatch',
    'invalid_chart_time',
    'invalid_duration',
    'sample_count_too_high',
    // Decoder / Opus / payload validation
    'decoder_error',
    'decoded_channel_mismatch',
    'decoded_sample_rate_mismatch',
    'decoded_overflow',
    'decoded_copy_failed',
    'opus_unsupported',
    'decoder_unavailable',
    'webcodecs_unavailable',
    'opus_payload_too_small',
    'opus_packet_count_invalid',
    'opus_truncated',
    'opus_packet_invalid',
    'opus_trailing_bytes',
    'decode_call_failed',
    'non_binary',
]);
let _audioQualityTimer = null;
let _audioQualitySnapshot = {
    scheduled: 0,
    dropped: 0,
    late: 0,
    decoderUnderruns: 0,
    broadcasterId: null,
};

function _audioQualityRealDropTotal() {
    let total = 0;
    for (const reason of AUDIO_QUALITY_REAL_DROP_REASONS) {
        total += _audioRxDropReasons[reason] || 0;
    }
    return total;
}

function _audioQualityDecoderUnderruns() {
    // Treat decoder_error as an underrun signal — the only drop
    // reason that maps to "the decoder couldn't keep up" semantically.
    return _audioRxDropReasons.decoder_error || 0;
}

function _audioQualitySendReport() {
    const broadcasterId = _audioListenerBroadcasterId;
    if (!broadcasterId || broadcasterId === _playerId) {
        // No remote broadcaster — reset snapshot so the next active
        // broadcast starts from zero rather than whatever stats had
        // accumulated against a previous broadcaster.
        if (_audioQualitySnapshot.broadcasterId !== null) {
            _audioQualitySnapshot = {
                scheduled: 0, dropped: 0, late: 0, decoderUnderruns: 0,
                broadcasterId: null,
            };
        }
        return;
    }
    // Snapshot of the current cumulative counters, computed once so
    // the post-send roll-forward uses the SAME values that produced
    // the deltas (otherwise a frame counted between delta-compute
    // and roll-forward would be lost from the next window).
    const cur = {
        scheduled: _audioRxFramesScheduled,
        dropped: _audioQualityRealDropTotal(),
        late: _audioRxFramesLate,
        decoderUnderruns: _audioQualityDecoderUnderruns(),
        broadcasterId: broadcasterId,
    };
    // Any broadcaster change reseeds the snapshot at the current
    // cumulative counters and skips this tick. Cumulative RX counters
    // only reset on session-recovery / room-change (see
    // _audioRxResetStats), NOT on broadcaster_changed: null. So a
    // null → broadcaster transition can still carry frames from a
    // previously-ended broadcast in the same session — baselining at
    // zero would attribute those historical frames to the new
    // broadcast. Reseeding at `cur` costs us the first 30 s window
    // but keeps reports correctly scoped to the active broadcast.
    // Round 2 of Copilot review on PR #10 caught the regression I
    // introduced in round 1 by trying to preserve the first window.
    if (_audioQualitySnapshot.broadcasterId !== broadcasterId) {
        _audioQualitySnapshot = cur;
        return;
    }
    const intervalsReceived = cur.scheduled - _audioQualitySnapshot.scheduled;
    const intervalsDropped = cur.dropped - _audioQualitySnapshot.dropped;
    const intervalsLate = cur.late - _audioQualitySnapshot.late;
    const decoderUnderruns = cur.decoderUnderruns - _audioQualitySnapshot.decoderUnderruns;
    // Roll snapshot forward UNCONDITIONALLY before any early return,
    // so a quiet window (or a window during a transient WS disconnect)
    // doesn't pile up into the next non-empty one. The WS send below
    // is the only thing gated on readyState. Spotted by codex review
    // on Phase 2e and Copilot review on PR #10.
    _audioQualitySnapshot = cur;
    // Skip the WS send when nothing changed — saves bandwidth without
    // affecting the snapshot rollover.
    if (
        intervalsReceived === 0
        && intervalsDropped === 0
        && intervalsLate === 0
        && decoderUnderruns === 0
    ) return;
    if (!_ws || _ws.readyState !== WebSocket.OPEN) return;
    try {
        _ws.send(JSON.stringify({
            type: 'audio_quality',
            broadcaster_id: broadcasterId,
            intervals_received: intervalsReceived,
            intervals_late: intervalsLate,
            intervals_dropped: intervalsDropped,
            decoder_underruns: decoderUnderruns,
            report_period_ms: AUDIO_QUALITY_REPORT_PERIOD_MS,
        }));
    } catch (_e) { /* WS could close concurrently; ignore */ }
}

function _audioQualityStartTimer() {
    if (_audioQualityTimer) return;
    _audioQualityTimer = setInterval(_audioQualityOnTick, AUDIO_QUALITY_REPORT_PERIOD_MS);
}

function _audioQualityStopTimer() {
    if (_audioQualityTimer) {
        clearInterval(_audioQualityTimer);
        _audioQualityTimer = null;
    }
    _audioQualitySnapshot = {
        scheduled: 0, dropped: 0, late: 0, decoderUnderruns: 0, broadcasterId: null,
    };
}

// Single timer tick handles both the listener-side send AND the
// broadcaster-side stale-listener eviction. Without doing both here,
// a broadcaster whose listeners all stop reporting (or disconnect
// without a clean broadcast_stop) would never re-render and never
// evict stale entries — the panel would show a phantom listener
// indefinitely. Spotted by codex review on Phase 2e.
function _audioQualityOnTick() {
    _audioQualitySendReport();
    if (_audioQualityRxByListener && _audioQualityRxByListener.size > 0) {
        _audioQualityRxRender();
    }
}

// ── Broadcaster-side audio_quality aggregation (Phase 2e) ────────────────────
//
// We're the broadcaster. Each listener sends us a 30s-window report
// (see PROTOCOL.md "audio_quality"); the server adds `from_player_id`
// before forwarding, so we can key reports per listener. The UI shows
// aggregate stats over the most recent window across all listeners
// who have reported recently — stale entries (no report in
// AUDIO_QUALITY_LISTENER_TTL_MS) are evicted so a listener that
// disconnected without a stop event isn't double-counted forever.

const AUDIO_QUALITY_LISTENER_TTL_MS = AUDIO_QUALITY_REPORT_PERIOD_MS * 3;
const _audioQualityRxByListener = new Map();

// Coerce a peer-supplied counter to a non-negative safe integer.
// `| 0` would force into a signed 32-bit range so values >2^31-1
// wrap negative; explicit Number() + clamp keeps both NaN/non-numeric
// inputs and oversized values from poisoning the aggregator. Spotted
// by Copilot review on PR #10.
function _audioQualitySanitizeCount(value) {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return 0;
    return Math.min(Math.floor(n), Number.MAX_SAFE_INTEGER);
}

function _audioQualityRxOnReport(msg) {
    const fromId = msg.from_player_id;
    if (!fromId) return;
    // Defensive: ignore reports keyed against a different broadcaster
    // (we already cleared/became broadcaster of a different session).
    if (msg.broadcaster_id && msg.broadcaster_id !== _playerId) return;
    // Note: msg.report_period_ms is informational metadata in the
    // protocol but not used here — eviction TTL is fixed at
    // 3 × AUDIO_QUALITY_REPORT_PERIOD_MS (the broadcaster's local
    // constant), regardless of what cadence the listener happens to
    // be reporting at. Spotted by Copilot review on PR #10 round 3.
    _audioQualityRxByListener.set(fromId, {
        receivedAt: Date.now(),
        intervalsReceived: _audioQualitySanitizeCount(msg.intervals_received),
        intervalsLate: _audioQualitySanitizeCount(msg.intervals_late),
        intervalsDropped: _audioQualitySanitizeCount(msg.intervals_dropped),
        decoderUnderruns: _audioQualitySanitizeCount(msg.decoder_underruns),
    });
    _audioQualityRxRender();
}

function _audioQualityRxEvictStale() {
    const now = Date.now();
    for (const [id, entry] of _audioQualityRxByListener) {
        if (now - entry.receivedAt > AUDIO_QUALITY_LISTENER_TTL_MS) {
            _audioQualityRxByListener.delete(id);
        }
    }
}

function _audioQualityRxRender() {
    _audioQualityRxEvictStale();
    const el = document.getElementById('mp-broadcast-stats');
    if (!el) return;
    // Show only when we're the broadcaster AND at least one listener
    // has reported recently. Otherwise hide the line entirely so the
    // panel stays compact in the common "broadcasting alone" case.
    const isBroadcasting = _captureBroadcasterAcked
        && _room && _room.broadcaster_id === _playerId;
    if (!isBroadcasting || _audioQualityRxByListener.size === 0) {
        el.classList.add('hidden');
        el.textContent = '';
        return;
    }
    let totalLate = 0;
    let totalDropped = 0;
    let totalUnderruns = 0;
    for (const entry of _audioQualityRxByListener.values()) {
        totalLate += entry.intervalsLate;
        totalDropped += entry.intervalsDropped;
        totalUnderruns += entry.decoderUnderruns;
    }
    const n = _audioQualityRxByListener.size;
    // `decoder_underruns` is a SUBSET of `intervals_dropped` (per
    // PROTOCOL.md). Show the rest of dropped (non-decoder) and the
    // decoder count side-by-side so the broadcaster can distinguish
    // "stream is malformed" from "listener can't keep up", without
    // double-counting them in the issue total. A buggy / older
    // client could report `decoder_underruns > intervals_dropped`
    // (violating the subset invariant); use Math.max to keep
    // underruns visible in that case. Spotted by Copilot review
    // on PR #10 rounds 1 + 4.
    const effectiveDropped = Math.max(totalDropped, totalUnderruns);
    const totalDroppedNonDecoder = Math.max(0, effectiveDropped - totalUnderruns);
    const issues = totalLate + effectiveDropped;
    let txt = `${n} listener${n === 1 ? '' : 's'}`;
    if (issues === 0) {
        txt += ' — no issues';
    } else {
        const parts = [];
        if (totalLate > 0) parts.push(`${totalLate} late`);
        if (totalDroppedNonDecoder > 0) parts.push(`${totalDroppedNonDecoder} dropped`);
        if (totalUnderruns > 0) parts.push(`${totalUnderruns} decoder`);
        txt += ` — ${parts.join(', ')}`;
    }
    el.textContent = txt;
    el.classList.remove('hidden');
}

function _audioQualityRxReset() {
    _audioQualityRxByListener.clear();
    _audioQualityRxRender();
}

// ── Listener pipeline (Phase 2c) ─────────────────────────────────────────────
//
// Decode validated SMAU frames via WebCodecs AudioDecoder and schedule
// playback so each interval starts when the listener's chart playback
// reaches `chartTimeStart + intervalDuration` — i.e. one interval AFTER the
// broadcaster recorded it (the structured Ninjam offset). Conversion from
// chart-time → AudioContext.currentTime uses the listener's <audio> element
// as the chart clock: at any given instant `audio.currentTime` is the
// listener's chart playback position and AudioContext.currentTime is the
// monotonic rendering clock. Their delta lets us schedule:
//
//   target_chart    = frame.chartTimeStart + (chartTimeEnd - chartTimeStart)
//   chart_now       = audio.currentTime           // (paused → bail out)
//   delta_sec       = target_chart - chart_now
//   target_ac_time  = audioCtx.currentTime + delta_sec
//
// If delta_sec is negative the frame is late (can't schedule into the past)
// and the frame is dropped with a 'late' counter increment. v1 ALSO bails out
// when the chart is paused or playing at non-1.0 speed: speed changes warp
// chart_now relative to AudioContext.currentTime, so a peer recording made
// at 1.0x would not stay in alignment. Documented limitation; Phase 2d /
// release polish may add `playbackRate` time-stretching on the source node.
//
// Decoder output is correlated to header metadata via the encoded chunk's
// timestamp field. We allocate a monotonic per-pipeline counter rather than
// reuse interval_index because EncodedAudioChunk.timestamp is Number-typed
// (microseconds) and interval_index is u64 / BigInt — Number can't hold it
// without precision loss above 2^53.
//
// WebCodecs AudioDecoder is required. v1 surfaces a one-shot console warning
// when it's missing (notably Firefox <130) and silently drops decode work.

const LATE_FRAME_GRACE_SEC = 0.005; // 5 ms slack for browser rAF jitter

let _audioListenerCtx = null;          // AudioContext, lazy
let _audioListenerBroadcasterId = null;
let _audioListenerBroadcastParams = null;
// _audioListenerActiveDecoder is { dec, pending, nextTs, epoch } or
// null. Each decoder instance owns its own `pending` Map so flush()
// output from a drained-but-not-yet-closed decoder can never collide
// with timestamps issued by a fresh replacement (codex round-N
// concurrency case: leave + rejoin can otherwise reuse timestamps from
// the old decoder's still-pending output and schedule stale audio
// against new metadata).
let _audioListenerActiveDecoder = null;
// Bumped on every chart-time-breaking event (chart pause / seek / song
// change / 4408 recovery / cleanup / new-broadcaster takeover). Each
// decoder wrapper captures the current epoch at creation; output
// handler drops its AudioData if the wrapper's epoch is no longer
// current. This catches the case where a graceful flush() of a
// broadcast_stop'd decoder is still in flight when chart-time gets
// invalidated — without the epoch check, those late outputs would
// schedule stale audio against the new chart timeline.
let _audioListenerEpoch = 0;
// Handoff suppression window. SMAU frames carry no sender identity, so
// when a broadcaster_changed event announces a NEW broadcaster, packets
// that the previous broadcaster's worker had already written to TCP
// can still arrive on /audio after the highway event reaches us. Those
// frames would otherwise be decoded under the new broadcaster_id.
// During the suppression window any inbound /audio frame is dropped
// with reason 'handoff_suppress'. Server-side worker purge is the
// primary defense (cancels in-flight sends + drains queues); this is
// belt-and-suspenders for bytes already in the OS TCP buffer at purge
// time. 200 ms is a tight bound on "TCP-buffered straggler RTT" while
// short enough to NOT clip the first frame from new broadcasters that
// use very short intervals (e.g. interval_beats=1 on fast tempos);
// PROTOCOL.md's selectInterval algorithm only uses 4 / 8 beats so v1
// broadcasters won't hit this in practice, but we keep the window
// tight as a forward-compat safety.
const HANDOFF_SUPPRESS_MS = 200;
let _audioListenerHandoffSuppressUntil = 0;
let _audioListenerGain = null;         // GainNode → destination
const _audioListenerScheduledSources = new Set();
let _audioListenerWebCodecsWarned = false;
// Sticky "this browser cannot decode our Opus stream" flag. Set on the
// first AudioDecoder.configure() failure; subsequent inbound frames take
// the cheap silent-no-op path instead of constructing + closing a fresh
// AudioDecoder per frame (browsers that expose AudioDecoder but lack
// Opus support throw on every configure). Reset only on _cleanup so
// reattaching to a new browser process is the only way to re-test.
let _audioListenerOpusUnsupported = false;

function _audioListenerHasWebCodecs() {
    return typeof AudioDecoder !== 'undefined'
        && typeof EncodedAudioChunk !== 'undefined';
}

function _audioListenerWarnOnceMissingWebCodecs() {
    if (_audioListenerWebCodecsWarned) return;
    _audioListenerWebCodecsWarned = true;
    console.warn('[MP] WebCodecs AudioDecoder unavailable; peer audio playback disabled in this browser.');
}

function _audioListenerEnsureContext() {
    if (_audioListenerCtx) return _audioListenerCtx;
    const Ctor = window.AudioContext || window.webkitAudioContext;
    if (!Ctor) return null;
    // sampleRate matches the v1 broadcast format so AudioBuffers from the
    // decoder don't need resampling on insert.
    _audioListenerCtx = new Ctor({ sampleRate: SMAU_V1_SAMPLE_RATE, latencyHint: 'interactive' });
    _audioListenerGain = _audioListenerCtx.createGain();
    _audioListenerGain.gain.value = 1.0;
    _audioListenerGain.connect(_audioListenerCtx.destination);
    // Best-effort resume — Chrome/Firefox in active tabs usually create
    // contexts in 'running' state, but on autoplay-restricted browsers
    // (Safari, sometimes Chrome) the context may be 'suspended' until
    // a user gesture. _audioListenerMaybeResumeContext() retries
    // opportunistically from chart play / broadcast-toggle paths so
    // peer audio comes online as soon as the user interacts. For
    // non-host listeners who never trigger those paths themselves,
    // also install a document-level user-gesture handler that calls
    // resume() the next time the user interacts with the page.
    _audioListenerMaybeResumeContext();
    _audioListenerInstallResumeGesture();
    return _audioListenerCtx;
}

let _audioListenerResumeGestureHandler = null;

function _audioListenerInstallResumeGesture() {
    if (_audioListenerResumeGestureHandler) return;
    _audioListenerResumeGestureHandler = () => _audioListenerMaybeResumeContext();
    // Capture phase + listed gesture types match what browsers
    // recognize as user activation for Web Audio resume(). Idempotent;
    // remains live until _audioListenerCleanup tears it down.
    document.addEventListener('pointerdown', _audioListenerResumeGestureHandler, true);
    document.addEventListener('keydown', _audioListenerResumeGestureHandler, true);
    document.addEventListener('touchstart', _audioListenerResumeGestureHandler, true);
}

function _audioListenerUninstallResumeGesture() {
    if (!_audioListenerResumeGestureHandler) return;
    document.removeEventListener('pointerdown', _audioListenerResumeGestureHandler, true);
    document.removeEventListener('keydown', _audioListenerResumeGestureHandler, true);
    document.removeEventListener('touchstart', _audioListenerResumeGestureHandler, true);
    _audioListenerResumeGestureHandler = null;
}

function _audioListenerMaybeResumeContext() {
    const ctx = _audioListenerCtx;
    if (!ctx || ctx.state !== 'suspended' || typeof ctx.resume !== 'function') return;
    // Suppress unhandled-rejection noise on browsers where resume()
    // requires a user gesture and the current call site isn't one
    // (we'll get another chance on the next chart play).
    try {
        const p = ctx.resume();
        if (p && typeof p.catch === 'function') p.catch(() => {});
    } catch (e) { /* */ }
}

function _audioListenerHandleDecodedAudio(audioData, wrapper) {
    try {
        const ctx = _audioListenerCtx;
        if (!ctx) return;
        // AudioContext is suspended (autoplay-restricted browsers,
        // notably Safari/iOS and some Chrome cases without a user
        // gesture). ctx.currentTime is effectively frozen, so any
        // schedule we'd compute would either play out of sync once
        // resume() lands or never become audible. Drop instead;
        // _audioListenerMaybeResumeContext() will pick the context
        // back up on the next user-gesture transport interaction,
        // and subsequent frames will schedule normally.
        if (ctx.state !== 'running') {
            _audioRxRecordDrop('context_suspended');
            return;
        }
        // Epoch gate: if chart-time has been invalidated (chart pause /
        // seek / song change / 4408 / cleanup) since this wrapper was
        // created, its pending metadata refers to a chart timeline
        // that's no longer in effect — drop instead of scheduling stale
        // audio against the new timeline. This is the path that catches
        // late outputs from a still-flushing decoder after broadcast_stop.
        if (wrapper.epoch !== _audioListenerEpoch) return;
        const meta = wrapper.pending.get(audioData.timestamp);
        wrapper.pending.delete(audioData.timestamp);
        if (!meta || !meta.acc) return; // stale or torn down
        const acc = meta.acc;
        if (acc.finalized) return;
        if (acc.epoch !== _audioListenerEpoch) {
            // Chart-time was invalidated after the interval was queued
            // for decode. Drop without scheduling.
            acc.finalized = true;
            return;
        }

        const numFrames = audioData.numberOfFrames;
        const channels = audioData.numberOfChannels || 1;
        // Channel count is bounded: v1 is mono only and the decoder
        // is configured for numberOfChannels=1, but a misconfigured /
        // patched browser could still produce multi-channel output.
        if (channels !== SMAU_V1_CHANNEL_COUNT) {
            _audioRxRecordDrop('decoded_channel_mismatch');
            acc.finalized = true;
            return;
        }
        // Sample rate is also bounded: the AudioBuffer we build below
        // hard-codes SMAU_V1_SAMPLE_RATE, so any mismatch from the
        // decoder (in principle should never happen — we configure
        // the decoder for 48k — but a future codec/config or a
        // patched browser could disagree) would play at the wrong
        // pitch. Spotted by Copilot review on PR #8 round 8.
        const dataSampleRate = audioData.sampleRate || SMAU_V1_SAMPLE_RATE;
        if (dataSampleRate !== SMAU_V1_SAMPLE_RATE) {
            _audioRxRecordDrop('decoded_sample_rate_mismatch');
            acc.finalized = true;
            return;
        }

        // Lazy-build the per-interval PCM buffer on the first packet
        // that contributes samples. Bound it against the validated
        // chart-time duration + slack so a malicious broadcaster can't
        // over-allocate by emitting too many or oversize Opus packets.
        if (!acc.pcmBuffer) {
            const duration = acc.header.chartTimeEnd - acc.header.chartTimeStart;
            const maxFrames = Math.ceil(SMAU_V1_SAMPLE_RATE * duration) + SMAU_MAX_SLACK_SAMPLES;
            acc.pcmBuffer = new Float32Array(maxFrames);
            acc.maxFrames = maxFrames;
        }

        if (numFrames > 0) {
            if (acc.writeOffset + numFrames > acc.maxFrames) {
                _audioRxRecordDrop('decoded_overflow');
                acc.finalized = true;
                return;
            }
            // copyTo() can throw on unsupported conversion formats /
            // internal decoder quirks. Catch in-place so the exception
            // doesn't bubble out of this output callback as an
            // unhandled error and destabilize the decode pipeline.
            try {
                const sub = acc.pcmBuffer.subarray(acc.writeOffset, acc.writeOffset + numFrames);
                audioData.copyTo(sub, { planeIndex: 0, format: 'f32-planar' });
            } catch (_err) {
                _audioRxRecordDrop('decoded_copy_failed');
                acc.finalized = true;
                return;
            }
            acc.writeOffset += numFrames;
        }
        acc.packetsReceived++;

        if (acc.packetsReceived < acc.packetsExpected) return;

        // All packets for this interval decoded. Build AudioBuffer +
        // schedule a single source against the chart clock.
        const totalFrames = acc.writeOffset;
        if (totalFrames === 0) {
            acc.finalized = true;
            return;
        }

        const audio = document.getElementById('audio');
        if (!audio || audio.paused) {
            _audioRxRecordDrop('listener_paused');
            acc.finalized = true;
            return;
        }
        const speed = audio.playbackRate || 1.0;
        if (Math.abs(speed - 1.0) > 0.01) {
            _audioRxRecordDrop('listener_speed');
            acc.finalized = true;
            return;
        }

        const meta_h = acc.header;
        const targetChart = meta_h.chartTimeStart + (meta_h.chartTimeEnd - meta_h.chartTimeStart);
        const chartNow = audio.currentTime;
        const deltaSec = targetChart - chartNow;
        if (deltaSec < -LATE_FRAME_GRACE_SEC) {
            _audioRxFramesLate++;
            _audioRxRecordDrop('late');
            acc.finalized = true;
            return;
        }

        let buffer;
        try {
            buffer = ctx.createBuffer(SMAU_V1_CHANNEL_COUNT, totalFrames, SMAU_V1_SAMPLE_RATE);
            buffer.getChannelData(0).set(acc.pcmBuffer.subarray(0, totalFrames));
        } catch (_err) {
            _audioRxRecordDrop('decoded_copy_failed');
            acc.finalized = true;
            return;
        }
        acc.finalized = true;

        const startAt = ctx.currentTime + Math.max(0, deltaSec);
        const source = ctx.createBufferSource();
        source.buffer = buffer;
        source.connect(_audioListenerGain);
        // Release the source's connection + buffer reference once it's
        // done playing (or failed to start). Without this the ended
        // sources stay wired into the AudioContext graph and hold
        // their AudioBuffer references — over long sessions that
        // accumulates unbounded memory. Spotted by Copilot review on
        // PR #7 round 7.
        const cleanupSource = () => {
            _audioListenerScheduledSources.delete(source);
            source.onended = null;
            try { source.disconnect(); } catch (e) { /* */ }
            try { source.buffer = null; } catch (e) { /* */ }
        };
        source.onended = cleanupSource;
        _audioListenerScheduledSources.add(source);
        try {
            source.start(startAt);
            _audioRxFramesScheduled++;
        } catch (e) {
            cleanupSource();
        }
    } finally {
        // AudioData is a transferable owning a chunk of decoder memory; close
        // it on every output to release that backing store.
        try { audioData.close(); } catch (e) { /* */ }
    }
}

function _audioListenerOnDecoderError(err, wrapper) {
    // Decoder errors usually mean the configuration didn't match the
    // stream. Tear the pipeline down so the next inbound frame
    // rebuilds it cleanly — but ONLY if the error came from the
    // currently-live decoder. _audioListenerFlushAndCloseDecoder()
    // leaves an old decoder draining asynchronously while a fresh
    // replacement may already be live; a late error from the drained
    // decoder must not kill the replacement.
    console.error('[MP] AudioDecoder error:', err);
    _audioRxRecordDrop('decoder_error');
    if (_audioListenerActiveDecoder === wrapper) {
        _audioListenerTeardownDecoder();
    }
}

function _audioListenerEnsureDecoder() {
    if (_audioListenerActiveDecoder) return _audioListenerActiveDecoder;
    if (_audioListenerOpusUnsupported) return null;
    if (!_audioListenerHasWebCodecs()) {
        _audioListenerWarnOnceMissingWebCodecs();
        return null;
    }
    // Each decoder instance gets its own per-instance pending map and
    // timestamp counter (see _audioListenerActiveDecoder doc above) so
    // late output from a flushing-but-not-yet-closed prior decoder can
    // never collide with timestamps issued by this new one. The epoch
    // is captured at creation so the output handler can drop late
    // AudioData if chart-time has since been broken.
    const wrapper = { dec: null, pending: new Map(), nextTs: 0, epoch: _audioListenerEpoch };
    let dec = null;
    try {
        dec = new AudioDecoder({
            output: (ad) => _audioListenerHandleDecodedAudio(ad, wrapper),
            // Capture `wrapper` in the closure so a late error from a
            // drained-but-not-yet-closed decoder doesn't tear down
            // a fresh replacement. See _audioListenerOnDecoderError.
            error: (err) => _audioListenerOnDecoderError(err, wrapper),
        });
        // configure() is synchronous and throws NotSupportedError on
        // browsers that expose AudioDecoder but lack Opus decode (e.g.
        // partial WebCodecs implementations). Catch here rather than
        // letting the exception propagate up through every inbound
        // frame; degrade to the same silent-no-op path as the
        // no-WebCodecs branch and remember the failure so we don't
        // re-throw per inbound frame for the rest of the session.
        dec.configure({
            codec: 'opus',
            sampleRate: SMAU_V1_SAMPLE_RATE,
            numberOfChannels: SMAU_V1_CHANNEL_COUNT,
        });
    } catch (err) {
        if (dec) {
            try { dec.close(); } catch (e) { /* */ }
        }
        // Only mark Opus permanently unsupported when the failure is
        // a clear NotSupportedError. Other errors (transient resource
        // limits, InvalidStateError from an unlucky lifecycle race)
        // should be retryable on the next inbound frame — without
        // this distinction, a single transient failure would mute
        // peer audio for the rest of the session. Spotted by Copilot
        // review on PR #7.
        const sticky = err && err.name === 'NotSupportedError';
        if (sticky) {
            _audioListenerOpusUnsupported = true;
        }
        if (!_audioListenerWebCodecsWarned) {
            _audioListenerWebCodecsWarned = true;
            console.warn(
                sticky
                    ? '[MP] AudioDecoder Opus configure failed (NotSupportedError); peer audio playback disabled for this session.'
                    : '[MP] AudioDecoder Opus configure failed transiently; will retry on next frame.',
                err
            );
        }
        return null;
    }
    wrapper.dec = dec;
    _audioListenerActiveDecoder = wrapper;
    return wrapper;
}

function _audioListenerStopAllSources() {
    for (const src of _audioListenerScheduledSources) {
        try { src.onended = null; src.stop(); } catch (e) { /* */ }
        try { src.disconnect(); } catch (e) { /* */ }
    }
    _audioListenerScheduledSources.clear();
}

function _audioListenerTeardownDecoder() {
    // Hard teardown — used when the chart-time mapping is broken
    // (chart pause / seek / cleanup / 4408 recovery / broadcaster
    // takeover) so any pending output would schedule into garbage.
    // Drops in-flight decode work along with the decoder.
    const wrapper = _audioListenerActiveDecoder;
    _audioListenerActiveDecoder = null;
    if (!wrapper) return;
    if (wrapper.dec) {
        try { wrapper.dec.close(); } catch (e) { /* */ }
    }
    wrapper.pending.clear();
}

function _audioListenerFlushAndCloseDecoder() {
    // Graceful drain — used on broadcast_stop / broadcaster_changed-null
    // so the broadcaster's last-emitted intervals (which may still be
    // sitting in the AudioDecoder pipeline) get a chance to schedule
    // before the decoder shuts down. Per-wrapper pending metadata stays
    // alive in the closure for the output handler to consume during
    // flush() and is released when the wrapper is garbage-collected
    // after close().
    const wrapper = _audioListenerActiveDecoder;
    if (!wrapper || !wrapper.dec) return;
    _audioListenerActiveDecoder = null;
    const dec = wrapper.dec;
    Promise.resolve()
        .then(() => dec.flush())
        .catch(() => { /* drain best-effort; close anyway */ })
        .finally(() => {
            try { dec.close(); } catch (e) { /* */ }
            // wrapper.pending and wrapper.nextTs are local to this
            // wrapper and become unreachable once dec is closed; they
            // are NOT touched here. Any new decoder created in the
            // meantime carries its own independent state.
        });
}

function _audioListenerHandleFrame(header, opus) {
    // Pre-decode short-circuit: if the listener can't play this frame
    // anyway (chart paused, mid-song-load, or non-1.0x effective
    // playback rate), drop it before paying for the opus copy +
    // decode + AudioBuffer expansion. The same checks run again
    // post-decode in case state changes mid-decode (paranoia layer;
    // the common case is the cheap path).
    //
    // Speed gate uses audio.playbackRate (the effective speed) rather
    // than _room.speed, since mpSetSpeed() updates audio.playbackRate
    // synchronously while the server-echoed _room.speed lags by one
    // round trip. The 0.01 tolerance is wider than _onHeartbeat()'s
    // ±0.002 drift-correction band (so routine sync nudges pass
    // through) but narrower than any plausible user-set speed
    // (the slider's smallest non-1x detent is well above 1%).
    if (_songLoading) {
        // _loadSong has a ~2 s plugin-setup window where the <audio>
        // element may briefly read as not-paused while the new song's
        // src/duration/state are still settling. Heartbeats already
        // skip this window via _songLoading (see _onHeartbeat); the
        // listener pipeline needs the same guard so frames arriving
        // during a song switch don't get scheduled against the
        // mid-transition chart timeline.
        _audioRxRecordDrop('song_loading');
        return;
    }
    const audio = document.getElementById('audio');
    if (!audio || audio.paused) {
        _audioRxRecordDrop('listener_paused');
        return;
    }
    const speed = audio.playbackRate || 1.0;
    if (Math.abs(speed - 1.0) > 0.01) {
        _audioRxRecordDrop('listener_speed');
        return;
    }
    // Short-circuit if peer-audio decoding is fundamentally unavailable
    // in this browser. Without this, _audioListenerEnsureContext would
    // construct an AudioContext (and install global resume-gesture
    // listeners) even on browsers that can never decode our frames,
    // tripping autoplay-policy warnings and wasting resources. Spotted
    // by Copilot review on PR #7 round 3.
    //
    // Distinguish the two failure modes (Copilot round 6): WebCodecs
    // entirely missing vs. WebCodecs present but Opus configure threw
    // NotSupportedError. The latter sets _audioListenerOpusUnsupported
    // and is reported with reason 'opus_unsupported' so dropped-frame
    // triage isn't misled by a generic 'webcodecs_unavailable' label.
    if (_audioListenerOpusUnsupported) {
        _audioRxRecordDrop('opus_unsupported');
        return;
    }
    if (!_audioListenerHasWebCodecs()) {
        _audioListenerWarnOnceMissingWebCodecs();
        _audioRxRecordDrop('webcodecs_unavailable');
        return;
    }
    // Build the listener pipeline lazily on first valid frame so
    // listeners-with-no-active-broadcast aren't holding an AudioContext open
    // (Chrome warns about unused autoplay-blocked contexts; some browsers
    // also rate-limit how many can exist).
    const ctx = _audioListenerEnsureContext();
    if (!ctx) return;
    if (ctx.state !== 'running') {
        // Pre-decode gate. The output handler also drops on suspended
        // context, but by then we've paid for the opus copy + decode.
        // Drop here so a long autoplay-restricted window (no user
        // gesture yet) doesn't burn decoder CPU per inbound frame.
        // Spotted by Copilot review on PR #7.
        _audioRxRecordDrop('context_suspended');
        return;
    }
    const wrapper = _audioListenerEnsureDecoder();
    if (!wrapper) {
        // Account the drop with a specific reason rather than failing
        // silently — distinguishes "this browser can't decode Opus
        // (sticky)" from "transient configure failure that may retry
        // next frame", which matters when triaging dropped-frame
        // reports. Spotted by Copilot review on PR #7 round 5.
        _audioRxRecordDrop(
            _audioListenerOpusUnsupported ? 'opus_unsupported' : 'decoder_unavailable'
        );
        return;
    }

    // Parse the SMAU opus payload as a length-prefixed sequence of
    // Opus packet records:
    //   [u32 LE packet_count]
    //   ([u32 LE packet_size][packet_size bytes])*
    //
    // Opus packets max out at ~120 ms (RFC 6716), so a multi-second
    // interval requires multiple packets. Each is fed independently
    // to AudioDecoder; the per-interval accumulator below collects
    // their outputs into one AudioBuffer that gets scheduled at the
    // chart-time anchor specified in the SMAU header. PROTOCOL.md
    // "Audio frame format / Payload" describes this framing.
    if (opus.byteLength < 4) {
        _audioRxRecordDrop('opus_payload_too_small');
        return;
    }
    const opusView = new DataView(opus.buffer, opus.byteOffset, opus.byteLength);
    const packetCount = opusView.getUint32(0, true);
    if (packetCount === 0 || packetCount > 4096) {
        // Hard cap. v1 broadcasters use Opus's default 20 ms frames
        // and don't configure shorter ones; 4096 covers every
        // legitimate interval up to MAX_INTERVAL_SEC = 32 even if a
        // sender used 8 ms frames. Spotted by Copilot review on PR
        // #8 round 2.
        _audioRxRecordDrop('opus_packet_count_invalid');
        return;
    }
    const packets = [];
    let parseOff = 4;
    for (let i = 0; i < packetCount; i++) {
        if (parseOff + 4 > opus.byteLength) {
            _audioRxRecordDrop('opus_truncated');
            return;
        }
        const pktLen = opusView.getUint32(parseOff, true);
        parseOff += 4;
        if (pktLen === 0 || pktLen > 8192 || parseOff + pktLen > opus.byteLength) {
            // 8 KB per Opus packet is well above the spec's max ~1275
            // byte typical packet — a defensive ceiling against
            // adversarial inputs.
            _audioRxRecordDrop('opus_packet_invalid');
            return;
        }
        // Subarray view into the source Uint8Array — copy happens per
        // packet below before handing to EncodedAudioChunk (which
        // takes ownership).
        packets.push(new Uint8Array(opus.buffer, opus.byteOffset + parseOff, pktLen));
        parseOff += pktLen;
    }
    if (parseOff !== opus.byteLength) {
        // Trailing bytes after the last record indicate a malformed
        // frame — drop with a distinct reason instead of silently
        // accepting partial-interval decode. Spotted by Copilot
        // review on PR #8 round 2.
        _audioRxRecordDrop('opus_trailing_bytes');
        return;
    }

    // Shared accumulator across all packets in this interval. Each
    // EncodedAudioChunk we feed to the decoder gets a unique
    // timestamp; pending entries point at the same `acc` so the
    // output handler can collect every packet's decoded samples
    // into one AudioBuffer and schedule it once.
    const acc = {
        header: {
            chartTimeStart: header.chartTimeStart,
            chartTimeEnd: header.chartTimeEnd,
            sampleCount: header.sampleCount,
            intervalIndex: header.intervalIndex,
        },
        epoch: wrapper.epoch,
        pcmBuffer: null,
        maxFrames: 0,
        writeOffset: 0,
        packetsExpected: packets.length,
        packetsReceived: 0,
        finalized: false,
    };

    for (const packet of packets) {
        const ts = wrapper.nextTs++;
        wrapper.pending.set(ts, { acc });
        try {
            // Copy each packet into a fresh ArrayBuffer — EncodedAudioChunk
            // takes ownership and may detach the source view.
            const data = new Uint8Array(packet.byteLength);
            data.set(packet);
            const chunk = new EncodedAudioChunk({
                type: 'key',
                timestamp: ts,
                data: data,
            });
            wrapper.dec.decode(chunk);
        } catch (e) {
            wrapper.pending.delete(ts);
            // Mark accumulator finalized so a partial decode of
            // earlier packets doesn't schedule with missing audio.
            acc.finalized = true;
            _audioRxRecordDrop('decode_call_failed');
            return;
        }
    }
}

function _audioListenerOnControlSet(broadcasterId, params) {
    if (!broadcasterId) {
        _audioListenerOnControlClear();
        return;
    }
    if (_audioListenerBroadcasterId === broadcasterId) {
        // Re-announcement of the current broadcaster (e.g. duplicate
        // broadcaster_changed under reconnection); just refresh params.
        _audioListenerBroadcastParams = params || _audioListenerBroadcastParams;
        return;
    }
    // Switching broadcasters — drop everything from the previous pipeline.
    // Bumping the epoch invalidates any still-flushing OLD wrapper from a
    // graceful broadcast_stop that's racing this broadcast_start: without
    // it, the old broadcaster's tail intervals could schedule after the
    // new broadcaster has been announced. The graceful stop→drain path
    // only matters when no new broadcaster takes over, so bumping here
    // doesn't cost the broadcast-tail behavior.
    const prevBroadcasterId = _audioListenerBroadcasterId;
    if (prevBroadcasterId !== null) {
        // True A → B handoff: arm the suppression window so late /audio
        // packets from A (already on the TCP wire when server-side
        // worker purge ran) are rejected at frame entry until it
        // expires. Skipped on null → B (fresh start) and on the
        // late-join `connected` snapshot path, where there is no
        // previous broadcaster's tail to filter out — those would
        // otherwise gap the start of the new broadcast for 500 ms.
        _audioListenerHandoffSuppressUntil = (
            (typeof performance !== 'undefined' && performance.now ? performance.now() : Date.now())
            + HANDOFF_SUPPRESS_MS
        );
    }
    _audioListenerEpoch++;
    _audioListenerStopAllSources();
    _audioListenerTeardownDecoder();
    _audioListenerBroadcasterId = broadcasterId;
    _audioListenerBroadcastParams = params || null;
    // The pipeline (decoder, source nodes) is rebuilt lazily on first
    // valid frame in _audioListenerHandleFrame. AudioContext is reused.
}

function _audioListenerOnSelfBroadcast() {
    // Self-takeover: the local player has just become the broadcaster.
    // Self-broadcasts disable listener playback entirely (we never want
    // to hear our own loopback echo), so any previous broadcaster's
    // tail that would otherwise drain via _audioListenerOnControlClear
    // must be hard-stopped instead. Bumps the epoch + stops scheduled
    // sources + tears down the decoder synchronously.
    //
    // Always bump the epoch unconditionally, even if the visible state
    // (broadcaster_id, activeDecoder, scheduledSources) all look empty:
    // _audioListenerFlushAndCloseDecoder may have moved the decoder
    // out of _audioListenerActiveDecoder while leaving its flush()
    // running asynchronously in a closure. Without the epoch bump,
    // that decoder's late AudioData would pass the wrapper.epoch
    // check and schedule the previous peer's tail on this machine
    // — exactly the leak this helper is meant to prevent.
    _audioListenerEpoch++;
    _audioListenerStopAllSources();
    _audioListenerTeardownDecoder();
    _audioListenerBroadcasterId = null;
    _audioListenerBroadcastParams = null;
}

function _audioListenerOnControlClear() {
    if (_audioListenerBroadcasterId === null) return;
    // PROTOCOL.md "Lifecycle §5 Stop": broadcast end is graceful —
    // flush() the active decoder so any opus packets we already handed
    // off but haven't yet received as AudioData reach the output
    // handler and get scheduled. Sources already on the AudioContext
    // clock self-remove via onended. v1 limitation: late SMAU frames
    // arriving over /audio AFTER 'broadcaster_changed: null' on the
    // highway WS are dropped with reason 'no_broadcaster' (since the
    // two endpoints are unordered relative to each other and we can't
    // safely accept "unannounced" frames without misattributing a new
    // broadcaster's first frames as the previous broadcaster's tail).
    // The most common case — decoder-internal latency from the last
    // already-fed interval — is handled by flush().
    //
    // Arm the handoff suppression window NOW. If a new broadcaster B
    // takes over within HANDOFF_SUPPRESS_MS, control_set will see
    // prevBroadcasterId === null (we just cleared it) and skip arming
    // its own window — but the timer set here is still counting down,
    // so any of A's TCP-buffered tail packets arriving after
    // broadcaster_changed:B are still rejected. Pre-stop suppression
    // covers the broadcaster_changed:null → broadcaster_changed:B
    // sequence that codex flagged as the regressed common case.
    _audioListenerHandoffSuppressUntil = (
        (typeof performance !== 'undefined' && performance.now ? performance.now() : Date.now())
        + HANDOFF_SUPPRESS_MS
    );
    _audioListenerFlushAndCloseDecoder();
    _audioListenerBroadcasterId = null;
    _audioListenerBroadcastParams = null;
}

function _audioListenerOnChartPause() {
    // Listener paused / seeked. Anything already scheduled on the
    // AudioContext clock would now play out of sync with the new chart
    // position; drop it. The active decoder's pending map is cleared
    // along with the decoder via _audioListenerTeardownDecoder — a
    // fresh decoder is built lazily on the next valid frame after
    // chart playback resumes. Bumping the epoch ensures any output
    // from a still-flushing OLD wrapper (from a previous broadcast_stop)
    // also drops, since their captured epoch is now stale.
    _audioListenerEpoch++;
    _audioListenerStopAllSources();
    _audioListenerTeardownDecoder();
}

function _audioListenerCleanup() {
    // Full teardown — the listener is leaving the room. Unlike a normal
    // broadcast stop, we're tearing the AudioContext itself down, so
    // scheduled sources can't drain. Stop them explicitly. Bumping the
    // epoch invalidates any still-flushing OLD wrappers' outputs.
    _audioListenerEpoch++;
    _audioListenerUninstallResumeGesture();
    _audioListenerStopAllSources();
    _audioListenerTeardownDecoder();
    _audioListenerBroadcasterId = null;
    _audioListenerBroadcastParams = null;
    if (_audioListenerGain) {
        try { _audioListenerGain.disconnect(); } catch (e) { /* */ }
        _audioListenerGain = null;
    }
    if (_audioListenerCtx) {
        try { _audioListenerCtx.close(); } catch (e) { /* */ }
        _audioListenerCtx = null;
    }
    // Reset sticky decoder-support state so a future room-join in the
    // same tab can re-test (the user may have updated the browser, or
    // the previous failure may have been a transient resource limit).
    _audioListenerOpusUnsupported = false;
    _audioListenerWebCodecsWarned = false;
    // No global decode-timestamp counter to reset: each decoder wrapper
    // owns its own counter + pending map. Late output from a flushing
    // old wrapper that survives this cleanup will see _audioListenerCtx
    // null and early-return without scheduling.
    _audioListenerHandoffSuppressUntil = 0;
    _audioRxFramesLate = 0;
    _audioRxFramesScheduled = 0;
}

function _mintSessionId() {
    // crypto.randomUUID is widely available; fall back through getRandomValues
    // if available, then to Math.random as last resort. Every reference to
    // `crypto` is gated by `typeof crypto !== 'undefined'` so environments
    // without Web Crypto don't throw a ReferenceError.
    const hasCrypto = typeof crypto !== 'undefined';
    if (hasCrypto && typeof crypto.randomUUID === 'function') {
        return crypto.randomUUID();
    }
    const bytes = new Uint8Array(16);
    if (hasCrypto && typeof crypto.getRandomValues === 'function') {
        crypto.getRandomValues(bytes);
    } else {
        for (let i = 0; i < bytes.length; i++) {
            bytes[i] = Math.floor(Math.random() * 256);
        }
    }
    return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}

function _getOrMintSessionId() {
    let sid = sessionStorage.getItem(SESSION_STORAGE_KEY);
    if (!sid) {
        sid = _mintSessionId();
        sessionStorage.setItem(SESSION_STORAGE_KEY, sid);
    }
    return sid;
}

function _resetSessionId() {
    _sessionId = _mintSessionId();
    sessionStorage.setItem(SESSION_STORAGE_KEY, _sessionId);
}

function _onSessionEnded() {
    // Idempotent across concurrent handlers — see _resetMarker comment above.
    if (_resetMarker !== null && _resetMarker === _sessionId) {
        return;
    }
    _resetSessionId();
    _resetMarker = _sessionId;
    // Both endpoints must re-confirm BEFORE we'll consider the recovery
    // complete and clear the dedupe marker. See _maybeClearResetMarker.
    _highwayAuthedAfterReset = false;
    _audioOpenedAfterReset = false;
    // Audio RX counters are documented as per-session; the new session_id
    // we just minted is a logically separate session even though no
    // _cleanup() runs on the 4408 grace-recovery path. Reset the counters
    // so getAudioRxStats() doesn't carry the previous session's frames
    // into the replacement session.
    _audioRxResetStats();
    // The listener pipeline is also per-session in the same sense: the
    // server will re-announce broadcaster_id on the new highway WS's
    // `connected` snapshot so we'd rebuild it cleanly anyway, but old
    // scheduled sources would otherwise keep firing against stale
    // chart-time math during the brief reconnect window. We additionally
    // need to clear _audioListenerBroadcasterId itself: if the audio WS
    // reattaches BEFORE the highway `connected` snapshot arrives,
    // inbound frames would otherwise be accepted and scheduled against
    // stale control-plane state (the broadcast may have stopped, or the
    // broadcaster may have changed, while we were disconnected).
    // Stop sources hard rather than draining — the chart-time mapping
    // is broken until the new `connected` arrives. Bump the epoch so
    // any still-flushing OLD wrapper from a prior broadcast_stop also
    // invalidates (matches the chart-time-breaking pattern in
    // _audioListenerOnChartPause / _audioListenerCleanup).
    _audioListenerEpoch++;
    _audioListenerStopAllSources();
    _audioListenerTeardownDecoder();
    _audioListenerBroadcasterId = null;
    _audioListenerBroadcastParams = null;
    // Don't carry handoff suppression across a session reset — a fresh
    // session from broadcaster B's first frames would otherwise be
    // dropped under handoff_suppress for the remainder of the window
    // even though they belong to a new logical session.
    _audioListenerHandoffSuppressUntil = 0;
    // Capture-side: if WE were broadcasting before the 4408, the
    // server has cleared our broadcaster_id slot. Drop the stale
    // ack so the encoder stops emitting frames into a server that
    // has forgotten us. The new highway WS's 'connected' handler
    // re-issues broadcast_start (see _bootstrapOnConnected) so a
    // reconnect doesn't require the user to manually toggle off /
    // on. Spotted by codex review on PR #8.
    _captureBroadcasterAcked = false;
    // Mark that we owe a broadcast_start re-issue to the server.
    // Only set when capture was actually up (otherwise there's
    // nothing to re-claim). Cleared after _maybeReissueBroadcastAfterRecovery
    // successfully fires, or in _broadcastStop when capture is
    // torn down explicitly.
    if (_captureCtx && _captureRequested) {
        _captureNeedsReissueAfterReset = true;
    }
    // Refresh broadcast UI so the pill flips from "⏺ Broadcasting"
    // back to "⏳ Connecting…" immediately while recovery is in
    // flight (otherwise it stays stale until the post-recovery
    // broadcaster_changed eventually fires). Spotted by Copilot
    // review on PR #9.
    _renderBroadcastUi();
    // Reset the in-flight interval state so the resumed broadcast
    // doesn't send a frame containing pre-reconnect PCM stamped
    // with stale chart_time. _captureChartTimeAtIntervalStart
    // re-anchors via _captureChartTime() when the broadcast
    // resumes; the buffer + interval index re-init from zero.
    if (_captureCtx) {
        _captureIntervalSampleCount = 0;
        _captureIntervalIndex = 0n;
        if (_captureIntervalBuffer) {
            _captureIntervalBuffer = new Float32Array(_captureIntervalSamplesTarget + CAPTURE_PCM_CHUNK_SAMPLES);
        }
        _captureChartTimeAtIntervalStart = _captureChartTime();
    }
    // Deliberately do NOT bump _captureGen here. _broadcastStart()
    // snapshots that generation before its awaits and treats any
    // mismatch as supersede; bumping it on session reset would
    // strand a concurrently-running start with _captureCtx
    // assigned but not fully initialized (the second
    // cancellation branch only stops the local stream).
    // Queued flush tasks from the active broadcast carry pre-
    // recovery chart times — they'll be marked late and dropped
    // by the listener-side drift guard, which is acceptable for
    // v1. Deliberately also do NOT reset _captureFlushQueue
    // here — if a flush is mid-execution it would race with the
    // new chain via the shared _captureEncoderChunks accumulator.
    // Spotted by codex on PR #8 (queue) and Copilot review on
    // PR #9 round 6 (gen).
    // Deliberately do NOT force _lastBootstrapSucceeded = false on
    // session reset. If the previous (pre-4408) bootstrap was
    // successful and a non-bootstrap path (host transport, song
    // change) bumps _bootstrapGen during the recovery bootstrap,
    // that bootstrap returns null (superseded) and our finally
    // doesn't overwrite — so leaving the flag at its previous true
    // value lets recovery resume on the slightly-stale chart state
    // (one interval, dropped via the drift guard at worst). The
    // alternative — forcing false here — would latch recovery off
    // permanently. Spotted by codex on PR #8.
}

function _maybeClearResetMarker() {
    if (_highwayAuthedAfterReset && _audioOpenedAfterReset) {
        _resetMarker = null;
    }
}

// ── Lobby ──────────────────────────────────────────────────────────────

function _loadSettings() {
    try {
        const s = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
        _playerName = s.name || '';
        const nameCreate = document.getElementById('mp-create-name');
        const nameJoin = document.getElementById('mp-join-name');
        if (nameCreate) nameCreate.value = _playerName;
        if (nameJoin) nameJoin.value = _playerName;
    } catch (e) { /* ignore */ }
}

function _saveSettings() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({
        name: _playerName,
    }));
}

let _lobbyErrorTimer = null;
function _showError(msg) {
    const el = document.getElementById('mp-lobby-error');
    if (el) {
        // Cancel any pending hide-timer from a prior error so the new message
        // gets a fresh 5-second window (otherwise an older error's timer can
        // hide a new message early — e.g. a takeover notice arriving moments
        // after a failed join attempt).
        if (_lobbyErrorTimer !== null) clearTimeout(_lobbyErrorTimer);
        el.textContent = msg;
        el.classList.remove('hidden');
        _lobbyErrorTimer = setTimeout(() => {
            el.classList.add('hidden');
            _lobbyErrorTimer = null;
        }, 5000);
    }
}

window.mpCreateRoom = async function () {
    const nameInput = document.getElementById('mp-create-name');
    const name = (nameInput?.value || '').trim();
    if (!name) { _showError('Enter your name'); return; }
    _playerName = name;
    _saveSettings();

    try {
        const resp = await fetch('/api/plugins/multiplayer/rooms', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name }),
        });
        const data = await resp.json();
        if (data.error) { _showError(data.error); return; }

        _roomCode = data.code;
        _playerId = data.player_id;
        _isHost = true;
        // Fresh join: always start with a new session_id so we can't accidentally
        // collide with a stale session left over from a previous tab.
        _resetSessionId();
        _resetMarker = null;  // fresh session — clear the 4408 dedupe marker
        sessionStorage.setItem('mp_room', _roomCode);
        sessionStorage.setItem('mp_player', _playerId);
        _connectWS();
        _connectAudioWs();
        _showRoomView();
    } catch (e) {
        _showError('Failed to create room');
    }
};

window.mpJoinRoom = async function () {
    const nameInput = document.getElementById('mp-join-name');
    const codeInput = document.getElementById('mp-join-code');
    const name = (nameInput?.value || '').trim();
    const code = (codeInput?.value || '').trim().toUpperCase();
    if (!name) { _showError('Enter your name'); return; }
    if (!code || code.length < 4) { _showError('Enter a valid room code'); return; }
    _playerName = name;
    _saveSettings();

    try {
        const resp = await fetch(`/api/plugins/multiplayer/rooms/${code}/join`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name }),
        });
        const data = await resp.json();
        if (data.error) { _showError(data.error); return; }

        _roomCode = data.code;
        _playerId = data.player_id;
        _isHost = false;
        _room = data.room;
        _resetSessionId();
        _resetMarker = null;
        sessionStorage.setItem('mp_room', _roomCode);
        sessionStorage.setItem('mp_player', _playerId);
        _connectWS();
        _connectAudioWs();
        _showRoomView();
    } catch (e) {
        _showError('Failed to join room');
    }
};

window.mpLeaveRoom = async function () {
    if (!_roomCode || !_playerId) return;
    _intentionalClose = true;

    try {
        await fetch(`/api/plugins/multiplayer/rooms/${_roomCode}/leave`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ player_id: _playerId }),
        });
    } catch (e) { /* ignore */ }

    _cleanup();
    _showLobbyView();
};

function _cleanup() {
    _stopRecording();
    _stopHeartbeat();
    _restorePlaybackControls();
    _audioListenerCleanup();
    _audioQualityStopTimer();
    _audioQualityRxReset();
    // Phase 2d: tear down the capture pipeline (mic stream, encoder,
    // worklet, AudioContext, level meter rAF). Silent — the server
    // will clear broadcaster_id via the leave_room path, no need to
    // send broadcast_stop after we've already kicked the highway WS
    // closed.
    _broadcastStop({ silent: true });
    // Set _intentionalClose BEFORE closing either socket so neither close
    // handler triggers a reconnect during teardown.
    _intentionalClose = true;
    if (_ws) {
        try { _ws.close(); } catch (e) { /* ignore */ }
        _ws = null;
    }
    if (_audioWs) {
        try { _audioWs.close(); } catch (e) { /* ignore */ }
        _audioWs = null;
    }
    if (_reconnectTimer) { clearTimeout(_reconnectTimer); _reconnectTimer = null; }
    if (_audioReconnectTimer) {
        clearTimeout(_audioReconnectTimer);
        _audioReconnectTimer = null;
    }
    _roomCode = null;
    _playerId = null;
    _sessionId = null;
    _isHost = false;
    _room = null;
    _reconnectAttempts = 0;
    _audioReconnectAttempts = 0;
    _resetMarker = null;
    _highwayAuthedAfterReset = true;
    _audioOpenedAfterReset = true;
    _audioRxResetStats();
    _loadedFilename = null;
    _loadedArrangement = null;
    _loadingPromise = null;
    // Invalidate any in-flight bootstrap / song load so its post-await
    // continuations can't apply state to (or cache song markers for)
    // a NEW room the user has just joined. Each helper captures these
    // generation counters at entry and bails / aborts persisting if
    // the captured value is stale.
    _bootstrapGen++;
    _loadGen++;
    // Reset bootstrap-recovery state so a failed bootstrap in room A
    // can't block automatic broadcast recovery in room B.
    // Deliberately do NOT zero _bootstrapInFlight here — a previously
    // started _bootstrapOnConnected may still be in its await window,
    // and its finally would later decrement an already-zeroed
    // counter to negative (breaking the in-flight gate forever).
    // The finally clamps with Math.max(0, ...) instead.
    // Spotted by Copilot review on PR #9.
    _lastBootstrapSucceeded = true;
    _captureNeedsReissueAfterReset = false;
    sessionStorage.removeItem('mp_room');
    sessionStorage.removeItem('mp_player');
    sessionStorage.removeItem(SESSION_STORAGE_KEY);
}

// ── Views ──────────────────────────────────────────────────────────────

function _showLobbyView() {
    const lobby = document.getElementById('mp-lobby-view');
    const room = document.getElementById('mp-room-view');
    const mixer = document.getElementById('mp-mixer-view');
    // If the mixer was open and previewing, stop the preview before hiding it —
    // otherwise AudioBufferSourceNodes keep playing with no UI to stop them.
    // _mixerStop is a no-op if no preview is active. Wrapped in a try guard so
    // that early lobby renders (before the mixer module is wired up) can't
    // throw and leave the lobby unrendered.
    if (mixer && !mixer.classList.contains('hidden')) {
        try { _mixerStop(); } catch (e) { /* ignore */ }
    }
    if (lobby) lobby.classList.remove('hidden');
    if (room) room.classList.add('hidden');
    if (mixer) mixer.classList.add('hidden');
}

function _showRoomView() {
    const lobby = document.getElementById('mp-lobby-view');
    const room = document.getElementById('mp-room-view');
    if (lobby) lobby.classList.add('hidden');
    if (room) room.classList.remove('hidden');

    const codeEl = document.getElementById('mp-room-code');
    if (codeEl) codeEl.textContent = _roomCode || '';

    _renderPlayers();
    _renderQueue();
    _updateControls();
}

// ── WebSocket ──────────────────────────────────────────────────────────

function _connectWS() {
    // Stale-out the old socket BEFORE closing it. If onclose fires synchronously
    // (or before we reassign _ws below), the early-return `if (ws !== _ws)` in
    // the close handler catches it and avoids touching state for a connection
    // we're intentionally replacing.
    if (_ws) {
        const old = _ws;
        _ws = null;
        try { old.close(); } catch (e) { /* */ }
    }
    _intentionalClose = false;
    if (!_sessionId) _sessionId = _getOrMintSessionId();

    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/ws/plugins/multiplayer/${_roomCode}`
        + `?player_id=${encodeURIComponent(_playerId)}`
        + `&session_id=${encodeURIComponent(_sessionId)}`;
    const ws = new WebSocket(url);
    _ws = ws;

    const statusEl = document.getElementById('mp-connection-status');

    ws.onopen = () => {
        _reconnectAttempts = 0;
        if (statusEl) statusEl.textContent = 'Connected';
        // Listener-side telemetry runs for the lifetime of the highway
        // WS — the report function gates on "are we listening to a
        // remote broadcaster" each tick, so an idle 30s timer is cheap.
        _audioQualityStartTimer();
        // Clock sync starts when we receive 'connected' message
    };

    ws.onmessage = (ev) => {
        try {
            const msg = JSON.parse(ev.data);
            _handleMessage(msg);
        } catch (e) {
            console.error('[MP] Bad message:', e);
        }
    };

    ws.onclose = (ev) => {
        // If a newer connection has already replaced _ws, this is the OLD socket
        // closing — don't touch global state or trigger reconnect.
        if (ws !== _ws) return;
        _stopHeartbeat();

        if (ev && ev.code === CLOSE_REPLACED) {
            // Same-session reconnect performed by us elsewhere; new socket has
            // already taken over (or is about to). Nothing to do here.
            if (statusEl) statusEl.textContent = 'Connection replaced';
            return;
        }
        if (ev && ev.code === CLOSE_SUPERSEDED) {
            // Different tab took over the session. Per PROTOCOL.md, we MUST NOT
            // auto-reconnect (would just steal back). Run the full cleanup path
            // (clears _room, _isHost, _roomCode/_playerId/_sessionId, restores
            // playback controls, removes sessionStorage keys) and bounce back to
            // the lobby so the UI doesn't keep showing the room view, host
            // controls, OR mixer view for a session this tab no longer owns.
            _cleanup();
            _showLobbyView();
            // Surface the takeover notice via the lobby-visible error element;
            // #mp-connection-status lives inside the now-hidden room view.
            _showError('Session moved to another tab');
            return;
        }
        if (ev && ev.code === CLOSE_GRACE_EXPIRED) {
            // Grace expired without reattach. The held session is dead; mint a
            // fresh session_id (idempotently — _onSessionEnded handles the
            // race with the audio WS handler that also sees 4408) and let
            // the reconnect path open a new one.
            _onSessionEnded();
            if (statusEl) statusEl.textContent = 'Reconnecting…';
            if (!_intentionalClose && _roomCode) {
                _scheduleReconnect();
                _scheduleAudioReconnect();
            }
            return;
        }

        if (statusEl) statusEl.textContent = 'Disconnected';
        if (!_intentionalClose && _roomCode) {
            _scheduleReconnect();
        }
    };

    ws.onerror = () => {
        if (statusEl) statusEl.textContent = 'Connection error';
    };
}

function _scheduleReconnect() {
    if (_reconnectTimer) return;
    _reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, _reconnectAttempts - 1), 30000);
    const statusEl = document.getElementById('mp-connection-status');
    if (statusEl) statusEl.textContent = `Reconnecting in ${Math.round(delay / 1000)}s...`;
    _reconnectTimer = setTimeout(() => {
        _reconnectTimer = null;
        if (_roomCode && _playerId) {
            _connectWS();
        }
    }, delay);
}

// ── Audio WS (Phase 2a) ───────────────────────────────────────────────────
//
// Lifecycle parallels _connectWS / _scheduleReconnect on the highway side.
// Per PROTOCOL.md "Per-endpoint grace", the audio WS has its own grace
// window and reconnect cadence independent of the highway. Phase 2a wires
// the connection lifecycle only — frame send / receive land in Phase 2b.

function _connectAudioWs() {
    // Stale-out the old socket BEFORE closing it so a synchronous onclose
    // for the OLD ws hits the early-return `if (ws !== _audioWs)` and
    // doesn't try to reconnect / mutate state we're intentionally
    // replacing. (Same pattern as _connectWS.)
    if (_audioWs) {
        const old = _audioWs;
        _audioWs = null;
        try { old.close(); } catch (e) { /* */ }
    }
    if (!_roomCode || !_playerId) return;
    if (!_sessionId) _sessionId = _getOrMintSessionId();

    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/ws/plugins/multiplayer/${_roomCode}/audio`
        + `?player_id=${encodeURIComponent(_playerId)}`
        + `&session_id=${encodeURIComponent(_sessionId)}`;
    const ws = new WebSocket(url);
    ws.binaryType = 'arraybuffer';
    _audioWs = ws;

    ws.onopen = () => {
        // PROTOCOL.md "Implementation note (client side)": open is NOT proof
        // of auth on the audio WS. Don't surface "ready" UI here — the
        // user-visible "audio is live" signal lives in the highway WS
        // control plane (broadcaster_changed) and the first inbound frame.
        _audioReconnectAttempts = 0;
        // Audio side of a session-recovery has reattached. _maybeClearResetMarker
        // only clears the dedupe marker once the highway side has ALSO
        // reattached. See _resetMarker comment.
        _audioOpenedAfterReset = true;
        _maybeClearResetMarker();
        // If we were broadcasting before a 4408 and the highway
        // already reconnected first, _bootstrapOnConnected couldn't
        // re-issue broadcast_start yet (server requires /audio
        // attached). Try here too — whichever endpoint reattaches
        // last fires the actual send.
        _maybeReissueBroadcastAfterRecovery();
    };

    ws.onmessage = (ev) => {
        // Stale-socket guard. _connectAudioWs() replaces the live socket
        // on reconnect / 4408 recovery / leave; in-flight frames already
        // queued on the old socket can still arrive between our
        // assignment of the new _audioWs and the OS-level close
        // handshake completing. Without this check those frames would
        // be parsed and counted against the current session — see
        // codex-flagged drift in getAudioRxStats(). Mirrors the same
        // pattern in onclose.
        if (ws !== _audioWs) return;
        // PROTOCOL.md "Audio WS — binary peer-audio relay" makes the audio
        // WS binary-only in v1. Drop any text frame outright — text frames
        // are spec-illegal and may indicate a server bug or middlebox
        // injection; counted under the dedicated 'non_binary' reason so
        // it's visible if it ever happens.
        if (typeof ev.data === 'string') {
            _audioRxRecordDrop('non_binary');
            return;
        }
        // Parse + validate the 40-byte SMAU header (Phase 2b) and route
        // valid frames into the WebCodecs decode + scheduled-playback
        // pipeline (Phase 2c). Frames whose broadcaster_id doesn't match
        // the control-plane's announced broadcaster are also dropped here
        // — the server's single-broadcaster gate normally catches this
        // but the receiver double-checks since fan-out is byte-for-byte
        // and the wire format does not name the broadcaster (broadcaster
        // identity comes from the highway WS).
        const result = _smauDecodeFrame(ev.data);
        if (!result.ok) {
            _audioRxRecordDrop(result.reason);
            return;
        }
        if (!_audioListenerBroadcasterId) {
            // No active broadcaster announced (or it's the local player).
            // Drop pre-announce / post-tear-down frames so they don't get
            // scheduled with stale chart-time metadata. v1 limitation
            // (codex multi-round): we deliberately do NOT keep a drain
            // grace window here because the highway WS and /audio WS
            // are unordered — accepting frames during a drain window
            // can misattribute a new broadcaster's first frame as the
            // previous broadcaster's tail. The flushAndClose path
            // already drains decoder-internal pending opus, which
            // handles the common case where the decoder has unflushed
            // work for an already-fed-but-not-yet-output interval.
            _audioRxRecordDrop('no_broadcaster');
            return;
        }
        if (_audioListenerHandoffSuppressUntil > 0) {
            // Handoff window guard: bytes already in flight from the
            // previous broadcaster's audio worker can arrive after we
            // process broadcaster_changed for the new broadcaster.
            // Reject during the window so they don't get decoded
            // against the new broadcaster's chart-time metadata.
            const now = (typeof performance !== 'undefined' && performance.now)
                ? performance.now() : Date.now();
            if (now < _audioListenerHandoffSuppressUntil) {
                _audioRxRecordDrop('handoff_suppress');
                return;
            }
            _audioListenerHandoffSuppressUntil = 0;
        }
        _audioRxFramesValid++;
        _audioListenerHandleFrame(result.header, result.opus);
    };

    ws.onclose = (ev) => {
        if (ws !== _audioWs) return;

        if (ev && ev.code === CLOSE_REPLACED) {
            // Same-session reconnect of the audio slot — a new audio ws has
            // already taken over. Nothing to do here.
            return;
        }
        if (ev && ev.code === CLOSE_SUPERSEDED) {
            // Different tab took over the whole session. Normally the
            // highway-side 4409 handler runs the lobby bounce — the server
            // closes both endpoints simultaneously, and the highway path
            // gets there first. But the spec explicitly allows audio-only
            // sessions during per-endpoint grace, so this audio close may
            // be the ONLY 4409 we see. If the highway socket is already
            // gone (or never attached), run the cleanup ourselves;
            // otherwise leave it to the highway handler.
            const highwayDead = (
                _ws === null
                || _ws.readyState === WebSocket.CLOSING
                || _ws.readyState === WebSocket.CLOSED
            );
            if (highwayDead) {
                _cleanup();
                _showLobbyView();
                _showError('Session moved to another tab');
            }
            return;
        }
        if (ev && ev.code === CLOSE_GRACE_EXPIRED) {
            // Grace expired. Reset session_id idempotently (the highway
            // 4408 handler may have already done it) and reconnect both
            // endpoints under the new session_id.
            _onSessionEnded();
            if (!_intentionalClose && _roomCode) {
                _scheduleReconnect();
                _scheduleAudioReconnect();
            }
            return;
        }
        // 4401 (auth fail), 1009 (frame too big), 1011 (server error), or
        // a transient drop. For 4401 we don't auto-reconnect — the highway
        // side rejection is the primary signal and will handle UI cleanup.
        // For 1009 we also don't auto-reconnect (we sent a bad frame). For
        // everything else (including 1006 abnormal close), reconnect.
        if (ev && (ev.code === 4401 || ev.code === 1009)) return;
        if (!_intentionalClose && _roomCode) _scheduleAudioReconnect();
    };

    ws.onerror = () => {
        // Errors that aren't followed by a close event are rare; the close
        // handler does the recovery work. Don't duplicate it here.
    };
}

function _scheduleAudioReconnect() {
    if (_audioReconnectTimer) return;
    _audioReconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, _audioReconnectAttempts - 1), 30000);
    _audioReconnectTimer = setTimeout(() => {
        _audioReconnectTimer = null;
        if (_roomCode && _playerId) {
            _connectAudioWs();
        }
    }, delay);
}

// ── Broadcast capture pipeline (Phase 2d) ─────────────────────────────────
//
// The capture path: getUserMedia → AudioContext (48k/mono) → AnalyserNode
// (level meter) + AudioWorkletNode (drains 480-sample PCM chunks into
// the main thread). Main thread accumulates chunks into a per-interval
// Float32 buffer; on each interval boundary, the buffer is fed to a
// WebCodecs AudioEncoder configured for Opus. The encoder emits one or
// more EncodedAudioChunks per interval; we concatenate them into a
// single Opus payload, build a SMAU header, and send as one binary
// /audio WS frame.
//
// PROTOCOL.md "Lifecycle §3 Broadcast" requires the broadcaster to
// wait for the broadcaster_changed ack on the highway WS before
// sending frames. We therefore start capture eagerly on broadcast_start
// (so the worklet + encoder are warm) but gate the actual frame send
// on `_captureBroadcasterAcked`, which flips true when broadcaster_changed
// arrives with our own player_id.
//
// Worklet code is loaded from a Blob URL because the slopsmith plugin
// loader serves only a single screen.js entry point per plugin (no
// generic asset path). audio/audio-capture-worklet.js is the source of
// truth; the constant below is a functionally equivalent inline copy
// (excluding comments / formatting). Keep the executable logic in sync
// when either is edited.

const _CAPTURE_WORKLET_CODE = `
class CaptureProcessor extends AudioWorkletProcessor {
    constructor(options) {
        super();
        const opts = (options && options.processorOptions) || {};
        this._chunkSize = opts.chunkSize || 480;
        this._chunk = new Float32Array(this._chunkSize);
        this._chunkPos = 0;
        this._mixScratch = null;
        this._validChannelsScratch = [];
    }
    process(inputs) {
        const input = inputs[0];
        if (!input || input.length === 0 || !input[0]) return true;
        // Downmix to mono — some browsers/loopback devices ignore
        // channelCount:1 and deliver stereo. Pre-allocated scratch
        // buffer avoids per-quantum allocation on the realtime audio
        // thread. See audio/audio-capture-worklet.js for full notes.
        const numChannels = input.length;
        const numSamples = input[0].length;
        let src;
        if (numChannels === 1) {
            src = input[0];
        } else {
            // Precompute valid channels list to avoid per-sample
            // validity checks on the realtime thread. The list
            // itself is a pre-allocated array on the processor
            // instance so there is no per-quantum allocation. See
            // worklet source-of-truth file for full rationale.
            const validChannelsArr = this._validChannelsScratch;
            validChannelsArr.length = 0;
            for (let c = 0; c < numChannels; c++) {
                const ch = input[c];
                if (ch && ch.length === numSamples) validChannelsArr.push(ch);
            }
            if (validChannelsArr.length === 0) return true;
            if (validChannelsArr.length === 1) {
                src = validChannelsArr[0];
            } else {
                if (!this._mixScratch || this._mixScratch.length !== numSamples) {
                    this._mixScratch = new Float32Array(numSamples);
                }
                src = this._mixScratch;
                const inv = 1 / validChannelsArr.length;
                const nValid = validChannelsArr.length;
                for (let s = 0; s < numSamples; s++) {
                    let sum = 0;
                    for (let c = 0; c < nValid; c++) {
                        sum += validChannelsArr[c][s];
                    }
                    src[s] = sum * inv;
                }
            }
        }
        let srcPos = 0;
        while (srcPos < src.length) {
            const space = this._chunkSize - this._chunkPos;
            const remaining = src.length - srcPos;
            const n = space < remaining ? space : remaining;
            this._chunk.set(src.subarray(srcPos, srcPos + n), this._chunkPos);
            this._chunkPos += n;
            srcPos += n;
            if (this._chunkPos === this._chunkSize) {
                this.port.postMessage(this._chunk, [this._chunk.buffer]);
                this._chunk = new Float32Array(this._chunkSize);
                this._chunkPos = 0;
            }
        }
        return true;
    }
}
registerProcessor('slopsmith-capture-processor', CaptureProcessor);
`;

// Default interval-selection helper, mirror of audio/select-interval.js.
// Inlined for the same plugin-loader reason as the SMAU codec.
const SELECT_INTERVAL_MIN_DURATION_SEC = 1.0;
const SELECT_INTERVAL_DEFAULT_BEATS = 4;
const SELECT_INTERVAL_FAST_TEMPO_BEATS = 8;
function _selectIntervalBeats(bpm) {
    if (!Number.isFinite(bpm) || bpm <= 0) return SELECT_INTERVAL_DEFAULT_BEATS;
    const secondsPerBeat = 60 / bpm;
    if (SELECT_INTERVAL_DEFAULT_BEATS * secondsPerBeat < SELECT_INTERVAL_MIN_DURATION_SEC) {
        return SELECT_INTERVAL_FAST_TEMPO_BEATS;
    }
    return SELECT_INTERVAL_DEFAULT_BEATS;
}

let _captureCtx = null;
let _captureStream = null;
let _captureSourceNode = null;
let _captureWorkletNode = null;
let _captureAnalyser = null;
let _captureAnalyserBuffer = null;
let _captureSilentSink = null;
let _captureLevelTimer = null;
let _captureEncoder = null;
let _captureEncoderChunks = [];     // EncodedAudioChunk byte payloads accumulated for the current interval
// Serialize interval flushes. Without this, fire-and-forget flushes
// for back-to-back intervals can interleave: interval N+1's encode
// + flush starts while interval N's flush is still draining, and
// the SHARED _captureEncoderChunks accumulator gets reset out from
// under interval N's collector — corrupting the SMAU payload built
// for interval N. Spotted by Copilot review on PR #8 round 1.
let _captureFlushQueue = Promise.resolve();
// Generation token. Bumped on every _broadcastStart entry and
// every _broadcastStop. Each queued interval flush captures the
// token and bails out if it's no longer current — without this,
// stale flush tasks from a previous broadcast session could run
// after a stop+start cycle and feed the NEW encoder with the OLD
// PCM/interval state. Also used as a re-entrancy guard so two
// concurrent _broadcastStart calls (e.g. rapid toggle) don't
// race to wire up parallel mic streams + AudioContexts. Spotted
// by Copilot review on PR #8 round 6.
let _captureGen = 0;
let _captureIntervalBuffer = null;  // Float32Array for the in-flight interval
let _captureIntervalSampleCount = 0;
let _captureIntervalIndex = 0n;     // BigInt — monotonic per session
let _captureIntervalBeats = SELECT_INTERVAL_DEFAULT_BEATS;
let _captureIntervalLengthSec = 2.0;
let _captureIntervalSamplesTarget = 96000; // = intervalLengthSec * sampleRate
let _captureChartTimeAtIntervalStart = 0;
let _captureBroadcasterAcked = false; // flips true on broadcaster_changed-self
let _captureRequested = false;        // user toggled broadcast on
const CAPTURE_DEVICE_STORAGE_KEY = 'mp_broadcast_device_id';
const CAPTURE_PCM_CHUNK_SAMPLES = 480; // ~10 ms at 48k

function _captureHasWebCodecs() {
    return typeof AudioEncoder !== 'undefined'
        && typeof AudioData !== 'undefined';
}

function _captureChartTime() {
    const audioEl = document.getElementById('audio');
    return audioEl ? (audioEl.currentTime || 0) : 0;
}

function _captureCurrentChartBpm() {
    // slopsmith core's highway exposes a beats array via getBeats(). We
    // estimate BPM from the median spacing of the first ~20 beats —
    // robust to a noisy first beat or a long intro pickup. Returns
    // null if no beats are available, in which case the caller falls
    // back to a 120 BPM default.
    if (typeof window === 'undefined' || !window.highway) return null;
    if (typeof window.highway.getBeats !== 'function') return null;
    let beats;
    try {
        beats = window.highway.getBeats();
    } catch (e) { return null; }
    if (!Array.isArray(beats) || beats.length < 2) return null;
    const spacings = [];
    const limit = Math.min(20, beats.length - 1);
    for (let i = 0; i < limit; i++) {
        const a = beats[i] && beats[i].time;
        const b = beats[i + 1] && beats[i + 1].time;
        if (typeof a === 'number' && typeof b === 'number' && b > a) {
            spacings.push(b - a);
        }
    }
    if (spacings.length === 0) return null;
    spacings.sort((x, y) => x - y);
    const median = spacings[Math.floor(spacings.length / 2)];
    if (!(median > 0)) return null;
    return 60 / median;
}

// Returns true iff the chart's beat-spacing changes by more than
// CAPTURE_TEMPO_CHANGE_THRESHOLD around `boundaryTime` (the chart-time
// where the just-flushed interval ends and the next one starts).
//
// Compares the local spacing on either side of the boundary: for the
// pair of beats that bracket `boundaryTime`, we look at the spacing
// immediately before that pair vs. immediately after. If the broadcast
// has been running long enough to be at the song's tail (or hasn't
// yet reached enough beats for either side), we conservatively return
// false — no flag, no UI warning, no false positives.
//
// Why this check is here (in v1): the plan explicitly accepts a one-
// measure misalignment burst at tempo changes. We just need to flag it
// so the listener can render a brief "tempo change ahead" cue and
// internally bias their schedule (Phase 2c already wires the
// `tempo_change_at_end` boolean through to header decode for future
// use).
//
// Mirror of `detectTempoChangeAtBoundary` in audio/tempo-change.js
// (see that file's tests for the algorithm contract — node audio/
// tempo-change.test.js). Kept inline here because the slopsmith
// plugin loader serves only screen.js — keep the two in sync.
const CAPTURE_TEMPO_CHANGE_THRESHOLD = 0.05; // 5% spacing delta

function _captureBeatsSnapshot() {
    if (typeof window === 'undefined' || !window.highway) return null;
    if (typeof window.highway.getBeats !== 'function') return null;
    try {
        const beats = window.highway.getBeats();
        return Array.isArray(beats) ? beats : null;
    } catch (_e) { return null; }
}

function _captureTempoChangeAtBoundary(boundaryTime) {
    const beats = _captureBeatsSnapshot();
    if (!beats || beats.length < 4) return false;
    if (typeof boundaryTime !== 'number' || !Number.isFinite(boundaryTime)) return false;
    // Find the index of the first beat at or after the boundary.
    // Linear scan is fine — beats arrays are typically a few hundred
    // entries and this runs once per interval (~once per 2 s).
    let nextIdx = -1;
    for (let i = 0; i < beats.length; i++) {
        const t = beats[i] && beats[i].time;
        if (typeof t === 'number' && Number.isFinite(t) && t >= boundaryTime) {
            nextIdx = i;
            break;
        }
    }
    // Need at least two beats on each side of the boundary to compute
    // a "before" and "after" spacing.
    if (nextIdx < 2 || nextIdx >= beats.length - 1) return false;
    const tPrevPrev = beats[nextIdx - 2] && beats[nextIdx - 2].time;
    const tPrev = beats[nextIdx - 1] && beats[nextIdx - 1].time;
    const tNext = beats[nextIdx] && beats[nextIdx].time;
    const tNextNext = beats[nextIdx + 1] && beats[nextIdx + 1].time;
    if (
        typeof tPrevPrev !== 'number' || !Number.isFinite(tPrevPrev)
        || typeof tPrev !== 'number' || !Number.isFinite(tPrev)
        || typeof tNext !== 'number' || !Number.isFinite(tNext)
        || typeof tNextNext !== 'number' || !Number.isFinite(tNextNext)
    ) return false;
    const spacingBefore = tPrev - tPrevPrev;
    const spacingAfter = tNextNext - tNext;
    if (!(spacingBefore > 0) || !(spacingAfter > 0)) return false;
    const ratio = Math.abs(spacingAfter - spacingBefore) / spacingBefore;
    return ratio > CAPTURE_TEMPO_CHANGE_THRESHOLD;
}

function _captureSetIntervalParams(bpm) {
    let safeBpm = (bpm && Number.isFinite(bpm) && bpm > 0) ? bpm : 120;
    // Clamp to a minimum so very slow tempos don't blow past the
    // listener's defensive packet_count cap (4096). 30 BPM × 4
    // beats = 8 s interval ≈ 400 packets at 20 ms — well within
    // the cap. Lower than 30 BPM is rare in practice and clamping
    // up means the broadcaster's interval boundaries don't align
    // perfectly with their chart's beat grid, but Phase 2d's
    // initial-BPM-only design already accepts that for tempo-
    // change cases. Spotted by Copilot review on PR #8 round 2.
    if (safeBpm < 30) safeBpm = 30;
    _captureIntervalBeats = _selectIntervalBeats(safeBpm);
    _captureIntervalLengthSec = (60 / safeBpm) * _captureIntervalBeats;
    _captureIntervalSamplesTarget = Math.round(_captureIntervalLengthSec * SMAU_V1_SAMPLE_RATE);
    // Slack so a chunk that crosses the boundary doesn't overrun.
    _captureIntervalBuffer = new Float32Array(_captureIntervalSamplesTarget + CAPTURE_PCM_CHUNK_SAMPLES);
    _captureIntervalSampleCount = 0;
}

async function _captureEnumerateInputDevices() {
    try {
        if (!navigator.mediaDevices || !navigator.mediaDevices.enumerateDevices) return [];
        const devices = await navigator.mediaDevices.enumerateDevices();
        return devices.filter((d) => d.kind === 'audioinput');
    } catch (e) {
        return [];
    }
}

async function _captureRequestPermissionAndRefreshDevices() {
    // Browsers hide device labels until the page has been granted at
    // least one getUserMedia for that kind. Trigger a tiny audio
    // request, then immediately stop it, so the dropdown can show
    // device names. The user will see a permission prompt the first
    // time; subsequent reloads are silent if permission is persisted.
    try {
        const tempStream = await navigator.mediaDevices.getUserMedia({ audio: true });
        tempStream.getTracks().forEach((t) => t.stop());
    } catch (e) {
        // User denied or no devices — caller will surface the error.
        return [];
    }
    return _captureEnumerateInputDevices();
}

function _captureStopLevelMeter() {
    if (_captureLevelTimer) {
        cancelAnimationFrame(_captureLevelTimer);
        _captureLevelTimer = null;
    }
}

function _captureStartLevelMeter() {
    _captureStopLevelMeter();
    if (!_captureAnalyser) return;
    const buf = _captureAnalyserBuffer || new Uint8Array(_captureAnalyser.fftSize);
    _captureAnalyserBuffer = buf;
    const tick = () => {
        if (!_captureAnalyser) return;
        _captureAnalyser.getByteTimeDomainData(buf);
        // Compute peak deviation from 128 (silence midpoint for u8).
        let peak = 0;
        for (let i = 0; i < buf.length; i++) {
            const v = Math.abs(buf[i] - 128);
            if (v > peak) peak = v;
        }
        const level = peak / 128;
        _renderCaptureLevel(level);
        _captureLevelTimer = requestAnimationFrame(tick);
    };
    _captureLevelTimer = requestAnimationFrame(tick);
}

function _renderCaptureLevel(level) {
    const bar = document.getElementById('mp-broadcast-level-fill');
    if (bar) {
        const pct = Math.min(100, Math.round(level * 100));
        bar.style.width = pct + '%';
    }
}

function _captureOnEncodedChunk(chunk) {
    // AudioEncoder.output: copy the chunk's bytes into a Uint8Array
    // and append to the per-interval accumulator. We don't act on it
    // until flush() resolves below — multiple chunks can be emitted
    // per encode() call (Opus's default 20ms frame duration means a
    // 2s interval typically produces ~100 chunks).
    const buf = new Uint8Array(chunk.byteLength);
    chunk.copyTo(buf);
    _captureEncoderChunks.push(buf);
}

function _captureOnEncoderError(err) {
    console.error('[MP] AudioEncoder error:', err);
    // Tear down — the next broadcast attempt will rebuild the encoder.
    // Do NOT use silent:true here. If the server already accepted us
    // as broadcaster, suppressing broadcast_stop would leave its
    // broadcaster_id stuck on this player until grace expiry, blocking
    // others. Spotted by Copilot review on PR #8 round 1.
    _broadcastStop();
    _showError('Audio encoder failed; broadcast stopped.');
}

function _captureFlushAndSendIntervalQueued() {
    // Capture interval state synchronously here (before queueing) so
    // each queued task encodes the correct interval even if more
    // boundaries fire while it's pending.
    const numFrames = _captureIntervalSampleCount;
    const pcm = _captureIntervalBuffer.slice(0, numFrames);
    const chartTimeStart = _captureChartTimeAtIntervalStart;
    const chartTimeNow = _captureChartTime();
    const chartTimeEnd = chartTimeStart + (numFrames / SMAU_V1_SAMPLE_RATE);
    const intervalIndex = _captureIntervalIndex;
    const gen = _captureGen;

    // Roll the interval pointer + buffer NOW so subsequent PCM
    // chunks belong to interval N+1.
    _captureIntervalSampleCount = 0;
    _captureIntervalBuffer = new Float32Array(_captureIntervalSamplesTarget + CAPTURE_PCM_CHUNK_SAMPLES);

    // Re-anchor _captureChartTimeAtIntervalStart to the current
    // chart clock for the NEXT interval, instead of just continuing
    // the chartTimeEnd of THIS interval. The previous "compute end
    // from start + sample count" approach would drift indefinitely
    // when chart time differs from real time (chart paused, host
    // seeked, host adjusted speed). Re-anchoring per interval
    // bounds the drift to one interval at most. Spotted by Copilot
    // review on PR #8 round 8.
    _captureChartTimeAtIntervalStart = chartTimeNow;

    // Skip the send if chart isn't currently advancing or if our
    // computed chartTimeEnd has drifted significantly past the
    // actual chart clock — sending in those cases would emit
    // SMAU frames whose chart-time metadata listeners can't
    // schedule correctly. The drop is silent (the next interval
    // will re-anchor and resume sending). NOTE: this check uses
    // the synchronously-known audio.paused state at boundary
    // time; rapid pause/play during the interval might still
    // produce one slightly-off interval, which is the v1 design
    // accepted in PROTOCOL.md "tempo / pause boundary cases".
    const audio = document.getElementById('audio');
    const chartPlaying = audio && !audio.paused;
    const drift = chartTimeEnd - chartTimeNow;
    // Symmetric drift guard. driftAhead > 0.5 catches "chart paused
    // mid-interval" or "host running slow"; drift < -0.5 (chart
    // jumped forward via seek mid-interval) catches the symmetric
    // case where listeners would receive an interval whose
    // chartTimeEnd is far behind their current clock and drop it
    // as 'late' anyway. Skipping locally saves the encoder + ws
    // work. Spotted by Copilot review on PR #8 round 9.
    if (!chartPlaying || drift > 0.5 || drift < -0.5) {
        // Drop this interval. Bump the interval index so listeners
        // never see a duplicate interval_index (in case some
        // intervals were sent for this generation already).
        _captureIntervalIndex = _captureIntervalIndex + 1n;
        return;
    }

    _captureIntervalIndex = _captureIntervalIndex + 1n;

    _captureFlushQueue = _captureFlushQueue.then(() => {
        // Generation gate: a stop+start cycle bumps _captureGen, so a
        // queued task from a previous broadcast bails out before
        // touching the NEW encoder / WS.
        if (gen !== _captureGen) return;
        return _captureFlushAndSendInterval(pcm, numFrames, chartTimeStart, chartTimeEnd, intervalIndex);
    }).catch((err) => {
        console.error('[MP] capture flush failed:', err);
    });
}

async function _captureFlushAndSendInterval(pcm, numFrames, chartTimeStart, chartTimeEnd, intervalIndex) {
    if (numFrames <= 0) return;
    const encoder = _captureEncoder;
    if (!encoder) return;
    _captureEncoderChunks = [];

    let audioData;
    try {
        audioData = new AudioData({
            format: 'f32',
            sampleRate: SMAU_V1_SAMPLE_RATE,
            numberOfChannels: SMAU_V1_CHANNEL_COUNT,
            numberOfFrames: numFrames,
            // Microsecond timestamps must be unique per AudioData fed
            // to the encoder. Use chart_time_start * 1e6 — it advances
            // monotonically with the broadcast.
            timestamp: Math.round(chartTimeStart * 1e6),
            data: pcm,
        });
    } catch (e) {
        console.error('[MP] AudioData construct failed:', e);
        return;
    }

    try {
        encoder.encode(audioData);
    } catch (e) {
        console.error('[MP] AudioEncoder.encode failed:', e);
        try { audioData.close(); } catch (_) { /* */ }
        return;
    }
    try { audioData.close(); } catch (_) { /* */ }

    try {
        await encoder.flush();
    } catch (e) {
        // Encoder is still alive; flush() rejecting can happen on
        // teardown races. Drop this interval.
        _captureEncoderChunks = [];
        return;
    }

    if (encoder !== _captureEncoder) {
        // Encoder was torn down during the flush — drop.
        _captureEncoderChunks = [];
        return;
    }

    // Build the SMAU opus payload as a length-prefixed sequence of
    // Opus packet records:
    //   [u32 LE packet_count]
    //   ([u32 LE packet_size][packet_size bytes])*
    //
    // PROTOCOL.md "Audio frame format / Payload" describes this.
    // Each Opus packet is at most ~120 ms by spec, so a 2-second
    // interval typically produces ~10–100 packets. Concatenating
    // them into one EncodedAudioChunk (the previous behavior) was
    // not decodable — Opus packets have to be fed independently.
    // Spotted by Copilot review on PR #8 round 1.
    const packets = _captureEncoderChunks;
    _captureEncoderChunks = [];
    if (packets.length === 0) return;

    let totalSize = 4; // packet_count
    for (const p of packets) totalSize += 4 + p.byteLength;

    const opus = new Uint8Array(totalSize);
    const view = new DataView(opus.buffer, opus.byteOffset, totalSize);
    view.setUint32(0, packets.length, true);
    let off = 4;
    for (const p of packets) {
        view.setUint32(off, p.byteLength, true);
        off += 4;
        opus.set(p, off);
        off += p.byteLength;
    }

    // Gate on broadcaster_changed ack and audio WS readiness.
    if (!_captureBroadcasterAcked) return;
    if (!_audioWs || _audioWs.readyState !== WebSocket.OPEN) return;

    // Detect tempo change at this interval's end boundary so the
    // listener can render the warning before our next-interval audio
    // arrives (which is exactly when the misalignment will be most
    // audible). v1 doesn't time-stretch around the boundary — the
    // flag is purely informational.
    const tempoChange = _captureTempoChangeAtBoundary(chartTimeEnd);
    if (tempoChange) _captureNotifyTempoChangeUi();

    let frame;
    try {
        frame = _smauEncodeFrame({
            intervalIndex: intervalIndex,
            chartTimeStart: chartTimeStart,
            chartTimeEnd: chartTimeEnd,
            sampleCount: numFrames,
            opus: opus,
            flags: tempoChange ? SMAU_FLAG_TEMPO_CHANGE_AT_END : 0,
        });
    } catch (e) {
        console.error('[MP] SMAU encode failed:', e);
        return;
    }
    try {
        _audioWs.send(frame);
    } catch (e) {
        // WS may have closed concurrently. Listener-side reconnect
        // logic recovers.
    }
}

function _captureOnPcmChunk(chunk) {
    // Loop because a single PCM chunk (10 ms = 480 samples) can
    // straddle an interval boundary: write the prefix into the
    // current interval, fire its flush, then write the remainder
    // into the freshly-allocated next-interval buffer. Without this
    // split-and-carry, the post-boundary tail would either be
    // dropped (drift vs chart_time over long sessions) or merged
    // into the wrong interval. Spotted by Copilot review on PR #8
    // round 2.
    let offset = 0;
    while (offset < chunk.length && _captureIntervalBuffer) {
        const need = _captureIntervalSamplesTarget - _captureIntervalSampleCount;
        const capacity = _captureIntervalBuffer.length - _captureIntervalSampleCount;
        const writable = Math.min(need, capacity, chunk.length - offset);
        if (writable <= 0) return;
        _captureIntervalBuffer.set(
            chunk.subarray(offset, offset + writable),
            _captureIntervalSampleCount
        );
        _captureIntervalSampleCount += writable;
        offset += writable;
        if (_captureIntervalSampleCount === _captureIntervalSamplesTarget) {
            // Boundary crossed — fire the flush. _captureFlushAndSendIntervalQueued
            // synchronously snapshots interval state and rolls
            // _captureIntervalBuffer / _captureIntervalSampleCount /
            // _captureChartTimeAtIntervalStart over to the next
            // interval, so the loop continues writing into the
            // FRESH buffer.
            _captureFlushAndSendIntervalQueued();
        }
    }
}

async function _broadcastStart(deviceId) {
    if (_captureCtx) return; // already broadcasting
    // Re-entrancy / start-in-progress guard. Bump _captureGen on
    // entry and capture the value; every await checks that no
    // newer start has bumped past us. Without this, two rapid
    // toggle clicks could each get past the _captureCtx==null
    // check (which stays null until late in the async flow) and
    // race to wire up parallel mic streams + AudioContexts.
    // Spotted by Copilot review on PR #8 round 6.
    const myGen = ++_captureGen;
    // Reset the flush queue so any stale tasks from a previous
    // broadcast session can't run against THIS session's encoder.
    _captureFlushQueue = Promise.resolve();
    if (!_captureHasWebCodecs()) {
        _showError('Peer audio broadcast requires WebCodecs (Chrome/Edge or Firefox 130+).');
        return;
    }
    if (!_audioWs || _audioWs.readyState !== WebSocket.OPEN) {
        _showError('Audio connection not ready — try again in a moment.');
        return;
    }
    if (!_ws || _ws.readyState !== WebSocket.OPEN) {
        // Without a live highway WS, broadcast_start can't be sent
        // and broadcaster_changed can't arrive — we'd burn CPU on
        // the encoder pipeline while the toggle sat at "Connecting…"
        // forever. Spotted by Copilot review on PR #8 round 3.
        _showError('Connection not ready — try again in a moment.');
        return;
    }
    _captureRequested = true;
    _captureBroadcasterAcked = false;
    if (deviceId) {
        // Persist the user's device choice for next-session restore
        // (mpRefreshBroadcastDevices reads this back). We don't keep
        // the value as in-memory state because the dropdown is the
        // single source of truth for the active selection while
        // broadcasting.
        try { localStorage.setItem(CAPTURE_DEVICE_STORAGE_KEY, deviceId); } catch (_) { /* */ }
    }

    let stream;
    try {
        const constraints = {
            audio: {
                echoCancellation: false,
                noiseSuppression: false,
                autoGainControl: false,
                sampleRate: SMAU_V1_SAMPLE_RATE,
                channelCount: SMAU_V1_CHANNEL_COUNT,
            },
        };
        if (deviceId) constraints.audio.deviceId = { exact: deviceId };
        stream = await navigator.mediaDevices.getUserMedia(constraints);
    } catch (e) {
        _captureRequested = false;
        _showError('Microphone access denied or unavailable: ' + (e && e.message || e));
        return;
    }
    // Cancellation check: the user may have toggled broadcast off,
    // OR triggered another _broadcastStart (re-entrancy), while the
    // permission prompt was open. Stop the just-acquired tracks and
    // bail. Spotted by Copilot review on PR #8 rounds 2 + 6.
    if (!_captureRequested || myGen !== _captureGen) {
        try { stream.getTracks().forEach((t) => t.stop()); } catch (_) { /* */ }
        return;
    }
    _captureStream = stream;

    const Ctor = window.AudioContext || window.webkitAudioContext;
    if (!Ctor) {
        _captureRequested = false;
        _showError('AudioContext unavailable in this browser.');
        _broadcastStop({ silent: true });
        return;
    }
    _captureCtx = new Ctor({ sampleRate: SMAU_V1_SAMPLE_RATE, latencyHint: 'interactive' });
    // Best-effort resume for autoplay-restricted browsers. Newly
    // created AudioContexts on Safari (and sometimes Chrome) start
    // suspended until resume() is called from a user gesture; the
    // toggle click that landed us here qualifies, but we still
    // need to actually call it. Without this the analyser / worklet
    // sit idle and the UI gets stuck at "Connecting…". Spotted by
    // Copilot review on PR #8 round 5.
    if (_captureCtx.state === 'suspended' && typeof _captureCtx.resume === 'function') {
        try {
            const p = _captureCtx.resume();
            if (p && typeof p.catch === 'function') p.catch(() => {});
        } catch (_) { /* */ }
    }

    // Inline worklet via Blob URL.
    const blob = new Blob([_CAPTURE_WORKLET_CODE], { type: 'application/javascript' });
    const url = URL.createObjectURL(blob);
    try {
        await _captureCtx.audioWorklet.addModule(url);
    } catch (e) {
        URL.revokeObjectURL(url);
        _captureRequested = false;
        _showError('Failed to install capture worklet: ' + (e && e.message || e));
        _broadcastStop({ silent: true });
        return;
    }
    URL.revokeObjectURL(url);
    // Second cancellation check — addModule is async, and the user
    // may have toggled off during it (or another start may have
    // raced past us).
    //
    // Fork the cleanup based on which case we're in:
    //   - If we're cancelled but still the current generation
    //     (user toggled off), it's safe to call _broadcastStop —
    //     the globals we created (_captureCtx, _captureStream)
    //     are still ours.
    //   - If a NEWER start has raced past us (myGen !== _captureGen),
    //     a different _broadcastStart may have already assigned
    //     _captureCtx / _captureStream. Calling _broadcastStop
    //     would tear DOWN the newer start's globals AND bump
    //     _captureGen further, which would in turn invalidate
    //     the newer start at its own gen check. Just stop our
    //     local resources without touching globals. Spotted by
    //     Copilot review on PR #8 round 10.
    if (!_captureRequested) {
        _broadcastStop({ silent: true });
        return;
    }
    if (myGen !== _captureGen) {
        // A newer start owns the globals now. Only stop the local
        // stream we acquired (the AudioContext we created may have
        // already been torn down + replaced by the newer start).
        try { stream.getTracks().forEach((t) => t.stop()); } catch (_) { /* */ }
        return;
    }

    _captureSourceNode = _captureCtx.createMediaStreamSource(stream);
    _captureAnalyser = _captureCtx.createAnalyser();
    _captureAnalyser.fftSize = 256;
    _captureSourceNode.connect(_captureAnalyser);

    // Silent sink — connect the analyser through a gain=0 node to
    // ctx.destination. Web Audio doesn't guarantee that nodes which
    // are not ultimately connected to destination get pulled/
    // rendered, which can prevent the analyser from getting fresh
    // data and (more critically) the worklet's process() callback
    // from being called at all on some browsers. The gain=0
    // multiplier means we don't actually output any sound to the
    // listener's speakers — we just keep the graph "alive". Spotted
    // by Copilot review on PR #8 round 3.
    _captureSilentSink = _captureCtx.createGain();
    _captureSilentSink.gain.value = 0;
    _captureAnalyser.connect(_captureSilentSink);
    _captureSilentSink.connect(_captureCtx.destination);

    try {
        _captureWorkletNode = new AudioWorkletNode(_captureCtx, 'slopsmith-capture-processor', {
            numberOfInputs: 1,
            // numberOfOutputs:1 (default) plus connect to the silent
            // sink BELOW. With numberOfOutputs:0 the worklet node
            // sits off the rendered graph and some browsers stop
            // calling its process() — see the silent-sink comment
            // for the analyser. Spotted by Copilot review on PR #8
            // round 4.
            numberOfOutputs: 1,
            outputChannelCount: [SMAU_V1_CHANNEL_COUNT],
            processorOptions: { chunkSize: CAPTURE_PCM_CHUNK_SAMPLES },
        });
    } catch (e) {
        _captureRequested = false;
        _showError('Failed to construct capture worklet node: ' + (e && e.message || e));
        _broadcastStop({ silent: true });
        return;
    }
    _captureSourceNode.connect(_captureWorkletNode);
    // Route the worklet's silent output through the gain=0 sink to
    // ctx.destination so the worklet is on a rendered path. The
    // worklet's process() leaves outputs[] zero-filled (it captures
    // input only), so this contributes no audio.
    _captureWorkletNode.connect(_captureSilentSink);
    _captureWorkletNode.port.onmessage = (ev) => {
        if (ev.data instanceof Float32Array) {
            _captureOnPcmChunk(ev.data);
        }
    };

    // Compute interval params from chart bpm.
    const bpm = _captureCurrentChartBpm();
    _captureSetIntervalParams(bpm);
    _captureIntervalIndex = 0n;
    _captureChartTimeAtIntervalStart = _captureChartTime();

    // WebCodecs Opus encoder.
    try {
        _captureEncoder = new AudioEncoder({
            output: (chunk) => _captureOnEncodedChunk(chunk),
            error: (err) => _captureOnEncoderError(err),
        });
        _captureEncoder.configure({
            codec: 'opus',
            sampleRate: SMAU_V1_SAMPLE_RATE,
            numberOfChannels: SMAU_V1_CHANNEL_COUNT,
            bitrate: 96000,
            opus: { application: 'audio' },
        });
    } catch (e) {
        _captureRequested = false;
        _showError('Audio encoder configure failed: ' + (e && e.message || e));
        _broadcastStop({ silent: true });
        return;
    }

    // Post-await readiness re-check. Multiple awaits sit between
    // the entry-point WS checks and here (getUserMedia, addModule,
    // encoder.configure); the highway or audio WS could have closed
    // during any of them. If we just silently skipped the
    // broadcast_start send, the toggle would sit at "Connecting…"
    // forever while burning encoder CPU. Spotted by Copilot review
    // on PR #8 round 4.
    if (
        !_ws || _ws.readyState !== WebSocket.OPEN
        || !_audioWs || _audioWs.readyState !== WebSocket.OPEN
    ) {
        _captureRequested = false;
        _showError('Connection dropped while preparing broadcast — try again.');
        _broadcastStop({ silent: true });
        return;
    }

    _captureStartLevelMeter();

    // Send broadcast_start; the broadcaster_changed ack flips
    // _captureBroadcasterAcked = true and unlocks frame send. The
    // send() can still throw if the WS closes between the readiness
    // re-check above and this call (the close-and-send race is
    // typically microseconds but real); catch + tear down so the
    // toggle doesn't stay stuck in "Connecting…". Spotted by
    // Copilot review on PR #8 round 7.
    try {
        _ws.send(JSON.stringify({
            type: 'broadcast_start',
            interval_beats: _captureIntervalBeats,
            sample_rate: SMAU_V1_SAMPLE_RATE,
            channel_count: SMAU_V1_CHANNEL_COUNT,
            codec: 'opus',
            bitrate: 96000,
        }));
    } catch (err) {
        _captureRequested = false;
        _showError('Connection dropped while starting broadcast — try again.');
        _broadcastStop({ silent: true });
        return;
    }
    _renderBroadcastUi();
}

function _broadcastStop(opts) {
    const silent = opts && opts.silent;
    const wasRequested = _captureRequested;
    _captureRequested = false;
    _captureBroadcasterAcked = false;
    _captureNeedsReissueAfterReset = false;
    // Bump the generation token so any queued flush tasks from this
    // session bail out at their gate before touching the next
    // session's encoder / WS. Reset the queue too — no point keeping
    // tasks chained that will all early-return.
    _captureGen++;
    _captureFlushQueue = Promise.resolve();
    _captureStopLevelMeter();
    if (_captureWorkletNode) {
        try { _captureWorkletNode.port.onmessage = null; } catch (_) { /* */ }
        try { _captureWorkletNode.disconnect(); } catch (_) { /* */ }
        _captureWorkletNode = null;
    }
    if (_captureSourceNode) {
        try { _captureSourceNode.disconnect(); } catch (_) { /* */ }
        _captureSourceNode = null;
    }
    if (_captureAnalyser) {
        try { _captureAnalyser.disconnect(); } catch (_) { /* */ }
        _captureAnalyser = null;
    }
    _captureAnalyserBuffer = null;
    if (_captureSilentSink) {
        try { _captureSilentSink.disconnect(); } catch (_) { /* */ }
        _captureSilentSink = null;
    }
    if (_captureEncoder) {
        try { _captureEncoder.close(); } catch (_) { /* */ }
        _captureEncoder = null;
    }
    _captureEncoderChunks = [];
    if (_captureStream) {
        try { _captureStream.getTracks().forEach((t) => t.stop()); } catch (_) { /* */ }
        _captureStream = null;
    }
    if (_captureCtx) {
        // AudioContext.close() returns a Promise — async rejections
        // would otherwise surface as unhandled-rejection warnings
        // during teardown races. Match the resume() handling.
        // Spotted by Copilot review on PR #8 round 2.
        try {
            const closePromise = _captureCtx.close();
            if (closePromise && typeof closePromise.catch === 'function') {
                closePromise.catch(() => {});
            }
        } catch (_) { /* */ }
        _captureCtx = null;
    }
    _captureIntervalBuffer = null;
    _captureIntervalSampleCount = 0;
    _renderCaptureLevel(0);
    _captureClearTempoChangeUi();
    _renderBroadcastUi();
    // Tell the server only if we actually owned a broadcast and not
    // during a forced silent teardown (e.g. encoder error already on
    // the way to a user-visible error).
    if (wasRequested && !silent && _ws && _ws.readyState === WebSocket.OPEN) {
        try {
            _ws.send(JSON.stringify({ type: 'broadcast_stop' }));
        } catch (_) { /* */ }
    }
}

function _captureOnBroadcasterChanged(broadcasterId) {
    // Highway dispatch already routes to the listener pipeline. The
    // capture path observes the broadcaster_changed event to know
    // whether the server accepted our broadcast_start (ack flips
    // _captureBroadcasterAcked = true so frame send unlocks).
    if (broadcasterId === _playerId) {
        // Only treat this as an ack if we still intend to broadcast.
        // A stale self-ack (e.g. server re-emitted broadcaster_changed
        // for a session that we've since stopped locally) shouldn't
        // flip the pill back to "Broadcasting".
        if (_captureRequested || _captureCtx) {
            _captureBroadcasterAcked = true;
            // Fresh broadcast — stale per-listener stats from a previous
            // broadcast (if any) shouldn't appear under this new run.
            _audioQualityRxReset();
            _renderBroadcastUi();
        }
        return;
    }
    // Either someone else is broadcasting or the slot was cleared.
    // Always clear any stale self-ack first so the UI cannot remain
    // stuck on "Broadcasting" after a local stop followed by a later
    // server update. Spotted by Copilot review on PR #8 round 3.
    const hadAck = _captureBroadcasterAcked;
    _captureBroadcasterAcked = false;
    // We are no longer the broadcaster — drop any aggregated listener
    // telemetry so the stats line hides itself the next render.
    _audioQualityRxReset();
    // If WE thought we were broadcasting, our broadcast was preempted —
    // tear down without sending another broadcast_stop (the server
    // already moved on).
    if (_captureRequested || _captureCtx) {
        _broadcastStop({ silent: true });
        return;
    }
    if (hadAck) {
        _renderBroadcastUi();
    }
}

// Brief banner shown to the broadcaster when an interval boundary
// landed on a tempo change. Per the plan: v1 accepts a one-measure
// misalignment burst at tempo changes; the user should know to expect
// a brief shift. Shows for ~one interval (the duration over which the
// listener-side misalignment will play out), then auto-clears.
let _captureTempoChangeUiTimer = null;

function _captureNotifyTempoChangeUi() {
    const el = document.getElementById('mp-broadcast-tempo-warning');
    if (!el) return;
    el.classList.remove('hidden');
    if (_captureTempoChangeUiTimer) clearTimeout(_captureTempoChangeUiTimer);
    // Hold the warning for one interval — long enough for the
    // listener to actually hear the shift, short enough not to
    // pile up across consecutive tempo-change measures.
    const holdMs = Math.max(1000, Math.round(_captureIntervalLengthSec * 1000));
    _captureTempoChangeUiTimer = setTimeout(() => {
        const e = document.getElementById('mp-broadcast-tempo-warning');
        if (e) e.classList.add('hidden');
        _captureTempoChangeUiTimer = null;
    }, holdMs);
}

function _captureClearTempoChangeUi() {
    if (_captureTempoChangeUiTimer) {
        clearTimeout(_captureTempoChangeUiTimer);
        _captureTempoChangeUiTimer = null;
    }
    const el = document.getElementById('mp-broadcast-tempo-warning');
    if (el) el.classList.add('hidden');
}

function _renderBroadcastUi() {
    const toggle = document.getElementById('mp-broadcast-toggle');
    const pill = document.getElementById('mp-broadcast-pill');
    const dropdown = document.getElementById('mp-broadcast-device');
    if (toggle) {
        toggle.checked = !!_captureRequested;
    }
    if (pill) {
        if (_captureBroadcasterAcked) {
            pill.classList.remove('hidden');
            pill.textContent = '⏺ Broadcasting';
        } else if (_captureRequested) {
            pill.classList.remove('hidden');
            pill.textContent = '⏳ Connecting…';
        } else {
            pill.classList.add('hidden');
        }
    }
    if (dropdown) {
        // Disable device picker while broadcasting; switching mid-broadcast
        // would require teardown + restart and is out of scope for v1.
        dropdown.disabled = !!_captureRequested;
    }
}

window.mpToggleBroadcast = async function () {
    const toggle = document.getElementById('mp-broadcast-toggle');
    const wantOn = toggle ? !!toggle.checked : !_captureRequested;
    if (wantOn && _captureCtx) return; // already on
    if (!wantOn && !_captureCtx && !_captureRequested) return;
    if (wantOn) {
        const dropdown = document.getElementById('mp-broadcast-device');
        const deviceId = (dropdown && dropdown.value) || null;
        await _broadcastStart(deviceId);
        // _broadcastStart may bail out early on permission denial etc.;
        // sync the toggle to whatever final state we ended up in.
        _renderBroadcastUi();
    } else {
        _broadcastStop({});
    }
};

window.mpRefreshBroadcastDevices = async function () {
    const dropdown = document.getElementById('mp-broadcast-device');
    if (!dropdown) return;
    // _captureRequestPermissionAndRefreshDevices swallows getUserMedia
    // exceptions and returns [] (so this caller doesn't have to know
    // about specific Permissions API errors). An empty list therefore
    // means either "permission denied" or "no input devices" — either
    // way, nothing useful to populate, and the user needs to know.
    // The previous try/catch was unreachable. Spotted by Copilot
    // review on PR #8 round 6.
    const devices = await _captureRequestPermissionAndRefreshDevices();
    if (!Array.isArray(devices) || devices.length === 0) {
        _showError('Microphone permission was denied or no input devices are available.');
        return;
    }
    const persisted = (() => {
        try { return localStorage.getItem(CAPTURE_DEVICE_STORAGE_KEY) || ''; } catch (_) { return ''; }
    })();
    dropdown.innerHTML = '';
    const defaultOpt = document.createElement('option');
    defaultOpt.value = '';
    defaultOpt.textContent = 'Default device';
    dropdown.appendChild(defaultOpt);
    for (const d of devices) {
        const opt = document.createElement('option');
        opt.value = d.deviceId;
        opt.textContent = d.label || ('Microphone ' + d.deviceId.slice(0, 8));
        if (d.deviceId === persisted) opt.selected = true;
        dropdown.appendChild(opt);
    }
};

// ── Connected-snapshot recovery ────────────────────────────────────────
//
// Brings the local <audio> element + listener pipeline online after a
// fresh join or reconnect, given the room snapshot in `_room`. Sequenced
// so peer-audio scheduling can latch onto a freshly synced chart clock
// without ever observing an unloaded / mid-pause audio element:
//
//   1. _loadSong  — guarantees the <audio> element has a src loaded.
//                   Runs for hosts and guests; both can be listening
//                   to another player's broadcast (host case: the
//                   server suppresses self-echo of playback_state, so
//                   the snapshot is the host's only chance to realign).
//   2. Apply room.time/speed/state — _loadSong pauses non-host audio
//                                    on completion, so the play() must
//                                    run AFTER it. Significant chart
//                                    time jumps fire _audioListenerOnChartPause
//                                    to invalidate any in-flight
//                                    schedule from before the reconnect.
//   3. Activate listener pipeline — broadcaster_id from snapshot
//                                   selects control_set / self / clear.
//
// _bootstrapGen guards against stale completions: another `connected`
// snapshot or `song_changed` can arrive during _loadSong's ~2 s async
// setup window. Each call captures its own generation; after every
// await, it bails out if the generation no longer matches (a newer
// bootstrap has superseded it). Without this, the OLD load can finish
// and apply chart-time / listener state for a song that's no longer
// the room's current song.

let _bootstrapGen = 0;

async function _bootstrapOnConnected() {
    if (!_room) return;
    _bootstrapInFlight++;
    // Inner returns:
    //   true  — bootstrap completed and applied state successfully.
    //   false — bootstrap genuinely failed (e.g. _loadSong threw).
    //   null  — superseded (newer bootstrap took over OR non-
    //           bootstrap path invalidated us mid-load); don't
    //           overwrite _lastBootstrapSucceeded with our stale
    //           result.
    let bootstrapResult = null;
    try {
        const myGen = ++_bootstrapGen;
        try {
            bootstrapResult = await _bootstrapOnConnectedInner(myGen);
        } catch (err) {
            // An unexpected throw inside the inner is a definitive
            // failure for recovery gating — record it as such and
            // re-raise so the outer .catch on the call site still
            // sees the error. Without this, bootstrapResult would
            // stay null and the finally would skip the write,
            // leaving _lastBootstrapSucceeded stuck at the previous
            // (possibly true) value. Spotted by Copilot review on
            // PR #9.
            bootstrapResult = false;
            throw err;
        }
    } finally {
        // Clamp at 0 so if _cleanup ran during our await (which used
        // to zero the counter — now doesn't), or any future path
        // resets it independently, this decrement can't drive the
        // counter negative and permanently break the in-flight
        // gate. Spotted by Copilot review on PR #9.
        _bootstrapInFlight = Math.max(0, _bootstrapInFlight - 1);
        // Only write _lastBootstrapSucceeded when:
        //   1. No other bootstrap is still in flight (counter 0), AND
        //   2. We have a definitive result (not superseded).
        // If a newer bootstrap is in-flight or has already written,
        // we let it stand. If we were superseded by a non-bootstrap
        // path bumping _bootstrapGen, _lastBootstrapSucceeded keeps
        // its previous value (typically true from the most recent
        // successful bootstrap), so recovery isn't permanently
        // gated.
        if (_bootstrapInFlight === 0) {
            // Only write _lastBootstrapSucceeded when we have a
            // definitive result (not superseded). If a newer
            // bootstrap or a non-bootstrap invalidation superseded
            // us, leave the previous value intact so recovery
            // gating keeps reflecting the most recent real outcome.
            if (bootstrapResult !== null) {
                _lastBootstrapSucceeded = bootstrapResult === true;
            }
            // Always give recovery a chance to fire when the last
            // in-flight bootstrap drains, even on supersede. If we
            // ARE a superseded bootstrap and /audio is already
            // open, this would otherwise be the only remaining
            // call site to flush a pending re-issue, leaving
            // _captureNeedsReissueAfterReset latched.
            // _maybeReissueBroadcastAfterRecovery internally gates
            // on _lastBootstrapSucceeded so a (definitively or
            // transitively) failed bootstrap won't actually re-send.
            // Spotted by Copilot review on PR #9.
            _maybeReissueBroadcastAfterRecovery();
        }
    }
}

async function _bootstrapOnConnectedInner(myGen) {
    if (!_room) return null;
    // Hard-teardown the listener pipeline ONLY if the snapshot
    // represents a meaningful state change (different broadcaster,
    // different song, or different arrangement). A bare highway
    // reconnect that re-issues the same `connected` snapshot
    // shouldn't interrupt peer audio — its scheduled sources are
    // still anchored to the right chart timeline, and tearing them
    // down here would cause an audible dropout for what is otherwise
    // a transparent control-plane blip. The state-apply step below
    // will still re-seek if local audio.currentTime has drifted
    // (which calls _audioListenerOnChartPause itself), so we don't
    // miss legitimate chart-time invalidations.
    const sameBroadcaster = _audioListenerBroadcasterId === (_room.broadcaster_id || null);
    const wantedFilename = (
        typeof _room.now_playing === 'number'
        && _room.now_playing >= 0
        && _room.queue
        && _room.queue[_room.now_playing]
    ) ? _room.queue[_room.now_playing].filename : null;
    const wantedArrangement = (_room.players && _room.players[_playerId])
        ? _room.players[_playerId].arrangement : 'Lead';
    const sameSong = wantedFilename === _loadedFilename && wantedArrangement === _loadedArrangement;
    if (!sameBroadcaster || !sameSong) {
        // If we previously had a broadcaster and now the snapshot
        // shows a different one (or none), the audio WS may still
        // hold buffered packets from the previous broadcaster that
        // arrive after the snapshot. Arm the handoff suppression
        // window now — clearing _audioListenerBroadcasterId before
        // _audioListenerOnControlSet runs at the bottom of bootstrap
        // would otherwise make that call see a null→new transition
        // and skip its own suppression arming.
        if (
            _audioListenerBroadcasterId !== null
            && _audioListenerBroadcasterId !== (_room.broadcaster_id || null)
        ) {
            _audioListenerHandoffSuppressUntil = (
                (typeof performance !== 'undefined' && performance.now ? performance.now() : Date.now())
                + HANDOFF_SUPPRESS_MS
            );
        }
        _audioListenerOnChartPause();
        _audioListenerBroadcasterId = null;
        _audioListenerBroadcastParams = null;
    }
    if (
        typeof _room.now_playing === 'number'
        && _room.now_playing >= 0
        && _room.queue
        && _room.queue[_room.now_playing]
    ) {
        const wantedItem = _room.queue[_room.now_playing];
        const wantedArr = (_room.players && _room.players[_playerId])
            ? _room.players[_playerId].arrangement : 'Lead';
        // Skip the reload if the local <audio> element already has
        // the right track + arrangement loaded — _loadSong sets
        // _songLoading = true for ~2 s and both _onHeartbeat and
        // _audioListenerHandleFrame drop work during that window, so a
        // transient WS reconnect would otherwise needlessly mute peer
        // audio + heartbeat corrections. A reload is only needed when
        // the snapshot's current song or this player's arrangement
        // differs from what we last loaded.
        if (
            wantedItem.filename !== _loadedFilename
            || wantedArr !== _loadedArrangement
        ) {
            let loadFailed = false;
            try {
                await _loadSong(wantedItem);
            } catch (e) {
                // Fail closed: don't apply chart state or activate the
                // listener pipeline against whatever stale <audio>
                // source was previously loaded. The listener will
                // continue dropping inbound frames as 'listener_paused'
                // (or 'no_broadcaster', since broadcaster_id is still
                // null — we cleared it at the top) until the user
                // intervenes (reload, manual song pick).
                loadFailed = true;
            }
            // A newer connected snapshot or song_changed may have superseded
            // this bootstrap during the ~2 s _loadSong window. If so, the
            // newer call has already done (or will do) its own load + state
            // apply — DON'T touch chart state here against stale _room
            // values that may still be sitting in scope.
            // Distinguish supersede from failure:
            //   - _bootstrapGen changed → some path invalidated us
            //     (newer bootstrap, or non-bootstrap state change).
            //     Return null so outer keeps the previous
            //     _lastBootstrapSucceeded value.
            //   - loadFailed → genuine error; return false so the
            //     outer flips _lastBootstrapSucceeded to false.
            if (myGen !== _bootstrapGen) return null;
            if (loadFailed) return false;
        }
    }
    // Snapshot may have changed during the await (a 4408 / leave can
    // null _room). Treat as supersede, not failure.
    if (!_room) return null;
    if (typeof _room.time === 'number') {
        const audioEl = document.getElementById('audio');
        if (audioEl) {
            // Detect whether the snapshot actually requires a state
            // change to the local <audio>. If everything matches
            // (transient highway reconnect with no real change),
            // skip the chartPause so peer-audio scheduling continues
            // uninterrupted.
            const drift = Math.abs((audioEl.currentTime || 0) - _room.time);
            const wantedSpeed = _room.speed || 1.0;
            const speedDelta = Math.abs((audioEl.playbackRate || 1.0) - wantedSpeed);
            const wantPaused = _room.state === 'paused' || _room.state === 'stopped';
            const wantPlaying = _room.state === 'playing';
            const stateChange = (
                drift > 0.05
                || speedDelta > 0.01
                || (wantPlaying && audioEl.paused)
                || (wantPaused && !audioEl.paused)
            );
            if (stateChange) {
                // Snapshot-driven state restore is a chart-time-breaking
                // event from the listener pipeline's perspective: any
                // sources scheduled before the reconnect were anchored
                // against the PRE-reconnect chart timeline / speed /
                // play-pause state. Drop them; new frames will rebuild
                // a fresh schedule against the synced clock.
                _audioListenerOnChartPause();
                // 0.05s threshold (vs the prior 0.5s): _onHeartbeat's
                // normal drift correction nudges playbackRate by only
                // ±0.002, which would take many seconds to close a
                // 50–500 ms post-reconnect drift.
                if (drift > 0.05) {
                    try { audioEl.currentTime = _room.time; } catch (e) { /* */ }
                }
                if (speedDelta > 0.01) {
                    audioEl.playbackRate = wantedSpeed;
                }
                if (wantPlaying && audioEl.paused) {
                    _audioListenerMaybeResumeContext();
                    audioEl.play().catch(() => {});
                } else if (wantPaused && !audioEl.paused) {
                    // Both 'paused' (user-pressed pause) and 'stopped'
                    // (no song loaded / queue finished) imply the
                    // chart clock should not be running locally.
                    try { audioEl.pause(); } catch (e) { /* */ }
                }
            }
        }
    }
    // Activate the listener pipeline LAST, against the now-synced
    // chart clock. Doing this earlier would let frames arriving
    // during _loadSong / state-apply schedule against the stale
    // pre-reconnect chart timeline.
    if (_room.broadcaster_id && _room.broadcaster_id !== _playerId) {
        _audioListenerOnControlSet(_room.broadcaster_id, _room.broadcast_params || null);
    } else if (_room.broadcaster_id === _playerId) {
        _audioListenerOnSelfBroadcast();
    } else {
        _audioListenerOnControlClear();
    }
    // Capture-side recovery is handled by the OUTER wrapper's
    // finally block (after _bootstrapInFlight decrements to 0)
    // and by the audio WS onopen handler if /audio attaches
    // last. Calling _maybeReissueBroadcastAfterRecovery here
    // would always early-return because _bootstrapInFlight is
    // still > 0. Spotted by Copilot review on PR #9.
    return true;
}

// Re-issue broadcast_start after a 4408 session reset, but ONLY
// when both highway and audio WSs are open (the server requires
// /audio attached before accepting broadcast_start, returning
// 'audio_ws_not_open' otherwise — which would tear the local
// capture down via the error handler in _handleMessage). Called
// from both the highway 'connected' branch (where the highway
// just reattached) and the audio WS onopen handler (which fires
// after the highway one if audio reconnects last). Spotted by
// codex review on PR #8.
function _maybeReissueBroadcastAfterRecovery() {
    // Only fires after a 4408 reset that left an active broadcast
    // owing the server a re-issue. Without this explicit flag, the
    // helper would also fire when the user toggles broadcast on
    // during the initial connected bootstrap — duplicate
    // broadcast_start traffic and harder-to-reason behavior.
    // Spotted by Copilot review on PR #9.
    if (!_captureNeedsReissueAfterReset) return;
    if (
        !_captureRequested
        || !_captureCtx
        || _captureBroadcasterAcked
    ) return;
    // Key off the post-reset attach markers, NOT raw readyState.
    // During a 4408 reset both _ws and _audioWs still point at the
    // OLD session's sockets until the reconnect timers replace
    // them; their readyState can read OPEN for those stale
    // sockets and we'd send broadcast_start through the wrong
    // session. _highwayAuthedAfterReset / _audioOpenedAfterReset
    // are only flipped true after the NEW session's endpoint
    // has been confirmed (highway 'connected' message / audio
    // onopen). Spotted by codex review on PR #8.
    if (!_highwayAuthedAfterReset || !_audioOpenedAfterReset) return;
    if (!_ws || _ws.readyState !== WebSocket.OPEN) return;
    if (!_audioWs || _audioWs.readyState !== WebSocket.OPEN) return;
    // Gate on bootstrap being fully done. Otherwise we could re-ack
    // the broadcast while _captureChartTime() is still based on
    // pre-reconnect chart state (loaded song / time / paused /
    // speed not yet resynced), and outbound frames would carry
    // wrong chart positions. _bootstrapOnConnected calls us again
    // in its finally block once the chart state is ready.
    // Two-part bootstrap gate:
    //   1. _bootstrapInFlight > 0 means a bootstrap is mid-run; chart
    //      state may not be fully resynced. Wait.
    //   2. _lastBootstrapSucceeded is the most recent COMPLETED
    //      bootstrap's result. False if it failed (e.g. _loadSong
    //      threw and the path failed-closed). Recovery would
    //      otherwise resume broadcasting against unsynced chart
    //      state.
    if (_bootstrapInFlight > 0) return;
    if (!_lastBootstrapSucceeded) return;
    try {
        _ws.send(JSON.stringify({
            type: 'broadcast_start',
            interval_beats: _captureIntervalBeats,
            sample_rate: SMAU_V1_SAMPLE_RATE,
            channel_count: SMAU_V1_CHANNEL_COUNT,
            codec: 'opus',
            bitrate: 96000,
        }));
        // Clear the flag so subsequent calls (from the OTHER
        // gating path that didn't fire first) don't re-send.
        // The server's broadcaster_changed ack will flip
        // _captureBroadcasterAcked which is the longer-term
        // guard.
        _captureNeedsReissueAfterReset = false;
    } catch (_err) {
        // WS could close between the readyState check and send;
        // tear down the capture rather than leaving the encoder
        // running silently against a server that's forgotten us.
        _broadcastStop();
    }
}

// ── Message Handling ───────────────────────────────────────────────────

function _handleMessage(msg) {
    switch (msg.type) {
        case 'connected':
            _room = msg.room;
            _isHost = _room.host === _playerId;
            _renderPlayers();
            _renderQueue();
            _updateControls();
            _doClockSync();
            // Highway side of the recovery is confirmed. Set this
            // BEFORE invoking _bootstrapOnConnected — the bootstrap
            // can run synchronously up to its first await (e.g. when
            // the chart is dedup-cached and _loadSong is skipped),
            // and any internal call to _maybeReissueBroadcastAfterRecovery
            // would gate-fail if _highwayAuthedAfterReset is still
            // false. Spotted by codex review on PR #8.
            _highwayAuthedAfterReset = true;
            _maybeClearResetMarker();
            // The recovery flow (load song, sync chart clock, resume
            // playback, activate listener) is async because _loadSong
            // pauses non-host audio when it completes. Running play()
            // before _loadSong finishes would briefly play and then be
            // re-paused by the load. Defer to a fire-and-forget helper
            // that awaits the load before applying state and listener
            // setup.
            _bootstrapOnConnected().catch(() => {});
            break;

        case 'clock_sync_response':
            if (_pendingSyncResolve) {
                _pendingSyncResolve(msg);
                _pendingSyncResolve = null;
            }
            break;

        case 'player_joined':
            if (_room) {
                _room.players[msg.player_id] = {
                    name: msg.name,
                    arrangement: msg.arrangement || 'Lead',
                    connected: false,
                };
                _renderPlayers();
            }
            break;

        case 'player_left':
            if (_room) {
                delete _room.players[msg.player_id];
                _renderPlayers();
            }
            break;

        case 'player_connected':
            if (_room && _room.players[msg.player_id]) {
                _room.players[msg.player_id].connected = true;
                _renderPlayers();
            }
            break;

        case 'player_disconnected':
            if (_room && _room.players[msg.player_id]) {
                _room.players[msg.player_id].connected = false;
                _renderPlayers();
            }
            break;

        case 'host_changed':
            if (_room) {
                _room.host = msg.new_host_id;
                _isHost = _room.host === _playerId;
                if (_isHost) _startHeartbeat();
                else _stopHeartbeat();
                _renderPlayers();
                _updateControls();
            }
            break;

        case 'arrangement_changed':
            if (_room && _room.players[msg.player_id]) {
                _room.players[msg.player_id].arrangement = msg.arrangement;
                _renderPlayers();
            }
            break;

        case 'playback_state':
            if (!_isHost) _onPlaybackState(msg);
            if (_room) {
                _room.state = msg.state;
                _room.time = msg.time;
                _room.speed = msg.speed;
            }
            break;

        case 'heartbeat':
            if (!_isHost) _onHeartbeat(msg);
            break;

        case 'queue_updated':
            if (_room) {
                _room.queue = msg.queue;
                _room.now_playing = msg.now_playing;
                _renderQueue();
            }
            break;

        case 'song_changed':
            if (_room) {
                _room.now_playing = msg.now_playing;
                _room.state = 'stopped';
                _room.time = 0;
                _renderQueue();
                _updateNowPlaying();
                // Chart timeline just changed under any in-flight peer-audio
                // schedule. Drop it so already-queued sources don't keep
                // playing on top of the new song; a `playback_state` update
                // will start the new schedule once the new song is loaded.
                _audioListenerOnChartPause();
                // Invalidate any in-flight _bootstrapOnConnected so its
                // mid-await loadSong-of-the-old-song doesn't proceed to
                // apply state for the wrong track.
                _bootstrapGen++;
                // Host already called _loadSong in mpLoadSong — skip to avoid double-load
                if (!_isHost && msg.queue_item) _loadSong(msg.queue_item);
            }
            break;

        case 'queue_finished':
            if (_room) {
                _room.now_playing = -1;
                _room.state = 'stopped';
                _renderQueue();
                _updateNowPlaying();
                // Chart timeline ended. Same reasoning as song_changed.
                _audioListenerOnChartPause();
                _bootstrapGen++;
            }
            break;

        case 'vote_skip':
            _updateSkipCount(msg.votes, msg.needed);
            break;

        case 'recording_state':
            if (_room) _room.recording = msg.recording;
            _onRecordingState(msg.recording);
            break;

        case 'recording_uploaded':
            if (_room) {
                if (!_room.recordings_received) _room.recordings_received = [];
                if (msg.player_id && !_room.recordings_received.includes(msg.player_id)) {
                    _room.recordings_received.push(msg.player_id);
                }
            }
            _updateRecordingStatus(msg);
            break;

        case 'mixdown_ready':
            _onMixdownReady(msg.url);
            break;

        case 'room_destroyed':
            _cleanup();
            _showLobbyView();
            _showError('Room was closed');
            break;

        case 'broadcaster_changed':
            // PROTOCOL.md "Audio control messages — broadcast_start" /
            // "broadcast_stop": single source of truth for who (if anyone)
            // is broadcasting in this room. Mirror the room state so a
            // subsequent reconnect's `connected` snapshot stays in sync,
            // and route to the listener pipeline. v1: hosts don't listen
            // to themselves — the broadcaster_id check skips self-loop.
            if (_room) {
                _room.broadcaster_id = msg.broadcaster_id || null;
                _room.broadcast_params = msg.broadcaster_id ? {
                    interval_beats: msg.interval_beats,
                    sample_rate: msg.sample_rate,
                    channel_count: msg.channel_count,
                    codec: msg.codec,
                    bitrate: msg.bitrate,
                } : null;
            }
            if (msg.broadcaster_id && msg.broadcaster_id !== _playerId) {
                _audioListenerOnControlSet(msg.broadcaster_id, _room ? _room.broadcast_params : null);
            } else if (msg.broadcaster_id === _playerId) {
                // Self-takeover: hard-stop instead of graceful drain;
                // see _audioListenerOnSelfBroadcast for why we don't
                // want to keep playing the previous broadcaster's tail
                // on the new broadcaster's machine.
                _audioListenerOnSelfBroadcast();
            } else {
                _audioListenerOnControlClear();
            }
            // Phase 2d: capture path observes the same event to know
            // whether the server accepted our broadcast_start. Self ack
            // unlocks frame send; non-self / null tears down our own
            // capture if we thought we were broadcasting.
            _captureOnBroadcasterChanged(msg.broadcaster_id || null);
            break;

        case 'audio_quality':
            // Listener-reported telemetry forwarded by the server
            // (only the broadcaster receives these). Server adds
            // `from_player_id` so we can key per-listener.
            _audioQualityRxOnReport(msg);
            break;

        case 'error':
            console.error('[MP] Server error:', msg.message);
            // Capture-side rejection of broadcast_start: if the user
            // toggled broadcast on but the server refused (another
            // broadcaster is active, or our /audio WS isn't attached),
            // tear our local capture pipeline down and surface a
            // user-facing message. Without this the pill sits at
            // "Connecting…" forever and the encoder keeps producing
            // intervals that frame-send drops on the broadcaster_acked
            // gate. Spotted by Copilot review (suppressed) on PR #8
            // round 2.
            if (
                (_captureRequested || _captureCtx)
                && (msg.message === 'broadcaster_busy' || msg.message === 'audio_ws_not_open')
            ) {
                _broadcastStop({ silent: true });
                _showError(
                    msg.message === 'broadcaster_busy'
                        ? 'Another player is already broadcasting.'
                        : 'Audio connection not ready. Please try again.'
                );
            }
            break;
    }
}

// ── Clock Sync ─────────────────────────────────────────────────────────

async function _doClockSync() {
    const rounds = [];

    for (let i = 0; i < SYNC_ROUNDS; i++) {
        const t1 = performance.now();
        _ws.send(JSON.stringify({ type: 'clock_sync_request', client_t1: t1 }));

        const result = await new Promise((resolve) => {
            _pendingSyncResolve = resolve;
            setTimeout(() => {
                if (_pendingSyncResolve === resolve) {
                    _pendingSyncResolve = null;
                    resolve(null);
                }
            }, 2000);
        });

        if (result) {
            const t4 = performance.now();
            const rtt = (t4 - result.client_t1) - (result.server_t3 - result.server_t2);
            const offset = ((result.server_t2 - result.client_t1) + (result.server_t3 - t4)) / 2;
            rounds.push({ rtt, offset });
        }

        await new Promise(r => setTimeout(r, 50));
    }

    if (rounds.length >= 3) {
        rounds.sort((a, b) => a.rtt - b.rtt);
        const mid = Math.floor(rounds.length / 2);
        _clockOffset = rounds[mid].offset;
    }

    console.log(`[MP] Clock sync: offset=${_clockOffset.toFixed(1)}ms (${rounds.length} rounds)`);

    // If host, start heartbeat
    if (_isHost) _startHeartbeat();

    // Intercept playback controls
    _interceptPlaybackControls();
}

// ── Playback Sync ──────────────────────────────────────────────────────

function _startHeartbeat() {
    _stopHeartbeat();
    _heartbeatInterval = setInterval(() => {
        if (!_ws || _ws.readyState !== WebSocket.OPEN || !_isHost) return;
        const audio = document.getElementById('audio');
        if (!audio || audio.paused) return;
        _ws.send(JSON.stringify({
            type: 'heartbeat',
            time: audio.currentTime,
            client_time: performance.now(),
        }));
    }, HEARTBEAT_HZ);
}

function _stopHeartbeat() {
    if (_heartbeatInterval) {
        clearInterval(_heartbeatInterval);
        _heartbeatInterval = null;
    }
}

function _onPlaybackState(msg) {
    const audio = document.getElementById('audio');
    if (!audio) return;

    if (msg.state === 'playing') {
        // Any in-flight peer-audio scheduling was anchored against the
        // PREVIOUS chart position; a hard seek invalidates it. Drop the
        // schedule and let new frames rebuild it organically.
        _audioListenerOnChartPause();
        // The same play→audio.play() call site is the user-gesture
        // window where a previously-suspended listener AudioContext
        // can resume. Best-effort; no-op if the context is already
        // running or doesn't yet exist.
        _audioListenerMaybeResumeContext();
        audio.currentTime = msg.time;
        audio.playbackRate = msg.speed || 1.0;
        audio.play().catch(() => {});
        if (typeof isPlaying !== 'undefined') isPlaying = true;
        const btn = document.getElementById('btn-play');
        if (btn) btn.textContent = '\u23F8 Pause';
        const mpBtn = document.getElementById('mp-btn-play');
        if (mpBtn) mpBtn.textContent = 'Pause';
        _startRecordingNow();
    } else if (msg.state === 'paused') {
        // Listener paused — schedule must be cleared, future frames will
        // be dropped with reason 'listener_paused' until play resumes.
        _audioListenerOnChartPause();
        audio.currentTime = msg.time;
        audio.pause();
        if (typeof isPlaying !== 'undefined') isPlaying = false;
        const btn = document.getElementById('btn-play');
        if (btn) btn.textContent = '\u25B6 Play';
        const mpBtn = document.getElementById('mp-btn-play');
        if (mpBtn) mpBtn.textContent = 'Play';
    }

    // Update speed UI
    const speedLabel = document.getElementById('mp-speed-label');
    const speedSlider = document.getElementById('mp-speed');
    if (speedLabel) speedLabel.textContent = (msg.speed || 1.0).toFixed(2) + 'x';
    if (speedSlider) speedSlider.value = Math.round((msg.speed || 1.0) * 100);
}

function _onHeartbeat(msg) {
    if (_isHost || _songLoading) return;
    const audio = document.getElementById('audio');
    if (!audio || audio.paused) return;

    const drift = audio.currentTime - msg.time;
    const absDrift = Math.abs(drift);

    if (absDrift > 0.5) {
        // Hard seek — drop scheduled peer audio so it doesn't play out of
        // sync with the new chart position.
        _audioListenerOnChartPause();
        audio.currentTime = msg.time;
        if (typeof highway !== 'undefined') highway.setTime(msg.time);
    } else if (absDrift > 0.05) {
        // Micro-adjust: if ahead slow down, if behind speed up
        const baseSpeed = (_room && _room.speed) || 1.0;
        const correction = drift > 0 ? -0.002 : 0.002;
        audio.playbackRate = baseSpeed + correction;

        if (_driftResetTimer) clearTimeout(_driftResetTimer);
        _driftResetTimer = setTimeout(() => {
            audio.playbackRate = baseSpeed;
            _driftResetTimer = null;
        }, 500);
    }
}

// ── Playback Interception ──────────────────────────────────────────────

function _injectPlayerRecBtn() {
    const c = document.getElementById('player-controls');
    if (!c || document.getElementById('mp-player-rec-btn')) return;
    const btn = document.createElement('button');
    btn.id = 'mp-player-rec-btn';
    btn.className = 'px-3 py-1.5 bg-dark-600 hover:bg-dark-500 rounded-lg text-xs text-gray-400 transition';
    btn.textContent = 'REC';
    btn.title = _isHost ? 'Toggle recording' : 'Recording controlled by host';
    btn.disabled = !_isHost;
    btn.style.opacity = _isHost ? '' : '0.5';
    btn.onclick = () => {
        if (!_isHost || !_ws) return;
        const isRec = _room && _room.recording;
        _ws.send(JSON.stringify({ type: isRec ? 'stop_recording' : 'start_recording' }));
    };
    // Update appearance based on current state
    if (_room && _room.recording) {
        btn.className = 'px-3 py-1.5 bg-red-900/50 hover:bg-red-800/50 rounded-lg text-xs text-red-400 transition';
        btn.textContent = '⏺ REC';
    }
    const separator = c.querySelector('span.text-gray-700');
    if (separator) c.insertBefore(btn, separator);
    else c.appendChild(btn);
}

function _removePlayerRecBtn() {
    const btn = document.getElementById('mp-player-rec-btn');
    if (btn) btn.remove();
}

function _updatePlayerRecBtn(recording) {
    const btn = document.getElementById('mp-player-rec-btn');
    if (!btn) return;
    if (recording) {
        btn.className = 'px-3 py-1.5 bg-red-900/50 hover:bg-red-800/50 rounded-lg text-xs text-red-400 transition';
        btn.textContent = '⏺ REC';
    } else {
        btn.className = 'px-3 py-1.5 bg-dark-600 hover:bg-dark-500 rounded-lg text-xs text-gray-400 transition';
        btn.textContent = 'REC';
    }
}

function _interceptPlaybackControls() {
    if (_origTogglePlay) return; // already intercepted
    _injectPlayerRecBtn();
    _origTogglePlay = window.togglePlay;
    _origSeekBy = window.seekBy;
    _origSetSpeed = window.setSpeed;

    window.togglePlay = function () {
        if (_roomCode) {
            if (_isHost) {
                mpTogglePlay();
            }
            // Non-host: no-op
        } else if (_origTogglePlay) {
            _origTogglePlay();
        }
    };

    window.seekBy = function (s) {
        if (_roomCode) {
            if (_isHost) mpSeek(s);
        } else if (_origSeekBy) {
            _origSeekBy(s);
        }
    };

    window.setSpeed = function (v) {
        if (_roomCode) {
            if (_isHost) mpSetSpeed(v);
        } else if (_origSetSpeed) {
            _origSetSpeed(v);
        }
    };
}

function _restorePlaybackControls() {
    if (_origTogglePlay) { window.togglePlay = _origTogglePlay; _origTogglePlay = null; }
    if (_origSeekBy) { window.seekBy = _origSeekBy; _origSeekBy = null; }
    if (_origSetSpeed) { window.setSpeed = _origSetSpeed; _origSetSpeed = null; }
    _removePlayerRecBtn();
}

// ── Host Playback Controls ─────────────────────────────────────────────

function mpTogglePlay() {
    const audio = document.getElementById('audio');
    if (!audio || !_ws || !_isHost) return;

    if (audio.paused) {
        // Mirror non-host transport hooks: a host listening to another
        // player's broadcast also has scheduled peer audio anchored to
        // the chart timeline, and the host's own play/pause never goes
        // through _onPlaybackState.
        _audioListenerOnChartPause();
        // Invalidate any in-flight _bootstrapOnConnected so its mid-
        // await tail can't re-pause/re-seek us back to the stale
        // pre-transport snapshot state after we've just acted.
        _bootstrapGen++;
        _audioListenerMaybeResumeContext();
        audio.play().catch(() => {});
        if (typeof isPlaying !== 'undefined') isPlaying = true;
        _ws.send(JSON.stringify({
            type: 'play',
            time: audio.currentTime,
            speed: audio.playbackRate,
        }));
        const btn = document.getElementById('mp-btn-play');
        if (btn) btn.textContent = 'Pause';
        const mainBtn = document.getElementById('btn-play');
        if (mainBtn) mainBtn.textContent = '\u23F8 Pause';
        _startRecordingNow();
    } else {
        _audioListenerOnChartPause();
        _bootstrapGen++;
        audio.pause();
        if (typeof isPlaying !== 'undefined') isPlaying = false;
        _ws.send(JSON.stringify({
            type: 'pause',
            time: audio.currentTime,
        }));
        const btn = document.getElementById('mp-btn-play');
        if (btn) btn.textContent = 'Play';
        const mainBtn = document.getElementById('btn-play');
        if (mainBtn) mainBtn.textContent = '\u25B6 Play';
    }
}
window.mpTogglePlay = mpTogglePlay;

function mpSeek(delta) {
    const audio = document.getElementById('audio');
    if (!audio || !_ws || !_isHost) return;
    // Hard seek invalidates any peer-audio schedule that was anchored
    // against the previous chart position. Also bump _bootstrapGen so
    // an in-flight reconnect bootstrap can't re-seek us back to its
    // stale snapshot time after this action.
    _audioListenerOnChartPause();
    _bootstrapGen++;
    audio.currentTime = Math.max(0, audio.currentTime + delta);
    _ws.send(JSON.stringify({
        type: 'seek',
        time: audio.currentTime,
    }));
}
window.mpSeek = mpSeek;

function mpSetSpeed(val) {
    const audio = document.getElementById('audio');
    if (!audio || !_ws || !_isHost) return;
    const speed = parseFloat(val) / 100;
    // Any speed change invalidates the peer-audio schedule. We don't
    // time-stretch in v1, so a non-1.0x setting silences peer audio
    // (frames dropped via the listener_speed gate). But returning TO
    // 1.0x also has to flush: sources scheduled at the old non-1.0
    // chart→AudioContext mapping would otherwise still fire after the
    // reset. Non-host listeners don't have this gap because their
    // _onPlaybackState path clears on every echoed set_speed; mirror
    // that here for the host's local schedule. Also bump
    // _bootstrapGen so an in-flight reconnect bootstrap can't re-set
    // playbackRate to its stale snapshot value after this action.
    _audioListenerOnChartPause();
    _bootstrapGen++;
    audio.playbackRate = speed;
    _ws.send(JSON.stringify({
        type: 'set_speed',
        speed: speed,
    }));
    const label = document.getElementById('mp-speed-label');
    if (label) label.textContent = speed.toFixed(2) + 'x';
}
window.mpSetSpeed = mpSetSpeed;

// ── Song Loading ───────────────────────────────────────────────────────

async function _loadSong(queueItem) {
    // Deduplicate concurrent loads. _loadSong has a tail that pauses
    // non-host audio, so two overlapping invocations can step on each
    // other: the second's bootstrap resumes playback and then the
    // first's pause runs after, leaving the guest stuck stopped.
    // Concurrent calls share the same in-flight promise so callers
    // see a single completion.
    //
    // Loop because two callers waiting on the SAME _loadingPromise
    // both fall through after it resolves; if the awaited load didn't
    // produce the file we want, we'd otherwise both kick a new
    // _doLoadSong (overlapping again). The loop re-checks
    // _loadingPromise after each wait — the first waiter to fall
    // through claims the next slot; subsequent waiters await its
    // promise instead of starting a fresh one.
    while (_loadingPromise) {
        const inflight = _loadingPromise;
        await inflight.catch(() => {});
        // If the in-flight load already loaded the requested file +
        // arrangement, don't restart. Reload if either the filename
        // OR the player's selected arrangement has changed.
        const wantedArr = (_room && _room.players && _room.players[_playerId])
            ? _room.players[_playerId].arrangement : 'Lead';
        if (queueItem
            && _loadedFilename === queueItem.filename
            && _loadedArrangement === wantedArr) {
            return;
        }
        // Another caller may have already chained the next load while
        // we were awaiting — if so, await theirs instead of starting
        // a fresh overlapping one.
        if (_loadingPromise && _loadingPromise !== inflight) continue;
        break;
    }
    const myPromise = _doLoadSong(queueItem);
    _loadingPromise = myPromise;
    try {
        await myPromise;
    } finally {
        // Only clear the slot if it still points at OUR promise. A
        // newer caller waiting on us above may have already chained
        // its own _doLoadSong into _loadingPromise during the await
        // (the chain pattern in the loop). Clobbering would let yet
        // another caller think no load is active and start a third
        // overlapping _doLoadSong, which is the exact race this
        // helper exists to prevent.
        if (_loadingPromise === myPromise) {
            _loadingPromise = null;
        }
    }
}

async function _doLoadSong(queueItem) {
    const myLoadGen = _loadGen;
    _songLoading = true;
    // Find arrangement index matching this player's chosen arrangement
    const myArrangement = (_room && _room.players[_playerId])
        ? _room.players[_playerId].arrangement : 'Lead';
    let arrIndex;
    const arrs = queueItem.arrangements || [];
    const idx = arrs.findIndex(a =>
        (typeof a === 'string' ? a : a.name) === myArrangement
    );
    if (idx >= 0) arrIndex = idx;

    let succeeded = false;
    try {
        // Call global playSong (await to let the full plugin chain set up).
        // If playSong isn't installed (slopsmith core not loaded yet, or
        // some unexpected page state), THROW rather than silently
        // returning — _bootstrapOnConnected and other callers wrap
        // _loadSong in try/catch and treat exceptions as load
        // failures (so they abort the rest of bootstrap). A silent
        // return would let those callers continue applying chart
        // state and activating the listener for a song that never
        // loaded. Spotted by Copilot review on PR #7 round 4.
        if (typeof playSong !== 'function') {
            throw new Error('playSong is not available');
        }
        await playSong(queueItem.filename, arrIndex);
        // Give plugins time to finish async setup (stems, highway _onReady, etc.)
        await new Promise(r => setTimeout(r, 2000));
        succeeded = true;
    } finally {
        // Always clear _songLoading so heartbeats and the listener
        // pipeline stop dropping under 'song_loading' even if playSong
        // or the plugin chain throws — otherwise a single failed load
        // would gate them for the rest of the session.
        _songLoading = false;
        // Only persist the cache markers on a SUCCESSFUL load that's
        // still authoritative. Two reasons:
        //   1. If the load throws, _bootstrapOnConnected uses the
        //      markers as proof of readiness; we want a retry on the
        //      next reconnect.
        //   2. _cleanup() bumps _loadGen to invalidate stale loads —
        //      a load that started in room A and completed after the
        //      user joined room B would otherwise mark room A's song
        //      as "already loaded", making a subsequent bootstrap
        //      skip a needed reload.
        if (succeeded && _loadGen === myLoadGen) {
            _loadedFilename = queueItem ? queueItem.filename : null;
            _loadedArrangement = myArrangement;
        }
    }

    // If a newer room/cleanup invalidated us mid-load, suppress the
    // post-load side effects so a stale load from a previous room
    // can't pause/seek the user's now-current room. Spotted by
    // Copilot review on PR #7 round 8 (suppressed/low-confidence).
    if (_loadGen !== myLoadGen) return;

    if (!_isHost) {
        const audio = document.getElementById('audio');
        if (audio) {
            audio.pause();
            if (typeof isPlaying !== 'undefined') isPlaying = false;
        }
    }
}

// Host loads and starts a song from the queue
window.mpLoadSong = function (index) {
    if (!_isHost || !_ws) return;
    _ws.send(JSON.stringify({ type: 'load_song', index: index }));
    // Also load locally
    if (_room && _room.queue[index]) {
        _room.now_playing = index;
        _room.state = 'stopped';
        _room.time = 0;
        _renderQueue();
        _updateNowPlaying();
        // Chart timeline is about to change — drop any peer-audio
        // schedule anchored against the old chart. The server emits
        // song_changed with exclude=this player, so the inbound
        // dispatch's song_changed branch never fires for the host;
        // mirror its _audioListenerOnChartPause() call here so a
        // host listening to another player's broadcast doesn't keep
        // hearing the previous chart's peer audio over the new song.
        // Also bump _bootstrapGen so an in-flight reconnect bootstrap
        // (which can be mid-_loadSong of the OLD now_playing for ~2 s)
        // doesn't run its post-await tail against the now-stale
        // snapshot, e.g. by re-pause/seek/listener-set against the
        // previous song.
        _audioListenerOnChartPause();
        _bootstrapGen++;
        _loadSong(_room.queue[index]);
    }
};

// ── Queue UI ───────────────────────────────────────────────────────────

window.mpSearchSongs = async function () {
    const q = document.getElementById('mp-search')?.value.trim();
    if (!q) return;
    const resp = await fetch(`/api/library?q=${encodeURIComponent(q)}&page=0&size=10&sort=artist`);
    const data = await resp.json();
    const container = document.getElementById('mp-search-results');
    if (!container) return;

    if (!data.songs || data.songs.length === 0) {
        container.innerHTML = '<p class="text-gray-500 text-xs py-2">No results</p>';
        return;
    }

    container.innerHTML = data.songs.map(s => {
        const arrs = (s.arrangements || []).map(a => a.name || a);
        const arrsJson = JSON.stringify(arrs).replace(/'/g, "\\'").replace(/"/g, '&quot;');
        return `<div class="flex items-center gap-3 py-2 px-3 rounded-lg hover:bg-dark-700/50 transition">
            <div class="flex-1 min-w-0">
                <span class="text-sm text-white">${esc(s.title)}</span>
                <span class="text-xs text-gray-500 ml-2">${esc(s.artist)}</span>
            </div>
            <button onclick="mpAddToQueue('${encodeURIComponent(s.filename)}','${esc(s.title).replace(/'/g,"\\'")}','${esc(s.artist).replace(/'/g,"\\'")}', '${arrsJson}')"
                class="px-3 py-1 bg-dark-600 hover:bg-accent/30 rounded text-xs text-gray-300 hover:text-white transition flex-shrink-0">+ Add</button>
        </div>`;
    }).join('');
};

window.mpAddToQueue = async function (filename, title, artist, arrsJson) {
    if (!_roomCode || !_playerId) return;
    let arrangements = [];
    try { arrangements = JSON.parse(arrsJson.replace(/&quot;/g, '"')); } catch (e) { /* */ }
    await fetch(`/api/plugins/multiplayer/rooms/${_roomCode}/queue`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            player_id: _playerId,
            filename: decodeURIComponent(filename),
            title, artist, arrangements,
        }),
    });
};

window.mpRemoveFromQueue = async function (index) {
    if (!_roomCode) return;
    await fetch(`/api/plugins/multiplayer/rooms/${_roomCode}/queue/${index}?player_id=${_playerId}`, {
        method: 'DELETE',
    });
};

window.mpVoteSkip = async function () {
    if (!_roomCode || !_playerId) return;
    await fetch(`/api/plugins/multiplayer/rooms/${_roomCode}/vote-skip`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ player_id: _playerId }),
    });
};

function _renderQueue() {
    if (!_room) return;
    const container = document.getElementById('mp-queue-list');
    const countEl = document.getElementById('mp-queue-count');
    if (!container) return;

    if (countEl) countEl.textContent = _room.queue.length ? `${_room.queue.length} song${_room.queue.length !== 1 ? 's' : ''}` : '';

    if (_room.queue.length === 0) {
        container.innerHTML = '<p class="text-gray-500 text-sm">No songs in queue.</p>';
        _updateNowPlaying();
        return;
    }

    container.innerHTML = _room.queue.map((item, i) => {
        const isCurrent = i === _room.now_playing;
        const addedBy = _findPlayerName(item.added_by);
        return `<div class="flex items-center gap-3 ${isCurrent ? 'bg-accent/10 border-accent/30' : 'bg-dark-700/30 border-gray-800/30'} border rounded-lg p-3 transition">
            <span class="text-xs ${isCurrent ? 'text-accent' : 'text-gray-600'} w-6 text-center font-mono">${isCurrent ? '\u25B6' : i + 1}</span>
            <div class="flex-1 min-w-0 ${_isHost && i !== _room.now_playing ? 'cursor-pointer' : ''}"
                 ${_isHost && i !== _room.now_playing ? `onclick="mpLoadSong(${i})"` : ''}>
                <span class="text-sm text-white truncate block">${esc(item.title || item.filename)}</span>
                <span class="text-xs text-gray-500">${esc(item.artist || '')}${addedBy ? ' \u00B7 added by ' + esc(addedBy) : ''}</span>
            </div>
            <button onclick="mpRemoveFromQueue(${i})"
                class="px-2 py-1 text-gray-600 hover:text-red-400 transition text-xs flex-shrink-0">\u2715</button>
        </div>`;
    }).join('');

    _updateNowPlaying();
}

function _updateNowPlaying() {
    const infoEl = document.getElementById('mp-now-playing-info');
    if (!infoEl || !_room) return;

    if (_room.now_playing >= 0 && _room.now_playing < _room.queue.length) {
        const item = _room.queue[_room.now_playing];
        infoEl.innerHTML = `
            <div class="text-white font-semibold">${esc(item.title || item.filename)}</div>
            <div class="text-xs text-gray-500 mt-0.5">${esc(item.artist || '')}</div>
        `;
    } else {
        infoEl.textContent = 'No song loaded. Add songs to the queue below.';
    }
}

function _updateSkipCount(votes, needed) {
    const el1 = document.getElementById('mp-skip-count');
    const el2 = document.getElementById('mp-skip-count-guest');
    const text = `(${votes}/${needed})`;
    if (el1) el1.textContent = text;
    if (el2) el2.textContent = text;
}

function _findPlayerName(playerId) {
    if (!_room || !playerId) return '';
    const p = _room.players[playerId];
    return p ? p.name : '';
}

// ── Player List ────────────────────────────────────────────────────────

function _renderPlayers() {
    const container = document.getElementById('mp-player-list');
    if (!container || !_room) return;

    const pids = Object.keys(_room.players);
    container.innerHTML = pids.map(pid => {
        const p = _room.players[pid];
        const isMe = pid === _playerId;
        const isPlayerHost = pid === _room.host;
        return `<div class="flex items-center gap-3 bg-dark-800/50 rounded-lg p-3 ${isMe ? 'ring-1 ring-accent/30' : ''}">
            <div class="w-2 h-2 rounded-full flex-shrink-0 ${p.connected ? 'bg-green-400' : 'bg-gray-600'}"></div>
            <div class="flex-1 min-w-0">
                <div class="flex items-center gap-2">
                    <span class="text-sm text-white truncate">${esc(p.name)}</span>
                    ${isPlayerHost ? '<span class="text-[10px] bg-amber-900/40 text-amber-400 px-1.5 py-0.5 rounded">Host</span>' : ''}
                    ${isMe ? '<span class="text-[10px] bg-accent/20 text-accent-light px-1.5 py-0.5 rounded">You</span>' : ''}
                </div>
            </div>
            ${isMe
                ? `<select onchange="mpSetArrangement(this.value)"
                    class="bg-dark-700 border border-gray-700 rounded px-2 py-1 text-xs text-gray-300 focus:outline-none focus:border-accent/50">
                    <option value="Lead" ${p.arrangement === 'Lead' ? 'selected' : ''}>Lead</option>
                    <option value="Rhythm" ${p.arrangement === 'Rhythm' ? 'selected' : ''}>Rhythm</option>
                    <option value="Bass" ${p.arrangement === 'Bass' ? 'selected' : ''}>Bass</option>
                  </select>`
                : `<span class="text-xs text-gray-500">${esc(p.arrangement)}</span>`
            }
        </div>`;
    }).join('');
}

window.mpSetArrangement = function (arr) {
    if (!_ws || _ws.readyState !== WebSocket.OPEN) return;
    _ws.send(JSON.stringify({ type: 'set_arrangement', arrangement: arr }));
    if (_room && _room.players[_playerId]) {
        _room.players[_playerId].arrangement = arr;
    }
};

// ── Controls Visibility ────────────────────────────────────────────────

function _updateControls() {
    const hostControls = document.getElementById('mp-host-controls');
    const guestControls = document.getElementById('mp-guest-controls');
    const recSection = document.getElementById('mp-recording-section');

    if (hostControls) hostControls.classList.toggle('hidden', !_isHost);
    if (guestControls) guestControls.classList.toggle('hidden', _isHost);
    if (recSection) recSection.classList.toggle('hidden', !_isHost);
}

// ── Copy Room Code ─────────────────────────────────────────────────────

window.mpCopyCode = function () {
    if (!_roomCode) return;
    navigator.clipboard.writeText(_roomCode).then(() => {
        const toast = document.getElementById('mp-copy-toast');
        if (toast) {
            toast.classList.remove('opacity-0');
            toast.classList.add('opacity-100');
            setTimeout(() => {
                toast.classList.remove('opacity-100');
                toast.classList.add('opacity-0');
            }, 1500);
        }
    }).catch(() => {});
};

// ── Recording ──────────────────────────────────────────────────────────

window.mpToggleRecording = function () {
    if (!_isHost || !_ws) return;
    const toggle = document.getElementById('mp-rec-toggle');
    const enabling = toggle?.checked;
    _ws.send(JSON.stringify({
        type: enabling ? 'start_recording' : 'stop_recording',
    }));
};

let _recArmed = false;  // armed but not yet recording (waiting for play)

function _onRecordingState(recording) {
    const indicator = document.getElementById('mp-rec-indicator');
    const recToggle = document.getElementById('mp-rec-toggle');
    const guestRec = document.getElementById('mp-guest-rec-status');

    if (indicator) indicator.classList.toggle('hidden', !recording);
    if (recToggle) recToggle.checked = recording;
    _updatePlayerRecBtn(recording);

    if (recording) {
        _armRecording();
        if (!_isHost && guestRec) guestRec.classList.remove('hidden');
    } else {
        _recArmed = false;
        _stopAndUploadRecording();
        if (guestRec) setTimeout(() => guestRec.classList.add('hidden'), 3000);
    }
}

async function _armRecording() {
    // Get mic access and prepare MediaRecorder, but don't start yet — wait for play.
    try {
        // Use desktop bridge if available
        if (window.slopsmithDesktop?.audio?.startRecording) {
            _recArmed = true;
            const statusEl = _isHost
                ? document.getElementById('mp-rec-status')
                : document.getElementById('mp-guest-rec-info');
            if (statusEl) statusEl.textContent = 'Armed — will record when song plays';
            return;
        }

        _mediaStream = await navigator.mediaDevices.getUserMedia({
            audio: {
                channelCount: 1,
                echoCancellation: false,
                noiseSuppression: false,
                autoGainControl: false,
            }
        });
        _recordedChunks = [];

        const mimeType = MediaRecorder.isTypeSupported('audio/webm;codecs=opus')
            ? 'audio/webm;codecs=opus' : 'audio/webm';
        _mediaRecorder = new MediaRecorder(_mediaStream, { mimeType });

        _mediaRecorder.ondataavailable = (e) => {
            if (e.data.size > 0) _recordedChunks.push(e.data);
        };

        // Don't start yet — armed and waiting for playback
        _recArmed = true;

        const statusEl = _isHost
            ? document.getElementById('mp-rec-status')
            : document.getElementById('mp-guest-rec-info');
        if (statusEl) statusEl.textContent = 'Armed — will record when song plays';
    } catch (e) {
        console.error('[MP] getUserMedia failed:', e);
        const statusEl = _isHost
            ? document.getElementById('mp-rec-status')
            : document.getElementById('mp-guest-rec-info');
        if (statusEl) statusEl.textContent = 'Mic access denied: ' + e.message;
    }
}

function _startRecordingNow() {
    // Actually start the MediaRecorder — called when playback begins.
    if (!_recArmed || _isRecording) return;

    // Capture the song position when recording starts — this is the shared
    // reference both host and client agree on (synced via heartbeat).
    const audio = document.getElementById('audio');
    _recStartServerTime = audio ? audio.currentTime * 1000 : 0;  // ms into song

    if (window.slopsmithDesktop?.audio?.startRecording) {
        window.slopsmithDesktop.audio.startRecording();
        _isRecording = true;
        return;
    }

    if (_mediaRecorder && _mediaRecorder.state === 'inactive') {
        _recordedChunks = [];
        _mediaRecorder.start(1000);
        _isRecording = true;
        console.log('[MP] Recording started at song position:', (_recStartServerTime / 1000).toFixed(3), 's');

        const statusEl = _isHost
            ? document.getElementById('mp-rec-status')
            : document.getElementById('mp-guest-rec-info');
        if (statusEl) statusEl.textContent = 'Recording...';
    }
}

function _stopRecording() {
    _isRecording = false;
    if (_mediaRecorder && _mediaRecorder.state !== 'inactive') {
        try { _mediaRecorder.stop(); } catch (e) { /* */ }
    }
    if (_mediaStream) {
        _mediaStream.getTracks().forEach(t => t.stop());
        _mediaStream = null;
    }
    _mediaRecorder = null;
}

async function _stopAndUploadRecording() {
    if (!_isRecording) return;

    // Desktop bridge
    if (window.slopsmithDesktop?.audio?.stopRecording) {
        const blob = await window.slopsmithDesktop.audio.stopRecording();
        _isRecording = false;
        if (blob) await _uploadBlob(blob, 'recording.wav');
        return;
    }

    if (!_mediaRecorder || _mediaRecorder.state === 'inactive') {
        _isRecording = false;
        return;
    }

    _mediaRecorder.stop();
    if (_mediaStream) {
        _mediaStream.getTracks().forEach(t => t.stop());
        _mediaStream = null;
    }

    // Wait for final chunks
    await new Promise(resolve => {
        _mediaRecorder.onstop = resolve;
    });

    _isRecording = false;

    if (_recordedChunks.length === 0) return;
    const blob = new Blob(_recordedChunks, { type: 'audio/webm' });
    _recordedChunks = [];

    await _uploadBlob(blob, 'recording.webm');
}

async function _uploadBlob(blob, filename) {
    const progressContainer = _isHost
        ? document.getElementById('mp-upload-progress')
        : document.getElementById('mp-guest-upload-progress');
    const progressBar = _isHost
        ? document.getElementById('mp-upload-bar')
        : document.getElementById('mp-guest-upload-bar');

    if (progressContainer) progressContainer.classList.remove('hidden');

    const formData = new FormData();
    formData.append('file', blob, filename);
    formData.append('player_id', _playerId);
    formData.append('start_server_time', String(_recStartServerTime));

    return new Promise((resolve) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', `/api/plugins/multiplayer/rooms/${_roomCode}/upload`);
        xhr.upload.onprogress = (e) => {
            if (e.lengthComputable && progressBar) {
                progressBar.style.width = Math.round((e.loaded / e.total) * 100) + '%';
            }
        };
        xhr.onload = () => {
            if (progressBar) progressBar.style.width = '100%';
            setTimeout(() => {
                if (progressContainer) progressContainer.classList.add('hidden');
            }, 2000);
            resolve();
        };
        xhr.onerror = () => {
            console.error('[MP] Upload failed');
            if (progressContainer) progressContainer.classList.add('hidden');
            resolve();
        };
        xhr.send(formData);
    });
}

function _updateRecordingStatus(msg) {
    if (!_isHost) return;
    const statusEl = document.getElementById('mp-rec-status');
    const mixSection = document.getElementById('mp-mixdown-section');
    const mixStatus = document.getElementById('mp-mixdown-status');

    if (statusEl) {
        statusEl.textContent = `${msg.total_uploads}/${msg.total_players} recordings received`;
    }

    if (mixSection && msg.total_uploads > 0) {
        mixSection.classList.remove('hidden');
        if (mixStatus) {
            mixStatus.textContent = `${msg.total_uploads} track${msg.total_uploads !== 1 ? 's' : ''} ready to mix`;
        }
        _renderStemToggles();
    }
}

function _renderStemToggles() {
    const container = document.getElementById('mp-stem-toggles');
    if (!container) return;

    // Get stem names from the song info
    const info = (typeof highway !== 'undefined' && highway.getSongInfo) ? highway.getSongInfo() : null;
    const stems = (info && info.stems) || [];
    if (stems.length === 0) {
        container.innerHTML = '';
        return;
    }

    // Also list player recordings
    const players = (_room && _room.players) || {};

    container.innerHTML =
        '<div class="text-[10px] text-gray-500 uppercase tracking-wider mb-1">Include in mixdown</div>' +
        stems.map(s => {
            const id = s.id || s;
            return `<label class="flex items-center gap-2 text-xs text-gray-300 cursor-pointer">
                <input type="checkbox" class="mp-stem-cb accent-accent" data-stem="${esc(id)}" checked>
                ${esc(id)}
            </label>`;
        }).join('') +
        Object.keys(players).filter(pid => _room.recordings_received && _room.recordings_received.includes(pid)).map(pid => {
            const p = players[pid];
            return `<label class="flex items-center gap-2 text-xs text-gray-300 cursor-pointer">
                <input type="checkbox" class="mp-rec-cb accent-accent" data-player="${pid}" checked>
                ${esc(p.name)} (recording)
            </label>`;
        }).join('');
}

// ── Mixdown ────────────────────────────────────────────────────────────

window.mpTriggerMixdown = async function () {
    if (!_isHost || !_roomCode || !_playerId) return;
    const btn = document.getElementById('mp-btn-mixdown');
    if (btn) { btn.disabled = true; btn.textContent = 'Mixing...'; }

    // Read stem/recording checkboxes
    const includedStems = [];
    document.querySelectorAll('.mp-stem-cb').forEach(cb => {
        if (cb.checked) includedStems.push(cb.dataset.stem);
    });
    const includedRecordings = [];
    document.querySelectorAll('.mp-rec-cb').forEach(cb => {
        if (cb.checked) includedRecordings.push(cb.dataset.player);
    });

    try {
        const resp = await fetch(`/api/plugins/multiplayer/rooms/${_roomCode}/mixdown`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                player_id: _playerId,
                include_stems: includedStems,
                include_recordings: includedRecordings,
            }),
        });
        const data = await resp.json();
        if (data.error) {
            if (btn) { btn.disabled = false; btn.textContent = 'Mix & Download'; }
            console.error('[MP] Mixdown failed:', data.error);
            return;
        }
        if (data.url) _onMixdownReady(data.url);
    } catch (e) {
        if (btn) { btn.disabled = false; btn.textContent = 'Mix & Download'; }
        console.error('[MP] Mixdown error:', e);
    }
};

function _onMixdownReady(url) {
    const btn = document.getElementById('mp-btn-mixdown');
    const link = document.getElementById('mp-mixdown-link');
    const mixSection = document.getElementById('mp-mixdown-section');

    if (btn) { btn.disabled = false; btn.textContent = 'Mix & Download'; }
    if (mixSection) mixSection.classList.remove('hidden');
    if (link) {
        link.href = url;
        link.classList.remove('hidden');
        link.textContent = 'Download Mixdown (MP3)';
    }
}

// ── Mini DAW Mixer ────────────────────────────────────────────────────

let _mixerCtx = null;       // AudioContext for preview
let _mixerTracks = [];      // [{id, label, type, url, buffer, source, gain, offset, volume, muted, waveCanvas}]
let _mixerPlaying = false;
let _mixerStartTime = 0;
let _mixerDuration = 0;
let _mixerRaf = null;
let _mixerDragTrack = null;
let _mixerDragStartX = 0;
let _mixerDragStartOffset = 0;

window.mpOpenMixer = async function () {
    if (!_roomCode) return;

    // Show mixer, hide room
    document.getElementById('mp-room-view').classList.add('hidden');
    document.getElementById('mp-mixer-view').classList.remove('hidden');

    // Fetch tracks
    try {
        const resp = await fetch(`/api/plugins/multiplayer/rooms/${_roomCode}/tracks`);
        const data = await resp.json();
        if (data.error) { console.error('[MP] Tracks error:', data.error); return; }
        await _mixerLoadTracks(data.tracks);
    } catch (e) {
        console.error('[MP] Failed to load tracks:', e);
    }
};

window.mpCloseMixer = function () {
    _mixerStop();
    document.getElementById('mp-mixer-view').classList.add('hidden');
    document.getElementById('mp-room-view').classList.remove('hidden');
};

async function _mixerLoadTracks(trackList) {
    if (!_mixerCtx) _mixerCtx = new (window.AudioContext || window.webkitAudioContext)();

    _mixerTracks = [];
    const container = document.getElementById('mp-mixer-tracks');
    container.innerHTML = '<div class="p-4 text-xs text-gray-500">Loading audio tracks...</div>';

    for (const t of trackList) {
        const track = {
            id: t.id,
            label: t.label,
            type: t.type,
            url: t.url,
            buffer: null,
            source: null,
            gain: null,
            offset: t.start_ms || 0,  // ms offset from song start
            volume: 1.0,
            muted: false,
        };

        // Fetch and decode audio
        try {
            const resp = await fetch(t.url);
            const arrayBuf = await resp.arrayBuffer();
            track.buffer = await _mixerCtx.decodeAudioData(arrayBuf);
        } catch (e) {
            console.warn(`[MP] Failed to decode ${t.label}:`, e);
            continue;
        }

        _mixerTracks.push(track);
    }

    // Find max duration including offsets
    _mixerDuration = 0;
    for (const t of _mixerTracks) {
        const end = (t.offset / 1000) + t.buffer.duration;
        if (end > _mixerDuration) _mixerDuration = end;
    }

    _mixerRenderTracks();
    _mixerDrawRuler();
}

function _mixerRenderTracks() {
    const container = document.getElementById('mp-mixer-tracks');
    container.innerHTML = '';

    for (const t of _mixerTracks) {
        const row = document.createElement('div');
        row.className = 'flex items-center gap-3 px-3 py-2';
        row.dataset.trackId = t.id;

        // Label + controls
        const controls = document.createElement('div');
        controls.className = 'w-40 flex-shrink-0 space-y-1';
        controls.innerHTML = `
            <div class="text-xs text-white font-medium truncate" title="${esc(t.label)}">${esc(t.label)}</div>
            <div class="flex items-center gap-2">
                <button onclick="mpMixerToggleMute('${t.id}')" class="mp-mute-btn text-[10px] px-1.5 py-0.5 rounded ${t.muted ? 'bg-red-900/50 text-red-400' : 'bg-dark-600 text-gray-400'}" data-track="${t.id}">M</button>
                <button onclick="mpMixerSolo('${t.id}')" class="text-[10px] px-1.5 py-0.5 rounded bg-dark-600 text-gray-400 hover:bg-amber-900/50 hover:text-amber-400">S</button>
                <input type="range" min="0" max="100" value="${Math.round(t.volume * 100)}"
                    class="w-16 accent-accent" oninput="mpMixerSetVolume('${t.id}', this.value)">
                <span class="text-[10px] text-gray-500 w-8 mp-vol-label" data-track="${t.id}">${Math.round(t.volume * 100)}%</span>
            </div>
            <div class="flex items-center gap-1">
                <span class="text-[10px] text-gray-600">Offset:</span>
                <button onclick="mpMixerNudge('${t.id}', -50)" class="text-[10px] px-1 rounded bg-dark-600 text-gray-400 hover:bg-dark-500">-50ms</button>
                <button onclick="mpMixerNudge('${t.id}', -10)" class="text-[10px] px-1 rounded bg-dark-600 text-gray-400 hover:bg-dark-500">-10</button>
                <span class="text-[10px] text-gray-400 w-14 text-center mp-offset-label" data-track="${t.id}">${t.offset}ms</span>
                <button onclick="mpMixerNudge('${t.id}', 10)" class="text-[10px] px-1 rounded bg-dark-600 text-gray-400 hover:bg-dark-500">+10</button>
                <button onclick="mpMixerNudge('${t.id}', 50)" class="text-[10px] px-1 rounded bg-dark-600 text-gray-400 hover:bg-dark-500">+50ms</button>
            </div>`;
        row.appendChild(controls);

        // Waveform canvas (draggable)
        const waveWrap = document.createElement('div');
        waveWrap.className = 'flex-1 h-14 relative bg-dark-800 rounded overflow-hidden cursor-grab';
        const canvas = document.createElement('canvas');
        canvas.className = 'w-full h-full';
        canvas.style.display = 'block';
        waveWrap.appendChild(canvas);
        row.appendChild(waveWrap);
        container.appendChild(row);

        t.waveCanvas = canvas;

        // Drag to offset
        waveWrap.addEventListener('mousedown', (e) => {
            _mixerDragTrack = t;
            _mixerDragStartX = e.clientX;
            _mixerDragStartOffset = t.offset;
            waveWrap.style.cursor = 'grabbing';
            e.preventDefault();
        });

        // Draw waveform after layout
        requestAnimationFrame(() => _mixerDrawWaveform(t));
    }

    // Global mouse handlers for drag
    document.addEventListener('mousemove', _mixerOnDrag);
    document.addEventListener('mouseup', _mixerOnDragEnd);
}

function _mixerDrawWaveform(track) {
    const canvas = track.waveCanvas;
    if (!canvas || !track.buffer) return;

    const rect = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);

    const W = rect.width;
    const H = rect.height;
    const data = track.buffer.getChannelData(0);
    const dur = track.buffer.duration;
    const totalW = _mixerDuration > 0 ? W : W;
    const pxPerSec = W / _mixerDuration;
    const offsetPx = (track.offset / 1000) * pxPerSec;
    const trackW = dur * pxPerSec;

    ctx.clearRect(0, 0, W, H);

    // Background for track extent
    const color = track.type === 'recording' ? '#2563eb' : '#059669';
    ctx.fillStyle = color + '20';
    ctx.fillRect(offsetPx, 0, trackW, H);

    // Waveform
    ctx.fillStyle = color + '80';
    const samples = data.length;
    const samplesPerPx = Math.max(1, Math.floor(samples / trackW));
    const mid = H / 2;

    for (let px = 0; px < trackW && px + offsetPx < W; px++) {
        const start = Math.floor((px / trackW) * samples);
        const end = Math.min(start + samplesPerPx, samples);
        let max = 0;
        for (let i = start; i < end; i++) {
            const v = Math.abs(data[i]);
            if (v > max) max = v;
        }
        const h = max * mid * 0.9;
        ctx.fillRect(offsetPx + px, mid - h, 1, h * 2);
    }

    // Label
    ctx.fillStyle = '#fff8';
    ctx.font = '10px sans-serif';
    ctx.fillText(track.label, offsetPx + 4, 12);
}

function _mixerOnDrag(e) {
    if (!_mixerDragTrack) return;
    const t = _mixerDragTrack;
    const canvas = t.waveCanvas;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const pxPerMs = rect.width / (_mixerDuration * 1000);
    const dx = e.clientX - _mixerDragStartX;
    t.offset = Math.max(0, Math.round(_mixerDragStartOffset + dx / pxPerMs));
    const label = document.querySelector(`.mp-offset-label[data-track="${t.id}"]`);
    if (label) label.textContent = t.offset + 'ms';
    _mixerDrawWaveform(t);
}

function _mixerOnDragEnd() {
    if (_mixerDragTrack) {
        const wrap = _mixerDragTrack.waveCanvas?.parentElement;
        if (wrap) wrap.style.cursor = 'grab';
        _mixerDragTrack = null;
    }
}

function _mixerDrawRuler() {
    const canvas = document.getElementById('mp-mixer-ruler-canvas');
    if (!canvas) return;
    const rect = canvas.parentElement.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    const W = rect.width, H = rect.height;

    ctx.clearRect(0, 0, W, H);
    ctx.fillStyle = '#333';
    ctx.fillRect(0, 0, W, H);

    const step = _mixerDuration > 60 ? 10 : _mixerDuration > 20 ? 5 : 1;
    ctx.fillStyle = '#888';
    ctx.font = '9px sans-serif';
    for (let t = 0; t <= _mixerDuration; t += step) {
        const x = (t / _mixerDuration) * W;
        ctx.fillRect(x, H - 4, 1, 4);
        const m = Math.floor(t / 60);
        const s = Math.floor(t % 60);
        ctx.fillText(`${m}:${String(s).padStart(2, '0')}`, x + 2, H - 5);
    }
}

// ── Mixer playback (Web Audio) ──

function _mixerPlay() {
    if (!_mixerCtx || _mixerTracks.length === 0) return;
    if (_mixerCtx.state === 'suspended') _mixerCtx.resume();

    _mixerStartTime = _mixerCtx.currentTime;

    for (const t of _mixerTracks) {
        if (!t.buffer || t.muted) continue;
        const source = _mixerCtx.createBufferSource();
        source.buffer = t.buffer;
        const gain = _mixerCtx.createGain();
        gain.gain.value = t.volume;
        source.connect(gain).connect(_mixerCtx.destination);
        const offsetSec = t.offset / 1000;
        source.start(_mixerCtx.currentTime + offsetSec);
        t.source = source;
        t.gain = gain;
    }

    _mixerPlaying = true;
    _mixerUpdatePlayhead();
}

function _mixerStop() {
    for (const t of _mixerTracks) {
        if (t.source) {
            try { t.source.stop(); } catch (e) { /* */ }
            t.source = null;
        }
    }
    _mixerPlaying = false;
    if (_mixerRaf) { cancelAnimationFrame(_mixerRaf); _mixerRaf = null; }
    const playhead = document.getElementById('mp-mixer-playhead');
    if (playhead) playhead.style.left = '0%';
    const timeEl = document.getElementById('mp-mixer-time');
    if (timeEl) timeEl.textContent = '0:00.0';
    const btn = document.getElementById('mp-mixer-play');
    if (btn) btn.textContent = 'Preview';
}

function _mixerUpdatePlayhead() {
    if (!_mixerPlaying) return;
    const elapsed = _mixerCtx.currentTime - _mixerStartTime;
    const pct = Math.min(100, (elapsed / _mixerDuration) * 100);
    const playhead = document.getElementById('mp-mixer-playhead');
    if (playhead) playhead.style.left = pct + '%';
    const timeEl = document.getElementById('mp-mixer-time');
    if (timeEl) {
        const m = Math.floor(elapsed / 60);
        const s = (elapsed % 60).toFixed(1);
        timeEl.textContent = `${m}:${s.padStart(4, '0')}`;
    }
    if (elapsed >= _mixerDuration) {
        _mixerStop();
        return;
    }
    _mixerRaf = requestAnimationFrame(_mixerUpdatePlayhead);
}

window.mpMixerPlayPause = function () {
    if (_mixerPlaying) {
        _mixerStop();
    } else {
        _mixerPlay();
        const btn = document.getElementById('mp-mixer-play');
        if (btn) btn.textContent = 'Stop';
    }
};

window.mpMixerSeek = function (e) {
    // Not implemented for buffer sources (would need to restart all)
};

window.mpMixerToggleMute = function (trackId) {
    const t = _mixerTracks.find(t => t.id === trackId);
    if (!t) return;
    t.muted = !t.muted;
    if (t.gain) t.gain.gain.value = t.muted ? 0 : t.volume;
    const btn = document.querySelector(`.mp-mute-btn[data-track="${trackId}"]`);
    if (btn) {
        btn.className = `mp-mute-btn text-[10px] px-1.5 py-0.5 rounded ${t.muted ? 'bg-red-900/50 text-red-400' : 'bg-dark-600 text-gray-400'}`;
    }
};

window.mpMixerSolo = function (trackId) {
    // Mute everything except this track
    for (const t of _mixerTracks) {
        t.muted = (t.id !== trackId);
        if (t.gain) t.gain.gain.value = t.muted ? 0 : t.volume;
        const btn = document.querySelector(`.mp-mute-btn[data-track="${t.id}"]`);
        if (btn) {
            btn.className = `mp-mute-btn text-[10px] px-1.5 py-0.5 rounded ${t.muted ? 'bg-red-900/50 text-red-400' : 'bg-dark-600 text-gray-400'}`;
        }
    }
};

window.mpMixerSetVolume = function (trackId, val) {
    const t = _mixerTracks.find(t => t.id === trackId);
    if (!t) return;
    t.volume = parseFloat(val) / 100;
    if (t.gain && !t.muted) t.gain.gain.value = t.volume;
    const label = document.querySelector(`.mp-vol-label[data-track="${trackId}"]`);
    if (label) label.textContent = Math.round(t.volume * 100) + '%';
};

window.mpMixerNudge = function (trackId, deltaMs) {
    const t = _mixerTracks.find(t => t.id === trackId);
    if (!t) return;
    t.offset = Math.max(0, t.offset + deltaMs);
    const label = document.querySelector(`.mp-offset-label[data-track="${trackId}"]`);
    if (label) label.textContent = t.offset + 'ms';
    _mixerDrawWaveform(t);
};

window.mpMixerExport = async function () {
    if (!_roomCode || !_playerId) return;
    const btn = document.getElementById('mp-mixer-export');
    if (btn) { btn.disabled = true; btn.textContent = 'Exporting...'; }

    // Collect mixer state
    const track_offsets = {};
    const track_volumes = {};
    const track_mutes = [];
    for (const t of _mixerTracks) {
        track_offsets[t.id] = t.offset;
        track_volumes[t.id] = t.volume;
        if (t.muted) track_mutes.push(t.id);
    }

    try {
        const resp = await fetch(`/api/plugins/multiplayer/rooms/${_roomCode}/mixdown`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                player_id: _playerId,
                track_offsets,
                track_volumes,
                track_mutes,
            }),
        });
        const data = await resp.json();
        if (btn) { btn.disabled = false; btn.textContent = 'Export Mix'; }
        if (data.error) {
            console.error('[MP] Export failed:', data.error);
            return;
        }
        if (data.url) {
            const link = document.getElementById('mp-mixer-download');
            if (link) {
                link.href = data.url;
                link.classList.remove('hidden');
                link.textContent = 'Download Mixdown (MP3)';
            }
        }
    } catch (e) {
        if (btn) { btn.disabled = false; btn.textContent = 'Export Mix'; }
        console.error('[MP] Export error:', e);
    }
};

// ── Audio ended hook (host sends song_ended) ───────────────────────────

(function () {
    const audio = document.getElementById('audio');
    if (audio) {
        audio.addEventListener('ended', () => {
            if (_isHost && _ws && _ws.readyState === WebSocket.OPEN && _room && _room.now_playing >= 0) {
                _ws.send(JSON.stringify({ type: 'song_ended' }));
            }
        });
    }
})();

// ── Debug hook ─────────────────────────────────────────────────────────
//
// window.slopsmithMultiplayerDebug surfaces a small, intentionally
// internal/unstable shape for inspecting the audio WS state from dev
// tools. Not a public API; subject to change between phases.

window.slopsmithMultiplayerDebug = {
    getAudioRxStats: _audioGetRxStats,
    getListenerState: () => ({
        broadcasterId: _audioListenerBroadcasterId,
        params: _audioListenerBroadcastParams,
        scheduledCount: _audioListenerScheduledSources.size,
        pendingDecodeCount: _audioListenerActiveDecoder
            ? _audioListenerActiveDecoder.pending.size
            : 0,
        hasContext: _audioListenerCtx !== null,
        hasDecoder: _audioListenerActiveDecoder !== null,
    }),
};

// ── Screen show/hide hook ──────────────────────────────────────────────

(function () {
    const origShowScreen = window.showScreen;
    window.showScreen = function (id) {
        origShowScreen(id);
        if (id === 'plugin-multiplayer') {
            _loadSettings();
            // Check if we have an active room
            const savedRoom = sessionStorage.getItem('mp_room');
            const savedPlayer = sessionStorage.getItem('mp_player');
            if (savedRoom && savedPlayer && _ws && _ws.readyState === WebSocket.OPEN) {
                _showRoomView();
            } else if (savedRoom && savedPlayer && !_ws) {
                // Try to reconnect both WSs under the same persisted
                // session_id (sessionStorage carried it over the screen
                // switch). _connectWS / _connectAudioWs read _sessionId
                // via _getOrMintSessionId on connect.
                _roomCode = savedRoom;
                _playerId = savedPlayer;
                _connectWS();
                _connectAudioWs();
                _showRoomView();
            } else {
                _showLobbyView();
            }
        }
    };
})();

// ── Page unload: tear down the broadcast cleanly ───────────────────────
//
// Without this, a tab close mid-broadcast leaves the encoder + mic
// stream up for the GC and leaves the server-side broadcaster_id stuck
// until the session grace timer expires. broadcast_stop is best-effort
// (the highway WS may already be tearing down), but stopping the local
// capture pipeline immediately releases the mic + encoder resources.
window.addEventListener('beforeunload', () => {
    if (_captureCtx || _captureRequested) {
        _broadcastStop({});
    }
});

})();
