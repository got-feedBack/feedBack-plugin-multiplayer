// Slopsmith Multiplayer — interval-quantized peer audio
// Picks the number of beats per Ninjam-style interval based on the chart's BPM.
//
// Always picks a whole-measure (or multi-measure) interval — never sub-measure.
// Default: 4 beats (one measure of 4/4). For very fast charts where one measure
// would be shorter than MIN_DURATION_SEC (so the encode/send budget per interval
// shrinks to nothing), bump to 8 beats. Slow charts where one measure exceeds
// MAX_DURATION_SEC are accepted as-is — a longer-than-target interval is musical;
// a half-measure interval is not.
//
// See PROTOCOL.md for the algorithm rationale and the BPM→beats decision table.
//
// Currently consumed only by the Node test runner (audio/select-interval.test.js).
// Phase 2 will inline this into screen.js (the only JS file slopsmith core's
// plugin loader actually serves, /api/plugins/{id}/screen.js — see
// plugins/__init__.py in core). At that point the helper becomes browser-side
// too. Keeping the file CommonJS-only here avoids advertising a browser API
// that no <script> tag actually loads today.

var MIN_DURATION_SEC = 1.0;
// MAX_DURATION_SEC is a soft documentation-only target. selectInterval does NOT
// enforce it — slow tempos legitimately exceed it (e.g. 60 BPM × 4 beats = 4.0s)
// and we still return one whole measure because a sub-measure interval is worse
// musically than a longer one. Kept as an export so UI and docs can reference
// the shared constant.
var MAX_DURATION_SEC = 3.0;
var DEFAULT_BEATS = 4;
var FAST_TEMPO_BEATS = 8;

function selectInterval(bpm) {
    if (!Number.isFinite(bpm) || bpm <= 0) return DEFAULT_BEATS;
    var secondsPerBeat = 60 / bpm;
    if (DEFAULT_BEATS * secondsPerBeat < MIN_DURATION_SEC) {
        return FAST_TEMPO_BEATS;
    }
    return DEFAULT_BEATS;
}

function intervalDuration(bpm, beats) {
    if (!Number.isFinite(bpm) || bpm <= 0 || !Number.isFinite(beats) || beats <= 0) return 0;
    return (60 / bpm) * beats;
}

module.exports = {
    selectInterval: selectInterval,
    intervalDuration: intervalDuration,
    MIN_DURATION_SEC: MIN_DURATION_SEC,
    MAX_DURATION_SEC: MAX_DURATION_SEC,
    DEFAULT_BEATS: DEFAULT_BEATS,
    FAST_TEMPO_BEATS: FAST_TEMPO_BEATS,
};
