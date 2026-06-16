// Pure-Node test runner for tempo-change.js. No deps. Run: `node audio/tempo-change.test.js`.
// Exits with code 0 on success, 1 on any assertion failure.

var assert = require('assert');
var {
    detectTempoChangeAtBoundary,
    DEFAULT_THRESHOLD,
} = require('./tempo-change.js');

var failures = 0;
function test(name, fn) {
    try {
        fn();
        console.log('  ok  ' + name);
    } catch (err) {
        console.log('  FAIL  ' + name);
        console.log('        ' + (err && err.message ? err.message : err));
        failures++;
    }
}

// Helper: build a beats array at a constant BPM for `count` beats
// starting at `startTime`.
function constantBeats(bpm, count, startTime) {
    var spacing = 60 / bpm;
    var t = startTime || 0;
    var out = [];
    for (var i = 0; i < count; i++) {
        out.push({ time: t });
        t += spacing;
    }
    return out;
}

// Helper: concatenate two constant-BPM runs, with the first joining
// directly into the second (no gap). Useful for "tempo changes at
// chart-time T" scenarios.
function tempoChangeBeats(bpm1, count1, bpm2, count2) {
    var first = constantBeats(bpm1, count1, 0);
    var lastTime = first[first.length - 1].time;
    var second = constantBeats(bpm2, count2, lastTime + 60 / bpm2);
    return first.concat(second);
}

// ── Sanity ──

test('returns false for non-array beats', function () {
    assert.strictEqual(detectTempoChangeAtBoundary(null, 1.0), false);
    assert.strictEqual(detectTempoChangeAtBoundary(undefined, 1.0), false);
    assert.strictEqual(detectTempoChangeAtBoundary({}, 1.0), false);
});

test('returns false for fewer than 4 beats', function () {
    assert.strictEqual(detectTempoChangeAtBoundary([], 1.0), false);
    assert.strictEqual(detectTempoChangeAtBoundary([{ time: 0 }], 1.0), false);
    assert.strictEqual(detectTempoChangeAtBoundary(constantBeats(120, 3, 0), 1.0), false);
});

test('returns false for non-finite boundaryTime', function () {
    var beats = constantBeats(120, 8, 0);
    assert.strictEqual(detectTempoChangeAtBoundary(beats, NaN), false);
    assert.strictEqual(detectTempoChangeAtBoundary(beats, Infinity), false);
    assert.strictEqual(detectTempoChangeAtBoundary(beats, undefined), false);
});

// ── No tempo change ──

test('returns false for steady 120 BPM throughout', function () {
    var beats = constantBeats(120, 16, 0);
    // Boundary in the middle of the run
    assert.strictEqual(detectTempoChangeAtBoundary(beats, 4.0), false);
});

test('returns false for steady 60 BPM throughout', function () {
    var beats = constantBeats(60, 16, 0);
    assert.strictEqual(detectTempoChangeAtBoundary(beats, 8.0), false);
});

test('returns false for tiny floating-point jitter under threshold', function () {
    // Spacings that differ by < 5% (default threshold) — e.g.
    // alternating 0.500 / 0.501 / 0.499 / 0.500 should not trip.
    var beats = [
        { time: 0 },
        { time: 0.500 },
        { time: 1.001 },
        { time: 1.500 },
        { time: 2.001 },
        { time: 2.500 },
    ];
    assert.strictEqual(detectTempoChangeAtBoundary(beats, 1.001), false);
});

// ── Real tempo change ──

test('returns true for 120→90 BPM change at boundary', function () {
    // 4 beats at 120 BPM (spacing 0.5s) then 4 beats at 90 BPM (spacing 0.667s).
    // 0.667 vs 0.5 is +33%, well above 5% threshold.
    var beats = tempoChangeBeats(120, 4, 90, 4);
    var changeTime = beats[3].time + 60 / 90; // first beat at the new tempo
    assert.strictEqual(detectTempoChangeAtBoundary(beats, changeTime - 0.01), true);
});

test('returns true for 90→120 BPM change at boundary', function () {
    var beats = tempoChangeBeats(90, 4, 120, 4);
    var changeTime = beats[3].time + 60 / 120;
    assert.strictEqual(detectTempoChangeAtBoundary(beats, changeTime - 0.01), true);
});

test('returns true for 120→180 BPM (50% delta)', function () {
    var beats = tempoChangeBeats(120, 4, 180, 4);
    // Boundary right at the seam — first beat after boundary is the new tempo's first beat.
    var changeTime = beats[3].time + 60 / 180;
    assert.strictEqual(detectTempoChangeAtBoundary(beats, changeTime - 0.001), true);
});

test('exact threshold boundary case (5% delta is NOT a change)', function () {
    // Spacings 0.5 → 0.525 is exactly +5%; default threshold is "ratio > 0.05",
    // so EXACTLY 5% should return false. (Rephrased: only strictly greater triggers.)
    var beats = [
        { time: 0 },
        { time: 0.5 },
        { time: 1.0 },
        { time: 1.525 },
        { time: 2.05 },
    ];
    assert.strictEqual(detectTempoChangeAtBoundary(beats, 1.0), false);
});

test('returns true just above threshold (5.5% delta)', function () {
    // Spacings before boundary = 0.5, after = 0.5275 (5.5% delta).
    var beats = [
        { time: 0 },
        { time: 0.5 },
        { time: 1.0 },
        { time: 1.5275 },
        { time: 2.055 },
    ];
    assert.strictEqual(detectTempoChangeAtBoundary(beats, 1.0), true);
});

test('honors custom threshold (10%)', function () {
    // 6% delta: trips at default 5%, NOT at 10%.
    var beats = [
        { time: 0 },
        { time: 0.5 },
        { time: 1.0 },
        { time: 1.530 },
        { time: 2.060 },
    ];
    assert.strictEqual(detectTempoChangeAtBoundary(beats, 1.0), true);
    assert.strictEqual(detectTempoChangeAtBoundary(beats, 1.0, 0.10), false);
});

// ── Edge cases (boundary near start/end) ──

test('returns false for boundary before second beat (not enough lookback)', function () {
    var beats = constantBeats(120, 8, 0);
    // First beat is at t=0. With boundary at 0.1, nextIdx=1 → fails the
    // "nextIdx < 2" guard.
    assert.strictEqual(detectTempoChangeAtBoundary(beats, 0.1), false);
});

test('returns false for boundary on/after last beat (not enough lookahead)', function () {
    var beats = constantBeats(120, 8, 0);
    var lastTime = beats[beats.length - 1].time;
    // Boundary >= last beat's time → either no nextIdx found or
    // nextIdx === beats.length - 1 → fails the "nextIdx >= length - 1" guard.
    assert.strictEqual(detectTempoChangeAtBoundary(beats, lastTime), false);
    assert.strictEqual(detectTempoChangeAtBoundary(beats, lastTime + 1), false);
});

test('returns true at the second valid boundary in a long song', function () {
    // 8 beats at 100 BPM, 8 beats at 140 BPM (40% delta)
    var beats = tempoChangeBeats(100, 8, 140, 8);
    var changeTime = beats[7].time + 60 / 140;
    assert.strictEqual(detectTempoChangeAtBoundary(beats, changeTime - 0.01), true);
});

// ── Malformed inputs ──

test('returns false when one of the 4 surrounding beats has non-numeric time', function () {
    var beats = [
        { time: 0 },
        { time: 0.5 },
        { time: 1.0 },
        { time: 'oops' }, // malformed
        { time: 2.0 },
    ];
    assert.strictEqual(detectTempoChangeAtBoundary(beats, 1.0), false);
});

test('returns false when a beat time is NaN', function () {
    var beats = [
        { time: 0 },
        { time: 0.5 },
        { time: 1.0 },
        { time: NaN },
        { time: 2.0 },
    ];
    assert.strictEqual(detectTempoChangeAtBoundary(beats, 1.0), false);
});

test('returns false when surrounding spacings are zero (overlapping beats)', function () {
    var beats = [
        { time: 0 },
        { time: 0.5 },
        { time: 0.5 }, // duplicate
        { time: 0.5 },
        { time: 1.0 },
    ];
    assert.strictEqual(detectTempoChangeAtBoundary(beats, 0.5), false);
});

test('returns false when beats array has null entries', function () {
    var beats = [
        { time: 0 },
        null,
        { time: 1.0 },
        { time: 1.5 },
        { time: 2.0 },
    ];
    // The scan looks at each beat's `.time`; a null entry is skipped
    // by the "beats[i] && beats[i].time" pattern. We still need 2
    // valid beats on each side. Result should be false because the
    // back-lookup can hit the null and bail.
    assert.strictEqual(detectTempoChangeAtBoundary(beats, 1.5), false);
});

// ── Default threshold export ──

test('DEFAULT_THRESHOLD is 0.05 (5%)', function () {
    assert.strictEqual(DEFAULT_THRESHOLD, 0.05);
});

if (failures > 0) {
    console.log('\n' + failures + ' test(s) failed');
    process.exit(1);
} else {
    console.log('\nAll tempo-change tests passed');
}
