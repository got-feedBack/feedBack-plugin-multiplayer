// Pure-Node test runner for select-interval.js. No deps. Run: `node audio/select-interval.test.js`.
// Exits with code 0 on success, 1 on any assertion failure.

var assert = require('assert');
var {
    selectInterval,
    intervalDuration,
    DEFAULT_BEATS,
    FAST_TEMPO_BEATS,
    MIN_DURATION_SEC,
} = require('./select-interval.js');

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

// Canonical / common-tempo cases — one measure (4 beats) always wins inside the band.
test('120 BPM → 4 beats (canonical, exactly 2.0s)', function () {
    assert.strictEqual(selectInterval(120), 4);
    assert.strictEqual(intervalDuration(120, 4), 2);
});

test('80 BPM → 4 beats (3.0s, boundary accepted)', function () {
    assert.strictEqual(selectInterval(80), 4);
});

test('90 BPM → 4 beats (~2.67s, in band)', function () {
    assert.strictEqual(selectInterval(90), 4);
});

test('100 BPM → 4 beats (2.4s, in band)', function () {
    assert.strictEqual(selectInterval(100), 4);
});

test('180 BPM → 4 beats (~1.33s, in band)', function () {
    assert.strictEqual(selectInterval(180), 4);
});

test('240 BPM → 4 beats (1.0s, lower boundary)', function () {
    assert.strictEqual(selectInterval(240), 4);
    assert.strictEqual(intervalDuration(240, 4), 1);
});

// Slow tempos: one measure exceeds the target window, but we keep the whole-measure
// shape rather than fall to a half-measure — interval stays musical.
test('60 BPM → 4 beats (4.0s, exceeds target but accepted)', function () {
    assert.strictEqual(selectInterval(60), 4);
    assert.strictEqual(intervalDuration(60, 4), 4);
});

test('45 BPM → 4 beats (~5.33s, exceeds target but accepted)', function () {
    assert.strictEqual(selectInterval(45), 4);
});

test('30 BPM → 4 beats (8.0s, extreme slow ballad)', function () {
    assert.strictEqual(selectInterval(30), 4);
});

// Fast tempos: one measure shrinks below MIN_DURATION_SEC; bump to 8 beats so
// the encode/send budget per interval doesn't shrink to nothing.
test('250 BPM → 8 beats (4 beats = 0.96s, below MIN)', function () {
    assert.strictEqual(selectInterval(250), FAST_TEMPO_BEATS);
});

test('300 BPM → 8 beats (4 beats = 0.8s, below MIN)', function () {
    assert.strictEqual(selectInterval(300), 8);
});

test('480 BPM → 8 beats (4 beats = 0.5s; 8 beats = 1.0s exactly)', function () {
    assert.strictEqual(selectInterval(480), 8);
});

test('600 BPM → 8 beats (pathological tempo; still pick whole-measure, no half-measures ever)', function () {
    assert.strictEqual(selectInterval(600), 8);
});

// Pathological / defensive inputs.
test('zero BPM → default 4', function () {
    assert.strictEqual(selectInterval(0), DEFAULT_BEATS);
});

test('negative BPM → default 4', function () {
    assert.strictEqual(selectInterval(-120), DEFAULT_BEATS);
});

test('NaN BPM → default 4', function () {
    assert.strictEqual(selectInterval(NaN), DEFAULT_BEATS);
});

test('Infinity BPM → default 4', function () {
    assert.strictEqual(selectInterval(Infinity), DEFAULT_BEATS);
});

test('string BPM ("120") → default 4 (Number.isFinite rejects non-numbers)', function () {
    assert.strictEqual(selectInterval('120'), DEFAULT_BEATS);
});

test('null BPM → default 4', function () {
    assert.strictEqual(selectInterval(null), DEFAULT_BEATS);
});

test('undefined BPM → default 4', function () {
    assert.strictEqual(selectInterval(undefined), DEFAULT_BEATS);
});

test('boolean BPM → default 4', function () {
    assert.strictEqual(selectInterval(true), DEFAULT_BEATS);
    assert.strictEqual(selectInterval(false), DEFAULT_BEATS);
});

// intervalDuration sanity.
test('intervalDuration at 120 BPM × 4 beats = 2.0s', function () {
    assert.strictEqual(intervalDuration(120, 4), 2);
});

test('intervalDuration with bad inputs returns 0', function () {
    assert.strictEqual(intervalDuration(0, 4), 0);
    assert.strictEqual(intervalDuration(120, 0), 0);
    assert.strictEqual(intervalDuration(-120, 4), 0);
    assert.strictEqual(intervalDuration(NaN, 4), 0);
});

// Self-consistency: every selected interval at any sane BPM is a whole number of measures.
test('selectInterval never returns a sub-measure value', function () {
    for (var bpm = 30; bpm <= 600; bpm += 1) {
        var beats = selectInterval(bpm);
        assert.ok(beats === 4 || beats === 8, 'unexpected beats=' + beats + ' at bpm=' + bpm);
    }
});

// Self-consistency: at boundary BPM the duration crosses MIN_DURATION_SEC cleanly.
test('boundary check around MIN_DURATION_SEC', function () {
    // At 4 beats × secondsPerBeat = MIN_DURATION_SEC, should still return 4 (>=).
    var thresholdBpm = (60 * 4) / MIN_DURATION_SEC; // 240
    assert.strictEqual(selectInterval(thresholdBpm), 4);
    assert.strictEqual(selectInterval(thresholdBpm + 0.001), FAST_TEMPO_BEATS);
});

if (failures > 0) {
    console.log('');
    console.log(failures + ' test(s) failed');
    process.exit(1);
}
console.log('');
console.log('all tests passed');
