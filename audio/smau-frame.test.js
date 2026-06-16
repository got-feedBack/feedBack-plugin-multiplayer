// Pure-Node test runner for smau-frame.js. No deps. Run:
//   node audio/smau-frame.test.js
// Exits with code 0 on success, 1 on any assertion failure.
//
// Tests are intentionally exhaustive on the validator side because the SMAU
// header is the trust boundary between peers — server fan-out is byte-for-byte,
// so every receiver runs decodeSmauFrame on potentially malicious input.

var assert = require('assert');
var {
    encodeSmauFrame,
    decodeSmauFrame,
    MAGIC_BYTES,
    MAGIC_STRING,
    VERSION,
    HEADER_LEN,
    MAX_FRAME_BYTES,
    V1_SAMPLE_RATE,
    MAX_INTERVAL_SEC,
    MAX_SLACK_SAMPLES,
    FLAG_TEMPO_CHANGE_AT_END,
} = require('./smau-frame.js');

var failures = 0;
function test(name, fn) {
    try {
        fn();
        console.log('  ok  ' + name);
    } catch (err) {
        console.log('  FAIL  ' + name);
        console.log('        ' + (err && err.stack ? err.stack : err));
        failures++;
    }
}

// Helpers ────────────────────────────────────────────────────────────────────
function fakeOpus(n) {
    var u = new Uint8Array(n);
    for (var i = 0; i < n; i++) u[i] = (i * 37 + 11) & 0xff;
    return u;
}

function buildValid(over) {
    var fields = {
        intervalIndex: 7n,
        chartTimeStart: 1.5,
        chartTimeEnd: 3.5,
        sampleCount: 96000,   // 2 s × 48k
        flags: 0,
        opus: fakeOpus(120),
    };
    if (over) for (var k in over) fields[k] = over[k];
    return fields;
}

function corruptHeader(buf, mutator) {
    var u = new Uint8Array(buf.byteLength);
    u.set(new Uint8Array(buf));
    mutator(u, new DataView(u.buffer));
    return u.buffer;
}

// ── Constants ────────────────────────────────────────────────────────────────
test('header constants match PROTOCOL.md', function () {
    assert.strictEqual(HEADER_LEN, 40);
    assert.strictEqual(VERSION, 1);
    assert.strictEqual(MAX_FRAME_BYTES, 262144);
    assert.strictEqual(V1_SAMPLE_RATE, 48000);
    assert.strictEqual(MAX_INTERVAL_SEC, 32);
    assert.strictEqual(MAX_SLACK_SAMPLES, 480);
    assert.strictEqual(FLAG_TEMPO_CHANGE_AT_END, 0x0001);
    assert.strictEqual(MAGIC_STRING, 'SMAU');
    assert.deepStrictEqual(MAGIC_BYTES, [0x53, 0x4d, 0x41, 0x55]);
});

// ── Encode happy path ────────────────────────────────────────────────────────
test('encode produces a buffer with header + payload, magic + version + LE fields', function () {
    var opus = fakeOpus(64);
    var buf = encodeSmauFrame({
        intervalIndex: 42n,
        chartTimeStart: 1.0,
        chartTimeEnd: 3.0,
        sampleCount: 96000,
        opus: opus,
    });
    assert.ok(buf instanceof ArrayBuffer);
    assert.strictEqual(buf.byteLength, HEADER_LEN + 64);

    var bytes = new Uint8Array(buf);
    assert.deepStrictEqual([bytes[0], bytes[1], bytes[2], bytes[3]], MAGIC_BYTES);

    var view = new DataView(buf);
    assert.strictEqual(view.getUint16(4, true), 1);
    assert.strictEqual(view.getUint16(6, true), 0);
    assert.strictEqual(view.getBigUint64(8, true), 42n);
    assert.strictEqual(view.getFloat64(16, true), 1.0);
    assert.strictEqual(view.getFloat64(24, true), 3.0);
    assert.strictEqual(view.getUint32(32, true), 96000);
    assert.strictEqual(view.getUint32(36, true), 64);

    // Payload bytes start at offset 40.
    for (var i = 0; i < 64; i++) {
        assert.strictEqual(bytes[40 + i], opus[i]);
    }
});

test('encode + decode round-trip preserves all header fields', function () {
    var fields = {
        intervalIndex: 1234567890n,
        chartTimeStart: 12.5,
        chartTimeEnd: 14.5,
        sampleCount: 96000,
        flags: FLAG_TEMPO_CHANGE_AT_END,
        opus: fakeOpus(200),
    };
    var buf = encodeSmauFrame(fields);
    var result = decodeSmauFrame(buf);
    assert.strictEqual(result.ok, true);
    assert.strictEqual(result.header.version, 1);
    assert.strictEqual(result.header.flags, FLAG_TEMPO_CHANGE_AT_END);
    assert.strictEqual(result.header.tempoChangeAtEnd, true);
    assert.strictEqual(result.header.intervalIndex, 1234567890n);
    assert.strictEqual(result.header.chartTimeStart, 12.5);
    assert.strictEqual(result.header.chartTimeEnd, 14.5);
    assert.strictEqual(result.header.sampleCount, 96000);
    assert.strictEqual(result.header.opusSize, 200);
    assert.strictEqual(result.header.frameLength, HEADER_LEN + 200);
    assert.strictEqual(result.opus.byteLength, 200);
    assert.strictEqual(result.opus[0], fields.opus[0]);
    assert.strictEqual(result.opus[199], fields.opus[199]);
});

test('encode accepts integer Number for intervalIndex (coerced to BigInt)', function () {
    var buf = encodeSmauFrame(buildValid({ intervalIndex: 1000 }));
    var r = decodeSmauFrame(buf);
    assert.strictEqual(r.ok, true);
    assert.strictEqual(r.header.intervalIndex, 1000n);
});

test('decode parses interval_index above 2^53 without precision loss (BigInt)', function () {
    var bigIdx = (1n << 60n) + 17n;
    var buf = encodeSmauFrame(buildValid({ intervalIndex: bigIdx }));
    var r = decodeSmauFrame(buf);
    assert.strictEqual(r.ok, true);
    assert.strictEqual(r.header.intervalIndex, bigIdx);
    // And the field exceeds Number.MAX_SAFE_INTEGER, so a Number-based parse
    // would have lost low bits.
    assert.ok(bigIdx > BigInt(Number.MAX_SAFE_INTEGER));
});

test('decode accepts Uint8Array, ArrayBuffer, and other ArrayBufferView inputs', function () {
    var buf = encodeSmauFrame(buildValid());
    assert.strictEqual(decodeSmauFrame(buf).ok, true);
    assert.strictEqual(decodeSmauFrame(new Uint8Array(buf)).ok, true);
    assert.strictEqual(decodeSmauFrame(new DataView(buf)).ok, true);
});

test('decode of an offset Uint8Array view points opus at the right slice', function () {
    var inner = encodeSmauFrame(buildValid({ opus: fakeOpus(50) }));
    // Wrap the inner buffer inside a larger one with leading + trailing padding.
    var outer = new Uint8Array(inner.byteLength + 16);
    outer.set(new Uint8Array(inner), 8);
    var view = new Uint8Array(outer.buffer, 8, inner.byteLength);
    var r = decodeSmauFrame(view);
    assert.strictEqual(r.ok, true);
    assert.strictEqual(r.opus.byteLength, 50);
    // Confirm the bytes match the original payload.
    var expected = fakeOpus(50);
    for (var i = 0; i < 50; i++) {
        assert.strictEqual(r.opus[i], expected[i]);
    }
});

// ── Encode validation ───────────────────────────────────────────────────────
test('encode rejects missing fields object', function () {
    assert.throws(function () { encodeSmauFrame(); }, TypeError);
    assert.throws(function () { encodeSmauFrame(null); }, TypeError);
});

test('encode rejects non-finite chart times', function () {
    assert.throws(function () { encodeSmauFrame(buildValid({ chartTimeStart: NaN })); }, RangeError);
    assert.throws(function () { encodeSmauFrame(buildValid({ chartTimeEnd: Infinity })); }, RangeError);
    assert.throws(function () { encodeSmauFrame(buildValid({ chartTimeStart: -Infinity })); }, RangeError);
});

test('encode rejects zero or negative duration', function () {
    assert.throws(function () { encodeSmauFrame(buildValid({ chartTimeStart: 5, chartTimeEnd: 5 })); }, RangeError);
    assert.throws(function () { encodeSmauFrame(buildValid({ chartTimeStart: 6, chartTimeEnd: 5 })); }, RangeError);
});

test('encode rejects duration exceeding MAX_INTERVAL_SEC', function () {
    assert.throws(function () {
        encodeSmauFrame(buildValid({ chartTimeStart: 0, chartTimeEnd: MAX_INTERVAL_SEC + 0.001 }));
    }, RangeError);
});

test('encode accepts duration exactly equal to MAX_INTERVAL_SEC', function () {
    var buf = encodeSmauFrame(buildValid({
        chartTimeStart: 0,
        chartTimeEnd: MAX_INTERVAL_SEC,
        sampleCount: V1_SAMPLE_RATE * MAX_INTERVAL_SEC,
    }));
    assert.strictEqual(decodeSmauFrame(buf).ok, true);
});

test('encode rejects negative or non-integer sample_count', function () {
    assert.throws(function () { encodeSmauFrame(buildValid({ sampleCount: -1 })); }, RangeError);
    assert.throws(function () { encodeSmauFrame(buildValid({ sampleCount: 1.5 })); }, RangeError);
    assert.throws(function () { encodeSmauFrame(buildValid({ sampleCount: NaN })); }, RangeError);
});

test('encode rejects sample_count above the receiver duration cap', function () {
    // 1 s duration → cap = 48000 + 480 = 48480.
    assert.throws(function () {
        encodeSmauFrame(buildValid({
            chartTimeStart: 0, chartTimeEnd: 1, sampleCount: 48481,
        }));
    }, RangeError);
    // Boundary still accepted so the helper's bound matches the decoder's.
    var buf = encodeSmauFrame(buildValid({
        chartTimeStart: 0, chartTimeEnd: 1, sampleCount: 48480,
    }));
    assert.strictEqual(decodeSmauFrame(buf).ok, true);
});

test('encode rejects intervalIndex out of u64 range', function () {
    assert.throws(function () { encodeSmauFrame(buildValid({ intervalIndex: -1n })); }, RangeError);
    assert.throws(function () { encodeSmauFrame(buildValid({ intervalIndex: 1n << 64n })); }, RangeError);
    assert.throws(function () { encodeSmauFrame(buildValid({ intervalIndex: -1 })); }, RangeError);
});

test('encode rejects flags out of u16 range', function () {
    assert.throws(function () { encodeSmauFrame(buildValid({ flags: -1 })); }, RangeError);
    assert.throws(function () { encodeSmauFrame(buildValid({ flags: 0x10000 })); }, RangeError);
    assert.throws(function () { encodeSmauFrame(buildValid({ flags: 1.5 })); }, RangeError);
});

test('encode rejects non-Uint8Array opus payload', function () {
    assert.throws(function () { encodeSmauFrame(buildValid({ opus: [1, 2, 3] })); }, TypeError);
    assert.throws(function () { encodeSmauFrame(buildValid({ opus: 'abc' })); }, TypeError);
    assert.throws(function () { encodeSmauFrame(buildValid({ opus: new ArrayBuffer(10) })); }, TypeError);
});

test('encode rejects frame length exceeding MAX_FRAME_BYTES', function () {
    var huge = new Uint8Array(MAX_FRAME_BYTES); // header + this would be > MAX
    assert.throws(function () { encodeSmauFrame(buildValid({ opus: huge })); }, RangeError);
});

test('encode accepts frame length exactly MAX_FRAME_BYTES', function () {
    var opus = new Uint8Array(MAX_FRAME_BYTES - HEADER_LEN);
    var buf = encodeSmauFrame(buildValid({
        opus: opus,
        // Need a duration big enough to satisfy the sample_count cap
        chartTimeStart: 0,
        chartTimeEnd: MAX_INTERVAL_SEC,
        sampleCount: 0,
    }));
    assert.strictEqual(buf.byteLength, MAX_FRAME_BYTES);
    var r = decodeSmauFrame(buf);
    assert.strictEqual(r.ok, true);
});

// ── Decode validation ───────────────────────────────────────────────────────
test('decode rejects buffer smaller than HEADER_LEN', function () {
    var r = decodeSmauFrame(new Uint8Array(HEADER_LEN - 1));
    assert.deepStrictEqual(r, { ok: false, reason: 'too_small' });
});

test('decode rejects buffer larger than MAX_FRAME_BYTES', function () {
    // Construct an oversize buffer with a valid-looking header so the size
    // check is what trips it.
    var size = MAX_FRAME_BYTES + 1;
    var u = new Uint8Array(size);
    u[0] = MAGIC_BYTES[0]; u[1] = MAGIC_BYTES[1];
    u[2] = MAGIC_BYTES[2]; u[3] = MAGIC_BYTES[3];
    var v = new DataView(u.buffer);
    v.setUint16(4, 1, true);
    v.setUint32(36, size - HEADER_LEN, true);
    v.setFloat64(16, 0, true);
    v.setFloat64(24, 1, true);
    var r = decodeSmauFrame(u);
    assert.deepStrictEqual(r, { ok: false, reason: 'frame_too_big' });
});

test('decode rejects magic mismatch', function () {
    var buf = encodeSmauFrame(buildValid());
    var bad = corruptHeader(buf, function (u) { u[0] = 0x4e; }); // 'N'
    var r = decodeSmauFrame(bad);
    assert.deepStrictEqual(r, { ok: false, reason: 'magic_mismatch' });
});

test('decode rejects version != 1', function () {
    var buf = encodeSmauFrame(buildValid());
    var bad = corruptHeader(buf, function (_u, view) { view.setUint16(4, 2, true); });
    var r = decodeSmauFrame(bad);
    assert.deepStrictEqual(r, { ok: false, reason: 'version_mismatch' });
    var bad2 = corruptHeader(buf, function (_u, view) { view.setUint16(4, 0, true); });
    var r2 = decodeSmauFrame(bad2);
    assert.deepStrictEqual(r2, { ok: false, reason: 'version_mismatch' });
});

test('decode rejects size_mismatch when opus_size does not match buffer length', function () {
    var buf = encodeSmauFrame(buildValid({ opus: fakeOpus(100) }));
    var bad = corruptHeader(buf, function (_u, view) { view.setUint32(36, 200, true); });
    var r = decodeSmauFrame(bad);
    assert.deepStrictEqual(r, { ok: false, reason: 'size_mismatch' });
});

test('decode rejects non-finite chart_time_start', function () {
    var buf = encodeSmauFrame(buildValid());
    var bad = corruptHeader(buf, function (_u, view) { view.setFloat64(16, NaN, true); });
    assert.deepStrictEqual(decodeSmauFrame(bad), { ok: false, reason: 'invalid_chart_time' });
    var bad2 = corruptHeader(buf, function (_u, view) { view.setFloat64(16, Infinity, true); });
    assert.deepStrictEqual(decodeSmauFrame(bad2), { ok: false, reason: 'invalid_chart_time' });
});

test('decode rejects non-finite chart_time_end', function () {
    var buf = encodeSmauFrame(buildValid());
    var bad = corruptHeader(buf, function (_u, view) { view.setFloat64(24, -Infinity, true); });
    assert.deepStrictEqual(decodeSmauFrame(bad), { ok: false, reason: 'invalid_chart_time' });
});

test('decode rejects zero or negative duration', function () {
    var buf = encodeSmauFrame(buildValid());
    var zero = corruptHeader(buf, function (_u, view) {
        view.setFloat64(16, 5, true);
        view.setFloat64(24, 5, true);
    });
    assert.deepStrictEqual(decodeSmauFrame(zero), { ok: false, reason: 'invalid_duration' });
    var neg = corruptHeader(buf, function (_u, view) {
        view.setFloat64(16, 6, true);
        view.setFloat64(24, 5, true);
    });
    assert.deepStrictEqual(decodeSmauFrame(neg), { ok: false, reason: 'invalid_duration' });
});

test('decode rejects duration exceeding MAX_INTERVAL_SEC', function () {
    var buf = encodeSmauFrame(buildValid());
    var bad = corruptHeader(buf, function (_u, view) {
        view.setFloat64(16, 0, true);
        view.setFloat64(24, MAX_INTERVAL_SEC + 0.001, true);
    });
    assert.deepStrictEqual(decodeSmauFrame(bad), { ok: false, reason: 'invalid_duration' });
});

test('decode rejects sample_count beyond ceil(48000 * duration) + 480', function () {
    // 1 second duration → cap = 48000 + 480 = 48480.
    var buf = encodeSmauFrame(buildValid({
        chartTimeStart: 0, chartTimeEnd: 1, sampleCount: 48480,
    }));
    assert.strictEqual(decodeSmauFrame(buf).ok, true);

    var bad = corruptHeader(buf, function (_u, view) { view.setUint32(32, 48481, true); });
    assert.deepStrictEqual(decodeSmauFrame(bad), { ok: false, reason: 'sample_count_too_high' });
});

test('decode tolerates sample_count = 0 (decoder is authoritative)', function () {
    var buf = encodeSmauFrame(buildValid({ sampleCount: 0 }));
    var r = decodeSmauFrame(buf);
    assert.strictEqual(r.ok, true);
    assert.strictEqual(r.header.sampleCount, 0);
});

test('decode rejects bogus input types', function () {
    var r = decodeSmauFrame('abc');
    assert.deepStrictEqual(r, { ok: false, reason: 'invalid_buffer' });
    var r2 = decodeSmauFrame(null);
    assert.deepStrictEqual(r2, { ok: false, reason: 'invalid_buffer' });
});

// ── Flags ───────────────────────────────────────────────────────────────────
test('decode treats unknown flag bits as 0 in tempoChangeAtEnd, but preserves raw flags', function () {
    var buf = encodeSmauFrame(buildValid({ flags: 0xfffe })); // bit 0 cleared, others set
    var r = decodeSmauFrame(buf);
    assert.strictEqual(r.ok, true);
    assert.strictEqual(r.header.tempoChangeAtEnd, false);
    assert.strictEqual(r.header.flags, 0xfffe);
});

test('decode reports tempoChangeAtEnd true when bit 0 is set', function () {
    var buf = encodeSmauFrame(buildValid({ flags: FLAG_TEMPO_CHANGE_AT_END }));
    var r = decodeSmauFrame(buf);
    assert.strictEqual(r.ok, true);
    assert.strictEqual(r.header.tempoChangeAtEnd, true);
});

// ── Empty payload edge case ────────────────────────────────────────────────
test('encode + decode round-trip with empty opus payload', function () {
    var buf = encodeSmauFrame(buildValid({ opus: new Uint8Array(0), sampleCount: 0 }));
    assert.strictEqual(buf.byteLength, HEADER_LEN);
    var r = decodeSmauFrame(buf);
    assert.strictEqual(r.ok, true);
    assert.strictEqual(r.opus.byteLength, 0);
    assert.strictEqual(r.header.opusSize, 0);
});

if (failures > 0) {
    console.log('');
    console.log(failures + ' test(s) failed');
    process.exit(1);
}
console.log('');
console.log('all tests passed');
