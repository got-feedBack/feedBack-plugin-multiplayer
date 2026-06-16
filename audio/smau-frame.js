// Slopsmith Multiplayer — SMAU audio frame codec
//
// Wire format defined by PROTOCOL.md "Audio frame format (Audio WS)". Header
// is a fixed-size 40-byte little-endian block; payload is a single Opus
// EncodedAudioChunk produced by WebCodecs AudioEncoder. encodeSmauFrame()
// builds the binary frame the host puts on the audio WS; decodeSmauFrame()
// is the strict, no-allocation-before-validation parser receivers run on
// every inbound frame (server fan-out is byte-for-byte, so we cannot trust
// peer-supplied header values).
//
// All multi-byte integers are little-endian. interval_index is u64 →
// JavaScript MUST use BigInt for it (Number loses precision above 2^53−1).
// Per PROTOCOL.md "Receiver validation", every check below is O(1) and
// performed before any allocation, decoder call, or peer-fanout.
//
// Like select-interval.js, this file is currently CommonJS-only. The
// browser-side copy is inlined into screen.js because slopsmith core's
// plugin loader serves only /api/plugins/{id}/screen.js (one entry point
// per plugin) — there is no generic asset path. Keeping a single source
// of truth here for the Node test runner; screen.js carries a documented
// duplicate.

var MAGIC_BYTES = [0x53, 0x4d, 0x41, 0x55]; // "SMAU"
var MAGIC_STRING = 'SMAU';
var VERSION = 1;
var HEADER_LEN = 40;
var MAX_FRAME_BYTES = 262144;        // 256 KB, inclusive of header
var V1_SAMPLE_RATE = 48000;
var V1_CHANNEL_COUNT = 1;
var MAX_INTERVAL_SEC = 32;
var MAX_SLACK_SAMPLES = 480;         // ~10 ms at 48 kHz
var FLAG_TEMPO_CHANGE_AT_END = 0x0001;

// u64 / u32 numeric bounds — used to reject overflowing integer inputs at
// encode time before DataView would silently wrap them.
var U64_MAX = (1n << 64n) - 1n;
var U32_MAX = 0xffffffff;

function _isFiniteNumber(n) {
    return typeof n === 'number' && Number.isFinite(n);
}

function _isU32(n) {
    return Number.isInteger(n) && n >= 0 && n <= U32_MAX;
}

function _toBigUint64(value, fieldName) {
    var bi;
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
    if (bi < 0n || bi > U64_MAX) {
        throw new RangeError(fieldName + ' out of u64 range');
    }
    return bi;
}

function _asUint8Array(buf) {
    if (buf instanceof Uint8Array) return buf;
    if (buf instanceof ArrayBuffer) return new Uint8Array(buf);
    if (ArrayBuffer.isView(buf)) {
        return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
    }
    throw new TypeError('expected ArrayBuffer, Uint8Array, or ArrayBufferView');
}

// encodeSmauFrame — build a binary frame for the audio WS.
//
// Required fields:
//   intervalIndex   : BigInt | non-negative integer Number
//   chartTimeStart  : finite Number (seconds, chart playback time)
//   chartTimeEnd    : finite Number (seconds, end-exclusive)
//   sampleCount     : u32 (PCM samples per channel; hint, see PROTOCOL.md)
//   opus            : Uint8Array (the encoded payload bytes)
//
// Optional:
//   flags           : u16 bitfield. Bit 0 = tempo_change_at_end. Default 0.
//
// Throws on invalid input. The encoder is "strict at the source" — the
// host should never produce a frame that decodeSmauFrame would reject, so
// fail fast here instead of letting bad data hit the wire.
function encodeSmauFrame(fields) {
    if (!fields || typeof fields !== 'object') {
        throw new TypeError('encodeSmauFrame: fields object required');
    }
    var intervalIndex = _toBigUint64(fields.intervalIndex, 'intervalIndex');
    var chartTimeStart = fields.chartTimeStart;
    var chartTimeEnd = fields.chartTimeEnd;
    var sampleCount = fields.sampleCount;
    var flags = fields.flags == null ? 0 : fields.flags;
    var opus = fields.opus;

    if (!_isFiniteNumber(chartTimeStart)) {
        throw new RangeError('chartTimeStart must be a finite number');
    }
    if (!_isFiniteNumber(chartTimeEnd)) {
        throw new RangeError('chartTimeEnd must be a finite number');
    }
    var duration = chartTimeEnd - chartTimeStart;
    if (!_isFiniteNumber(duration) || duration <= 0) {
        throw new RangeError('chartTimeEnd must be strictly greater than chartTimeStart');
    }
    if (duration > MAX_INTERVAL_SEC) {
        throw new RangeError('interval duration ' + duration + 's exceeds MAX_INTERVAL_SEC=' + MAX_INTERVAL_SEC);
    }
    if (!_isU32(sampleCount)) {
        throw new RangeError('sampleCount must be a u32 integer');
    }
    // Mirror the decoder's sample_count cap so encode/decode round-trips
    // never produce a frame the receiver will discard with
    // sample_count_too_high. Using the v1 constant V1_SAMPLE_RATE matches
    // the receiver's bound exactly.
    var maxSamples = Math.ceil(V1_SAMPLE_RATE * duration) + MAX_SLACK_SAMPLES;
    if (sampleCount > maxSamples) {
        throw new RangeError(
            'sampleCount ' + sampleCount + ' exceeds receiver cap '
            + maxSamples + ' for duration ' + duration + 's'
        );
    }
    if (!Number.isInteger(flags) || flags < 0 || flags > 0xffff) {
        throw new RangeError('flags must be a u16 integer');
    }
    if (!(opus instanceof Uint8Array)) {
        throw new TypeError('opus must be a Uint8Array');
    }
    if (!_isU32(opus.byteLength)) {
        throw new RangeError('opus payload size out of u32 range');
    }
    var frameLen = HEADER_LEN + opus.byteLength;
    if (frameLen > MAX_FRAME_BYTES) {
        throw new RangeError('frame length ' + frameLen + ' exceeds MAX_FRAME_BYTES=' + MAX_FRAME_BYTES);
    }

    var buf = new ArrayBuffer(frameLen);
    var bytes = new Uint8Array(buf);
    var view = new DataView(buf);

    bytes[0] = MAGIC_BYTES[0];
    bytes[1] = MAGIC_BYTES[1];
    bytes[2] = MAGIC_BYTES[2];
    bytes[3] = MAGIC_BYTES[3];
    view.setUint16(4, VERSION, true);
    view.setUint16(6, flags, true);
    view.setBigUint64(8, intervalIndex, true);
    view.setFloat64(16, chartTimeStart, true);
    view.setFloat64(24, chartTimeEnd, true);
    view.setUint32(32, sampleCount, true);
    view.setUint32(36, opus.byteLength, true);
    bytes.set(opus, HEADER_LEN);

    return buf;
}

// decodeSmauFrame — parse + validate a frame off the audio WS.
//
// Returns either:
//   { ok: true, header: { version, flags, intervalIndex (BigInt),
//                         chartTimeStart, chartTimeEnd, sampleCount,
//                         opusSize, frameLength, tempoChangeAtEnd },
//     opus: Uint8Array }                          — view into source buffer
//   { ok: false, reason: <string> }               — frame must be dropped
//
// reason values (stable; tests / metrics may match on these):
//   "too_small"             — frame_length < HEADER_LEN
//   "frame_too_big"         — frame_length > MAX_FRAME_BYTES
//   "magic_mismatch"        — first 4 bytes != "SMAU"
//   "version_mismatch"      — version != 1
//   "size_mismatch"         — frame_length != HEADER_LEN + opus_size
//   "invalid_chart_time"    — chart_time_start or _end not finite
//   "invalid_duration"      — duration ≤ 0 or > MAX_INTERVAL_SEC, or non-finite
//   "sample_count_too_high" — sample_count > ceil(48000 * duration) + 480
//
// Returning a verdict instead of throwing lets callers count drop reasons
// for the per-broadcaster quality telemetry and keeps the hot path
// allocation-free for the success case (opus is a view, not a copy).
function decodeSmauFrame(buf) {
    var bytes;
    try {
        bytes = _asUint8Array(buf);
    } catch (err) {
        return { ok: false, reason: 'invalid_buffer' };
    }
    var len = bytes.byteLength;
    if (len < HEADER_LEN) return { ok: false, reason: 'too_small' };
    if (len > MAX_FRAME_BYTES) return { ok: false, reason: 'frame_too_big' };

    if (bytes[0] !== MAGIC_BYTES[0]
        || bytes[1] !== MAGIC_BYTES[1]
        || bytes[2] !== MAGIC_BYTES[2]
        || bytes[3] !== MAGIC_BYTES[3]) {
        return { ok: false, reason: 'magic_mismatch' };
    }

    var view = new DataView(bytes.buffer, bytes.byteOffset, len);
    var version = view.getUint16(4, true);
    if (version !== VERSION) return { ok: false, reason: 'version_mismatch' };

    var flags = view.getUint16(6, true);
    var intervalIndex = view.getBigUint64(8, true);
    var chartTimeStart = view.getFloat64(16, true);
    var chartTimeEnd = view.getFloat64(24, true);
    var sampleCount = view.getUint32(32, true);
    var opusSize = view.getUint32(36, true);

    if (HEADER_LEN + opusSize !== len) return { ok: false, reason: 'size_mismatch' };

    if (!Number.isFinite(chartTimeStart) || !Number.isFinite(chartTimeEnd)) {
        return { ok: false, reason: 'invalid_chart_time' };
    }
    var duration = chartTimeEnd - chartTimeStart;
    if (!Number.isFinite(duration) || duration <= 0 || duration > MAX_INTERVAL_SEC) {
        return { ok: false, reason: 'invalid_duration' };
    }
    // Cap sample_count using the v1 constant V1_SAMPLE_RATE — NOT any
    // peer-supplied rate — so malicious broadcasters cannot enlarge the
    // bound by lying in broadcast_start.
    var maxSamples = Math.ceil(V1_SAMPLE_RATE * duration) + MAX_SLACK_SAMPLES;
    if (sampleCount > maxSamples) {
        return { ok: false, reason: 'sample_count_too_high' };
    }

    var opus = new Uint8Array(bytes.buffer, bytes.byteOffset + HEADER_LEN, opusSize);

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
            tempoChangeAtEnd: (flags & FLAG_TEMPO_CHANGE_AT_END) !== 0,
        },
        opus: opus,
    };
}

module.exports = {
    encodeSmauFrame: encodeSmauFrame,
    decodeSmauFrame: decodeSmauFrame,
    MAGIC_STRING: MAGIC_STRING,
    MAGIC_BYTES: MAGIC_BYTES.slice(),
    VERSION: VERSION,
    HEADER_LEN: HEADER_LEN,
    MAX_FRAME_BYTES: MAX_FRAME_BYTES,
    V1_SAMPLE_RATE: V1_SAMPLE_RATE,
    V1_CHANNEL_COUNT: V1_CHANNEL_COUNT,
    MAX_INTERVAL_SEC: MAX_INTERVAL_SEC,
    MAX_SLACK_SAMPLES: MAX_SLACK_SAMPLES,
    FLAG_TEMPO_CHANGE_AT_END: FLAG_TEMPO_CHANGE_AT_END,
};
