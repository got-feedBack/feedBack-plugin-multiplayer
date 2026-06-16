// Slopsmith Multiplayer — tempo-change detection
//
// Pure helper that determines whether the chart's beat grid changes
// tempo across a given boundary time. Used by the capture pipeline
// to set the SMAU tempo_change_at_end flag and surface a UI warning
// for the broadcaster on the interval whose end falls on (or just
// past) a tempo shift.
//
// Definition: a tempo change at `boundaryTime` is a >threshold delta
// between (a) the spacing of the two beats immediately preceding the
// boundary and (b) the spacing of the two beats immediately following
// it. We need at least 4 valid beats in that 4-beat window — a chart
// shorter than that, or a boundary that falls in the very first or
// very last beat, is conservatively reported as "no tempo change"
// since we don't have enough context to call it.
//
// See PROTOCOL.md for the SMAU header bit and screen.js for the v1
// behavior (warn the broadcaster, set the flag — no time-stretch).
//
// Currently consumed only by the Node test runner. screen.js inlines
// an equivalent `_captureTempoChangeAtBoundary` since slopsmith core's
// plugin loader serves only screen.js. Keep the two in sync; tests
// here are the source of truth for the algorithm.

var DEFAULT_THRESHOLD = 0.05; // 5% spacing delta

// detectTempoChangeAtBoundary(beats, boundaryTime, threshold?)
//   beats:        array of { time: number } objects, monotonically
//                 increasing. Entries with non-finite times are
//                 treated as malformed and bail out conservatively.
//   boundaryTime: chart time at which the interval boundary sits.
//   threshold:    fractional delta above which we report a change.
//                 Default 0.05 (5%).
//
// Returns: boolean — true iff |spacingAfter - spacingBefore| / spacingBefore > threshold.
function detectTempoChangeAtBoundary(beats, boundaryTime, threshold) {
    if (!Array.isArray(beats) || beats.length < 4) return false;
    if (typeof boundaryTime !== 'number' || !Number.isFinite(boundaryTime)) return false;
    var t = (typeof threshold === 'number' && Number.isFinite(threshold) && threshold >= 0)
        ? threshold
        : DEFAULT_THRESHOLD;

    // Find the index of the first beat at or after the boundary.
    var nextIdx = -1;
    for (var i = 0; i < beats.length; i++) {
        var bt = beats[i] && beats[i].time;
        if (typeof bt === 'number' && Number.isFinite(bt) && bt >= boundaryTime) {
            nextIdx = i;
            break;
        }
    }
    // Need two beats on each side of the boundary to compute deltas.
    if (nextIdx < 2 || nextIdx >= beats.length - 1) return false;

    var tPrevPrev = beats[nextIdx - 2] && beats[nextIdx - 2].time;
    var tPrev = beats[nextIdx - 1] && beats[nextIdx - 1].time;
    var tNext = beats[nextIdx] && beats[nextIdx].time;
    var tNextNext = beats[nextIdx + 1] && beats[nextIdx + 1].time;
    if (
        typeof tPrevPrev !== 'number' || !Number.isFinite(tPrevPrev)
        || typeof tPrev !== 'number' || !Number.isFinite(tPrev)
        || typeof tNext !== 'number' || !Number.isFinite(tNext)
        || typeof tNextNext !== 'number' || !Number.isFinite(tNextNext)
    ) return false;

    var spacingBefore = tPrev - tPrevPrev;
    var spacingAfter = tNextNext - tNext;
    if (!(spacingBefore > 0) || !(spacingAfter > 0)) return false;

    var ratio = Math.abs(spacingAfter - spacingBefore) / spacingBefore;
    return ratio > t;
}

module.exports = {
    detectTempoChangeAtBoundary: detectTempoChangeAtBoundary,
    DEFAULT_THRESHOLD: DEFAULT_THRESHOLD,
};
