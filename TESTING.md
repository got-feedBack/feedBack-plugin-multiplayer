# Live Audio (Peer Broadcast) — Manual Test Matrix

This is the Phase 5 verification checklist for the Ninjam-style peer-audio feature (`broadcast_start` / `audio_quality` / SMAU frames). All scenarios are **manual** — they require two real machines on a network and a real chart playing through a real loopback device. Automated tests (`pytest`, `node --test audio/*.test.js`) cover the protocol and unit-level behavior; this doc covers the end-to-end behavior the unit tests cannot.

Run through this matrix before tagging a release. Mark each row pass/fail in your own notes; this file is the **canonical scenario list**, not a status tracker.

---

## Prerequisites

### Two machines on the same network

Two-machine testing is mandatory. Two browser tabs on the same machine **share the same `<audio>` element clock**, so split-tab can't reproduce the listener-side scheduling bugs that real two-machine playback can.

- **Machine A (host)** — runs Slopsmith with the multiplayer plugin, hosts a room, plays the chart.
- **Machine B (listener)** — joins the same room, plays the same chart.

Both should be running the same plugin version. Check `git log --oneline -1` matches.

### Loopback audio device

The host machine needs a virtual loopback device wired to the output of your amp sim / DAW. See `README.md` "Loopback input setup" for OS-specific instructions (VB-Audio Cable on Windows, BlackHole on macOS, PipeWire null-sink on Linux).

**Verify the loopback before you start the matrix:**

1. Open the multiplayer panel on Machine A.
2. Pick the loopback device from the broadcast input dropdown.
3. Toggle **Broadcast my sound** ON.
4. Strum your guitar — the **level meter** under the dropdown should move with each pluck.
5. If it doesn't move, fix the loopback before proceeding. Common causes: device set to disabled in OS; amp sim output not routed to the loopback; channel count mismatch.

### A chart with a known tempo change

Scenario 8 needs a Rocksmith chart that contains an actual mid-song tempo change (BPM jump, ritardando, fermata measure). Confirm before starting — running scenario 8 on a steady-tempo chart proves nothing.

---

## Scenario 1 — LAN sanity test

**Goal:** confirm the basic happy path works on a low-latency wired network.

1. Both machines on the same wired LAN (or solid 5 GHz Wi-Fi to the same access point).
2. Machine A creates a room. Machine B joins by code.
3. Machine A queues a chart and hits play. Confirm both machines' highways are scrolling in sync.
4. Machine A toggles **Broadcast my sound** ON, strums.
5. Machine B should hear the host's instrument **one measure delayed**, perfectly aligned to the listener's chart playback.

**Pass criteria:**

- The listener's own chart audio + host's delayed instrument sound musically coherent (same key, same downbeat).
- No glitches, dropouts, or interval boundary artifacts.
- The listener's `mp-broadcast-pill`-equivalent indicator (or the broadcaster_changed event surfacing in the UI) shows that Machine A is broadcasting.
- After ~30 s, Machine A's broadcast panel shows `1 listener — no issues` (or similar). If it shows late/dropped/decoder counts, note them — small numbers (1-2) may be tolerable; large or growing numbers indicate a real problem.

---

## Scenario 2 — Tethered hotspot (~80 ms RTT)

**Goal:** confirm the interval delay model masks RTT — the experience should be **identical** to scenario 1 despite the slower link.

1. Tether Machine B off a phone hotspot (or any cellular link with ~50-150 ms RTT to the server).
2. Run the same play-through-as-scenario-1 cycle.

**Pass criteria:**

- Same musical coherence as scenario 1.
- The peer audio still arrives **one measure delayed**, not "more delayed than on LAN". The interval delay (~2 s at 120 BPM) dwarfs RTT — if the delay perceptibly increases, something is wrong (likely a frame arriving past its scheduled chart-time and being dropped as `late`).
- Verify Machine A's broadcast panel reports `0 late` after a few measures. A non-zero `late` count on a connection well within the 2 s budget points to either a clock-sync bug or a frame-processing latency issue.

---

## Scenario 3 — Listener network drop (5-10 s)

**Goal:** confirm graceful recovery from a transient listener disconnect.

1. Start the same flow as scenario 1.
2. After Machine B has been hearing the host's audio for at least one minute (and Machine A's panel shows the listener), kill Machine B's network briefly:
   - Toggle Wi-Fi off for ~5-10 seconds, then back on.
   - Or use `nmcli`/`netsh` to disable the adapter and re-enable.
3. Watch Machine B during the outage and after recovery.

**Pass criteria:**

- During the outage: Machine B's peer audio cuts out cleanly; no static / repeated last-frame ghosting.
- After reconnect: the listener pipeline rebinds. New frames resume playing within ~2 s of network restoration.
- Machine A's broadcast panel — depending on timing — may briefly show the listener as still active during the outage, then drop them when the TTL (~90 s with the default 30 s reporting cadence) expires. **No** ghost listener should remain after the TTL window.
- No browser console errors that indicate state corruption (uncaught promise rejections, dangling `AudioBufferSourceNode` warnings).

---

## Scenario 4 — Pause / seek mid-broadcast

**Goal:** confirm peer audio re-aligns cleanly when the chart playback position changes.

1. Start broadcasting; Machine B is hearing the host.
2. **Pause test:** Machine A pauses playback. Machine B's player should also pause (existing host-controlled sync behavior).
   - Peer audio should stop within ~1 interval. No frames should keep playing after the pause.
   - Resume — peer audio should re-sync within ~1 interval.
3. **Seek test:** Machine A seeks forward by ~10 s.
   - Machine B follows the seek (existing sync).
   - Peer audio should pick up at the new chart position within ~1 interval. Any in-flight scheduled frames from the pre-seek position should be silently dropped (not played at the new position).

**Pass criteria:**

- No audible ghost peer audio at the wrong chart position.
- No "double playback" artifacts (a queued source playing while a new schedule starts).
- The broadcast panel on Machine A continues to show the listener as active after the seek.

---

## Scenario 5 — Listener mid-broadcast refresh

**Goal:** confirm a listener that hard-refreshes mid-broadcast cleanly rejoins.

1. Start broadcasting; Machine B is hearing the host.
2. On Machine B, hit Ctrl-F5 (hard refresh). The browser drops the page entirely and reloads.
3. Machine B rejoins the room (or auto-rejoins if `sessionStorage` carries the credentials).

**Pass criteria:**

- Within ~2 s of the page reloading and re-entering the player, Machine B starts hearing the host's audio again.
- Machine A's broadcast panel briefly drops the listener count (when their highway WS times out) then re-shows them once the new session attaches.
- No duplicate listener entries (if Machine A shows `2 listeners` you've found a session-aliasing bug).

---

## Scenario 6 — Host mid-broadcast refresh

**Goal:** confirm a broadcaster that hard-refreshes is correctly reported as stopped.

1. Start broadcasting; Machine B is hearing the host.
2. On Machine A, hit Ctrl-F5.

**Pass criteria:**

- Machine B stops hearing the host's instrument within ~1 interval. (The last interval already-in-flight may finish playing; that's expected.)
- Machine B's UI surfaces a `broadcast_stop` (or equivalent broadcaster-cleared signal) — the listener pipeline should tear down cleanly.
- When Machine A returns to the player and rejoins as host, the host promotion / session lifecycle should fire normally. Machine A re-toggling broadcast back on should be heard by Machine B again.
- No state where Machine B keeps hearing ghost host audio after Machine A's refresh.

---

## Scenario 7 — 30-minute soak

**Goal:** catch slow leaks, gradual drift, or accumulating glitches.

1. Three machines: one host (broadcasting), two listeners.
2. Queue a long playlist (~30 min total). Or loop a 5-minute song.
3. Start broadcast. Walk away for 30 min. Come back.

**Watch for, on the broadcaster:**
- Memory growth in the host's tab (Chrome DevTools → Memory → Take heap snapshot at start and end). >50 MB growth over 30 min is suspicious.
- The broadcast panel's `late` / `dropped` / `decoder` counters. Healthy: stay near zero. Concerning: growing linearly.
- AudioContext / encoder warnings in the console.

**Watch for, on each listener:**
- Same memory check.
- `_audioGetRxStats()` from devtools — `dropped` and `late` should be small relative to `valid` after a 30 min run.
- Audible glitches at interval boundaries (clicks, pops, gaps). Should not happen — if they do, it's a per-interval scheduling bug.
- Drift: by 30 minutes in, the peer audio should still be one interval delayed, not visibly more or less. If the delay accumulates, it's a chart-time-vs-AudioContext-clock drift bug.

---

## Scenario 8 — Tempo change

**Goal:** confirm the tempo-change detector + warning UI fire on a real chart.

1. Pick a chart with a known tempo change (e.g. a song with a rit. or a bridge that drops to half-time).
2. Start broadcasting. Play through the tempo change boundary.

**Pass criteria:**

- On Machine A, the **`⚠ Tempo change at this measure`** banner appears in the broadcast panel for one interval (~2 s) at the change boundary. The banner auto-clears after one interval.
- On Machine B, the listener may briefly hear a slight misalignment for one measure. This is **expected behavior in v1** — the plan explicitly accepts a one-measure misalignment burst. After the burst, the peer audio should re-align on the new tempo's beat grid.
- The misalignment should NOT propagate forward — if peer audio stays misaligned for more than one measure, the interval re-anchoring logic is broken.

If you have a chart with multiple tempo changes, run through several of them. The detector should fire on each.

---

## Scenario 9 — Multi-broadcaster preemption (negative test)

**Goal:** confirm the v1 single-broadcaster invariant holds.

1. Start broadcasting on Machine A. Machine B is listening.
2. On Machine B, also try to toggle **Broadcast my sound** ON.

**Pass criteria:**

- Machine B's broadcast attempt fails with a user-visible message like `Another player is already broadcasting.` (the `broadcaster_busy` server error path).
- Machine B's local capture pipeline does **not** start (no level meter movement, no encoder spin-up).
- Machine A's broadcast remains unaffected; Machine A's listener (now Machine B) keeps hearing them.
- v2 will allow N-way mesh; for v1 the second broadcaster must be cleanly rejected.

---

## Telemetry verification

For scenarios where the broadcast panel should show listener stats:

- **Reports cadence:** the listener sends one `audio_quality` report per 30 s (current default). After the listener has been receiving for ~30-60 s, the broadcaster's panel should show a non-empty stats line.
- **Empty windows are silent:** if the listener is paused, you should NOT see a stats update every 30 s — the listener skips empty reports. The TTL keeps the listener visible until 90 s of silence.
- **Stats deltas, not totals:** consecutive non-empty reports should reflect *the last 30 s* of activity, not page-load cumulative. If you see a 30 s window report `intervals_received: 1000+` for a normal song, the delta-rollover is broken.

The listener's raw cumulative counters can be inspected from devtools via `_audioGetRxStats()` in the screen.js global scope. Compare these against the aggregate stats on the broadcaster's panel.

---

## Common failure modes

- **No peer audio at all on the listener** — first check Machine A's broadcast pill is `⏺ Broadcasting` (not `⏳ Connecting…`). If stuck on connecting, the audio WS hasn't attached or the server rejected `broadcast_start`. Check both consoles.
- **Peer audio plays but with audible gaps every interval** — likely an Opus encode/decode bug or a missing packet in the multi-packet payload. Check `_audioGetRxStats()` for non-zero `decoder_error` / `opus_*` drop reasons.
- **Peer audio is at the wrong tempo / pitch** — the listener's `AudioContext` sample rate isn't 48 kHz, or the encoder negotiated different parameters than the listener expected. Check broadcast params in the `broadcaster_changed` message.
- **Tempo-change warning fires constantly on a steady-tempo chart** — the beat-spacing detector is hitting the 5% threshold on chart-side jitter. Capture the beats array via `window.highway.getBeats()` and report the chart filename so we can tune the threshold or fix the upstream beat-extraction.
- **Listeners stay visible forever after disconnecting** — the TTL eviction isn't running. The broadcaster's `_audioQualityOnTick` should evict every 30 s; if it isn't, the audio_quality timer didn't start.

---

## Reporting

When something fails, gather:

1. The scenario number and step that failed.
2. Console output from BOTH machines (host + listener).
3. The output of `_audioGetRxStats()` on the failing listener.
4. Network conditions (LAN / Wi-Fi / hotspot).
5. The chart filename + arrangement (and BPM if you know it).
6. Browser version on each machine.

File as a GitHub issue on this repo with the title prefixed `[v1 manual test]`.
