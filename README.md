# Multiplayer

A [Slopsmith](https://github.com/byrongamatos/slopsmith) plugin that lets multiple players join a room and play songs together with synced highways, a shared song queue, and optional post-song recording with a built-in mixer.

## Features

### Synced Rooms
- Create or join a room with a 6-character code
- Host controls playback (play/pause/seek/speed) — all players stay in sync
- NTP-style clock sync with playback drift correction (micro-adjusts playbackRate for smooth alignment)
- Automatic host promotion if the host disconnects
- Rooms are ephemeral — destroyed 60 seconds after the last player leaves

### Shared Song Queue
- Any player can search the library and add songs to the queue
- Drag to reorder, remove songs, vote to skip (majority rules)
- Auto-advance to the next song when one finishes
- Host can click any queued song to load it immediately

### Per-Player Arrangements
- Each player picks their arrangement (Lead/Rhythm/Bass)
- Everyone sees their own highway with their chosen arrangement
- Multiple players can pick the same arrangement

### Recording
- Host toggles "Record" — arms mic input on all connected players
- Recording starts automatically when the host hits play (not when armed)
- Each player's recording is uploaded to the server when recording stops
- Uses `getUserMedia` on web, JUCE audio engine on desktop

### Live Audio (Peer Broadcast)

Hear another player's instrument **while playing the same chart**, locked to the chart's beat grid. Open the multiplayer panel, pick your loopback input from the device dropdown, flip on **Broadcast my sound**, and every other player in the room will hear your sound through their browser one interval later (typically one measure; very fast or very slow tempos use 8 or 2 beats per interval).

#### How it sounds — the honest description

Peer audio plays **one interval (typically one measure) delayed**, not "lagged". This isn't network lag — it's a structured offset built into the protocol so every listener gets a perfectly aligned, glitch-free playback regardless of their connection. You'll hear it as: "I'm playing along to the chart, and I can also hear my bandmate playing along to the chart, but their part is one measure behind". The chart audio is the shared anchor — that's what stays in perfect sync.

The model is borrowed from [Ninjam](https://www.cockos.com/ninjam/) and works over any non-pathological internet connection (LAN, home Wi-Fi, tethered phone hotspot — all feel the same).

#### Loopback input setup

Slopsmith does **not** host amp simulation. You run your full signal chain (amp sim → effects → DAW out) into a virtual loopback device, then point the broadcast picker at that device. Per OS:

**Windows — VB-Audio Cable**
1. Install [VB-Audio Cable](https://vb-audio.com/Cable/) (free).
2. In your DAW / amp sim, set the audio output to **CABLE Input (VB-Audio Virtual Cable)**.
3. In the multiplayer panel, pick **CABLE Output** as the broadcast input.
4. To still hear yourself locally, also set up VB-Audio's "Listen to this device" passthrough or use [VoiceMeeter](https://vb-audio.com/Voicemeeter/) for finer routing.

**macOS — BlackHole**
1. Install [BlackHole 2ch](https://github.com/ExistentialAudio/BlackHole) (free, via Homebrew: `brew install blackhole-2ch`).
2. In Audio MIDI Setup, create a **Multi-Output Device** combining BlackHole 2ch + your normal output, so you still hear yourself.
3. In your DAW / amp sim, set the audio output to the Multi-Output Device.
4. In the multiplayer panel, pick **BlackHole 2ch** as the broadcast input.

**Linux — PipeWire / PulseAudio loopback**
1. PipeWire (modern distros) ships a `null-sink` you can route to:
   ```bash
   pactl load-module module-null-sink sink_name=slopsmith_loop \
     sink_properties=device.description=SlopsmithLoop
   ```
2. In your DAW / amp sim, set the output to **SlopsmithLoop**.
3. In the multiplayer panel, pick the **Monitor of SlopsmithLoop** input.
4. To hear yourself locally, route the same source to your normal output via `pactl load-module module-loopback`.

After setup: load any song, hit play, strum your guitar — you should see the level meter move and the **Broadcasting** pill turn red. Other room members will see a corresponding "Live audio: <broadcaster>" indicator and hear your instrument starting one measure after you played it.

#### What to expect at tempo changes

The interval boundaries are anchored to the chart's beat grid using the BPM at broadcast start. When the chart enters a section with a different tempo, the broadcaster's interval boundaries no longer line up perfectly with the chart's beats for **one measure** — listeners may hear a brief shift before alignment recovers automatically on the next measure.

The broadcaster sees a `⚠ Tempo change at this measure` banner when this happens, and the wire format carries a `tempo_change_at_end` flag so future versions can time-stretch around the boundary. v1 just lets the one measure pass through.

#### Telemetry

While broadcasting, the panel shows aggregate listener stats from each peer's listener pipeline (e.g. `2 listeners — 1 late, 0 dropped`). Reports are 30-second rolling deltas from the listener's perspective; "late" means a frame arrived after its scheduled chart-time, "dropped" means decoder/validation failure. Healthy operation reports `no issues`.

#### Testing

Before tagging a release, run through the manual test matrix in [TESTING.md](TESTING.md) — covers two-machine LAN sanity, hotspot/RTT, network drop, pause/seek, refresh on either side, 30-min soak, tempo-change verification, and multi-broadcaster preemption.

### Mini DAW Mixer
After recording, the host opens the Mixer to align and export:
- **Waveform display** for every track (stems in green, recordings in blue)
- **Drag tracks** left/right to visually align recordings with the stems
- **Nudge buttons** (-50ms, -10ms, +10ms, +50ms) for fine-tuning
- **Volume slider** per track
- **Mute (M)** and **Solo (S)** buttons per track
- **Preview** button to hear the mix in-browser via Web Audio
- **Export Mix** sends offsets, volumes, and mutes to the server for FFmpeg mixdown
- Download the result as MP3

## Requirements

- Slopsmith with WebSocket support (standard)
- For recording: browser mic access (Chrome/Edge recommended)
- For peer audio broadcast: WebCodecs (Chrome/Edge or Firefox 130+) and a loopback input device (see "Live Audio" below for OS-specific setup)
- For stem mixing: sloppak songs with split stems (use the Sloppak Converter plugin)
- FFmpeg available in the Docker container (included by default)

## Install

```bash
cd plugins
git clone https://github.com/byrongamatos/slopsmith-plugin-multiplayer.git multiplayer
```

Restart Slopsmith and the plugin will appear in the Plugins dropdown.

## Cross-Platform

Web app and desktop app players can be in the same room. The sync protocol is WebSocket-based and works identically for both. Desktop players record via JUCE (higher quality WAV), web players record via MediaRecorder (webm-opus converted to WAV on upload).

## License

This repository is licensed under **GPL-3.0-or-later**. See the [LICENSE](LICENSE) file for the full text. Slopsmith core and other plugins are separate projects published under their own licenses; this license statement applies only to the files in this repository.
