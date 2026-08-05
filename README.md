# restream-controller

Continuous OBS -> Twitch restreaming: if your internet connection
drops, the Twitch stream doesn't end — it switches to a backup video
until the connection comes back (or until a timeout is reached).

## What it does

`restream-controller` sits between OBS and Twitch on a VPS you
control. OBS publishes to the VPS over RTMP; the VPS relays it to
Twitch. As long as the RTMP connection between the VPS and Twitch
stays open, Twitch keeps the channel live — even while your own
connection to the VPS is down.

- **Seamless fallback.** If OBS disconnects unexpectedly (bad
  internet, crashed PC, closed OBS), the stream switches to a backup
  video instead of ending. Viewers see a placeholder, not a "stream
  offline" screen.
- **Seamless recovery.** When OBS reconnects, the service waits for a
  clean keyframe from the live feed and cuts back to it without ever
  dropping the Twitch connection — no visible freeze, no re-buffering
  on Twitch's side.
- **Graceful stop, not just a timeout.** Consciously clicking "Stop
  Streaming" in OBS ends the broadcast immediately and cleanly, with
  no backup video and no waiting around — see "Stop vs. disconnect"
  below for why this distinction needs a small OBS-side script.
- **Automatic timeout.** If the connection doesn't come back within a
  configurable window (30 minutes by default), the Twitch broadcast
  ends on its own instead of looping the backup video forever.
- **No Docker, no heavy dependencies.** `ffmpeg` for encoding/relaying,
  a single Go binary ([MediaMTX](https://github.com/bluenviron/mediamtx))
  for RTMP ingest, and a stdlib-only Python controller — nothing to
  build, nothing to containerize.

This project intentionally does *not* try to be a general-purpose
restreaming platform (no multi-destination fan-out, no transcoding
ladder, no web UI) — it solves exactly one scenario: keep a single
OBS -> Twitch stream alive through connection drops.

## Prerequisites

- A VPS (or a local Linux box/WSL for testing) running Debian or
  Ubuntu, with SSH access and sudo.
- A public IP on the VPS (so OBS can reach it).
- A Twitch account and its Primary Stream Key (Twitch Creator
  Dashboard -> Settings -> Stream).
- OBS Studio on the machine you stream from.
- A backup video file (H.264 + AAC) to loop on Twitch while the
  connection is down.

## Setup

### 1. Clone the repository

```bash
git clone <this-repo-url> restream
cd restream
```

### 2. Install dependencies

```bash
bash install.sh
```

This installs `ffmpeg`, `python3`, and the MediaMTX binary, and
generates two config files with random passwords: `mediamtx.yml` and
`controller/config.json`. At the end it prints:

- the RTMP login/password for OBS,
- the URL and token for the OBS script `obs-plugin/obs_graceful_stop.py`.

You'll need these in step 6. If you forget them, or you're re-running
`install.sh` after it already ran once (a second run prints nothing
new, it just skips what already exists), you can look them up again
any time:

```bash
./restreamctl.sh credentials
```

### 3. Add a backup video

Copy your file to `backup/backup.mp4`.

Any format `ffmpeg` understands works (mp4, mkv, mov, avi...) — you
don't need to manually match codec, resolution, fps, or audio channel
count to your OBS stream. On every stream start, the service compares
the backup file's parameters to the live OBS stream and, if they
differ, transcodes the backup into a separate prepared copy in the
background (this doesn't interrupt the live broadcast). It takes a
few seconds to a couple of minutes after OBS first connects — well
before the backup would actually be needed on a disconnect.

**For best results**, use a file encoded with the same settings as
your stream. The easiest way to get one: in OBS, click "Start
Recording" (with the same Settings -> Output you use for streaming)
and record a few minutes — a recording like that matches the live
stream exactly, so no automatic transcoding is needed and the backup
is ready to use immediately.

### 4. Edit `controller/config.json`

Open `controller/config.json` and change:

- `twitch_url` — your RTMP URL with your real Twitch stream key,
  format: `rtmp://live.twitch.tv/app/YOUR_STREAM_KEY`
- optionally `disconnect_timeout_sec` — how many seconds to wait for
  the connection to come back before ending the Twitch broadcast
  entirely (default 1800 = 30 minutes).

You can leave the `output_*` fields alone — they no longer set the
output parameters directly (those are read from the live OBS stream
automatically); `output_video_bitrate_kbps`/`output_audio_bitrate_kbps`
only affect the quality of the one-time backup-video transcode from
step 3.

### 5. Check readiness

```bash
./restreamctl.sh check
```

Prints `[OK]`/`[WARNING]`/`[ERROR]` for each item (config files, the
backup video and its codecs, whether `twitch_url` was actually
changed). Fix anything marked `[ERROR]` before continuing.

### 6. Configure OBS

1. Settings -> Stream -> Service: **"Custom..."** (not a linked Twitch
   account — OBS won't let you override the server if it's linked).
2. Server: `rtmp://YOUR_VPS_IP:1935/live`
3. Stream Key: `main?user=obs&pass=PASSWORD` (password from step 2,
   `install.sh`'s output; this exact format — `user=...&pass=...`
   inside the stream key — is required, not `rtmp://user:pass@host`,
   because that's how MediaMTX expects RTMP auth).
4. Tools -> Scripts -> "+" -> select
   `obs-plugin/obs_graceful_stop.py`. In the script's properties, fill
   in the controller URL and token (both from `install.sh`'s output):
   - URL: `http://YOUR_VPS_IP:8790/obs/graceful-stop`
   - Token: the `obs_webhook_token` value
5. Settings -> Output (Advanced mode) -> Streaming -> **Keyframe
   Interval: 2** (instead of "Auto"). This affects how long it takes
   for live video to appear after a start/recovery: the service waits
   for the first keyframe from OBS before showing anything (to avoid
   a corrupted-looking picture at the cut) — a longer default interval
   ("Auto" is often more than 2s) means a longer visible pause. The
   same interval (2s) is already used for the backup video, and it's
   Twitch's own recommendation anyway.
6. Settings -> Output -> **Rate Control: CBR** (not "Lossless").
   "Lossless" is meant for local recording, not streaming — it
   produces an unpredictably high and variable bitrate that Twitch
   ingests with visible corruption (confirmed: the decoder reports the
   wrong codec profile, and the player's `Download Bitrate` drops by a
   large factor). Set an explicit bitrate instead (e.g. 6000 Kbps —
   Twitch's typical cap for non-partners).

   > Note: this specific problem shows up right when you switch the
   > Service from "Twitch" (the built-in one, with a linked account)
   > to "Custom..." (required here, since we need our own RTMP server
   > and auth). It looks like OBS itself handles/limits encoder
   > settings differently depending on which server is selected — with
   > "Twitch" selected, risky combinations like "Lossless" are
   > apparently either blocked or behave differently than with
   > "Custom". In other words, the root cause is in OBS itself, not in
   > this controller — but since "Custom" is unavoidable here, you
   > have to set safe encoder settings manually (step 6 above).

### 7. Start

```bash
./restreamctl.sh start
```

Starts MediaMTX and the controller, verifies both came up, and prints
the current state.

### 8. Check status

```bash
./restreamctl.sh status
```

Shows whether the processes are running, and the current broadcast
state: `OFFLINE` (nothing is streaming), `LIVE` (live video is going
out), `FALLBACK` (backup video is playing, waiting for OBS to come
back).

### Day-to-day management

```bash
./restreamctl.sh stop          # stop everything
./restreamctl.sh restart       # restart
./restreamctl.sh logs          # recent log lines + paths to the ffmpeg logs
./restreamctl.sh credentials   # print the OBS password and webhook token again
```

### Troubleshooting

1. `./restreamctl.sh check` — did the backup video disappear? Is
   `twitch_url` actually set?
2. `./restreamctl.sh logs` — the controller's own log.
3. `controller/ffmpeg-relay.log`, `controller/ffmpeg-backup.log`,
   `controller/ffmpeg-outbound.log` — logs of the individual ffmpeg
   processes, if the problem looks like a video/audio issue rather
   than a switching-logic issue.
4. If the backup video looks "wrong" (not the file you expected) —
   check `backup/backup.prepared.mp4`. This is the auto-prepared copy
   from step 3; delete it along with
   `backup/backup.prepared.meta.json` to have the service rebuild it
   from scratch on the next stream start.
5. Corrupted/"shattered"-looking picture on Twitch, or a strange
   bitrate/codec in the player's stream stats — check OBS Settings ->
   Output -> **Rate Control**. If it's set to "Lossless", that's the
   cause (see step 6 of "Configure OBS" above) — switch to CBR with an
   explicit bitrate.

## Everyday scenarios

**Everything is working normally.** OBS is publishing, viewers see
the live feed. Nothing special happens.

**Internet dropped / PC crashed / OBS closed unexpectedly.** Twitch
doesn't notice the drop — the backup video takes over instead of the
live feed, and stays on until either:
- the connection comes back (a normal "Start Streaming" in OBS) — the
  backup video stops, live video returns, and viewers see a smooth cut
  with no visible pause (not instantly the moment OBS reconnects, but
  as soon as OBS actually sends a fresh keyframe — usually a couple of
  seconds);
- or `disconnect_timeout_sec` (30 minutes by default) passes with no
  recovery — the Twitch broadcast ends completely. You'll need to
  click "Start Streaming" in OBS again from scratch.

**You consciously click "Stop Streaming".** The broadcast ends
immediately and cleanly — no backup video, no 30-minute wait. This is
exactly what `obs_graceful_stop.py` is for: a plain connection drop
looks identical to the server whether it's a deliberate "Stop" or a
lost connection — there's no way to tell them apart at the network
level. The script warns the server *in advance*, before OBS actually
disconnects, that this is a deliberate stop, not a failure. Without
the script installed, every "Stop" click would look like a disconnect
to the server — viewers would see the backup video, and the broadcast
would only end after the full 30-minute timeout.

**You want to change settings (resolution, bitrate) mid-stream.** OBS
won't let you change these fields while actively publishing — you
have to stop the stream (the "Stop" button, as above — no backup video
shown), change the settings, and hit "Start" again. The service just
treats this as a fresh stream start.

If the settings happen to change *during* a disconnect (say, the
computer froze, you used the downtime to fix the bitrate, and only
then restarted OBS) — the service detects this on its own and does a
clean reconnect to Twitch instead of trying to swap the video
seamlessly on the fly (which isn't safe to do mid-connection). In this
specific case, a short pause on recovery is normal and expected.

**The problem is between the VPS and Twitch, not with the streamer**
(the server's own network, or an issue on Twitch's end). This doesn't
depend on anything OBS does — the service detects the drop or the
"connection alive but stalled" case on its own and reconnects to
Twitch automatically, retrying until it succeeds. Viewers might see a
brief "stream offline -> online again" on Twitch itself (that part is
unavoidable — the connection genuinely gets re-established), but
nothing needs to be done on the OBS/streamer side — it recovers by
itself.

**The controller or the server itself got restarted** (an update, a
crash, a VPS reboot) during an active broadcast. State isn't preserved
across controller restarts — after restarting, it comes up as "nothing
is streaming" even if there was a live broadcast right before. Run
`./restreamctl.sh start` (or confirm it came up automatically, if
you've set up autostart) and start streaming from OBS again.
