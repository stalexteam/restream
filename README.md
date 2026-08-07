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
  no backup video and no waiting around — detected automatically by
  an invisible OBS Browser Source, no button to click. See "Everyday
  scenarios" below.
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
- A backup video file to loop on Twitch while the connection is down.
  Any format `ffmpeg` can read works — you don't need to match your OBS
  codec/resolution/fps, it's adjusted automatically (see step 3).

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
`controller/config.json`. It asks for this server's public
IP/hostname (used to build the OBS Server URL and dashboard/obs-source
links) -- this part re-runs every time you run `install.sh` again,
since it's not a secret and might change (e.g. moving to a different
VPS); leave it empty to fill in later, either by re-running
`install.sh` or editing `public_host` in `controller/config.json`
directly. At the end it prints, highlighted:

- the RTMP login/password for OBS,
- the URL (with an access token baked in) for the status dashboard,
- the URL (same token) for the OBS browser-source stop control.

You'll need these in steps 6 and 7. If you forget them, you can look
them up again any time:

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

### 4. Check readiness

```bash
./restreamctl.sh check
```

Prints `[OK]`/`[WARNING]`/`[ERROR]` for each item (config files, the
backup video and its codecs). `twitch_url` still being the
placeholder value only prints a `[WARNING]` at this point -- it does
not block starting the service, because it's set from the dashboard in
step 6, after the service is already running. Fix anything marked
`[ERROR]` before continuing.

### 5. Start

```bash
./restreamctl.sh start
```

Starts MediaMTX and the controller, verifies both came up, and prints
the current state.

### 6. Set your Twitch key and other settings

Open the dashboard URL from `install.sh`'s output (or
`./restreamctl.sh credentials`) in any browser:

```
http://YOUR_VPS_IP:8790/dashboard?token=YOUR_TOKEN
```

Switch to the **Settings** tab and fill in:

- `twitch_url` — your RTMP URL with your real Twitch stream key,
  format: `rtmp://live.twitch.tv/app/YOUR_STREAM_KEY`. The field is
  masked by default (it's a secret) -- click "Show" to check it before
  saving.
- optionally `offline_timeout_sec` — how many seconds to wait for
  the connection to come back before ending the Twitch broadcast
  entirely (default 1800 = 30 minutes, minimum 60).
- `backup_file` — pre-filled from step 3, change it only if you placed
  the file somewhere else.
- **Connect timeout (ms)** / **Read timeout (ms)** (advanced, both
  optional) — two-phase silent-drop detection. Connect timeout (default
  5000, minimum 2500) is how long to wait for OBS's first frame after
  it connects; too low and the RTMP handshake itself starts failing --
  a real OBS client needs noticeably more of this than you'd expect
  (encoder warm-up plus the wait for a first keyframe). Keep your OBS
  Keyframe Interval (step 7) below this value, or that first keyframe
  may never arrive within the window.
  Read timeout (default 500, minimum 300) is how fast the service
  reacts to a stalled connection once video is already flowing,
  without dropping the OBS connection itself. Changing either of these
  two also restarts MediaMTX (not just the controller) when applied,
  since MediaMTX reads its own copy of this value from a separate
  config file that only gets regenerated on a restart.

Click **Apply & Restart** to save and restart the controller (and, for
the two timeout fields above, MediaMTX too) with the new values
immediately (this is safe here -- nothing is streaming yet). Plain
**Apply** saves without restarting, for changing settings later
without interrupting an active broadcast (see "You want to change the
Twitch key, timeout, or backup video while already live" below).

You don't have to use the dashboard for this -- editing
`controller/config.json` directly and running
`./restreamctl.sh restart` works exactly the same way, and is a
reasonable fallback if the VPS isn't reachable from a browser yet.

You can leave the `output_*` fields alone — they no longer set the
output parameters directly (those are read from the live OBS stream
automatically); `output_video_bitrate_kbps`/`output_audio_bitrate_kbps`
only affect the quality of the one-time backup-video transcode from
step 3. These aren't exposed in the dashboard; edit `config.json`
directly if you ever need them.

### 7. Configure OBS

1. Settings -> Stream -> Service: **"Custom..."** (not a linked Twitch
   account — OBS won't let you override the server if it's linked).
2. Server: `rtmp://YOUR_VPS_IP:1935/live`
3. Stream Key: `main?user=obs&pass=PASSWORD` (password from step 2,
   `install.sh`'s output; this exact format — `user=...&pass=...`
   inside the stream key — is required, not `rtmp://user:pass@host`,
   because that's how MediaMTX expects RTMP auth).
4. Docks -> Custom Browser Docks -> add a dock pointing at the
   dashboard URL (`install.sh`'s output, or `./restreamctl.sh
   credentials`). Shows live status (broadcast state, components,
   CPU/mem) right inside OBS, and has a Settings tab for
   `twitch_url`/timeouts/backup path -- this is monitoring/config
   only, it doesn't need or use anything from OBS itself.
5. Add a **Browser Source** (not a dock) to any scene, pointing at the
   obs-source URL (`install.sh`'s output, or `./restreamctl.sh
   credentials`). Required for correctly detecting Start/Stop
   Streaming clicks (see "Everyday scenarios" below) -- it has to be a
   Browser Source specifically, not a dock: OBS's `window.obsstudio`
   API has long-standing bugs in Custom Browser Docks (reported since
   2021), but works reliably in a Browser Source. It renders nothing
   and doesn't need to be visible -- add it to any scene, hidden or
   not. Set its **Page permission** to **"Full access to OBS"**
   (recommended) so it can also stop the stream in OBS right away if
   Twitch turns out to be unreachable at the start of the broadcast
   (e.g. a wrong stream key) -- without that permission level, the
   service still stops on its own end, but OBS keeps publishing into
   the void until you stop it yourself.
6. Settings -> Output (Advanced mode) -> Streaming -> **Keyframe
   Interval: 2** (instead of "Auto"). This affects how long it takes
   for live video to appear after a start/recovery: the service waits
   for the first keyframe from OBS before showing anything (to avoid
   a corrupted-looking picture at the cut) — a longer default interval
   ("Auto" is often more than 2s) means a longer visible pause. The
   same interval (2s) is already used for the backup video, and it's
   Twitch's own recommendation anyway. Either way, keep it below the
   **Connect timeout** (step 6, 5s by default): MediaMTX drops the
   connection if OBS's first keyframe doesn't arrive within that
   window, so a keyframe interval at or above the Connect timeout can
   make OBS fail to connect at all — not just pause longer.
7. Settings -> Output -> **Rate Control: CBR** (not "Lossless").
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
   > have to set safe encoder settings manually (see above).

### 8. Check status

```bash
./restreamctl.sh status
```

Shows whether the processes are running, and the current broadcast
state: `OFFLINE` (nothing is streaming), `LIVE` (live video is going
out), `FALLBACK` (backup video is playing, waiting for OBS to come
back). The dashboard shows a fourth, purely visual state, **Halt**
(red) -- same underlying `OFFLINE`, but flagged because the last
broadcast ended due to an error (e.g. an unreachable Twitch URL/key)
rather than a clean stop or timeout; it clears the moment you start a
new broadcast.

### Day-to-day management

```bash
./restreamctl.sh stop          # stop everything
./restreamctl.sh restart       # restart
./restreamctl.sh logs          # recent log lines + paths to the ffmpeg logs
./restreamctl.sh credentials   # print the OBS password and dashboard URL again
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
   cause (see step 7 of "Configure OBS" above) — switch to CBR with an
   explicit bitrate.
6. A brief flash of the backup video right when you click "Stop
   Streaming" in OBS — known, harmless timing edge. Normally the
   obs-source Browser Source signals the deliberate stop *before* OBS
   drops its RTMP connection (see "You consciously click Stop
   Streaming" below), so the backup never shows — this is the usual
   case. If the signal ever lands just after the drop, the backup
   appears for a moment before the broadcast ends. First thing to
   check: the obs-source Browser Source is actually added to a scene
   (step 7, item 5), otherwise there's no stop signal at all and every
   "Stop" looks like an ordinary disconnect (backup + timeout).

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
- or `offline_timeout_sec` (30 minutes by default) passes with no
  recovery — the Twitch broadcast ends completely. You'll need to
  click "Start Streaming" in OBS again from scratch.

**Twitch was never reached this broadcast** (typically a wrong stream
key). Looping the backup video while retrying a connection that has
never once succeeded wouldn't accomplish anything, so the service
gives up after the very first failed connection attempt: it skips the
backup video and the 30-minute wait entirely, ends the broadcast on
its own end, and (if the obs-source Browser Source has "Full access to
OBS" permission, step 7, item 5 above) tells OBS itself to stop streaming
too -- otherwise OBS would keep publishing into the void until you
stop it manually. You'll see it as an error toast on the dashboard,
and the broadcast indicator turns into a red **Halt** badge instead of
the usual grey Offline, so a failed start doesn't look the same as a
clean stop.

**Twitch connection drops after already working.** Different from the
above: if the connection to Twitch was working and *then* starts
failing (a real network blip, not a bad key), the service doesn't give
up — it keeps retrying indefinitely, same as before, just with the
error toasts rate-limited so they don't spam you on every retry.

**You consciously click "Stop Streaming" in OBS.** A plain connection
drop looks identical to the server whether it's a deliberate "Stop" or
a lost connection — there's no way to tell them apart at the network
level. This is exactly what the obs-source Browser Source (step 7,
item 5 above) is for: it watches OBS's own streaming status (primarily via
OBS's `obsStreamingStopping`/`obsStreamingStarted` events, for the
fastest possible signal, with `window.obsstudio.getStatus()` polling
as a backup in case an event doesn't fire) and signals the server —
before OBS's RTMP connection to the VPS drops — that this is a
deliberate stop, not a failure. The broadcast then ends immediately,
with no backup video and no timeout. (In rare cases you might catch a
brief flash of the backup video first — see Troubleshooting.) Without that Browser Source
added, every "Stop" click looks like an ordinary disconnect: backup
video, then the timeout.

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

**You want to change the Twitch key, timeout, or backup video while
already live.** The dashboard's Settings tab writes straight to
`controller/config.json` either way, but only takes effect once the
controller process actually restarts -- and restarting ends the
current broadcast (same as `restreamctl.sh restart`). Use plain
**Apply** to save the new value now without touching the live stream,
then hit **Apply & Restart** (or restart from the dashboard) once
you're ready to end the broadcast anyway. **Apply & Restart** used
while already live asks for confirmation first, since it ends the
stream immediately.

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
