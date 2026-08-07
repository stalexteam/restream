# restream-controller

Continuous OBS -> multi-platform restreaming: publish once from OBS and relay to one **primary** platform plus any number of extra **restream** platforms (Twitch, YouTube, Kick, …). If your internet connection drops, the stream doesn't end — it switches to a backup video until the connection comes back (or until a timeout is reached).

## What it does

`restream-controller` sits between OBS and your streaming platforms on a VPS you control. OBS publishes to the VPS over RTMP; the VPS relays it to each enabled platform. As long as the RTMP connection between the VPS and a platform stays open, that platform keeps the channel live — even while your own connection to the VPS is down.

- **One primary + live-toggleable restreams.** The primary platform is always configured; extra platforms are a list you can turn on and off **live** from the dashboard's Control tab, without restarting anything and without interrupting the platforms already streaming. Want to send content to Kick but not Twitch for a while? Just uncheck Twitch.
- **Seamless fallback.** If OBS disconnects unexpectedly (bad internet, crashed PC, closed OBS), the stream switches to a backup video instead of ending. Viewers on every enabled platform see a placeholder, not a "stream offline" screen.
- **Seamless recovery.** When OBS reconnects, the service waits for a clean keyframe from the live feed and cuts back to it without ever dropping the platform connections — no visible freeze, no re-buffering.
- **A slow/broken platform never affects the others.** Each platform has its own output pipeline; if one can't keep up or its key is wrong, it retries (or stops) on its own while the rest keep streaming.
- **Aggregate failsafe.** The broadcast is stopped hard (and OBS is told to stop) only if **none** of the enabled platforms can be reached at the start — a wrong key on one platform, while others connect, just drops that one.
- **Graceful stop, not just a timeout.** Consciously clicking "Stop Streaming" in OBS ends the broadcast immediately and cleanly, with no backup video and no waiting around — detected automatically by an invisible OBS Browser Source, no button to click.
- **Automatic timeout.** If the connection doesn't come back within a configurable window (30 minutes by default), the broadcast ends on its own instead of looping the backup video forever.
- **No Docker, no heavy dependencies.** `ffmpeg` for relaying, a single Go binary ([MediaMTX](https://github.com/bluenviron/mediamtx)) for RTMP ingest, and a stdlib-only Python controller.

Everything is relayed with `-c copy` (no re-encoding on the VPS): every platform gets exactly the bitrate/codec OBS produces. This is a focused tool, not a general platform — no transcoding ladder, no per-platform resolution. One OBS output, fanned out to several destinations, kept alive through drops.

## Prerequisites

- A VPS (or a local Linux box/WSL for testing) running Debian or Ubuntu, with SSH access and sudo.
- A public IP on the VPS (so OBS can reach it).
- The RTMP ingest URL + stream key for each platform you want (e.g. Twitch: Creator Dashboard -> Settings -> Stream -> Primary Stream Key; YouTube: Live Control Room -> Stream key; Kick: Stream settings). You need at least the primary; restreams are optional and can be added later.
- OBS Studio on the machine you stream from.
- A backup video file to loop while the connection is down. Any format `ffmpeg` can read works — you don't need to match your OBS codec/resolution/fps, it's adjusted automatically (see step 3).

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/stalexteam/restream.git restream
cd restream
```

### 2. Install dependencies

```bash
bash install.sh
```

This installs `ffmpeg`, `python3`, and the MediaMTX binary, and generates two config files with random passwords: `mediamtx.yml` and `controller/config.json`. It **asks for this server's public IP/hostname** (used to build the OBS Server URL and dashboard/obs-source links) -- this part re-runs every time you run `install.sh` again, since it's not a secret and might change (e.g. moving to a different VPS); leave it empty to fill in later, either by re-running `install.sh` or editing `public_host` in `controller/config.json` directly. **At the end it prints, highlighted:**

- the RTMP login/password for OBS,
- the URL (with an access token baked in) for the status dashboard,
- the URL (same token) for the OBS browser-source stop control.

**You'll need these in steps 6 and 7.** If you forget them, you can look them up again any time:

```bash
./restreamctl.sh credentials
```

### 3. Add a backup video

**Copy your file to `backup/backup.mp4`.**

Any format `ffmpeg` understands works (mp4, mkv, mov, avi...) — you don't need to manually match codec, resolution, fps, or audio channel count to your OBS stream. On every stream start, the service compares the backup file's parameters to the live OBS stream and, if they differ, transcodes the backup into a separate prepared copy in the background (this doesn't interrupt the live broadcast). It takes a few seconds to a couple of minutes after OBS first connects — well before the backup would actually be needed on a disconnect.

`backup/backup.mp4` is just the default path. To use a different filename or location (e.g. `backup/idle.mkv`), set the **`backup_file`** field to its path — on the dashboard's Settings tab (step 6) or in `controller/config.json` directly. A relative path is resolved from the project root.

**For best results**, use a file encoded with the same settings as your stream. **The easiest way to get one: in OBS, click "Start Recording"** (with the same Settings -> Output you use for streaming) and record a few minutes — a recording like that matches the live stream exactly, so no automatic transcoding is needed and the backup is ready immediately.

### 4. Check readiness

```bash
./restreamctl.sh check
```

Prints `[OK]`/`[WARNING]`/`[ERROR]` for each item (config files, the backup video and its codecs). `primary_url` still being the placeholder value only prints a `[WARNING]` at this point -- it does not block starting the service, because you set it from the dashboard in step 6, after the service is already running. **Fix anything marked `[ERROR]` before continuing.**

### 5. Start

```bash
./restreamctl.sh start
```

Starts MediaMTX and the controller, verifies both came up, and prints the current state.

### 6. Configure platforms and settings

**Open the dashboard URL** from `install.sh`'s output (or `./restreamctl.sh credentials`) in any browser:

```
http://YOUR_VPS_IP:8790/dashboard?token=YOUR_TOKEN
```

The dashboard has three tabs:

- **Status** — live broadcast state, the OBS input (resolution / codec / measured bitrate, also in the OBS indicator's tooltip), and component health (CPU/mem).
- **Control** — one checkbox per platform (primary + each restream) to turn it on/off **live**, with each platform's status, uptime, health (whether it's keeping up with the bitrate), and ping.
- **Settings** — the list of platforms (add/edit/remove) plus system settings.

On the **Settings** tab you'll find a **Platforms** list: primary first (it can't be removed), then your restreams, and an **Add platform** button. Each row shows the platform name and a masked URL — only the domain is visible (e.g. `••••••twitch.tv••••••`), enough to tell platforms apart while the key stays hidden; click **Show** to reveal the full URL — plus **Modify** and **Delete**. Add / Modify / Delete apply **immediately** — there's no Apply for platforms (Delete asks for confirmation first).

**Add** and **Modify** open a small dialog with three fields — **Name**, **Server URL**, **Stream key** — that you fill in exactly as the platform gives them to you (the same two boxes as OBS's "Server" + "Stream Key"). You don't stitch them together: the controller builds the final push URL itself and shows a live preview in the dialog. Both `rtmp://` and `rtmps://` (TLS) work. Examples:

- **Twitch** — Server `rtmp://live.twitch.tv/app`, Stream key = your Primary Stream Key.
- **YouTube** — Server `rtmp://a.rtmp.youtube.com/live2`, Stream key = your key.
- **Kick** (AWS IVS, RTMPS) — paste the two values from Kick's stream settings **as shown**: Server `rtmps://<id>.global-contribute.live-video.net/`, Stream key `sk_...`. The controller adds the required `:443/app` for you (that's specific to Kick/IVS).

New restreams start **disabled** — enable them on the Control tab when ready. If a server URL or key is slightly wrong, that platform just shows **Failed** on the Control tab (it doesn't affect the others), so it's safe to try.

Below the platforms list is a **System settings** block, applied together with an **Apply** button:

- `backup_file` — pre-filled from step 3, change it only if you placed the file somewhere else.
- optionally `offline_timeout_sec` — seconds to wait for the connection to come back before ending the broadcast entirely (default 1800 = 30 minutes, minimum 60).
- **Connect timeout (ms)** / **Read timeout (ms)** (advanced) — two-phase silent-drop detection. Connect timeout (default 5000, minimum 2500) is how long to wait for OBS's first frame after it connects; too low and the RTMP handshake itself starts failing -- a real OBS client needs noticeably more of this than you'd expect (encoder warm-up plus the wait for a first keyframe). Keep your OBS Keyframe Interval (step 7) below this value. Read timeout (default 500, minimum 300) is how fast the service reacts to a stalled connection once video is already flowing.
- **Use ICMP ping** (checkbox, default **off**) — how the Control tab's Ping column is measured. Off (default) = the time to open a TCP connection to the platform's RTMP/RTMPS port, which always works without special privileges. On = a real ICMP ping (average RTT via the system `ping`), closer to a classic ping — but it needs the `ping` binary to be allowed for a normal user (grant it with `sudo setcap cap_net_raw+ep "$(command -v ping)"` if needed) and can be blocked by a firewall; if a ping fails, that platform's Ping shows a dash. Leave it off unless you specifically want ICMP RTT.

Backup path, offline timeout and the ICMP-ping toggle apply without interrupting anything. The **only** change that restarts MediaMTX and ends the current broadcast is the connect/read timeout (MediaMTX reads its own copy of that value from a separate file that's only regenerated on restart) — **Apply** asks for confirmation first if a broadcast is live.

You don't have to use the dashboard for this -- editing `controller/config.json` directly and running `./restreamctl.sh restart` works too, and is a reasonable fallback if the VPS isn't reachable from a browser yet. Each platform is stored as `server` + `key` (primary as `primary_server`/`primary_key`; restreams as a list of `{name, server, key, enabled}`), and the controller joins + normalizes them the same way the dashboard does.

The `output_video_bitrate_kbps`/`output_audio_bitrate_kbps` fields no longer set output parameters (those pass through untouched with `-c copy`); they only affect the quality of the one-time backup-video transcode from step 3. They aren't exposed in the dashboard.

### 6b. Turn platforms on and off (Control tab)

Once platforms are defined, the **Control** tab is your live switchboard:

- **Check** a platform to stream to it. If OBS is live, it starts within a couple of seconds (it waits for the next keyframe); if you're offline, it starts on the next broadcast.
- **Uncheck** a platform to stop streaming to it immediately (the other platforms are unaffected). Re-check it to bring it back.

Example: primary = Twitch, restream = Kick. To stream some content only to Kick, uncheck Twitch — Twitch stops, Kick keeps going. Re-check Twitch later to resume it. Toggling primary works the same as any other platform.

### 7. Configure OBS

1. Settings -> Stream -> Service: **"Custom..."** (not a linked account — OBS won't let you override the server if it's linked).
2. Server: `rtmp://YOUR_VPS_IP:1935/live`
3. Stream Key: `main?user=obs&pass=PASSWORD` (password from step 2, `install.sh`'s output; this exact format — `user=...&pass=...` inside the stream key — is required, not `rtmp://user:pass@host`, because that's how MediaMTX expects RTMP auth).
4. Docks -> Custom Browser Docks -> add a dock for the dashboard. Shows live status, the Control tab, and a Settings tab -- this is monitoring/config only, it doesn't need or use anything from OBS itself. Two ways to point the dock at it:
   - **Recommended:** `install.sh` generates `obs-dock.html` in the project root. Copy it to the OBS machine and set the dock URL to that local file -- OBS takes a plain Windows path (`C:\obs-dock.html`). It holds the dashboard in an iframe and, when the server is down (VPS rebooting, controller restarting), shows a "retrying…" screen and reconnects on its own -- instead of OBS's bare "Couldn't load that page". (The dashboard URL with your token is embedded in the file; it's hidden behind a "Show dashboard address" button.)
   - Or point the dock straight at the dashboard URL. Simpler, but no retry screen.
5. Add a **Browser Source** (not a dock) to any scene. `install.sh` generates `obs-source.html` in the project root -- copy it to the OBS machine and point the Browser Source at that local file -- OBS takes a plain Windows path (`C:\obs-source.html`). Set its **Width and Height to 32 x 32**. Required for correctly detecting Start/Stop Streaming clicks (see "Everyday scenarios" below).

Why a local file and a Browser Source, specifically:
   - It has to be a **Browser Source**, not a dock: OBS's `window.obsstudio` API has long-standing bugs in Custom Browser Docks (reported since 2021), but works reliably in a Browser Source.
   - A **local file** (rather than the server URL) keeps the page self-contained: it connects to the controller's WebSocket directly (the address is baked into the file by `install.sh`).

It's almost entirely invisible: while connected it renders nothing (fully transparent in OBS). Connecting shows a small yellow spinner in the top-left 32x32; can't-reach-server shows a red dot. Add it to any **one** scene, hidden or not -- a Browser Source keeps running (and stays connected) even while its scene isn't active, as long as **"Shutdown source when not visible"** stays **unchecked** (the default).

Set its **Page permission** to **"Full access to OBS"** (recommended) so it can also stop the stream in OBS right away if none of the enabled platforms turn out to be reachable at the start of the broadcast (e.g. all wrong keys) -- without that permission level, the service still stops on its own end, but OBS keeps publishing into the void until you stop it yourself.
6. Settings -> Output (Advanced mode) -> Streaming -> **Keyframe Interval: 2** (instead of "Auto"). This affects how long it takes for live video to appear after a start/recovery: the service waits for the first keyframe from OBS before showing anything (to avoid a corrupted-looking picture at the cut) — a longer default interval ("Auto" is often more than 2s) means a longer visible pause. Keep it below the **Connect timeout** (step 6, 5s by default): MediaMTX drops the connection if OBS's first keyframe doesn't arrive within that window, so a keyframe interval at or above the Connect timeout can make OBS fail to connect at all.
7. Settings -> Output -> **Rate Control: CBR** (not "Lossless"). "Lossless" is meant for local recording, not streaming — it produces an unpredictably high and variable bitrate that platforms ingest with visible corruption (confirmed: the decoder reports the wrong codec profile, and the player's `Download Bitrate` drops by a large factor). Set an explicit bitrate instead (e.g. 6000 Kbps — Twitch's typical cap for non-partners; check each platform's limits).

   > Note: this specific problem shows up right when you switch the
   > Service from a built-in one (with a linked account) to "Custom..."
   > (required here, since we need our own RTMP server and auth). It looks
   > like OBS itself handles/limits encoder settings differently
   > depending on which server is selected. The root cause is in OBS, not
   > in this controller — but since "Custom" is unavoidable here, you have
   > to set safe encoder settings manually (see above).

### 8. Check status

```bash
./restreamctl.sh status
```

Shows whether the processes are running, and the current broadcast state. The CLI prints the raw internal state (`OFFLINE`, `LIVE`, `FALLBACK`). The dashboard's header badge maps these to clearer labels — it shows **what's going out**:

- **OFFLINE** — OBS isn't publishing; nothing anywhere.
- **ON AIR** (green) — OBS is publishing and it's going out to at least one enabled platform.
- **IDLE** (blue) — OBS is publishing, but every platform is unchecked, so nothing is actually being sent out.
- **BACKUP** (orange) — OBS dropped; the backup video is playing on the enabled platforms while it waits for OBS to return.
- **FAILURE** (red) — the last broadcast was stopped because none of the enabled platforms could be reached (e.g. wrong keys). Clears the moment you start a new broadcast.
- **HALTED** (red) — you stopped the broadcast with the HALT button (below) and the service is refusing to auto-restart it, even if OBS keeps reconnecting, until OBS is actually stopped and started again.

(The header also has three small indicators: **WS** = the dashboard's own link to the controller; **SRC** = whether the OBS browser-source, `obs-source.html`, is connected — green only when you've added it in OBS (step 7, item 5); **OBS** = whether OBS video data is actually arriving into the VPS.)

At the far right of the header there's a red **HALT** button, active only while a broadcast is running (ON AIR / BACKUP). It stops the broadcast on every platform immediately and tells OBS to stop streaming (asks for confirmation first). This is the "kill switch" for when you've lost your streaming machine — e.g. your PC froze and the backup video is looping: open the dashboard from your phone and hit HALT to end everything.

HALT **sticks**: if your OBS is still out there trying to reconnect, the service won't quietly bring the broadcast back when it does — the halted session stays down (badge shows **HALTED**) until OBS is genuinely stopped and started again. The browser-source tags each OBS streaming session with an id, so the service can tell "the same halted session reconnecting" (keep it down, and tell OBS to stop the moment the browser-source is reachable) apart from "a fresh Start" (allowed normally). Two caveats: telling OBS itself to stop needs the browser-source's Page permission set to "Full access to OBS" (step 7, item 5) — without it, OBS keeps publishing into the void, but nothing goes out to platforms either way; and if the browser-source was never present, HALT is a one-shot stop (no session id to latch onto), so a reconnecting OBS could restart the broadcast.

### Day-to-day management

```bash
./restreamctl.sh stop          # stop everything
./restreamctl.sh restart       # restart
./restreamctl.sh logs          # recent log lines + paths to the ffmpeg logs
./restreamctl.sh credentials   # print the OBS password and dashboard URL again
```

### Troubleshooting

1. `./restreamctl.sh check` — did the backup video disappear? Is `primary_url` actually set?
2. `./restreamctl.sh logs` — the controller's own log.
3. `controller/ffmpeg-relay.log`, `controller/ffmpeg-backup.log`, and one per platform, `controller/ffmpeg-out-<name>.log` — logs of the individual ffmpeg processes, if the problem looks like a video/audio issue on a specific platform rather than a switching-logic issue.
4. A platform shows **behind (N drops)** on the Control tab — that platform's upload can't keep up with the source bitrate; the pipeline drops frames and resyncs on the next keyframe. Check its ping and the VPS's upstream bandwidth to that platform.
5. If the backup video looks "wrong" (not the file you expected) — check `backup/backup.prepared.mp4`. This is the auto-prepared copy from step 3; delete it along with `backup/backup.prepared.meta.json` to have the service rebuild it on the next stream start.
6. Corrupted/"shattered"-looking picture, or a strange bitrate/codec in the player's stream stats — check OBS Settings -> Output -> **Rate Control**. If it's "Lossless", that's the cause (see step 7) — switch to CBR with an explicit bitrate.
7. A brief flash of the backup video right when you click "Stop Streaming" in OBS — known, harmless timing edge. Normally the obs-source Browser Source signals the deliberate stop *before* OBS drops its RTMP connection, so the backup never shows. First thing to check: the obs-source Browser Source is actually added to a scene (step 7, item 5), otherwise every "Stop" looks like an ordinary disconnect (backup + timeout).

## Everyday scenarios

**Everything is working normally.** OBS is publishing, viewers on every enabled platform see the live feed. Nothing special happens.

**You want to stream to only some platforms.** Use the Control tab: uncheck the platforms you want to skip (their streams end immediately), leave the rest checked. Re-check them later to bring them back live. This works for the primary too.

**Internet dropped / PC crashed / OBS closed unexpectedly.** The platforms don't notice the drop — the backup video takes over on all enabled platforms instead of the live feed, and stays on until either:
- the connection comes back (a normal "Start Streaming" in OBS) — the backup stops, live video returns, viewers see a smooth cut with no visible pause (as soon as OBS sends a fresh keyframe — usually a couple of seconds);
- or `offline_timeout_sec` (30 minutes by default) passes with no recovery — the broadcast ends completely and you'll need to click "Start Streaming" in OBS again.

**No platform was reachable this broadcast** (typically a wrong key on the only enabled platform, or wrong keys on all of them). Looping the backup while retrying connections that never once succeeded wouldn't accomplish anything, so the service gives up on the first failed attempt: it skips the backup and the 30-minute wait, ends the broadcast, and (if the obs-source Browser Source has "Full access to OBS", step 7, item 5) tells OBS itself to stop too. You'll see an error toast, and the broadcast indicator turns into a red **FAILURE** badge. Note the "**no** platform" — if at least one enabled platform connects, the broadcast proceeds and only the failed one drops (with a warning toast).

**One platform's key is wrong but others work.** That platform stops on its own (retrying an invalid key achieves nothing) with a warning toast naming it; every other platform keeps streaming, no FAILURE.

**A platform's connection drops after already working.** Different from a bad key: if a platform was working and *then* starts failing (a real network blip), the service keeps retrying it indefinitely, with the error toasts rate-limited so they don't spam you.

**You consciously click "Stop Streaming" in OBS.** A plain connection drop looks identical to the server whether it's a deliberate "Stop" or a lost connection. The obs-source Browser Source (step 7, item 5) watches OBS's own streaming status and signals the server — before OBS's RTMP connection drops — that this is a deliberate stop. The broadcast then ends immediately, with no backup video and no timeout. Without that Browser Source added, every "Stop" looks like an ordinary disconnect: backup video, then the timeout.

**You want to change settings (resolution, bitrate) mid-stream.** OBS won't let you change these fields while actively publishing — stop the stream (the "Stop" button, as above — no backup video shown), change the settings, and hit "Start" again. The service treats it as a fresh start.

If the settings change *during* a disconnect (the PC froze, you fixed the bitrate, then restarted OBS) — the service detects it and does a clean reconnect to every platform instead of an unsafe mid-connection swap. A short pause on recovery is normal in this case.

**You want to change a platform's URL/key, add a restream, or change the timeout/backup while already live.** All of these apply live from the Settings tab: editing a URL reconnects only that platform, adding a restream just adds it (enable it on Control when ready), and timeout/backup changes don't interrupt anything. Only the connect/read timeouts require a MediaMTX restart (which ends the broadcast — you'll be asked to confirm).

**The problem is between the VPS and a platform, not the streamer** (the server's own network, or an issue on the platform's end). The service detects the drop (or the "connection alive but stalled" case) and reconnects to that platform automatically, retrying until it succeeds. Viewers on that platform might see a brief "offline -> online again" (unavoidable — the connection genuinely re-establishes), but nothing needs to be done on the OBS side.

**The controller or the server itself got restarted** (an update, a crash, a VPS reboot) during an active broadcast. State isn't preserved across controller restarts — after restarting it comes up as "nothing is streaming". Run `./restreamctl.sh start` (or confirm autostart) and start streaming from OBS again.
