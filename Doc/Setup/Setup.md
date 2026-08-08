# Setup & operation

Install, configure, and run the controller end to end: prerequisites, installation, configuring platforms and OBS, multiple pipelines, and day-to-day management. For the project overview see the [README](../../README.md); for troubleshooting see [Troubleshooting](../Troubleshooting/Troubleshooting.md); for sending a different audio mix to one platform see [Remux](../Remux/Remux.md).

## Prerequisites

- A VPS (or a local Linux box/WSL for testing) running Debian or Ubuntu, with SSH access and sudo.
- A public IP on the VPS (so OBS can reach it).
- The RTMP ingest URL + stream key for each platform you want (e.g. Twitch: Creator Dashboard -> Settings -> Stream -> Primary Stream Key; YouTube: Live Control Room -> Stream key; Kick: Stream settings). You need at least the primary; restreams are optional and can be added later.
- OBS Studio on the machine you stream from.
- A backup video file to loop while the connection is down. Any format `ffmpeg` can read works — you don't need to match your OBS codec/resolution/fps, it's adjusted automatically (see step 3).


## 1. Clone the repository

```bash
git clone https://github.com/stalexteam/restream.git restream
cd restream
```

## 2. Install dependencies

```bash
bash install.sh
```

This installs `ffmpeg`, `python3`, and the MediaMTX binary, and generates `data/config.json` with random passwords (the single config you edit — it holds the RTMP passwords, dashboard token, pipelines, and platforms). MediaMTX's own `data/mediamtx.yml` is **not** something you edit: it's rendered from `data/config.json` + `controller/mediamtx.yml.template` automatically before every start. It **asks for this server's public IP/hostname** (used to build the OBS Server URL and dashboard/obs-source links) -- this part re-runs every time you run `install.sh` again, since it's not a secret and might change (e.g. moving to a different VPS); leave it empty to fill in later, either by re-running `install.sh` or editing `public_host` in `data/config.json` directly. **At the end it prints, highlighted:**

- the RTMP login/password for OBS,
- the URL (with an access token baked in) for the status dashboard,
- the URL (same token) for the OBS browser-source stop control.

**You'll need these in steps 6 and 7.** If you forget them, you can look them up again any time:

```bash
./restreamctl.sh credentials
```

## 3. Add a backup video

**Copy your file to `backup/backup.mp4`.**

Any format `ffmpeg` understands works (mp4, mkv, mov, avi...) — you don't need to manually match codec, resolution, fps, or audio channel count to your OBS stream. On every stream start, the service compares the backup file's parameters to the live OBS stream and, if they differ, transcodes the backup into a separate prepared copy in the background (this doesn't interrupt the live broadcast). It takes a few seconds to a couple of minutes after OBS first connects — well before the backup would actually be needed on a disconnect.

`backup/backup.mp4` is just the default path. To use a different filename or location (e.g. `backup/idle.mkv`), set the **`backup_file`** field to its path — on the dashboard's Settings tab (step 6) or in `data/config.json` directly. A relative path is resolved from the project root.

**For best results**, use a file encoded with the same settings as your stream. **The easiest way to get one: in OBS, click "Start Recording"** (with the same Settings -> Output you use for streaming) and record a few minutes — a recording like that matches the live stream exactly, so no automatic transcoding is needed and the backup is ready immediately.

## 4. Check readiness

```bash
./restreamctl.sh check
```

Prints `[OK]`/`[WARNING]`/`[ERROR]` for each item (config files, the default pipeline's backup video and its codecs). The primary platform still being the placeholder (`CHANGE_ME_STREAM_KEY`) only prints a `[WARNING]` at this point -- it does not block starting the service, because you set it from the dashboard in step 6, after the service is already running. **Fix anything marked `[ERROR]` before continuing.**

## 5. Start

```bash
./restreamctl.sh start
```

Starts MediaMTX and the controller, verifies both came up, and prints the current state.

## 6. Configure platforms and settings

**Open the dashboard URL** from `install.sh`'s output (or `./restreamctl.sh credentials`) in any browser:

```
http://YOUR_VPS_IP:8790/dashboard?token=YOUR_TOKEN
```

The dashboard has three tabs (each shows one block per pipeline; with the default single-pipeline setup that's just one block):

- **Status** — per pipeline: its broadcast badge and OBS input (resolution / codec / measured bitrate). Plus global component health (CPU/mem) for every process. The header indicators reflect the default pipeline (the main broadcast).
- **Control** — per pipeline: an enable toggle (except the default, which can't be disabled) and one checkbox per platform (primary + each restream) to turn it on/off **live**, with each platform's status, uptime, health (whether it's keeping up with the bitrate), and ping.
- **Settings** — per pipeline: its platforms (add/edit/remove) and a Modify/Delete for the pipeline itself; plus **+ Add pipeline** and a global system-settings block.

On the **Settings** tab you'll find a **Platforms** list: primary first (it can't be removed), then your restreams, and an **Add platform** button. Each row shows the platform name and a masked URL — only the domain is visible (e.g. `••••••twitch.tv••••••`), enough to tell platforms apart while the key stays hidden; click **Show** to reveal the full URL — plus **Modify** and **Delete**. Add / Modify / Delete apply **immediately** — there's no Apply for platforms (Delete asks for confirmation first).

**Add** and **Modify** open a small dialog with three fields — **Name**, **Server URL**, **Stream key** — that you fill in exactly as the platform gives them to you (the same two boxes as OBS's "Server" + "Stream Key"). You don't stitch them together: the controller builds the final push URL itself and shows a live preview in the dialog. Both `rtmp://` and `rtmps://` (TLS) work. Examples:

- **Twitch** — Server `rtmp://live.twitch.tv/app`, Stream key = your Primary Stream Key.
- **YouTube** — Server `rtmp://a.rtmp.youtube.com/live2`, Stream key = your key.
- **Kick** (AWS IVS, RTMPS) — paste the two values from Kick's stream settings **as shown**: Server `rtmps://<id>.global-contribute.live-video.net/`, Stream key `sk_...`. The controller adds the required `:443/app` for you (that's specific to Kick/IVS).

New restreams start **disabled** — enable them on the Control tab when ready. If a server URL or key is slightly wrong, that platform just shows **Failed** on the Control tab (it doesn't affect the others), so it's safe to try.

A pipeline's own setting — its **backup video path** — lives in that pipeline's **Modify** dialog (on the Settings tab, in the pipeline's own block), not here. For the default single-pipeline setup that's the one block at the top. See [Multiple pipelines](#multiple-pipelines-different-feeds-to-different-platforms).

Below the pipelines is a **System settings (global)** block, applied together with an **Apply** button — these apply to the whole server:

- **Offline timeout (seconds)** — how long to wait for OBS to reconnect before ending the broadcast entirely (default 1800 = 30 minutes, minimum 60). It's **global**: everything comes from one OBS, so this is one shared window. Only the main (default) pipeline runs the timer; when it expires, **all** pipelines stop together. Applies live, no restart.
- **Connect timeout (ms)** / **Read timeout (ms)** (advanced) — two-phase silent-drop detection. Connect timeout (default 5000, minimum 2500) is how long to wait for OBS's first frame after it connects; too low and the RTMP handshake itself starts failing -- a real OBS client needs noticeably more of this than you'd expect (encoder warm-up plus the wait for a first keyframe). Keep your OBS Keyframe Interval (step 7) below this value. Read timeout (default 500, minimum 300) is how fast the service reacts to a stalled connection once video is already flowing. These are **global**: MediaMTX has one `readTimeout` shared by every ingest path.
- **Use ICMP ping** (checkbox, default **off**) — how the Control tab's Ping column is measured. Off (default) = the time to open a TCP connection to the platform's RTMP/RTMPS port, which always works without special privileges. On = a real ICMP ping (average RTT via the system `ping`), closer to a classic ping — but it needs the `ping` binary to be allowed for a normal user (grant it with `sudo setcap cap_net_raw+ep "$(command -v ping)"` if needed) and can be blocked by a firewall; if a ping fails, that platform's Ping shows a dash. Leave it off unless you specifically want ICMP RTT.

The offline-timeout and ICMP-ping toggles apply without interrupting anything. The **only** change that restarts MediaMTX and ends the current broadcast is the connect/read timeout (MediaMTX reads its own copy of that value from a separate file that's only regenerated on restart) — **Apply** asks for confirmation first if a broadcast is live.

You don't have to use the dashboard for this -- editing `data/config.json` directly and running `./restreamctl.sh restart` works too, and is a reasonable fallback if the VPS isn't reachable from a browser yet. `offline_timeout_sec` and the timeouts are top-level (global). Pipelines are stored under a `pipelines` list; each carries its own `live_path`, `backup_file`, and platforms (primary as `primary_server`/`primary_key`, restreams as a list of `{name, server, key, enabled}`). The controller joins + normalizes URLs the same way the dashboard does. (An older flat config from before pipelines existed is read as a single default pipeline and migrated on the first save.)

The backup video's parameters are figured out automatically: on every broadcast start the controller reads the live stream's resolution/fps/channels and its **measured bitrate**, and transcodes the backup to match (cached, so it's a one-time cost per source + parameter set). There's nothing to set — the old `output_*_bitrate_kbps` fields are gone.

## 6b. Turn platforms on and off (Control tab)

Once platforms are defined, the **Control** tab is your live switchboard:

- **Check** a platform to stream to it. If OBS is live, it starts within a couple of seconds (it waits for the next keyframe); if you're offline, it starts on the next broadcast.
- **Uncheck** a platform to stop streaming to it immediately (the other platforms are unaffected). Re-check it to bring it back.

Example: primary = Twitch, restream = Kick. To stream some content only to Kick, uncheck Twitch — Twitch stops, Kick keeps going. Re-check Twitch later to resume it. Toggling primary works the same as any other platform.

## Multiple pipelines (different feeds to different platforms)

Everything above assumes **one** feed sent to all platforms. Sometimes you need platforms to receive *different* streams — the classic case is licensed music: Twitch/Kick get the stream with music, but YouTube needs a clean audio track to avoid copyright strikes. Because the fan-out uses `-c copy` (no re-encoding), it physically can't split audio — so a genuinely different feed needs its own **pipeline**.

A **pipeline** is an independent stream: its own OBS ingest path on the VPS, its own backup video, its own set of platforms, and its own fallback/continuity state machine. The first pipeline is the **default** (it can't be removed or disabled, and it's the one whose OBS Start/Stop the browser-source watches). You add extra pipelines from the dashboard.

**On the VPS (dashboard → Settings → Pipelines).** Each pipeline block shows its ingest path, backup, platforms, and — ready to copy — the **OBS output** for it (Server + Stream Key, with the key's password behind a Show button). **+ Add pipeline** opens a dialog with just two fields: a **name** and a **backup video** (may be the same file as another pipeline or a separate one — e.g. a clean, music-free clip for YouTube). Everything else is automatic: the **ingest path is assigned for you** (`live/<name>`, no restart, no limit), the backup's resolution/fps/bitrate are detected from the live stream, and the offline timeout is a single global setting (System block). New pipelines start **disabled**; enable one with its toggle on the **Control** tab (or in its Settings block) once its OBS output is set up. Add each platform to the pipeline exactly as before, in that pipeline's own platforms list.

> There's no fixed number of pipelines and nothing to pre-provision: MediaMTX accepts any `live/<name>` ingest path (a single regex path in `mediamtx.yml`), so a pipeline is live the moment its OBS output publishes to it — no MediaMTX restart, ever. `./restreamctl.sh credentials` prints the main path's key; each extra pipeline's key is shown in its dashboard block.

**In OBS (one extra output per aux pipeline).** Install the [`obs-multi-rtmp`](https://github.com/sorayuki/obs-multi-rtmp) plugin. Your **main** OBS Stream output stays pointed at the default pipeline (`main?user=obs&pass=…`, step 7). Then add one obs-multi-rtmp output per extra pipeline, and for each:

- **Video:** "Reuse the streaming encoder as OBS" (shares the main video encoder — encode once) — or a separate encoder if you want different video too.
- **Audio:** pick the **Audio Mixer** track that carries the sound this pipeline should get (e.g. Track 1 = with music for Twitch, Track 2 = clean for YouTube — set up in OBS Settings → Output → Audio tracks and Advanced Audio Properties).
- **Server** and **Stream Key:** copy them from this pipeline's block on the dashboard Settings tab (the **OBS output** line — click **Show** to reveal the key). The path was assigned automatically when you created the pipeline.
- **Other Settings:** turn **on** both **`Sync start with OBS`** and **`Sync stop with OBS`** — this ties the output's start/stop to OBS's main Start/Stop Streaming button. This is what lets a deliberate "Stop Streaming" cleanly end the aux pipelines too (see below). They're **off** by default.
- Keep each output's **Keyframe Interval below the Connect timeout** (step 6), just like the main output — MediaMTX's connect timeout is shared by all ingest paths, so an aux output whose first keyframe arrives too late won't connect.

**Graceful stop across pipelines.** The browser-source only sees OBS's *main* Stream output, so extra pipelines can't tell a deliberate "Stop" from a network drop on their own. Instead they consult the default pipeline: if OBS was just stopped deliberately (and the plugin outputs stopped with it, thanks to `Sync stop with OBS`), an aux pipeline dropping within a short window is treated as a clean end — no backup, no timeout. Without `Sync stop`, an aux output keeps running after the main stop, so its pipeline can't know the stop was deliberate.

**To deliberately end just one aux pipeline** (without showing its backup), use its **disable toggle** on the Control tab — *not* the plugin's own stop button for that output. From the server's point of view, stopping only the plugin output is indistinguishable from a network drop, so it would show the backup and wait out the full offline timeout; the disable toggle ends it cleanly instead.

**Failsafe is per-pipeline.** If none of an aux pipeline's platforms can be reached, it stops **only itself** — the main broadcast keeps going, and OBS is *not* told to stop. OBS is asked to stop only when the **default** pipeline fails, or when **every** pipeline is down.

## 7. Configure OBS

1. Settings -> Stream -> Service: **"Custom..."** (not a linked account — OBS won't let you override the server if it's linked).
2. Server: `rtmp://YOUR_VPS_IP:1935/live`
3. Stream Key: `main?user=obs&pass=PASSWORD` (password from step 2, `install.sh`'s output; this exact format — `user=...&pass=...` inside the stream key — is required, not `rtmp://user:pass@host`, because that's how MediaMTX expects RTMP auth). This `main?…` key feeds the **default** pipeline; extra pipelines get their own auto-assigned key (`<name>?user=obs&pass=…`) shown in their dashboard block, used on obs-multi-rtmp outputs (see [Multiple pipelines](#multiple-pipelines-different-feeds-to-different-platforms)).
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

   > Note: this specific problem shows up right when you switch the Service from a built-in one (with a linked account) to "Custom..." (required here, since we need our own RTMP server and auth). It looks like OBS itself handles/limits encoder settings differently depending on which server is selected. The root cause is in OBS, not in this controller — but since "Custom" is unavoidable here, you have to set safe encoder settings manually (see above).

## 8. Check status

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

## Day-to-day management

```bash
./restreamctl.sh stop          # stop everything
./restreamctl.sh restart       # restart
./restreamctl.sh logs          # recent log lines + paths to the ffmpeg logs
./restreamctl.sh credentials   # print the OBS password and dashboard URL again
```

## If something goes wrong

See **[Troubleshooting](../Troubleshooting/Troubleshooting.md)** — the backup video, per-platform ffmpeg logs, "behind (drops)", a corrupted picture, or a backup flash on Stop.

