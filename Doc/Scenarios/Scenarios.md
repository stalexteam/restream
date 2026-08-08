# Everyday scenarios

How the service behaves in common situations. For installing and configuring the service see the [Setup guide](../Setup/Setup.md); for the project overview see the [README](../../README.md).

**Everything is working normally.** OBS is publishing, viewers on every enabled platform see the live feed. Nothing special happens.

**You want to stream to only some platforms.** Use the Control tab: uncheck the platforms you want to skip (their streams end immediately), leave the rest checked. Re-check them later to bring them back live. This works for the primary too.

**Internet dropped / PC crashed / OBS closed unexpectedly.** The platforms don't notice the drop — the backup video takes over on all enabled platforms instead of the live feed, and stays on until either:
- the connection comes back (a normal "Start Streaming" in OBS) — the backup stops, live video returns, viewers see a smooth cut with no visible pause (as soon as OBS sends a fresh keyframe — usually a couple of seconds);
- or `offline_timeout_sec` (30 minutes by default) passes with no recovery — the broadcast ends completely and you'll need to click "Start Streaming" in OBS again.

**No platform was reachable this broadcast** (typically a wrong key on the only enabled platform, or wrong keys on all of them). Looping the backup while retrying connections that never once succeeded wouldn't accomplish anything, so the service gives up on the first failed attempt: it skips the backup and the 30-minute wait, ends the broadcast, and (if the obs-source Browser Source has "Full access to OBS", [step 7, item 5](../Setup/Setup.md#7-configure-obs)) tells OBS itself to stop too. You'll see an error toast, and the broadcast indicator turns into a red **FAILURE** badge. Note the "**no** platform" — if at least one enabled platform connects, the broadcast proceeds and only the failed one drops (with a warning toast).

**One platform's key is wrong but others work.** That platform stops on its own (retrying an invalid key achieves nothing) with a warning toast naming it; every other platform keeps streaming, no FAILURE.

**A platform's connection drops after already working.** Different from a bad key: if a platform was working and *then* starts failing (a real network blip), the service keeps retrying it indefinitely, with the error toasts rate-limited so they don't spam you.

**You consciously click "Stop Streaming" in OBS.** A plain connection drop looks identical to the server whether it's a deliberate "Stop" or a lost connection. The obs-source Browser Source ([step 7, item 5](../Setup/Setup.md#7-configure-obs)) watches OBS's own streaming status and signals the server — before OBS's RTMP connection drops — that this is a deliberate stop. The broadcast then ends immediately, with no backup video and no timeout. Without that Browser Source added, every "Stop" looks like an ordinary disconnect: backup video, then the timeout.

**You want to change settings (resolution, bitrate) mid-stream.** OBS won't let you change these fields while actively publishing — stop the stream (the "Stop" button, as above — no backup video shown), change the settings, and hit "Start" again. The service treats it as a fresh start.

If the settings change *during* a disconnect (the PC froze, you fixed the bitrate, then restarted OBS) — the service detects it and does a clean reconnect to every platform instead of an unsafe mid-connection swap. A short pause on recovery is normal in this case.

**You want to change a platform's URL/key, add a restream, or change the timeout/backup while already live.** All of these apply live from the Settings tab: editing a URL reconnects only that platform, adding a restream just adds it (enable it on Control when ready), and timeout/backup changes don't interrupt anything. Only the connect/read timeouts require a MediaMTX restart (which ends the broadcast — you'll be asked to confirm).

**The problem is between the VPS and a platform, not the streamer** (the server's own network, or an issue on the platform's end). The service detects the drop (or the "connection alive but stalled" case) and reconnects to that platform automatically, retrying until it succeeds. Viewers on that platform might see a brief "offline -> online again" (unavoidable — the connection genuinely re-establishes), but nothing needs to be done on the OBS side.

**The controller or the server itself got restarted** (an update, a crash, a VPS reboot) during an active broadcast. State isn't preserved across controller restarts — after restarting it comes up as "nothing is streaming". Run `./restreamctl.sh start` (or confirm autostart) and start streaming from OBS again.
