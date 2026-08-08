# Troubleshooting

When something misbehaves. See also the [Setup guide](../Setup/Setup.md).

1. `./restreamctl.sh check` — did the backup video disappear? Is the primary platform actually set?
2. `./restreamctl.sh logs` — the controller's own log.
3. `logs/ffmpeg-relay-<pipeline>.log`, `logs/ffmpeg-backup-<pipeline>.log`, and one per platform, `logs/ffmpeg-out-<pipeline>-<name>.log` — logs of the individual ffmpeg processes (named per pipeline), if the problem looks like a video/audio issue on a specific platform rather than a switching-logic issue.
4. A platform shows **behind (N drops)** on the Control tab — that platform's upload can't keep up with the source bitrate; the pipeline drops frames and resyncs on the next keyframe. Check its ping and the VPS's upstream bandwidth to that platform.
5. If the backup video looks "wrong" (not the file you expected) — the prepared backup artifacts live in `data/backup-cache/` (one per source + target-parameter combination, content-addressed). Delete the directory's contents to have the service rebuild them on the next stream start.
6. Corrupted/"shattered"-looking picture, or a strange bitrate/codec in the player's stream stats — check OBS Settings -> Output -> **Rate Control**. If it's "Lossless", that's the cause (see [step 7](../Setup/Setup.md#7-configure-obs)) — switch to CBR with an explicit bitrate.
7. A brief flash of the backup video right when you click "Stop Streaming" in OBS — known, harmless timing edge. Normally the obs-source Browser Source signals the deliberate stop *before* OBS drops its RTMP connection, so the backup never shows. First thing to check: the obs-source Browser Source is actually added to a scene ([step 7, item 5](../Setup/Setup.md#7-configure-obs)), otherwise every "Stop" looks like an ordinary disconnect (backup + timeout).

