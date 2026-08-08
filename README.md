# restream-controller

```mermaid
flowchart TB
  subgraph PC["Your PC — OBS"]
    direction LR
    M["Main output<br/>video + full audio"]
    X["Extra output<br/>obs-multi-rtmp<br/>(optional)"]
  end

  NET["Your flaky ISP / uplink<br/>(can drop any time)"]

  subgraph VPS["VPS — restream-controller"]
    direction TB
    I["MediaMTX<br/>RTMP ingest"]
    BK["Backup video"]
    subgraph PIPES["pipelines — independent, -c copy"]
      direction LR
      P1["restream 'main'<br/>full mix"]
      P2["input 'clean'<br/>music-free audio"]
      P3["remux<br/>main video + clean audio"]
      P1 -. video .-> P3
      P2 -. audio .-> P3
    end
    I --> PIPES
    BK -. on drop .-> PIPES
  end

  subgraph PLAT["Platforms"]
    direction LR
    T["Twitch"]
    K["Kick"]
    Y["YouTube"]
  end

  M -- RTMP --> NET
  X -- RTMP --> NET
  NET -- RTMP --> I

  P1 -- RTMP --> T
  P1 -- RTMP --> K
  P3 -- RTMP --> Y
```

OBS publishes **1+ RTMP streams** to the VPS; the controller relays each to its platforms with `-c copy` (no re-encoding) and switches to a **backup video** if your connection drops. Usually it's one stream in, fanned out to several platforms; with [multiple pipelines](Doc/Setup/Setup.md#multiple-pipelines-different-feeds-to-different-platforms) / [remux](Doc/Remux/Remux.md) it can be several in and several out (e.g. a music-free feed just for YouTube). *(Diagram renders on GitHub.)*

Continuous OBS -> multi-platform restreaming: publish once from OBS and relay to one **primary** platform plus any number of extra **restream** platforms (Twitch, YouTube, Kick, …). If your internet connection drops, the stream doesn't end — it switches to a backup video until the connection comes back (or until a timeout is reached).

## Documentation

- **[Overview - what it does](Doc/Overview/Overview.md)** - what the service does and its key features.
- **[Setup & operation](Doc/Setup/Setup.md)** - prerequisites, install, configuring platforms and OBS, multiple pipelines, and day-to-day management.
- **[Remux](Doc/Remux/Remux.md)** - send a different audio mix to one platform (e.g. music-free audio to YouTube) without doubling your uplink.
- **[Everyday scenarios](Doc/Scenarios/Scenarios.md)** - how the service behaves in common situations (drops, wrong keys, deliberate stop, mid-stream changes).
- **[Troubleshooting](Doc/Troubleshooting/Troubleshooting.md)** - when something misbehaves.
