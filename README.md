# restream-controller

```mermaid
flowchart TB
  subgraph PC["Your PC — OBS"]
    direction LR
    M["Main output"]
    X["Extra output"]
  end
  NET["Your flaky ISP"]
  subgraph VPS["VPS — restream-controller"]
    direction TB
    I["MediaMTX ingest"]
    BK["Backup video"]
    subgraph PIPES["pipelines (-c copy)"]
      direction LR
      P1["restream (main)"]
      P2["input (clean)"]
      P3["remux"]
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
  M --> NET
  X --> NET
  NET --> I
  P1 --> T
  P1 --> K
  P3 --> Y
```

Continuous OBS -> multi-platform restreaming: publish once from OBS and relay to one **primary** platform plus any number of extra **restream** platforms (Twitch, YouTube, Kick, …). If your internet connection drops, the stream doesn't end — it switches to a backup video until the connection comes back (or until a timeout is reached).

## Documentation

- **[Overview - what it does](Doc/Overview/Overview.md)** - what the service does and its key features.
- **[Setup & operation](Doc/Setup/Setup.md)** - prerequisites, install, configuring platforms and OBS, multiple pipelines, and day-to-day management.
- **[Remux](Doc/Remux/Remux.md)** - send a different audio mix to one platform (e.g. music-free audio to YouTube) without doubling your uplink.
- **[Everyday scenarios](Doc/Scenarios/Scenarios.md)** - how the service behaves in common situations (drops, wrong keys, deliberate stop, mid-stream changes).
- **[Troubleshooting](Doc/Troubleshooting/Troubleshooting.md)** - when something misbehaves.
