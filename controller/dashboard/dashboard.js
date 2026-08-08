(() => {
  "use strict";

  const token = new URLSearchParams(location.search).get("token") || "";

  const state = {};            // latest status snapshot: {pipelines, manual_halt, components, obs_source_connected}
  let settingsData = null;     // latest get_settings payload (globals + pipelines[])

  const els = {
    toastContainer: document.getElementById("toast-container"),
    wsIndicator: document.getElementById("ws-indicator"),
    sourceIndicator: document.getElementById("source-indicator"),
    obsIndicator: document.getElementById("obs-indicator"),
    broadcastIndicator: document.getElementById("broadcast-indicator"),
    haltBtn: document.getElementById("halt-btn"),
    componentsBody: document.querySelector("#components-table tbody"),
    statusPipelines: document.getElementById("status-pipelines"),
    controlPipelines: document.getElementById("control-pipelines"),
    tabButtons: document.querySelectorAll(".tab-button"),
    tabStatus: document.getElementById("tab-status"),
    tabControl: document.getElementById("tab-control"),
    tabSettings: document.getElementById("tab-settings"),
    pipelinesList: document.getElementById("pipelines-list"),
    btnAddPipeline: document.getElementById("btn-add-pipeline"),
    fieldConnectTimeout: document.getElementById("field-connect-timeout"),
    fieldReadTimeout: document.getElementById("field-read-timeout"),
    fieldOfflineTimeout: document.getElementById("field-offline-timeout"),
    fieldIcmpPing: document.getElementById("field-icmp-ping"),
    settingsErrors: document.getElementById("settings-errors"),
    btnApply: document.getElementById("btn-apply"),
    // platform modal
    modal: document.getElementById("platform-modal"),
    modalTitle: document.getElementById("platform-modal-title"),
    modalName: document.getElementById("modal-name"),
    modalServer: document.getElementById("modal-server"),
    modalKey: document.getElementById("modal-key"),
    modalSecretsShow: document.getElementById("modal-secrets-show"),
    modalPreview: document.getElementById("modal-preview"),
    modalErrors: document.getElementById("modal-errors"),
    modalOk: document.getElementById("modal-ok"),
    modalCancel: document.getElementById("modal-cancel"),
    // pipeline modal
    pmodal: document.getElementById("pipeline-modal"),
    pmodalTitle: document.getElementById("pipeline-modal-title"),
    pmodalName: document.getElementById("pmodal-name"),
    // create widget (type + name), shown on "Add pipeline"
    pcreateModal: document.getElementById("pipeline-create-modal"),
    pcreateType: document.getElementById("pcreate-type"),
    pcreateTypeDesc: document.getElementById("pcreate-type-desc"),
    pcreateName: document.getElementById("pcreate-name"),
    pcreateErrors: document.getElementById("pcreate-errors"),
    pcreateOk: document.getElementById("pcreate-ok"),
    pcreateCancel: document.getElementById("pcreate-cancel"),
    pmodalRemuxField: document.getElementById("pmodal-remux-field"),
    pmodalVideoSrc: document.getElementById("pmodal-video-src"),
    pmodalAudioSrc: document.getElementById("pmodal-audio-src"),
    pmodalTrimField: document.getElementById("pmodal-trim-field"),
    pmodalTrim: document.getElementById("pmodal-trim"),
    pmodalBackupField: document.getElementById("pmodal-backup-field"),
    pmodalBackup: document.getElementById("pmodal-backup"),
    pmodalIngestField: document.getElementById("pmodal-ingest-field"),
    pmodalIngestServer: document.getElementById("pmodal-ingest-server"),
    pmodalIngestKey: document.getElementById("pmodal-ingest-key"),
    pmodalIngestShow: document.getElementById("pmodal-ingest-show"),
    pmodalErrors: document.getElementById("pmodal-errors"),
    pmodalOk: document.getElementById("pmodal-ok"),
    pmodalCancel: document.getElementById("pmodal-cancel"),
    // templates
    tplStatus: document.getElementById("pipeline-status-template"),
    tplControl: document.getElementById("pipeline-control-template"),
    tplSettingsPipeline: document.getElementById("settings-pipeline-template"),
    platformTemplate: document.getElementById("platform-row-template"),
  };

  const RECONNECT_MIN_DELAY_MS = 1000;
  const RECONNECT_MAX_DELAY_MS = 15000;
  const TOAST_DURATION_MS = 5000;
  const LIVE_MIN_SEC = 3;

  let socket = null;
  let reconnectDelay = RECONNECT_MIN_DELAY_MS;
  let reconnectTimer = null;
  // platform modal context
  let modalMode = null;          // "add" | "edit" | null
  let modalPipeline = null;      // which pipeline the platform belongs to
  let modalEditingName = null;
  let modalSecretsShown = false; // reveal state of Server/Key/URL (masked by default)
  // pipeline modal context
  let pmodalMode = null;         // "add" | "edit" | null
  let pmodalType = "restream";   // the pipeline's type (fixed once created)
  let pmodalEditingName = null;
  // after a successful create, the edit widget for this pipeline auto-opens
  // once the refreshed settings arrive.
  let pendingEditName = null;
  let pmodalServerRaw = "";      // ingest Server (host:port) of the pipeline being edited
  let pmodalKeyRaw = "";         // full ingest Stream Key of the pipeline being edited
  let pmodalIngestShown = false; // reveal state of Server+Key in the modal
  let loadedSettings = null;     // to detect timeout changes on Apply

  function wsLive() { return !document.body.classList.contains("ws-disconnected"); }

  // --- toasts ---

  const toastQueue = [];
  let currentToast = null;
  let toastTimer = null;
  let toastRemainingMs = 0;
  let toastTimerStartedAt = 0;

  function enqueueToast(level, text) {
    toastQueue.push({ level, text });
    if (!currentToast) showNextToast();
  }
  function showNextToast() {
    const next = toastQueue.shift();
    if (!next) return;
    const el = document.createElement("div");
    el.className = "toast toast-" + next.level;
    el.textContent = next.text;
    el.addEventListener("click", dismissCurrentToast);
    el.addEventListener("mouseenter", pauseToastTimer);
    el.addEventListener("mouseleave", resumeToastTimer);
    els.toastContainer.appendChild(el);
    currentToast = el;
    toastRemainingMs = TOAST_DURATION_MS;
    resumeToastTimer();
  }
  function pauseToastTimer() {
    clearTimeout(toastTimer);
    toastRemainingMs -= Date.now() - toastTimerStartedAt;
  }
  function resumeToastTimer() {
    toastTimerStartedAt = Date.now();
    toastTimer = setTimeout(dismissCurrentToast, Math.max(0, toastRemainingMs));
  }
  function dismissCurrentToast() {
    clearTimeout(toastTimer);
    if (currentToast) { currentToast.remove(); currentToast = null; }
    showNextToast();
  }

  // --- websocket ---

  function wsUrl() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    return `${proto}//${location.host}/ws?token=${encodeURIComponent(token)}`;
  }

  const WS_SYMBOL = { connecting: "WS ⋯", live: "WS ✓", lost: "WS ✗" };
  const WS_TITLE = {
    connecting: "WS — this dashboard's live link to the controller: connecting…",
    live: "WS — this dashboard's live link to the controller: connected",
    lost: "WS — this dashboard's live link to the controller: lost, retrying…",
  };

  function setConnectionStatus(status) {
    els.wsIndicator.className = "indicator ind-" + status;
    els.wsIndicator.textContent = WS_SYMBOL[status];
    els.wsIndicator.title = WS_TITLE[status];
    document.body.classList.toggle("ws-disconnected", status !== "live");
    render();
  }

  function connect() {
    clearTimeout(reconnectTimer);
    setConnectionStatus("connecting");
    socket = new WebSocket(wsUrl());
    socket.onopen = () => {
      reconnectDelay = RECONNECT_MIN_DELAY_MS;
      setConnectionStatus("live");
      sendCommand("get_settings");
    };
    socket.onmessage = (event) => {
      let message;
      try { message = JSON.parse(event.data); } catch (e) { return; }
      switch (message.type) {
        case "full":
          for (const k of Object.keys(state)) delete state[k];
          Object.assign(state, message.data);
          render();
          break;
        case "delta":
          Object.assign(state, message.data);
          render();
          break;
        case "settings":
          settingsData = message.data;
          loadedSettings = message.data;
          populateSystemForm(message.data);
          renderPipelinesSettings();
          maybeOpenPendingEdit();  // auto-open edit after a successful create
          break;
        case "settings_saved":
          handleSettingsSaved(message);
          break;
        case "output_result":
          handleOutputResult(message);
          break;
        case "pipeline_result":
          handlePipelineResult(message);
          break;
        case "event":
          enqueueToast(message.level, message.text);
          break;
        default:
          break;
      }
    };
    socket.onclose = scheduleReconnect;
    socket.onerror = () => socket && socket.close();
  }

  function scheduleReconnect() {
    setConnectionStatus("lost");
    const seconds = Math.round(reconnectDelay / 1000);
    els.wsIndicator.title = `WS — link to the controller lost, retrying in ${seconds}s…`;
    reconnectTimer = setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_DELAY_MS);
  }

  function send(payload) {
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify(payload));
      return true;
    }
    return false;
  }
  function sendCommand(command) { send({ command }); }

  function formatDuration(totalSeconds) {
    const seconds = Math.max(0, Math.floor(totalSeconds));
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    const pad = (n) => String(n).padStart(2, "0");
    return h > 0 ? `${h}:${pad(m)}:${pad(s)}` : `${m}:${pad(s)}`;
  }

  function cssEscape(s) {
    return (window.CSS && CSS.escape) ? CSS.escape(s) : String(s).replace(/["\\]/g, "\\$&");
  }

  // --- shared helpers ---

  function pipelines() { return state.pipelines || []; }
  function defaultPipeline() {
    const ps = pipelines();
    return ps.find((p) => p.is_default) || ps[0] || null;
  }
  // A platform is effectively broadcasting only if the pipeline's master
  // switch (p.enabled) AND its own checkbox (d.enabled) are both on.
  function anyEnabledDest(p) { return !!p.enabled && (p.destinations || []).some((d) => d.enabled); }

  // A ★ marker with a hover hint, replacing the "(primary)"/"(default)"
  // suffixes. All setters below rebuild children -> safe to call each render.
  function appendStar(el, hint) {
    const star = document.createElement("span");
    star.className = "role-star";
    star.textContent = "★";
    star.title = hint;
    el.append(" ", star);
  }
  function setNameWithPrimary(el, name, isPrimary) {
    el.textContent = name;
    if (isPrimary) appendStar(el, "Primary");
  }
  // Pipeline title: ★ for the default pipeline; "(disabled)" only where asked
  // (Status has no toggle; Control/Settings do, so it's omitted there).
  function setPipelineTitle(el, p, withDisabled) {
    el.textContent = p.name;
    // Every pipeline carries a small type tag (restream/input/remux) so the
    // three kinds are visually distinct across Status/Control/Settings.
    const type = p.type || "restream";
    const tag = document.createElement("span");
    tag.className = "pl-type-tag pl-type-" + type;
    tag.textContent = type;
    el.append(" ", tag);
    if (p.is_default) { appendStar(el, "Default"); return; }
    if (withDisabled && !p.enabled) el.append(" (disabled)");
  }

  // An input is "used" if at least one remux references it as a source.
  function inputUsedBy(p) {
    return pipelines().filter(
      (x) => x.type === "remux" && (x.video_src_path === p.live_path || x.audio_src_path === p.live_path),
    );
  }

  // Broadcast badge (what goes OUT) for one pipeline. manual_halt is global.
  function badgeFor(p) {
    // Input pipelines don't broadcast -- the badge reflects whether OBS is
    // publishing to this input and whether a remux actually consumes it.
    if (p.type === "input") {
      if (p.state !== "LIVE") return { text: "OFFLINE", cls: "bstate-offline" };
      return inputUsedBy(p).length
        ? { text: "USED", cls: "bstate-live" }
        : { text: "IDLE", cls: "bstate-idle" };
    }
    if (p.state === "OFFLINE") {
      if (state.manual_halt) return { text: "HALTED", cls: "bstate-halt" };
      if (p.halted) return { text: "FAILURE", cls: "bstate-halt" };
      return { text: "OFFLINE", cls: "bstate-offline" };
    }
    if (p.state === "FALLBACK") return { text: "BACKUP", cls: "bstate-fallback" };
    if (p.state === "LIVE") {
      return anyEnabledDest(p)
        ? { text: "ON AIR", cls: "bstate-live" }
        : { text: "IDLE", cls: "bstate-idle" };
    }
    return { text: p.state || "?", cls: "bstate-offline" };
  }

  // Apply a pipeline's broadcast badge (class + text) and attach the same
  // status-legend hint the header broadcast-indicator carries (BADGE_HELP).
  function applyBadge(badge, p) {
    const bi = badgeFor(p);
    badge.className = "pill pl-badge " + bi.cls;
    badge.textContent = bi.text;
    badge.title = p.type === "input" ? INPUT_BADGE_HELP : BADGE_HELP;
  }

  const INPUT_BADGE_HELP =
    "Input source — a named ingest that feeds a remux (it doesn't broadcast on its own):\n" +
    "• USED — OBS is publishing to this input and at least one remux uses it as a source\n" +
    "• IDLE — OBS is publishing, but no remux references this input\n" +
    "• OFFLINE — OBS is not publishing to this input";

  // --- render ---

  function render() {
    renderSource();
    renderHeaderObs();
    renderHeaderBadge();
    renderStatusPipelines();
    renderComponents();
    renderControlPipelines();
    updateHaltButton();
  }

  function updateHaltButton() {
    const broadcasting = pipelines().some((p) => p.state === "LIVE" || p.state === "FALLBACK");
    els.haltBtn.disabled = !(wsLive() && broadcasting);
  }

  function renderSource() {
    const connected = !!state.obs_source_connected;
    els.sourceIndicator.className = "indicator ind-" + (connected ? "live" : "connecting");
    els.sourceIndicator.textContent = connected ? "SRC ✓" : "SRC ✗";
    els.sourceIndicator.title = connected
      ? "SRC — the OBS browser-source (obs-source.html) is connected; Start/Stop detection and remote HALT work"
      : "SRC — the OBS browser-source (obs-source.html) is not connected (grey is normal in a plain browser; green only when it's added in OBS). Without it, Start/Stop detection and telling OBS to stop won't work";
  }

  // Header OBS + badge reflect the DEFAULT pipeline (the main broadcast).
  function obsInfo(p) {
    const obs = (p && p.obs) || {};
    let status, symbol, title;
    if (obs.flowing) {
      status = "live"; symbol = "OBS ✓"; title = "OBS — video input into the VPS: flowing";
    } else if (p && p.state === "FALLBACK") {
      status = "lost"; symbol = "OBS ✗"; title = "OBS — video input into the VPS: none (OBS dropped, showing backup video)";
    } else {
      status = "connecting"; symbol = "OBS ✗"; title = "OBS — no video input from OBS into the VPS";
    }
    const vparts = [];
    if (obs.width && obs.height) vparts.push(`${obs.width}×${obs.height}@${obs.fps || "?"}`);
    if (obs.video_codec) vparts.push(obs.video_codec);
    if (obs.video_kbps != null) vparts.push(`${obs.video_kbps} kbps`);
    const aparts = [];
    if (obs.audio_codec) aparts.push(obs.audio_codec);
    if (obs.audio_kbps != null) aparts.push(`${obs.audio_kbps} kbps`);
    return { obs, status, symbol, title, vparts, aparts };
  }

  function renderHeaderObs() {
    const p = defaultPipeline();
    const info = obsInfo(p);
    els.obsIndicator.className = "indicator ind-" + info.status;
    els.obsIndicator.textContent = info.symbol;
    els.obsIndicator.title = info.vparts.length || info.aparts.length
      ? `${info.title}\n${[info.vparts.join(" · "), info.aparts.join(" · ")].filter(Boolean).join(" | ")}`
      : info.title;
  }

  function renderHeaderBadge() {
    const p = defaultPipeline();
    const info = p ? badgeFor(p) : { text: "OFFLINE", cls: "bstate-offline" };
    els.broadcastIndicator.className = "indicator " + info.cls;
    els.broadcastIndicator.textContent = info.text;
  }

  function tick(ok) { return ok ? "✓" : "✗"; }

  function renderStatusPipelines() {
    els.statusPipelines.innerHTML = "";
    for (const p of pipelines()) {
      const frag = els.tplStatus.content.cloneNode(true);
      const title = frag.querySelector(".pipeline-title");
      setPipelineTitle(title, p, true);
      applyBadge(frag.querySelector(".pl-badge"), p);
      const data = frag.querySelector(".obs-data");
      const vRow = frag.querySelector(".obs-video");
      const aRow = frag.querySelector(".obs-audio");

      if (p.type === "input") {
        // Named ingest -- has its own stats relay now: show flowing + media
        // params like a restream (its "OBS input" is the clean feed).
        const info = obsInfo(p);
        data.textContent = info.obs.flowing ? "flowing" : "no data";
        data.className = "obs-data " + (info.obs.flowing ? "status-up" : "status-down");
        vRow.textContent = info.vparts.length ? info.vparts.join(" · ") : "–";
        aRow.textContent = info.aparts.length ? info.aparts.join(" · ") : "–";
      } else if (p.type === "remux") {
        // Two-input health: video ✓/✗ + audio ✓/✗ (the merge itself lands in
        // phase 2; for now these mirror source availability).
        const src = p.sources || {};
        const both = !!src.video && !!src.audio;
        data.textContent = both ? "both sources up" : "waiting for sources";
        data.className = "obs-data " + (both ? "status-up" : "status-down");
        vRow.textContent = `video ${tick(!!src.video)}  (${p.video_src_path || "?"})`;
        vRow.className = "obs-video " + (src.video ? "status-up" : "status-down");
        aRow.textContent = `audio ${tick(!!src.audio)}  (${p.audio_src_path || "?"})`;
        aRow.className = "obs-audio " + (src.audio ? "status-up" : "status-down");
      } else {
        const info = obsInfo(p);
        data.textContent = info.obs.flowing ? "flowing" : "no data";
        data.className = "obs-data " + (info.obs.flowing ? "status-up" : "status-down");
        vRow.textContent = info.vparts.length ? info.vparts.join(" · ") : "–";
        aRow.textContent = info.aparts.length ? info.aparts.join(" · ") : "–";
      }
      els.statusPipelines.appendChild(frag);
    }
  }

  function componentOrder() {
    const order = ["mediamtx", "controller"];
    // Only the roles a pipeline of that type actually runs: restream ->
    // relay+backup; remux -> backup (relay lands in phase 2); input -> none.
    for (const p of pipelines()) {
      if (p.type === "input") continue;
      if (p.type === "remux") { order.push(`backup:${p.name}`); continue; }
      order.push(`relay:${p.name}`, `backup:${p.name}`);
    }
    return order;
  }

  function renderComponents() {
    const components = state.components || {};
    els.componentsBody.innerHTML = "";
    for (const name of componentOrder()) {
      const c = components[name];
      if (!c) continue;
      const row = document.createElement("tr");
      const cells = [
        name,
        c.running ? "running" : "stopped",
        c.pid != null ? String(c.pid) : "–",
        c.cpu_percent != null ? `${c.cpu_percent}%` : "–",
        c.rss_mb != null ? `${c.rss_mb} MB` : "–",
      ];
      cells.forEach((text, i) => {
        const td = document.createElement("td");
        td.textContent = text;
        if (i === 1) td.className = c.running ? "status-up" : "status-down";
        row.appendChild(td);
      });
      els.componentsBody.appendChild(row);
    }
  }

  function destStatus(d, master) {
    if (!d.enabled) return { text: "Disabled", cls: "pill-off" };
    // Master gate off: the platform is checked but suppressed pipeline-wide.
    if (!master) return { text: "Muted", cls: "pill-off" };
    if (d.failed) return { text: "Failed", cls: "pill-failed" };
    if (d.running) {
      return (d.uptime_sec || 0) >= LIVE_MIN_SEC
        ? { text: "Live", cls: "pill-live" }
        : { text: "Connecting", cls: "pill-connecting" };
    }
    return { text: "Offline", cls: "pill-off" };
  }
  function destHealth(d) {
    if (!d.enabled || !d.running) return { text: "–", cls: "" };
    if (d.behind) return { text: `behind (${d.dropped} drops)`, cls: "status-down" };
    if (d.dropped > 0) return { text: `${d.dropped} drops`, cls: "" };
    return { text: "OK", cls: "status-up" };
  }

  // Control tab: one panel per pipeline, keyed (so a just-clicked toggle
  // isn't clobbered by a delta). Rows keyed by platform name.
  function renderControlPipelines() {
    const live = wsLive();
    const seenPipes = new Set();
    for (const p of pipelines()) {
      seenPipes.add(p.name);
      let panel = els.controlPipelines.querySelector(`.pipeline-panel[data-pipeline="${cssEscape(p.name)}"]`);
      if (!panel) {
        const frag = els.tplControl.content.cloneNode(true);
        panel = frag.querySelector(".pipeline-panel");
        panel.dataset.pipeline = p.name;
        els.controlPipelines.appendChild(frag);
      }
      // header -- no "(disabled)": the master checkbox sits right next to
      // the title, so a status label would just duplicate it.
      setPipelineTitle(panel.querySelector(".pipeline-title"), p, false);
      applyBadge(panel.querySelector(".pl-badge"), p);

      const isInput = p.type === "input";
      const isRemux = p.type === "remux";
      // Input has no platforms -> hide its master toggle + table, show a note.
      panel.querySelector(".input-note").hidden = !isInput;
      panel.querySelector(".control-table").hidden = isInput;

      // The header checkbox is a master AND-gate over this pipeline's
      // platforms (every OUTPUT pipeline has one, default included): unchecking
      // it mutes them all at once. Input has none -> hide it.
      const toggle = panel.querySelector(".pipeline-toggle");
      if (toggle) {
        toggle.closest(".pipeline-toggle-wrap").classList.toggle("no-toggle", isInput);
        toggle.hidden = isInput;
        if (document.activeElement !== toggle) toggle.checked = p.enabled;
        toggle.disabled = !live || isInput;
        if (!toggle.dataset.bound) {
          toggle.dataset.bound = "1";
          toggle.addEventListener("change", () => {
            send({ command: toggle.checked ? "enable_pipeline" : "disable_pipeline", name: panel.dataset.pipeline });
          });
        }
      }

      // Remux: source ✓/✗ + a live audio-trim slider (applies without reconnect).
      const remuxControls = panel.querySelector(".remux-controls");
      remuxControls.hidden = !isRemux;
      if (isRemux) renderRemuxControls(panel, p);

      if (!isInput) renderControlRows(panel.querySelector("tbody"), p, live);
    }
    // drop panels for removed pipelines
    for (const panel of Array.from(els.controlPipelines.children)) {
      if (!seenPipes.has(panel.dataset.pipeline)) panel.remove();
    }
    // Keep panel order in sync with the pipeline order (default first) -- the
    // same order the Settings tab renders. Panels are reused across renders,
    // so a changed order wouldn't otherwise reflow; move only misplaced nodes
    // (in steady state this does nothing, so a focused toggle isn't disturbed).
    let expected = els.controlPipelines.firstElementChild;
    for (const p of pipelines()) {
      const panel = els.controlPipelines.querySelector(`.pipeline-panel[data-pipeline="${cssEscape(p.name)}"]`);
      if (!panel) continue;
      if (panel === expected) expected = expected.nextElementSibling;
      else els.controlPipelines.insertBefore(panel, expected);
    }
  }

  function renderControlRows(tbody, p, live) {
    const dests = p.destinations || [];
    const seen = new Set();
    for (const d of dests) {
      seen.add(d.name);
      let row = tbody.querySelector(`tr[data-name="${cssEscape(d.name)}"]`);
      if (!row) {
        row = document.createElement("tr");
        row.dataset.name = d.name;
        const tdName = document.createElement("td");
        tdName.className = "cell-name";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.className = "control-toggle";
        cb.addEventListener("change", () => {
          send({ command: cb.checked ? "enable_output" : "disable_output", pipeline: p.name, name: d.name });
        });
        const nameSpan = document.createElement("span");
        nameSpan.className = "pf-label";
        tdName.append(cb, nameSpan);
        const tdStatus = document.createElement("td");
        const tdUptime = document.createElement("td");
        const tdHealth = document.createElement("td");
        const tdPing = document.createElement("td");
        row.append(tdName, tdStatus, tdUptime, tdHealth, tdPing);
        tbody.appendChild(row);
      }
      const [tdName, tdStatus, tdUptime, tdHealth, tdPing] = row.children;
      const cb = tdName.querySelector(".control-toggle");
      if (document.activeElement !== cb) cb.checked = d.enabled;
      cb.disabled = !live;
      setNameWithPrimary(tdName.querySelector(".pf-label"), d.name, d.is_primary);
      const st = destStatus(d, p.enabled);
      tdStatus.innerHTML = "";
      const pill = document.createElement("span");
      pill.className = "pill " + st.cls;
      pill.textContent = st.text;
      tdStatus.appendChild(pill);
      tdUptime.textContent = d.running && d.uptime_sec ? formatDuration(d.uptime_sec) : "–";
      const h = destHealth(d);
      tdHealth.textContent = h.text;
      tdHealth.className = h.cls;
      tdPing.textContent = d.rtt_ms != null ? `${d.rtt_ms} ms` : "–";
    }
    for (const row of Array.from(tbody.children)) {
      if (!seen.has(row.dataset.name)) row.remove();
    }
  }

  // Remux control block: read-only two-input status + current A/V skew. The
  // audio-trim calibration itself lives in the remux edit widget (set once).
  function renderRemuxControls(panel, p) {
    const src = p.sources || {};
    panel.querySelector(".remux-sources").innerHTML =
      `<span class="${src.video ? "status-up" : "status-down"}">video ${tick(!!src.video)}</span> ` +
      `<span class="${src.audio ? "status-up" : "status-down"}">audio ${tick(!!src.audio)}</span> ` +
      `&nbsp; skew: ${p.skew_ms != null ? p.skew_ms + " ms" : "–"}` +
      `&nbsp; trim: ${p.audio_trim_ms || 0} ms`;
  }

  // --- clocks (fallback countdown on each pipeline badge tooltip) ---

  const BADGE_HELP =
    "Broadcast — what's actually going out to the platforms:\n" +
    "• ON AIR — streaming live to at least one enabled platform\n" +
    "• BACKUP — OBS dropped; the backup video is playing on the platforms\n" +
    "• IDLE — OBS is publishing, but nothing is going out (the pipeline's master switch is off, or no platform is enabled)\n" +
    "• OFFLINE — nothing is streaming\n" +
    "• FAILURE — stopped: none of the enabled platforms could be reached\n" +
    "• HALTED — stopped from the dashboard; won't auto-restart until OBS is stopped and started again";

  function tickClocks() {
    const p = defaultPipeline();
    let title = BADGE_HELP;
    if (p && p.fallback_deadline) {
      const remaining = p.fallback_deadline - Date.now() / 1000;
      const line = remaining > 0
        ? `Stopping in ${formatDuration(remaining)} if OBS doesn't reconnect.`
        : "Stopping now…";
      title = line + "\n\n" + BADGE_HELP;
    }
    els.broadcastIndicator.title = title;
  }

  // --- tabs ---

  function switchTab(name) {
    els.tabButtons.forEach((btn) => btn.classList.toggle("active", btn.dataset.tab === name));
    els.tabStatus.hidden = name !== "status";
    els.tabControl.hidden = name !== "control";
    els.tabSettings.hidden = name !== "settings";
    if (name === "settings") {
      hideSettingsMessages();
      sendCommand("get_settings");
    }
  }
  els.tabButtons.forEach((btn) => btn.addEventListener("click", () => switchTab(btn.dataset.tab)));
  els.obsIndicator.addEventListener("click", () => switchTab("status"));

  // --- settings: system form ---

  function populateSystemForm(data) {
    els.fieldConnectTimeout.value = data.connect_timeout_ms ?? "";
    els.fieldReadTimeout.value = data.read_timeout_ms ?? "";
    els.fieldOfflineTimeout.value = data.offline_timeout_sec ?? "";
    els.fieldIcmpPing.checked = !!data.icmp_ping;
  }
  function collectSystem() {
    return {
      connect_timeout_ms: Number(els.fieldConnectTimeout.value),
      read_timeout_ms: Number(els.fieldReadTimeout.value),
      offline_timeout_sec: Number(els.fieldOfflineTimeout.value),
      icmp_ping: els.fieldIcmpPing.checked,
    };
  }
  function showSettingsErrors(errors) {
    els.settingsErrors.innerHTML = "";
    for (const [field, text] of Object.entries(errors)) {
      const line = document.createElement("div");
      line.textContent = `${field}: ${text}`;
      els.settingsErrors.appendChild(line);
    }
    els.settingsErrors.hidden = Object.keys(errors).length === 0;
  }
  function hideSettingsMessages() { els.settingsErrors.hidden = true; }
  function handleSettingsSaved(message) {
    if (message.ok) hideSettingsMessages();
    else showSettingsErrors(message.errors || {});
  }
  function saveSettings() {
    hideSettingsMessages();
    const values = collectSystem();
    const timeoutsChanged = loadedSettings && (
      Number(loadedSettings.connect_timeout_ms) !== values.connect_timeout_ms ||
      Number(loadedSettings.read_timeout_ms) !== values.read_timeout_ms
    );
    const broadcasting = pipelines().some((p) => p.state && p.state !== "OFFLINE");
    if (timeoutsChanged && broadcasting) {
      if (!confirm("Changing the connect/read timeout restarts MediaMTX and ends the current broadcast. Continue?")) return;
    }
    send({ command: "save_settings", settings: values });
  }

  // --- settings: pipelines + their platforms ---

  function renderPipelinesSettings() {
    if (!settingsData) return;
    const live = wsLive();
    els.pipelinesList.innerHTML = "";
    for (const p of settingsData.pipelines || []) {
      const frag = els.tplSettingsPipeline.content.cloneNode(true);
      const panel = frag.querySelector(".pipeline-panel");
      // No "(disabled)" here -- enable/disable lives in the Control tab, so
      // showing that status in Settings only confuses.
      setPipelineTitle(panel.querySelector(".pipeline-title"), p, false);

      // Backup path + OBS output live in the Modify dialog, not in the list.
      const modifyBtn = panel.querySelector(".pl-modify");
      modifyBtn.disabled = !live;
      modifyBtn.addEventListener("click", () => openPipelineModal("edit", p));

      const delBtn = panel.querySelector(".pl-delete");
      if (p.is_default) {
        delBtn.remove();
      } else {
        delBtn.disabled = !live;
        delBtn.addEventListener("click", () => {
          if (confirm(`Delete pipeline "${p.name}"? This stops its stream and all its platforms immediately.`)) {
            send({ command: "remove_pipeline", name: p.name });
          }
        });
      }

      const isInput = p.type === "input";
      const addBtn = panel.querySelector(".pl-add-platform");

      if (p.type === "remux") {
        // Summarize which inputs feed this remux (resolve paths -> names).
        const summary = document.createElement("div");
        summary.className = "remux-src-summary";
        summary.textContent = `video ← ${srcName(p.video_src_path)}   ·   audio ← ${srcName(p.audio_src_path)}`;
        panel.querySelector(".pipeline-head").after(summary);
      }

      if (isInput) {
        // No platforms on an input -> hide the list and the add button, and
        // collapse the panel to just its title bar (no empty body).
        panel.querySelector(".platforms-list").hidden = true;
        addBtn.remove();
        panel.classList.add("compact");
      } else {
        renderPlatformList(panel.querySelector(".platforms-list"), p.name, p.platforms || [], live);
        addBtn.disabled = !live;
        addBtn.addEventListener("click", () => openPlatformModal("add", p.name, null));
      }

      els.pipelinesList.appendChild(frag);
    }
  }

  function renderPlatformList(container, pipelineName, platforms, live) {
    container.innerHTML = "";
    for (const p of platforms) {
      const frag = els.platformTemplate.content.cloneNode(true);
      const row = frag.querySelector(".platform-row");
      setNameWithPrimary(row.querySelector(".platform-name"), p.name, p.is_primary);
      // URL/key aren't shown in the list -- view/edit them in the Modify
      // dialog (masked there by default).

      const delBtn = row.querySelector(".pf-delete");
      if (p.is_primary) {
        delBtn.remove();  // primary can't be deleted
      } else {
        delBtn.disabled = !live;
        delBtn.addEventListener("click", () => {
          if (confirm(`Delete platform "${p.name}"? This stops its stream immediately.`)) {
            send({ command: "remove_output", pipeline: pipelineName, name: p.name });
          }
        });
      }

      // Modify is the rightmost button.
      const editBtn = row.querySelector(".pf-edit");
      editBtn.disabled = !live;
      editBtn.addEventListener("click", () => openPlatformModal("edit", pipelineName, p));

      container.appendChild(row);
    }
  }

  // --- platform modal ---

  function normalizeIvs(url) {
    if (!url.startsWith("rtmps://")) return url;
    const after = url.slice("rtmps://".length);
    const cut = after.search(/[/?#]/);
    const authority = cut === -1 ? after : after.slice(0, cut);
    const rest = cut === -1 ? "" : after.slice(cut);
    let hostpart = authority;
    const at = hostpart.lastIndexOf("@");
    if (at >= 0) hostpart = hostpart.slice(at + 1);
    const host = hostpart.replace(/:\d+$/, "");
    if (!(host === "live-video.net" || host.endsWith(".live-video.net"))) return url;
    const authWithPort = /:\d+$/.test(authority) ? authority : authority + ":443";
    let path = rest, query = "";
    const q = rest.search(/[?#]/);
    if (q !== -1) { path = rest.slice(0, q); query = rest.slice(q); }
    if (path !== "/app" && !path.startsWith("/app/")) {
      path = "/app" + (path.startsWith("/") ? path : "/" + path);
    }
    return "rtmps://" + authWithPort + path + query;
  }
  function buildPushUrl(server, key) {
    server = (server || "").trim();
    key = (key || "").trim();
    if (!server) return "";
    const url = key ? server.replace(/\/+$/, "") + "/" + key.replace(/^\/+/, "") : server;
    return normalizeIvs(url);
  }
  function syncModalMask() {
    // Server URL + Stream key + the assembled push URL are all sensitive
    // (host/IP:port, subdomain, key) -> masked by default, one toggle.
    const t = modalSecretsShown ? "text" : "password";
    els.modalServer.type = t;
    els.modalKey.type = t;
    els.modalSecretsShow.textContent = modalSecretsShown ? "Hide" : "Show";
    updateModalPreview();
  }
  function updateModalPreview() {
    const full = buildPushUrl(els.modalServer.value, els.modalKey.value) || "–";
    els.modalPreview.textContent = (modalSecretsShown || full === "–") ? full : "••••••••••••";
  }
  function openPlatformModal(mode, pipelineName, platform) {
    modalMode = mode;
    modalPipeline = pipelineName;
    modalEditingName = mode === "edit" ? platform.name : null;
    els.modalTitle.textContent = mode === "edit" ? `Modify ${platform.name} (${pipelineName})` : `Add platform to ${pipelineName}`;
    els.modalName.value = mode === "edit" ? platform.name : "";
    els.modalServer.value = mode === "edit" ? (platform.server || "") : "";
    els.modalKey.value = mode === "edit" ? (platform.key || "") : "";
    modalSecretsShown = false;  // sensitive -> masked by default
    syncModalMask();
    hideModalErrors();
    els.modal.hidden = false;
    els.modalName.focus();
  }
  function closePlatformModal() {
    els.modal.hidden = true;
    modalMode = null; modalPipeline = null; modalEditingName = null;
  }
  function submitPlatformModal() {
    hideModalErrors();
    const name = els.modalName.value.trim();
    const server = els.modalServer.value.trim();
    const key = els.modalKey.value.trim();
    if (modalMode === "add") {
      send({ command: "add_output", pipeline: modalPipeline, name, server, key });
    } else {
      send({ command: "update_output", pipeline: modalPipeline, name: modalEditingName, new_name: name, server, key });
    }
  }
  function handleOutputResult(message) {
    if (els.modal.hidden) return;
    if (message.ok) closePlatformModal();
    else showModalErrors(message.errors || {});
  }
  function showModalErrors(errors) {
    els.modalErrors.innerHTML = "";
    for (const [field, text] of Object.entries(errors)) {
      const line = document.createElement("div");
      line.textContent = `${field}: ${text}`;
      els.modalErrors.appendChild(line);
    }
    els.modalErrors.hidden = Object.keys(errors).length === 0;
  }
  function hideModalErrors() { els.modalErrors.hidden = true; }

  // --- pipeline modal ---

  // Resolve a source live_path to its pipeline name for display.
  function srcName(livePath) {
    const cands = (settingsData && settingsData.source_candidates) || [];
    const m = cands.find((c) => c.live_path === livePath);
    return m ? m.name : (livePath || "?");
  }

  function populateSourceSelect(select, selectedPath) {
    select.innerHTML = "";
    for (const c of (settingsData && settingsData.source_candidates) || []) {
      const opt = document.createElement("option");
      opt.value = c.live_path;
      opt.textContent = `${c.name} (${c.type})`;
      if (c.live_path === selectedPath) opt.selected = true;
      select.appendChild(opt);
    }
  }

  // Show only the fields a given pipeline type needs.
  function applyPmodalType(type) {
    const isInput = type === "input";
    const isRemux = type === "remux";
    els.pmodalRemuxField.hidden = !isRemux;
    els.pmodalTrimField.hidden = !isRemux;   // audio trim calibration is remux-only
    els.pmodalBackupField.hidden = isInput;  // input has no backup/output half
  }

  function openPipelineModal(mode, pipeline, presetType) {
    pmodalMode = mode;
    pmodalEditingName = mode === "edit" ? pipeline.name : null;
    const type = mode === "edit" ? (pipeline.type || "restream") : (presetType || "restream");
    pmodalType = type;
    // Type is fixed once created -> shown in the title, not as an editable field.
    els.pmodalTitle.textContent = mode === "edit"
      ? `Modify ${type} pipeline: ${pipeline.name}`
      : `Add ${type} pipeline`;
    els.pmodalName.value = mode === "edit" ? pipeline.name : "";

    // Remux sources: editable in the edit widget (this is where a freshly
    // created remux gets its two inputs assigned).
    populateSourceSelect(els.pmodalVideoSrc, mode === "edit" ? pipeline.video_src_path : null);
    populateSourceSelect(els.pmodalAudioSrc, mode === "edit" ? pipeline.audio_src_path : null);
    els.pmodalVideoSrc.disabled = false;
    els.pmodalAudioSrc.disabled = false;
    els.pmodalTrim.value = (mode === "edit" ? (pipeline.audio_trim_ms || 0) : 0);
    applyPmodalType(type);

    // New pipeline: default the backup file to the default pipeline's, since
    // a shared backup source is the common case (each pipeline still prepares
    // it under its own params); the operator can point it elsewhere. The
    // ingest path is assigned automatically by the controller (no field).
    const defaultPipe = (settingsData.pipelines || []).find((p) => p.is_default);
    els.pmodalBackup.value = mode === "edit"
      ? (pipeline.backup_file || "")
      : ((defaultPipe && defaultPipe.backup_file) || "");
    // OBS output (Server + Key) shown only when editing an existing pipeline
    // (the path is assigned on creation, so there's nothing to show on Add).
    if (mode === "edit" && pipeline.ingest_key) {
      els.pmodalIngestField.hidden = false;
      pmodalServerRaw = pipeline.ingest_server || "";
      pmodalKeyRaw = pipeline.ingest_key;
      pmodalIngestShown = false;
      renderModalIngest();
    } else {
      els.pmodalIngestField.hidden = true;
      pmodalServerRaw = "";
      pmodalKeyRaw = "";
      pmodalIngestShown = false;
    }
    hidePipelineModalErrors();
    els.pmodal.hidden = false;
    els.pmodalName.focus();
  }
  function renderModalIngest() {
    // Server (host:port -- potentially the VPS IP) and the Stream key are
    // both sensitive -> FULLY masked by default (partial key masking is
    // pointless when the server line is hidden whole), one Show toggle.
    els.pmodalIngestServer.textContent = pmodalServerRaw ? (pmodalIngestShown ? pmodalServerRaw : "••••••••••••") : "–";
    els.pmodalIngestKey.textContent = pmodalKeyRaw ? (pmodalIngestShown ? pmodalKeyRaw : "••••••••••••") : "–";
    els.pmodalIngestShow.textContent = pmodalIngestShown ? "Hide" : "Show";
  }
  function closePipelineModal() {
    els.pmodal.hidden = true;
    pmodalMode = null; pmodalEditingName = null;
  }
  function submitPipelineModal() {
    hidePipelineModalErrors();
    const name = els.pmodalName.value.trim();
    const backup_file = els.pmodalBackup.value.trim();
    // The edit widget is the only place that submits full config. On rename,
    // `name` must stay the OLD name (the lookup key); the new one goes in
    // `new_name`. For remux the two source paths travel too.
    const payload = { command: "update_pipeline", name: pmodalEditingName, new_name: name, backup_file };
    if (pmodalType === "remux") {
      payload.video_src_path = els.pmodalVideoSrc.value;
      payload.audio_src_path = els.pmodalAudioSrc.value;
    }
    send(payload);
  }
  function handlePipelineResult(message) {
    // Create widget open -> this is a create result. On success we close it and
    // let the edit widget auto-open once the refreshed settings arrive.
    if (!els.pcreateModal.hidden) {
      if (message.ok) {
        closeCreateModal();
      } else {
        pendingEditName = null;
        showErrorsInto(els.pcreateErrors, message.errors || {});
      }
      return;
    }
    if (els.pmodal.hidden) return;
    if (message.ok) closePipelineModal();
    else showPipelineModalErrors(message.errors || {});
  }

  function showErrorsInto(container, errors) {
    container.innerHTML = "";
    for (const [field, text] of Object.entries(errors)) {
      const line = document.createElement("div");
      line.textContent = field === "_" ? text : `${field}: ${text}`;
      container.appendChild(line);
    }
    container.hidden = Object.keys(errors).length === 0;
  }

  // --- create widget (type + name) ---

  const CREATE_TYPE_DESC = {
    restream: "Own RTMP input → one or more platforms (Twitch, YouTube, …), with a backup video.",
    input: "A named ingest path with no platforms — used only as a source for a remux (e.g. a clean audio feed).",
    remux: "Video taken from one input + audio from another, sent to platforms, with a backup video.",
  };
  function updateCreateTypeDesc() {
    els.pcreateTypeDesc.textContent = CREATE_TYPE_DESC[els.pcreateType.value] || "";
  }
  function openCreateModal() {
    els.pcreateType.value = "restream";
    els.pcreateName.value = "";
    updateCreateTypeDesc();
    els.pcreateErrors.hidden = true;
    els.pcreateModal.hidden = false;
    els.pcreateName.focus();
  }
  function closeCreateModal() { els.pcreateModal.hidden = true; }
  function submitCreate() {
    els.pcreateErrors.hidden = true;
    const type = els.pcreateType.value;
    const name = els.pcreateName.value.trim();
    if (!name) { showErrorsInto(els.pcreateErrors, { name: "name is required" }); return; }
    // Remember the name so its edit widget opens automatically on success.
    pendingEditName = name;
    if (type === "input") send({ command: "add_input_pipeline", name });
    else if (type === "remux") send({ command: "add_remux_pipeline", name });
    else send({ command: "add_pipeline", name });
  }

  // Once the settings refresh that follows a successful create arrives, open the
  // freshly created pipeline's edit widget so it can be configured.
  function maybeOpenPendingEdit() {
    if (!pendingEditName || !settingsData) return;
    const p = (settingsData.pipelines || []).find((x) => x.name === pendingEditName);
    pendingEditName = null;
    if (p) openPipelineModal("edit", p);
  }
  function showPipelineModalErrors(errors) {
    els.pmodalErrors.innerHTML = "";
    for (const [field, text] of Object.entries(errors)) {
      const line = document.createElement("div");
      line.textContent = `${field}: ${text}`;
      els.pmodalErrors.appendChild(line);
    }
    els.pmodalErrors.hidden = Object.keys(errors).length === 0;
  }
  function hidePipelineModalErrors() { els.pmodalErrors.hidden = true; }

  // --- listeners ---

  els.modalServer.addEventListener("input", updateModalPreview);
  els.modalKey.addEventListener("input", updateModalPreview);
  els.modalSecretsShow.addEventListener("click", () => { modalSecretsShown = !modalSecretsShown; syncModalMask(); });
  els.modalOk.addEventListener("click", submitPlatformModal);
  els.modalCancel.addEventListener("click", closePlatformModal);
  els.modal.addEventListener("click", (e) => { if (e.target === els.modal) closePlatformModal(); });

  els.pmodalIngestShow.addEventListener("click", () => { pmodalIngestShown = !pmodalIngestShown; renderModalIngest(); });
  // Audio trim applies live (set once by eye while watching the stream) --
  // independent of the OK button, which saves name/backup/sources.
  els.pmodalTrim.addEventListener("change", () => {
    if (pmodalMode !== "edit" || pmodalType !== "remux" || !pmodalEditingName) return;
    let v = Math.round(Number(els.pmodalTrim.value) || 0);
    v = Math.max(-2000, Math.min(2000, v));
    els.pmodalTrim.value = v;
    send({ command: "set_audio_trim", pipeline: pmodalEditingName, audio_trim_ms: v });
  });
  els.pmodalOk.addEventListener("click", submitPipelineModal);
  els.pmodalCancel.addEventListener("click", closePipelineModal);
  els.pmodal.addEventListener("click", (e) => { if (e.target === els.pmodal) closePipelineModal(); });

  // "Add pipeline" -> create widget (type + name). OK creates it, then its
  // edit widget opens automatically (maybeOpenPendingEdit) to configure the rest.
  els.btnAddPipeline.addEventListener("click", openCreateModal);
  els.pcreateType.addEventListener("change", updateCreateTypeDesc);
  els.pcreateOk.addEventListener("click", submitCreate);
  els.pcreateCancel.addEventListener("click", closeCreateModal);
  els.pcreateModal.addEventListener("click", (e) => { if (e.target === els.pcreateModal) closeCreateModal(); });
  els.pcreateName.addEventListener("keydown", (e) => { if (e.key === "Enter") submitCreate(); });

  els.btnApply.addEventListener("click", saveSettings);

  els.haltBtn.addEventListener("click", () => {
    if (confirm("Halt the broadcast now? This stops streaming to all platforms on all pipelines and tells OBS to stop streaming.")) {
      send({ command: "halt" });
    }
  });

  connect();
  setInterval(tickClocks, 1000);
})();
