(() => {
  "use strict";

  const token = new URLSearchParams(location.search).get("token") || "";

  const state = {};
  const els = {
    toastContainer: document.getElementById("toast-container"),
    wsIndicator: document.getElementById("ws-indicator"),
    broadcastIndicator: document.getElementById("broadcast-indicator"),
    componentsBody: document.querySelector("#components-table tbody"),
    tabButtons: document.querySelectorAll(".tab-button"),
    tabStatus: document.getElementById("tab-status"),
    tabSettings: document.getElementById("tab-settings"),
    settingsForm: document.getElementById("settings-form"),
    fieldTwitchUrl: document.getElementById("field-twitch-url"),
    toggleTwitchUrl: document.getElementById("toggle-twitch-url"),
    fieldOfflineTimeout: document.getElementById("field-offline-timeout"),
    fieldBackup: document.getElementById("field-backup"),
    fieldConnectTimeout: document.getElementById("field-connect-timeout"),
    fieldReadTimeout: document.getElementById("field-read-timeout"),
    settingsErrors: document.getElementById("settings-errors"),
    btnApply: document.getElementById("btn-apply"),
    btnApplyRestart: document.getElementById("btn-apply-restart"),
  };

  const BROADCAST_LABELS = {
    OFFLINE: { text: "Offline", cls: "bstate-offline" },
    LIVE: { text: "Live", cls: "bstate-live" },
    FALLBACK: { text: "Backup", cls: "bstate-fallback" },
  };

  const COMPONENT_ORDER = ["mediamtx", "controller", "relay", "backup", "outbound"];

  const RECONNECT_MIN_DELAY_MS = 1000;
  const RECONNECT_MAX_DELAY_MS = 15000;
  const TOAST_DURATION_MS = 5000;

  let socket = null;
  let reconnectDelay = RECONNECT_MIN_DELAY_MS;
  let reconnectTimer = null;

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
    // Наведення миші -- призупиняємо таймер (не скидаємо), щоб можна
    // було спокійно виділити/скопіювати текст, не боячись, що тост
    // зникне посеред цього.
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
    if (currentToast) {
      currentToast.remove();
      currentToast = null;
    }
    showNextToast();
  }

  function wsUrl() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    return `${proto}//${location.host}/ws?token=${encodeURIComponent(token)}`;
  }

  const WS_SYMBOL = { connecting: "WS ⋯", live: "WS ✓", lost: "WS ✗" };
  const WS_TITLE = {
    connecting: "Connecting…",
    live: "Connected",
    lost: "Connection lost — retrying…",
  };

  function setConnectionStatus(status) {
    els.wsIndicator.className = "indicator ind-" + status;
    els.wsIndicator.textContent = WS_SYMBOL[status];
    els.wsIndicator.title = WS_TITLE[status];
    setWsControlsEnabled(status === "live");
  }

  function setWsControlsEnabled(enabled) {
    document.body.classList.toggle("ws-disconnected", !enabled);
    els.settingsForm.querySelectorAll("input, button").forEach((el) => {
      el.disabled = !enabled;
    });
  }

  function connect() {
    clearTimeout(reconnectTimer);
    setConnectionStatus("connecting");

    socket = new WebSocket(wsUrl());

    socket.onopen = () => {
      reconnectDelay = RECONNECT_MIN_DELAY_MS;
      setConnectionStatus("live");
      sendCommand("get_settings"); // завжди, не лише при перемиканні табу
    };

    socket.onmessage = (event) => {
      let message;
      try {
        message = JSON.parse(event.data);
      } catch (error) {
        return;
      }
      switch (message.type) {
        case "full":
          for (const key of Object.keys(state)) delete state[key];
          Object.assign(state, message.data);
          render();
          break;
        case "delta":
          Object.assign(state, message.data);
          render();
          break;
        case "settings":
          populateSettingsForm(message.data);
          break;
        case "settings_saved":
          handleSettingsSaved(message);
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
    els.wsIndicator.title = `Connection lost — retrying in ${seconds}s…`;
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

  function sendCommand(command) {
    send({ command });
  }

  function formatDuration(totalSeconds) {
    const seconds = Math.max(0, Math.floor(totalSeconds));
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    const pad = (n) => String(n).padStart(2, "0");
    return h > 0 ? `${h}:${pad(m)}:${pad(s)}` : `${m}:${pad(s)}`;
  }

  function render() {
    renderBroadcastIndicator();
    renderComponents();
  }

  function renderBroadcastIndicator() {
    const info = state.state === "OFFLINE" && state.halted
      ? { text: "Halt", cls: "bstate-halt" }
      : BROADCAST_LABELS[state.state] || { text: state.state || "?", cls: "bstate-offline" };
    els.broadcastIndicator.className = "indicator " + info.cls;
    els.broadcastIndicator.textContent = info.text;
  }

  function renderComponents() {
    const components = state.components || {};
    els.componentsBody.innerHTML = "";
    for (const name of COMPONENT_ORDER) {
      const c = components[name];
      if (!c) continue;
      const row = document.createElement("tr");

      const cells = [
        name,
        c.running ? "running" : "stopped",
        c.pid !== null && c.pid !== undefined ? String(c.pid) : "–",
        c.cpu_percent !== null && c.cpu_percent !== undefined ? `${c.cpu_percent}%` : "–",
        c.rss_mb !== null && c.rss_mb !== undefined ? `${c.rss_mb} MB` : "–",
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

  function tickClocks() {
    if (state.fallback_deadline) {
      const remaining = state.fallback_deadline - Date.now() / 1000;
      els.broadcastIndicator.title = remaining > 0
        ? `Backup video active -- stopping in ${formatDuration(remaining)} if OBS doesn't reconnect`
        : "stopping now…";
    } else if (state.state === "OFFLINE" && state.halted) {
      els.broadcastIndicator.title = "Stopped due to an error -- see the last toast for details";
    } else {
      els.broadcastIndicator.title = "";
    }
  }

  // --- tabs ---

  function switchTab(name) {
    els.tabButtons.forEach((btn) => btn.classList.toggle("active", btn.dataset.tab === name));
    els.tabStatus.hidden = name !== "status";
    els.tabSettings.hidden = name !== "settings";
    if (name === "settings") {
      hideSettingsMessages();
      sendCommand("get_settings");
    }
  }

  els.tabButtons.forEach((btn) => {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  });

  // --- settings ---

  function populateSettingsForm(data) {
    els.fieldTwitchUrl.value = data.twitch_url || "";
    els.fieldOfflineTimeout.value = data.offline_timeout_sec ?? "";
    els.fieldBackup.value = data.backup_file || "";
    els.fieldConnectTimeout.value = data.connect_timeout_ms ?? "";
    els.fieldReadTimeout.value = data.read_timeout_ms ?? "";
  }

  function collectSettings() {
    return {
      twitch_url: els.fieldTwitchUrl.value.trim(),
      offline_timeout_sec: Number(els.fieldOfflineTimeout.value),
      backup_file: els.fieldBackup.value.trim(),
      connect_timeout_ms: Number(els.fieldConnectTimeout.value),
      read_timeout_ms: Number(els.fieldReadTimeout.value),
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

  function hideSettingsMessages() {
    els.settingsErrors.hidden = true;
  }

  function handleSettingsSaved(message) {
    if (message.ok) {
      hideSettingsMessages();
    } else {
      showSettingsErrors(message.errors || {});
    }
  }

  function saveSettings(restart) {
    hideSettingsMessages();
    // state.state -- поточний broadcast state з push-каналу (не з
    // самого запиту, що збираємось послати): рестарт під час LIVE/
    // FALLBACK обриває трансляцію, попереджаємо ДО відправки команди.
    if (restart && state.state && state.state !== "OFFLINE") {
      if (!confirm("This will end the current broadcast. Continue?")) return;
    }
    send({ command: "save_settings", settings: collectSettings(), restart });
  }

  els.toggleTwitchUrl.addEventListener("click", () => {
    const revealed = els.fieldTwitchUrl.type === "text";
    els.fieldTwitchUrl.type = revealed ? "password" : "text";
    els.toggleTwitchUrl.textContent = revealed ? "Show" : "Hide";
  });

  els.btnApply.addEventListener("click", () => saveSettings(false));
  els.btnApplyRestart.addEventListener("click", () => saveSettings(true));

  connect();
  setInterval(tickClocks, 1000);
})();
