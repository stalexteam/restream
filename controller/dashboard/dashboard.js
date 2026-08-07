(() => {
  "use strict";

  const token = new URLSearchParams(location.search).get("token") || "";

  const state = {};
  const els = {
    toastContainer: document.getElementById("toast-container"),
    wsIndicator: document.getElementById("ws-indicator"),
    sourceIndicator: document.getElementById("source-indicator"),
    obsIndicator: document.getElementById("obs-indicator"),
    broadcastIndicator: document.getElementById("broadcast-indicator"),
    haltBtn: document.getElementById("halt-btn"),
    componentsBody: document.querySelector("#components-table tbody"),
    controlBody: document.querySelector("#control-table tbody"),
    obsData: document.getElementById("obs-data"),
    obsVideo: document.getElementById("obs-video"),
    obsAudio: document.getElementById("obs-audio"),
    tabButtons: document.querySelectorAll(".tab-button"),
    tabStatus: document.getElementById("tab-status"),
    tabControl: document.getElementById("tab-control"),
    tabSettings: document.getElementById("tab-settings"),
    settingsForm: document.getElementById("settings-form"),
    platformsList: document.getElementById("platforms-list"),
    btnAddPlatform: document.getElementById("btn-add-platform"),
    platformTemplate: document.getElementById("platform-row-template"),
    fieldOfflineTimeout: document.getElementById("field-offline-timeout"),
    fieldBackup: document.getElementById("field-backup"),
    fieldConnectTimeout: document.getElementById("field-connect-timeout"),
    fieldReadTimeout: document.getElementById("field-read-timeout"),
    fieldIcmpPing: document.getElementById("field-icmp-ping"),
    settingsErrors: document.getElementById("settings-errors"),
    btnApply: document.getElementById("btn-apply"),
    modal: document.getElementById("platform-modal"),
    modalTitle: document.getElementById("platform-modal-title"),
    modalName: document.getElementById("modal-name"),
    modalServer: document.getElementById("modal-server"),
    modalKey: document.getElementById("modal-key"),
    modalPreview: document.getElementById("modal-preview"),
    modalErrors: document.getElementById("modal-errors"),
    modalOk: document.getElementById("modal-ok"),
    modalCancel: document.getElementById("modal-cancel"),
  };

  const COMPONENT_ORDER = ["mediamtx", "controller", "relay", "backup"];

  const RECONNECT_MIN_DELAY_MS = 1000;
  const RECONNECT_MAX_DELAY_MS = 15000;
  const TOAST_DURATION_MS = 5000;

  let socket = null;
  let reconnectDelay = RECONNECT_MIN_DELAY_MS;
  let reconnectTimer = null;
  // Останній список площадок із get_settings (для рендера й префілу модалки).
  let platforms = [];
  // Імена площадок, чий фінальний URL зараз показаний повністю (кнопка Show).
  const revealed = new Set();
  let modalMode = null;          // "add" | "edit" | null
  let modalEditingName = null;   // поточне ім'я площадки, яку редагуємо
  // Останні завантажені System-налаштування -- щоб на Apply зрозуміти,
  // чи змінились тайминги (єдина зміна, що рестартить MediaMTX і рве
  // ефір; для неї показуємо confirm).
  let loadedSettings = null;

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
    connecting: "WS — this dashboard's live link to the controller: connecting…",
    live: "WS — this dashboard's live link to the controller: connected",
    lost: "WS — this dashboard's live link to the controller: lost, retrying…",
  };

  function setConnectionStatus(status) {
    els.wsIndicator.className = "indicator ind-" + status;
    els.wsIndicator.textContent = WS_SYMBOL[status];
    els.wsIndicator.title = WS_TITLE[status];
    setWsControlsEnabled(status === "live");
    updateHaltButton();
  }

  function setWsControlsEnabled(enabled) {
    document.body.classList.toggle("ws-disconnected", !enabled);
    els.tabSettings.querySelectorAll("input, button").forEach((el) => {
      el.disabled = !enabled;
    });
    els.controlBody.querySelectorAll("input").forEach((el) => {
      el.disabled = !enabled;
    });
    els.modal.querySelectorAll("input, button").forEach((el) => {
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
      sendCommand("get_settings");
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
          loadedSettings = message.data;
          platforms = message.data.platforms || [];
          populateSystemForm(message.data);
          renderPlatforms();
          break;
        case "settings_saved":
          handleSettingsSaved(message);
          break;
        case "output_result":
          handleOutputResult(message);
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
    renderSource();
    renderObs();
    renderComponents();
    renderControl();
    updateHaltButton();
  }

  function updateHaltButton() {
    // Активна лише коли реально йде ефір (relay/backup) і є звʼязок із
    // контролером -- щоб можна було заглушити трансляцію звідусіль
    // (напр. із телефона, якщо ПК/OBS відпав, а backup крутиться).
    const wsLive = !document.body.classList.contains("ws-disconnected");
    const broadcasting = state.state === "LIVE" || state.state === "FALLBACK";
    els.haltBtn.disabled = !(wsLive && broadcasting);
  }

  function renderSource() {
    // Чи підключений невидимий obs-source.html (Browser Source) до
    // сервера. Поведінка як у WS: ✓/✗. Відсутність -- нейтральний
    // сірий (норма при перегляді в звичайному браузері, не в OBS).
    const connected = !!state.obs_source_connected;
    els.sourceIndicator.className = "indicator ind-" + (connected ? "live" : "connecting");
    els.sourceIndicator.textContent = connected ? "SRC ✓" : "SRC ✗";
    els.sourceIndicator.title = connected
      ? "SRC — the OBS browser-source (obs-source.html) is connected; Start/Stop detection and remote HALT work"
      : "SRC — the OBS browser-source (obs-source.html) is not connected (grey is normal in a plain browser; green only when it's added in OBS, README step 7). Without it, Start/Stop detection and telling OBS to stop won't work";
  }

  function anyEnabledDestination() {
    return (state.destinations || []).some((d) => d.enabled);
  }

  function renderBroadcastIndicator() {
    // Останній індикатор = "що уходить назовні" (доповнює OBS = вхід):
    //  OFFLINE  -- OBS не публікує, нікуди нічого;
    //  FAILURE  -- ефір зупинено через невалідні налаштування (halted);
    //  BACKUP   -- активна заглушка (fallback);
    //  ON AIR   -- OBS іде й транслюється хоч на одну площадку;
    //  IDLE     -- OBS іде, але всі галки зняті (нікуди не бродкастимо).
    let info;
    if (state.state === "OFFLINE") {
      if (state.manual_halt) {
        info = { text: "HALTED", cls: "bstate-halt" };
      } else if (state.halted) {
        info = { text: "FAILURE", cls: "bstate-halt" };
      } else {
        info = { text: "OFFLINE", cls: "bstate-offline" };
      }
    } else if (state.state === "FALLBACK") {
      info = { text: "BACKUP", cls: "bstate-fallback" };
    } else if (state.state === "LIVE") {
      info = anyEnabledDestination()
        ? { text: "ON AIR", cls: "bstate-live" }
        : { text: "IDLE", cls: "bstate-idle" };
    } else {
      info = { text: state.state || "?", cls: "bstate-offline" };
    }
    els.broadcastIndicator.className = "indicator " + info.cls;
    els.broadcastIndicator.textContent = info.text;
  }

  function renderObs() {
    const obs = state.obs || {};
    let status, symbol, title;
    if (obs.flowing) {
      status = "live"; symbol = "OBS ✓"; title = "OBS — video input into the VPS: flowing";
    } else if (state.state === "FALLBACK") {
      status = "lost"; symbol = "OBS ✗"; title = "OBS — video input into the VPS: none (OBS dropped, showing backup video)";
    } else {
      status = "connecting"; symbol = "OBS ✗"; title = "OBS — no video input from OBS into the VPS";
    }
    els.obsIndicator.className = "indicator ind-" + status;
    els.obsIndicator.textContent = symbol;

    // Тултип + панель Status: параметри живого потоку.
    const hasParams = obs.width && obs.height;
    const vparts = [];
    if (hasParams) vparts.push(`${obs.width}×${obs.height}@${obs.fps || "?"}`);
    if (obs.video_codec) vparts.push(obs.video_codec);
    if (obs.video_kbps !== undefined && obs.video_kbps !== null) vparts.push(`${obs.video_kbps} kbps`);
    const aparts = [];
    if (obs.audio_codec) aparts.push(obs.audio_codec);
    if (obs.audio_kbps !== undefined && obs.audio_kbps !== null) aparts.push(`${obs.audio_kbps} kbps`);

    els.obsData.textContent = obs.flowing ? "flowing" : "no data";
    els.obsData.className = obs.flowing ? "status-up" : "status-down";
    els.obsVideo.textContent = vparts.length ? vparts.join(" · ") : "–";
    els.obsAudio.textContent = aparts.length ? aparts.join(" · ") : "–";
    els.obsIndicator.title = vparts.length || aparts.length
      ? `${title}\n${[vparts.join(" · "), aparts.join(" · ")].filter(Boolean).join(" | ")}`
      : title;
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

  // --- control tab: per-platform toggles + metrics ---

  // Скільки секунд аптайму вважати вихід уже "Live" (а не "Connecting").
  // НЕ покладаємось на d.up (ever_ran_long): той виставляється лише
  // ПІСЛЯ виходу процесу, тож у живого потоку завжди false.
  const LIVE_MIN_SEC = 3;

  function destStatus(d) {
    if (!d.enabled) return { text: "Disabled", cls: "pill-off" };
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

  function renderControl() {
    const dests = state.destinations || [];
    const wsLive = !document.body.classList.contains("ws-disconnected");
    const seen = new Set();

    for (const d of dests) {
      seen.add(d.name);
      let row = els.controlBody.querySelector(`tr[data-name="${cssEscape(d.name)}"]`);
      if (!row) {
        row = document.createElement("tr");
        row.dataset.name = d.name;
        const tdName = document.createElement("td");
        tdName.className = "cell-name";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.className = "control-toggle";
        cb.addEventListener("change", () => {
          send({ command: cb.checked ? "enable_output" : "disable_output", name: d.name });
        });
        const nameSpan = document.createElement("span");
        nameSpan.className = "pf-label";
        tdName.append(cb, nameSpan);
        const tdStatus = document.createElement("td");
        const tdUptime = document.createElement("td");
        const tdHealth = document.createElement("td");
        const tdPing = document.createElement("td");
        row.append(tdName, tdStatus, tdUptime, tdHealth, tdPing);
        els.controlBody.appendChild(row);
      }

      const [tdName, tdStatus, tdUptime, tdHealth, tdPing] = row.children;
      const cb = tdName.querySelector(".control-toggle");
      // Не чіпаємо чекбокс, поки він у фокусі (щойно клікнутий) --
      // сервер підтвердить стан наступною дельтою.
      if (document.activeElement !== cb) cb.checked = d.enabled;
      cb.disabled = !wsLive;

      tdName.querySelector(".pf-label").textContent = d.name + (d.is_primary ? " (primary)" : "");
      const st = destStatus(d);
      tdStatus.innerHTML = "";
      const pill = document.createElement("span");
      pill.className = "pill " + st.cls;
      pill.textContent = st.text;
      tdStatus.appendChild(pill);
      tdUptime.textContent = d.running && d.uptime_sec ? formatDuration(d.uptime_sec) : "–";
      const h = destHealth(d);
      tdHealth.textContent = h.text;
      tdHealth.className = h.cls;
      tdPing.textContent = d.rtt_ms !== null && d.rtt_ms !== undefined ? `${d.rtt_ms} ms` : "–";
    }

    // прибираємо рядки видалених площадок
    for (const row of Array.from(els.controlBody.children)) {
      if (!seen.has(row.dataset.name)) row.remove();
    }
  }

  function cssEscape(s) {
    return (window.CSS && CSS.escape) ? CSS.escape(s) : s.replace(/["\\]/g, "\\$&");
  }

  const BADGE_HELP =
    "Broadcast — what's actually going out to the platforms:\n" +
    "• ON AIR — streaming live to at least one enabled platform\n" +
    "• BACKUP — OBS dropped; the backup video is playing on the platforms\n" +
    "• IDLE — OBS is publishing, but no platform is enabled (nothing goes out)\n" +
    "• OFFLINE — nothing is streaming\n" +
    "• FAILURE — stopped: none of the enabled platforms could be reached\n" +
    "• HALTED — stopped from the dashboard; won't auto-restart until OBS is stopped and started again";

  function tickClocks() {
    let title = BADGE_HELP;
    if (state.fallback_deadline) {
      const remaining = state.fallback_deadline - Date.now() / 1000;
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

  els.tabButtons.forEach((btn) => {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  });

  // клік по OBS-індикатору -> вкладка Status із деталями потоку
  els.obsIndicator.addEventListener("click", () => switchTab("status"));

  // --- settings: platforms list ---

  function hostOf(url) {
    // Власний розбір, НЕ new URL(): той не парсить нестандартні схеми
    // (rtmp/rtmps) у частині рушіїв і повертає порожній hostname.
    const m = /^[a-zA-Z][\w+.-]*:\/\/([^/?#]+)/.exec(url || "");
    if (!m) return "";
    let auth = m[1];
    const at = auth.lastIndexOf("@");
    if (at >= 0) auth = auth.slice(at + 1);
    return auth.replace(/:\d+$/, "");  // прибрати порт
  }

  function maskUrl(p) {
    // Показуємо лише реєстрований домен (2-й + 1-й рівень, напр.
    // twitch.tv / live-video.net / youtube.com) -- по ньому видно, що
    // за площадка; усе інше (схема, субдомен, шлях, ключ) під зірками:
    // ••••••twitch.tv••••••. "Останні дві мітки" -- груба, але достатня
    // евристика для ingest-хостів платформ. Show розкриває повний url.
    if (revealed.has(p.name)) return p.url || "";
    const labels = hostOf(p.url).split(".").filter(Boolean);
    const domain = labels.length >= 2 ? labels.slice(-2).join(".") : labels.join(".");
    const dots = "•".repeat(6);
    return domain ? `${dots}${domain}${dots}` : dots;
  }

  function renderPlatforms() {
    const wsLive = !document.body.classList.contains("ws-disconnected");
    els.platformsList.innerHTML = "";
    for (const p of platforms) {
      const frag = els.platformTemplate.content.cloneNode(true);
      const row = frag.querySelector(".platform-row");
      row.querySelector(".platform-name").textContent = p.name + (p.is_primary ? " (primary)" : "");
      const urlEl = row.querySelector(".platform-url");
      urlEl.textContent = maskUrl(p);
      // Розкритий url показуємо повністю (перенос + виділення одним
      // кліком), щоб було зручно скопіювати; замаскований -- в один
      // рядок з обрізанням.
      urlEl.classList.toggle("revealed", revealed.has(p.name));

      const showBtn = row.querySelector(".pf-show");
      showBtn.textContent = revealed.has(p.name) ? "Hide" : "Show";
      showBtn.disabled = !wsLive;
      showBtn.addEventListener("click", () => {
        if (revealed.has(p.name)) revealed.delete(p.name); else revealed.add(p.name);
        renderPlatforms();
      });

      const editBtn = row.querySelector(".pf-edit");
      editBtn.disabled = !wsLive;
      editBtn.addEventListener("click", () => openModal("edit", p));

      const delBtn = row.querySelector(".pf-delete");
      if (p.is_primary) {
        delBtn.remove();  // primary незнищенний
      } else {
        delBtn.disabled = !wsLive;
        delBtn.addEventListener("click", () => {
          if (confirm(`Delete platform "${p.name}"? This stops its stream immediately.`)) {
            send({ command: "remove_output", name: p.name });
          }
        });
      }
      els.platformsList.appendChild(row);
    }
  }

  // --- add / modify modal (applies immediately, no Apply) ---

  function normalizeIvs(url) {
    // Клієнтське дзеркало output_url._normalize_ivs -- лише для превʼю;
    // авторитетна збірка все одно на бекенді. Строковий розбір, НЕ
    // new URL() (той не парсить rtmps у частині рушіїв).
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

  function updateModalPreview() {
    els.modalPreview.textContent = buildPushUrl(els.modalServer.value, els.modalKey.value) || "–";
  }

  function openModal(mode, platform) {
    modalMode = mode;
    modalEditingName = mode === "edit" ? platform.name : null;
    els.modalTitle.textContent = mode === "edit" ? `Modify ${platform.name}` : "Add platform";
    els.modalName.value = mode === "edit" ? platform.name : "";
    els.modalServer.value = mode === "edit" ? (platform.server || "") : "";
    els.modalKey.value = mode === "edit" ? (platform.key || "") : "";
    hideModalErrors();
    updateModalPreview();
    els.modal.hidden = false;
    els.modalName.focus();
  }

  function closeModal() {
    els.modal.hidden = true;
    modalMode = null;
    modalEditingName = null;
  }

  function submitModal() {
    hideModalErrors();
    const name = els.modalName.value.trim();
    const server = els.modalServer.value.trim();
    const key = els.modalKey.value.trim();
    if (modalMode === "add") {
      send({ command: "add_output", name, server, key });
    } else {
      send({ command: "update_output", name: modalEditingName, new_name: name, server, key });
    }
    // Успіх -> сервер шле output_result(ok) + свіжі settings; модалку
    // закриває handleOutputResult. Помилка -> показуємо її в модалці.
  }

  function handleOutputResult(message) {
    if (els.modal.hidden) return;  // сторонній результат -- ігноруємо
    if (message.ok) closeModal();
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

  function hideModalErrors() {
    els.modalErrors.hidden = true;
  }

  // --- system settings block (Apply) ---

  function populateSystemForm(data) {
    els.fieldOfflineTimeout.value = data.offline_timeout_sec ?? "";
    els.fieldBackup.value = data.backup_file || "";
    els.fieldConnectTimeout.value = data.connect_timeout_ms ?? "";
    els.fieldReadTimeout.value = data.read_timeout_ms ?? "";
    els.fieldIcmpPing.checked = !!data.icmp_ping;
  }

  function collectSystem() {
    return {
      offline_timeout_sec: Number(els.fieldOfflineTimeout.value),
      backup_file: els.fieldBackup.value.trim(),
      connect_timeout_ms: Number(els.fieldConnectTimeout.value),
      read_timeout_ms: Number(els.fieldReadTimeout.value),
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

  function hideSettingsMessages() {
    els.settingsErrors.hidden = true;
  }

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
    if (timeoutsChanged && state.state && state.state !== "OFFLINE") {
      if (!confirm("Changing the connect/read timeout restarts MediaMTX and ends the current broadcast. Continue?")) return;
    }
    send({ command: "save_settings", settings: values });
  }

  els.modalServer.addEventListener("input", updateModalPreview);
  els.modalKey.addEventListener("input", updateModalPreview);
  els.modalOk.addEventListener("click", submitModal);
  els.modalCancel.addEventListener("click", closeModal);
  els.modal.addEventListener("click", (e) => { if (e.target === els.modal) closeModal(); });
  els.btnAddPlatform.addEventListener("click", () => openModal("add"));
  els.btnApply.addEventListener("click", saveSettings);

  els.haltBtn.addEventListener("click", () => {
    if (confirm("Halt the broadcast now? This stops streaming to all platforms and tells OBS to stop streaming.")) {
      send({ command: "halt" });
    }
  });

  connect();
  setInterval(tickClocks, 1000);
})();
