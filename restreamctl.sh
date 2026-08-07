#!/usr/bin/env bash
#
# Керування restream-controller: перевірка конфігурації, старт/стоп,
# статус, логи. Розрахований на використання ПІСЛЯ install.sh.
#
# Використання: ./restreamctl.sh <check|start|stop|restart|status|logs>

set -uo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="${BASE_DIR}/controller/config.json"
MEDIAMTX_YML="${BASE_DIR}/mediamtx.yml"
MEDIAMTX_BIN="${BASE_DIR}/bin/mediamtx"
MEDIAMTX_PID_FILE="${BASE_DIR}/.mediamtx.pid"
CONTROLLER_PID_FILE="${BASE_DIR}/.controller.pid"
MEDIAMTX_LOG="${BASE_DIR}/mediamtx.log"

# Читає одне поле з controller/config.json (через python3 — уникаємо
# залежності від jq).
cfg() {
  python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get(sys.argv[2], ''))" "${CONFIG}" "$1" 2>/dev/null
}

pid_alive() {
  # $1 — файл з PID. Повертає 0, якщо процес з цим PID справді живий.
  [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null
}

# Ті самі мінімуми, що й у controller/settings_store.py (MIN_CONNECT_
# TIMEOUT_MS/MIN_READ_TIMEOUT_MS) -- продубльовані тут константами
# bash-скрипта, без спільного джерела правди між bash і Python.
MIN_CONNECT_TIMEOUT_MS=2500
MIN_READ_TIMEOUT_MS=300

# readTimeout у mediamtx.yml = connect_timeout_ms + read_timeout_ms з
# controller/config.json -- перераховується й підміняється в файлі
# перед КОЖНИМ стартом MediaMTX (тут -- ручний шлях через
# restreamctl.sh; шлях через дашборд робить те саме сам, у Python,
# controller/mediamtx_control.py). КЛАМП, не відмова: значення могли
# потрапити в config.json ручним редагуванням, минаючи валідацію
# Settings-вкладки дашборда -- одна занижена цифра в конфізі не
# повинна блокувати старт усього сервісу.
sync_mediamtx_read_timeout() {
  local connect_ms read_ms total_ms
  connect_ms="$(cfg connect_timeout_ms)"
  read_ms="$(cfg read_timeout_ms)"

  if [ -z "${connect_ms}" ] || ! [ "${connect_ms}" -ge "${MIN_CONNECT_TIMEOUT_MS}" ] 2>/dev/null; then
    if [ -n "${connect_ms}" ]; then
      echo "[WARNING] connect_timeout_ms (${connect_ms}) is below the minimum (${MIN_CONNECT_TIMEOUT_MS}ms) -- using ${MIN_CONNECT_TIMEOUT_MS}ms"
    fi
    connect_ms="${MIN_CONNECT_TIMEOUT_MS}"
  fi

  if [ -z "${read_ms}" ] || ! [ "${read_ms}" -ge "${MIN_READ_TIMEOUT_MS}" ] 2>/dev/null; then
    if [ -n "${read_ms}" ]; then
      echo "[WARNING] read_timeout_ms (${read_ms}) is below the minimum (${MIN_READ_TIMEOUT_MS}ms) -- using ${MIN_READ_TIMEOUT_MS}ms"
    fi
    read_ms="${MIN_READ_TIMEOUT_MS}"
  fi

  total_ms=$((connect_ms + read_ms))
  sed -i -E "s/^readTimeout:.*/readTimeout: ${total_ms}ms/" "${MEDIAMTX_YML}"
}

# --- check ---------------------------------------------------------------

cmd_check() {
  local has_error=0

  echo "== Configuration check =="

  if [ ! -f "${CONFIG}" ]; then
    echo "[ERROR] controller/config.json not found -- run ./install.sh first"
    return 1
  fi
  echo "[OK] controller/config.json found"

  if [ ! -f "${MEDIAMTX_YML}" ]; then
    echo "[ERROR] mediamtx.yml not found -- run ./install.sh first"
    has_error=1
  else
    echo "[OK] mediamtx.yml found"
  fi

  if [ ! -x "${MEDIAMTX_BIN}" ]; then
    echo "[ERROR] bin/mediamtx not found -- run ./install.sh first"
    has_error=1
  else
    echo "[OK] bin/mediamtx found"
  fi

  local twitch_url backup_file
  twitch_url="$(cfg twitch_url)"
  backup_file="$(cfg backup_file)"

  if [ "${twitch_url}" = "rtmp://live.twitch.tv/app/CHANGE_ME_STREAM_KEY" ] || [ -z "${twitch_url}" ]; then
    echo "[WARNING] twitch_url in controller/config.json hasn't been changed to a real Twitch key yet"
  else
    echo "[OK] twitch_url is set"
  fi

  if [ -z "${backup_file}" ] || [ ! -f "${backup_file}" ]; then
    echo "[ERROR] Backup video file not found: '${backup_file}'"
    echo "        Place a video file at this path, or change"
    echo "        backup_file in controller/config.json."
    has_error=1
  else
    echo "[OK] Backup video file found: ${backup_file}"
    if command -v ffprobe >/dev/null 2>&1; then
      # УВАГА: це перевірка лише "ffprobe взагалі бачить відео- й
      # аудіодоріжку і зчитав кодек" — файл читається і не зіпсований.
      # Це НЕ перевірка відповідності живому потоку OBS: роздільність,
      # fps, канали і навіть сам кодек порівнюються з реальними
      # параметрами тільки під час старту трансляції (коли OBS вже
      # підключений і є з чим звіряти) — тут порівнювати ще нема з чим.
      # Якщо щось не збіжиться — контролер перекодує заглушку сам,
      # автоматично, у фоні.
      local vcodec acodec
      vcodec="$(ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 "${backup_file}" 2>/dev/null)"
      acodec="$(ffprobe -v error -select_streams a:0 -show_entries stream=codec_name -of csv=p=0 "${backup_file}" 2>/dev/null)"
      if [ -n "${vcodec}" ]; then
        echo "[OK] Backup video track is readable (codec: ${vcodec})"
      else
        echo "[ERROR] No video track found in the backup file"
        has_error=1
      fi
      if [ -n "${acodec}" ]; then
        echo "[OK] Backup audio track is readable (codec: ${acodec})"
      else
        echo "[ERROR] No audio track found in the backup file"
        has_error=1
      fi
      echo "     (this doesn't check for a match with the live OBS stream -- that"
      echo "     happens automatically when the broadcast starts; the controller"
      echo "     will transcode the backup in the background on its own if needed)"
    fi
  fi

  echo
  if [ "${has_error}" -eq 0 ]; then
    echo "Check passed. You can start: ./restreamctl.sh start"
    echo "(OBS login/password and token: ./restreamctl.sh credentials)"
    return 0
  else
    echo "Errors found above -- fix them before starting."
    return 1
  fi
}

# --- start / stop / restart ----------------------------------------------

cmd_start() {
  if ! cmd_check; then
    echo
    echo "Start aborted because of the errors above."
    return 1
  fi
  echo

  if pid_alive "${MEDIAMTX_PID_FILE}"; then
    echo "MediaMTX is already running (pid=$(cat "${MEDIAMTX_PID_FILE}"))"
  else
    sync_mediamtx_read_timeout
    echo "Starting MediaMTX..."
    nohup "${MEDIAMTX_BIN}" "${MEDIAMTX_YML}" > "${MEDIAMTX_LOG}" 2>&1 < /dev/null &
    echo $! > "${MEDIAMTX_PID_FILE}"
    sleep 1
    if ! pid_alive "${MEDIAMTX_PID_FILE}"; then
      echo "[ERROR] MediaMTX failed to start, see ${MEDIAMTX_LOG}"
      tail -n 20 "${MEDIAMTX_LOG}" 2>/dev/null
      return 1
    fi
    echo "MediaMTX started (pid=$(cat "${MEDIAMTX_PID_FILE}"))"
  fi

  if pid_alive "${CONTROLLER_PID_FILE}"; then
    echo "Controller is already running (pid=$(cat "${CONTROLLER_PID_FILE}"))"
  else
    echo "Starting the controller..."
    # stdout/stderr тут навмисно НЕ пишемо в controller/controller.log —
    # controller.py сам веде цей файл через logging.FileHandler; якщо
    # спрямувати сюди ж і stdout (який дублює ті самі повідомлення через
    # StreamHandler), кожен рядок логувався б двічі.
    nohup python3 "${BASE_DIR}/controller/controller.py" "${CONFIG}" \
      > "${BASE_DIR}/controller/controller.stdout.log" 2>&1 < /dev/null &
    echo $! > "${CONTROLLER_PID_FILE}"
    sleep 1
    if ! pid_alive "${CONTROLLER_PID_FILE}"; then
      echo "[ERROR] Controller failed to start, see controller/controller.stdout.log"
      tail -n 20 "${BASE_DIR}/controller/controller.stdout.log" 2>/dev/null
      return 1
    fi
    echo "Controller started (pid=$(cat "${CONTROLLER_PID_FILE}"))"
  fi

  echo
  cmd_status
}

cmd_stop() {
  if pid_alive "${CONTROLLER_PID_FILE}"; then
    echo "Stopping the controller (pid=$(cat "${CONTROLLER_PID_FILE}"))..."
    kill "$(cat "${CONTROLLER_PID_FILE}")" 2>/dev/null
  fi
  rm -f "${CONTROLLER_PID_FILE}"

  if pid_alive "${MEDIAMTX_PID_FILE}"; then
    echo "Stopping MediaMTX (pid=$(cat "${MEDIAMTX_PID_FILE}"))..."
    kill "$(cat "${MEDIAMTX_PID_FILE}")" 2>/dev/null
  fi
  rm -f "${MEDIAMTX_PID_FILE}"

  echo "Stopped."
}

cmd_restart() {
  cmd_stop
  sleep 1
  cmd_start
}

# --- status / logs ---------------------------------------------------------

cmd_status() {
  echo "== Process status =="
  if pid_alive "${MEDIAMTX_PID_FILE}"; then
    echo "MediaMTX:   running (pid=$(cat "${MEDIAMTX_PID_FILE}"))"
  else
    echo "MediaMTX:   not running"
  fi
  if pid_alive "${CONTROLLER_PID_FILE}"; then
    echo "Controller: running (pid=$(cat "${CONTROLLER_PID_FILE}"))"
  else
    echo "Controller: not running"
  fi

  local port
  port="$(cfg listen_port)"
  if [ -n "${port}" ]; then
    echo
    echo "== Broadcast state (controller/config.json: listen_port=${port}) =="
    curl -s --max-time 3 "http://127.0.0.1:${port}/status" || echo "(controller is not responding)"
    echo
  fi
}

cmd_logs() {
  echo "== controller/controller.log (last 50 lines) =="
  tail -n 50 "${BASE_DIR}/controller/controller.log" 2>/dev/null || echo "(log not created yet)"
  echo
  echo "Individual ffmpeg process logs: controller/ffmpeg-{relay,backup,outbound}.log"
  echo "MediaMTX log: ${MEDIAMTX_LOG}"
}

# --- credentials -----------------------------------------------------------

cmd_credentials() {
  # install.sh друкує ці значення лише ОДИН раз, при першій генерації
  # (при повторному запуску показує "(вже задано в ...)" замість
  # реального значення). Ця команда натомість щоразу читає їх напряму
  # з mediamtx.yml/config.json — працює завжди, скільки завгодно разів.
  if [ ! -f "${MEDIAMTX_YML}" ] || [ ! -f "${CONFIG}" ]; then
    echo "[ERROR] Config files don't exist yet -- run ./install.sh first"
    return 1
  fi

  local obs_pass dashboard_token port public_host
  obs_pass="$(grep -A2 'user: obs' "${MEDIAMTX_YML}" | grep 'pass:' | awk '{print $2}')"
  dashboard_token="$(cfg dashboard_token)"
  port="$(cfg listen_port)"
  public_host="$(cfg public_host)"
  [ -z "${public_host}" ] && public_host="YOUR_VPS_IP"

  local bold cyan reset
  bold="\033[1m"
  cyan="\033[36m"
  reset="\033[0m"

  echo "== Connection details =="
  echo
  echo "Dashboard -- OBS -> Docks -> Custom Browser Docks (or just open it"
  echo "in any browser -- keep the URL private):"
  echo -e "  ${bold}${cyan}http://${public_host}:${port}/dashboard?token=${dashboard_token}${reset}"
  echo "  Settings tab: Twitch RTMP URL + Stream Key (Twitch Creator"
  echo "  Dashboard -> Settings -> Stream -> Primary Stream Key)."
  echo
  echo "OBS -> Settings -> Stream -> Service: \"Custom...\":"
  echo -e "  Server:      ${bold}${cyan}rtmp://${public_host}:1935/live${reset}"
  echo -e "  Stream Key:  ${bold}${cyan}main?user=obs&pass=${obs_pass}${reset}"
  echo
  echo "OBS -> add a Browser Source pointing at:"
  echo -e "  ${bold}${cyan}http://${public_host}:${port}/obs-source?token=${dashboard_token}${reset}"
  echo "Required for correctly detecting Start/Stop Streaming clicks. Set"
  echo "Page permission to \"Full access to OBS\" (recommended) so it can"
  echo "also stop the stream right away if something goes wrong at the"
  echo "start of the broadcast."
}

# --- entrypoint ------------------------------------------------------------

usage() {
  echo "Usage: $0 <check|start|stop|restart|status|logs|credentials>"
}

case "${1:-}" in
  check) cmd_check ;;
  start) cmd_start ;;
  stop) cmd_stop ;;
  restart) cmd_restart ;;
  status) cmd_status ;;
  logs) cmd_logs ;;
  credentials) cmd_credentials ;;
  *) usage; exit 1 ;;
esac
