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

  local obs_pass webhook_token port
  obs_pass="$(grep -A2 'user: obs' "${MEDIAMTX_YML}" | grep 'pass:' | awk '{print $2}')"
  webhook_token="$(cfg obs_webhook_token)"
  port="$(cfg listen_port)"

  echo "== Connection details =="
  echo
  echo "OBS -> Settings -> Stream -> Service: \"Custom...\":"
  echo "  Server:      rtmp://YOUR_VPS_IP:1935/live"
  echo "  Stream Key:  main?user=obs&pass=${obs_pass}"
  echo
  echo "OBS -> Tools -> Scripts -> obs-plugin/obs_graceful_stop.py:"
  echo "  URL:   http://YOUR_VPS_IP:${port}/obs/graceful-stop"
  echo "  Token: ${webhook_token}"
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
