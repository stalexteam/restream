#!/usr/bin/env bash
#
# Керування restream-controller: перевірка конфігурації, старт/стоп,
# статус, логи. Розрахований на використання ПІСЛЯ install.sh.
#
# Використання: ./restreamctl.sh <check|start|stop|restart|status|logs>

set -uo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${BASE_DIR}/data"
LOGS_DIR="${BASE_DIR}/logs"
CONFIG="${DATA_DIR}/config.json"
# data/mediamtx.yml -- generated artifact, rendered from the template +
# config.json before every MediaMTX start (render_mediamtx_yml below).
MEDIAMTX_TEMPLATE="${BASE_DIR}/controller/mediamtx.yml.template"
MEDIAMTX_YML="${DATA_DIR}/mediamtx.yml"
MEDIAMTX_BIN="${BASE_DIR}/bin/mediamtx"
MEDIAMTX_PID_FILE="${DATA_DIR}/.mediamtx.pid"
CONTROLLER_PID_FILE="${DATA_DIR}/.controller.pid"
MEDIAMTX_LOG="${LOGS_DIR}/mediamtx.log"

# Кольорові статус-мітки -- лише коли stdout це термінал, інакше порожні
# (щоб `check`/`status` у файл/пайп лишались без ESC-сміття).
if [ -t 1 ]; then
  _c_green=$'\033[32m'; _c_yellow=$'\033[33m'; _c_red=$'\033[31m'; _c_reset=$'\033[0m'
else
  _c_green=; _c_yellow=; _c_red=; _c_reset=
fi
TAG_OK="${_c_green}[OK]${_c_reset}"
TAG_WARN="${_c_yellow}[WARNING]${_c_reset}"
TAG_ERR="${_c_red}[ERROR]${_c_reset}"

# Читає одне поле з data/config.json (через python3 — уникаємо
# залежності від jq).
cfg() {
  python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get(sys.argv[2], ''))" "${CONFIG}" "$1" 2>/dev/null
}

# Читає одне поле з ДЕФОЛТНОГО пайплайна (pipelines[] з is_default, інакше
# перший). Fallback на старе плоске top-level поле -- для інсталяцій до
# розрізу на пайплайни (back-compat).
pcfg() {
  python3 -c "
import json,sys
c=json.load(open(sys.argv[1]))
key=sys.argv[2]
pls=c.get('pipelines')
if isinstance(pls,list) and pls:
    d=next((p for p in pls if p.get('is_default')), pls[0])
    v=d.get(key)
    if v not in (None,''):
        print(v); raise SystemExit
print(c.get(key,'') or '')
" "${CONFIG}" "$1" 2>/dev/null
}

pid_alive() {
  # $1 — файл з PID. Повертає 0, якщо процес з цим PID справді живий.
  [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null
}

# Рендер data/mediamtx.yml з шаблону + config.json перед КОЖНИМ стартом
# MediaMTX (варіант Б: config.json -- єдине джерело правди для паролів і
# таймаутів). Уся логіка (підстановка obs/internal паролів, readTimeout =
# connect+read з клампом до мінімумів) -- у controller/mediamtx_config.py,
# щоб bash і Python не дублювали правил. Шлях через дашборд рендерить те
# саме сам (controller/mediamtx_control.py).
render_mediamtx_yml() {
  python3 "${BASE_DIR}/controller/mediamtx_config.py" "${CONFIG}" "${MEDIAMTX_TEMPLATE}" "${MEDIAMTX_YML}"
}

# --- check ---------------------------------------------------------------

cmd_check() {
  local has_error=0

  echo "== Configuration check =="

  if [ ! -f "${CONFIG}" ]; then
    echo "${TAG_ERR} data/config.json not found -- run ./install.sh first"
    return 1
  fi
  echo "${TAG_OK} data/config.json found"

  # mediamtx.yml is generated at start from this template -- check the template.
  if [ ! -f "${MEDIAMTX_TEMPLATE}" ]; then
    echo "${TAG_ERR} controller/mediamtx.yml.template not found -- reinstall/re-clone the repo"
    has_error=1
  else
    echo "${TAG_OK} controller/mediamtx.yml.template found"
  fi

  if [ ! -x "${MEDIAMTX_BIN}" ]; then
    echo "${TAG_ERR} bin/mediamtx not found -- run ./install.sh first"
    has_error=1
  else
    echo "${TAG_OK} bin/mediamtx found"
  fi

  local primary_server primary_key backup_file
  # primary/backup now live inside the default pipeline (pcfg); pcfg also
  # falls back to the old flat top-level fields (pre-split installs).
  primary_server="$(pcfg primary_server)"
  [ -z "${primary_server}" ] && primary_server="$(pcfg primary_url)"
  [ -z "${primary_server}" ] && primary_server="$(pcfg twitch_url)"
  primary_key="$(pcfg primary_key)"
  backup_file="$(pcfg backup_file)"

  if [ "${primary_key}" = "CHANGE_ME_STREAM_KEY" ] || [ -z "${primary_server}" ]; then
    echo "${TAG_WARN} primary platform isn't set yet -- set its server URL + stream key in the dashboard Settings tab"
  else
    echo "${TAG_OK} primary platform is set"
  fi

  if [ -z "${backup_file}" ] || [ ! -f "${backup_file}" ]; then
    echo "${TAG_ERR} Backup video file not found: '${backup_file}'"
    echo "        Place a video file at this path, or change"
    echo "        backup_file in data/config.json."
    has_error=1
  else
    echo "${TAG_OK} Backup video file found: ${backup_file}"
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
        echo "${TAG_OK} Backup video track is readable (codec: ${vcodec})"
      else
        echo "${TAG_ERR} No video track found in the backup file"
        has_error=1
      fi
      if [ -n "${acodec}" ]; then
        echo "${TAG_OK} Backup audio track is readable (codec: ${acodec})"
      else
        echo "${TAG_ERR} No audio track found in the backup file"
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

  mkdir -p "${DATA_DIR}" "${LOGS_DIR}"

  if pid_alive "${MEDIAMTX_PID_FILE}"; then
    echo "MediaMTX is already running (pid=$(cat "${MEDIAMTX_PID_FILE}"))"
  else
    render_mediamtx_yml
    echo "Starting MediaMTX..."
    # cwd=data so any relative artifacts MediaMTX may create (auto.crt/key)
    # land in data/, not the project root. `exec nohup` keeps the subshell's
    # pid ($!) equal to the MediaMTX process pid.
    ( cd "${DATA_DIR}" && exec nohup "${MEDIAMTX_BIN}" "${MEDIAMTX_YML}" > "${MEDIAMTX_LOG}" 2>&1 < /dev/null ) &
    echo $! > "${MEDIAMTX_PID_FILE}"
    sleep 1
    if ! pid_alive "${MEDIAMTX_PID_FILE}"; then
      echo "${TAG_ERR} MediaMTX failed to start, see ${MEDIAMTX_LOG}"
      tail -n 20 "${MEDIAMTX_LOG}" 2>/dev/null
      return 1
    fi
    echo "MediaMTX started (pid=$(cat "${MEDIAMTX_PID_FILE}"))"
  fi

  if pid_alive "${CONTROLLER_PID_FILE}"; then
    echo "Controller is already running (pid=$(cat "${CONTROLLER_PID_FILE}"))"
  else
    echo "Starting the controller..."
    # stdout/stderr тут навмисно НЕ пишемо в logs/controller.log —
    # controller.py сам веде цей файл через logging.FileHandler; якщо
    # спрямувати сюди ж і stdout (який дублює ті самі повідомлення через
    # StreamHandler), кожен рядок логувався б двічі.
    nohup python3 "${BASE_DIR}/controller/controller.py" "${CONFIG}" \
      > "${LOGS_DIR}/controller.stdout.log" 2>&1 < /dev/null &
    echo $! > "${CONTROLLER_PID_FILE}"
    sleep 1
    if ! pid_alive "${CONTROLLER_PID_FILE}"; then
      echo "${TAG_ERR} Controller failed to start, see logs/controller.stdout.log"
      tail -n 20 "${LOGS_DIR}/controller.stdout.log" 2>/dev/null
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
    echo "== Broadcast state (data/config.json: listen_port=${port}) =="
    curl -s --max-time 3 "http://127.0.0.1:${port}/status" || echo "(controller is not responding)"
    echo
  fi
}

cmd_logs() {
  echo "== logs/controller.log (last 50 lines) =="
  tail -n 50 "${LOGS_DIR}/controller.log" 2>/dev/null || echo "(log not created yet)"
  echo
  echo "Individual ffmpeg process logs (per pipeline): logs/ffmpeg-relay-<pipeline>.log,"
  echo "ffmpeg-backup-<pipeline>.log, and one per output platform:"
  echo "logs/ffmpeg-out-<pipeline>-<name>.log"
  echo "MediaMTX log: ${MEDIAMTX_LOG}"
}

# --- credentials -----------------------------------------------------------

cmd_credentials() {
  # install.sh друкує ці значення лише ОДИН раз, при першій генерації
  # (при повторному запуску показує "(вже задано в ...)" замість
  # реального значення). Ця команда натомість щоразу читає їх напряму
  # з config.json — працює завжди, скільки завгодно разів.
  if [ ! -f "${CONFIG}" ]; then
    echo "${TAG_ERR} data/config.json doesn't exist yet -- run ./install.sh first"
    return 1
  fi

  local obs_pass dashboard_token port public_host
  obs_pass="$(cfg obs_pass)"
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
  echo "  Settings tab: primary platform RTMP URL (+ stream key) and extra"
  echo "  restream platforms. Control tab: toggle each platform on/off live."
  echo
  echo "  Tip: for a dock that survives the server being down (retries"
  echo "  instead of a bare error page), copy this generated wrapper to"
  echo "  the OBS machine and point the dock at it via a local file path:"
  echo -e "  ${bold}${cyan}${BASE_DIR}/obs-dock.html${reset}"
  echo
  local default_path
  default_path="$(pcfg live_path)"
  [ -z "${default_path}" ] && default_path="live/main"
  echo "OBS -> Settings -> Stream -> Service: \"Custom...\":"
  echo -e "  Server:      ${bold}${cyan}rtmp://${public_host}:1935/live${reset}"
  echo -e "  Stream Key:  ${bold}${cyan}${default_path#live/}?user=obs&pass=${obs_pass}${reset}"
  echo "  (This is the default pipeline. Extra pipelines are created in the"
  echo "  dashboard, which shows each one's ready-to-copy Stream Key -- the"
  echo "  ingest path is assigned automatically.)"
  echo
  echo "OBS -> add a Browser Source (in a scene, size 32x32). Copy this"
  echo "generated file to the OBS machine and point the source at it via a"
  echo "local file path:"
  echo -e "  ${bold}${cyan}${BASE_DIR}/obs-source.html${reset}"
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
