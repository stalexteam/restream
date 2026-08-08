#!/usr/bin/env bash
#
# Встановлення restream-controller: ffmpeg, MediaMTX, конфігурація з автогенерованими паролями, (опційно) systemd-юніти.
#
# Розрахований на Debian/Ubuntu (apt). Ідемпотентний: повторний запуск не перезаписує вже створений data/config.json, щоб не затерти ваші ручні правки (primary_server/primary_key, restreams, backup_file тощо). data/mediamtx.yml НЕ створюється тут -- це генерований артефакт (рендериться з controller/mediamtx.yml.template + config.json перед кожним стартом MediaMTX).

set -euo pipefail

MEDIAMTX_VERSION="v1.19.3"
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Project directory: ${BASE_DIR}"

echo "==> Installing system packages (ffmpeg, python3, curl)"
sudo apt-get update -qq
sudo apt-get install -y -qq ffmpeg python3 curl ca-certificates

DATA_DIR="${BASE_DIR}/data"
CONFIG="${DATA_DIR}/config.json"
mkdir -p "${BASE_DIR}/bin" "${BASE_DIR}/backup" "${BASE_DIR}/controller" "${DATA_DIR}" "${BASE_DIR}/logs"
chmod +x "${BASE_DIR}/restreamctl.sh" 2>/dev/null || true

if [ ! -x "${BASE_DIR}/bin/mediamtx" ]; then
  echo "==> Downloading MediaMTX ${MEDIAMTX_VERSION}"
  tmp_tar="$(mktemp)"
  curl -sL -o "${tmp_tar}" \
    "https://github.com/bluenviron/mediamtx/releases/download/${MEDIAMTX_VERSION}/mediamtx_${MEDIAMTX_VERSION}_linux_amd64.tar.gz"
  tar -xzf "${tmp_tar}" -C "${BASE_DIR}/bin" mediamtx
  rm -f "${tmp_tar}"
  chmod +x "${BASE_DIR}/bin/mediamtx"
else
  echo "==> MediaMTX already installed, skipping download"
fi

gen_secret() {
  # python3 і так обов'язкова залежність -- окремий openssl лише заради рандомного hex-ключа не потрібен (secrets -- CSPRNG зі stdlib).
  python3 -c "import secrets; print(secrets.token_hex(16))"
}

# config.json -- єдине джерело правди для секретів (обидва RTMP-паролі
# obs/internal + dashboard token). data/mediamtx.yml тут НЕ створюється:
# він рендериться з шаблону + config.json перед стартом MediaMTX.
if [ ! -f "${CONFIG}" ]; then
  echo "==> Creating data/config.json with new passwords"
  OBS_PASS="$(gen_secret)"
  INTERNAL_PASS="$(gen_secret)"
  DASHBOARD_TOKEN="$(gen_secret)"
  sed \
    -e "s/__OBS_PASS__/${OBS_PASS}/g" \
    -e "s/__INTERNAL_PASS__/${INTERNAL_PASS}/g" \
    -e "s/__DASHBOARD_TOKEN__/${DASHBOARD_TOKEN}/g" \
    -e "s/__PUBLIC_HOST__/YOUR_VPS_IP/g" \
    -e "s#__BASE_DIR__#${BASE_DIR}#g" \
    "${BASE_DIR}/controller/config.example.json" > "${CONFIG}"
else
  echo "==> data/config.json already exists, skipping"
  # Back-compat: installs from before variant B kept obs_pass only in
  # mediamtx.yml. If config.json has no obs_pass, adopt it from a legacy
  # mediamtx.yml (old root path or data/) so the existing OBS key keeps working.
  python3 - "${CONFIG}" "${BASE_DIR}/mediamtx.yml" "${DATA_DIR}/mediamtx.yml" <<'PYEOF'
import json, os, re, sys
cfg_path, *legacy = sys.argv[1:]
with open(cfg_path) as f:
    cfg = json.load(f)
if not cfg.get("obs_pass"):
    obs = ""
    for p in legacy:
        try:
            t = open(p, encoding="utf-8").read()
        except OSError:
            continue
        m = re.search(r"user:\s*obs\b.*?pass:\s*(\S+)", t, re.DOTALL)
        if m:
            obs = m.group(1); break
    if obs:
        cfg["obs_pass"] = obs
        tmp = cfg_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(cfg, f, indent=2); f.write("\n")
        os.replace(tmp, cfg_path)
        print("    migrated obs_pass from a legacy mediamtx.yml into data/config.json")
    else:
        print("    WARNING: config.json has no obs_pass and no legacy mediamtx.yml to adopt -- set obs_pass manually or delete config.json to regenerate")
PYEOF
fi

# public_host -- операційна адреса, не секрет (на відміну від паролів/токена вище): могла змінитись (переїзд на інший VPS, зміна DNS), тож питаємо ЩОРАЗУ, а не лише при першому створенні config.json. Оновлюємо ЛИШЕ це одне поле в уже наявному файлі -- не перегенеровуємо його з шаблону повністю, щоб не затерти ручні правки primary_server/primary_key/restreams/backup_file/тощо.
CURRENT_PUBLIC_HOST="$(python3 -c "import json; print(json.load(open('${CONFIG}')).get('public_host','') or '')" 2>/dev/null)"
[ "${CURRENT_PUBLIC_HOST}" = "YOUR_VPS_IP" ] && CURRENT_PUBLIC_HOST=""
# `|| true` -- без stdin (напр. `curl ... | bash`) read повертає ненульовий код на EOF, і `set -e` обірвав би install.sh на цьому місці; порожній ввід і так коректно падає до попереднього/дефолтного значення нижче.
read -rp "Public IP or hostname of this server [${CURRENT_PUBLIC_HOST:-leave empty to fill in later}]: " PUBLIC_HOST_INPUT || true
PUBLIC_HOST="${PUBLIC_HOST_INPUT:-${CURRENT_PUBLIC_HOST:-YOUR_VPS_IP}}"
python3 -c "
import json, os
path = '${CONFIG}'
with open(path) as f:
    cfg = json.load(f)
cfg['public_host'] = '${PUBLIC_HOST}'
tmp = path + '.tmp'
with open(tmp, 'w') as f:
    json.dump(cfg, f, indent=2)
    f.write('\n')
os.replace(tmp, path)
"

# Read the values needed both for the local files generated below and for the printed instructions at the end.
read -r GEN_PUBLIC_HOST GEN_DASHBOARD_TOKEN GEN_PORT <<< "$(python3 -c "
import json
c = json.load(open('${CONFIG}'))
print(c.get('public_host', 'YOUR_VPS_IP'), c.get('dashboard_token', ''), c.get('listen_port', 8790))
")"
DASHBOARD_URL="http://${GEN_PUBLIC_HOST}:${GEN_PORT}/dashboard?token=${GEN_DASHBOARD_TOKEN}"
OBS_WS_URL="ws://${GEN_PUBLIC_HOST}:${GEN_PORT}/ws?token=${GEN_DASHBOARD_TOKEN}"

# Two local files the user copies to the OBS machine (both regenerated on every run -- unlike config.json/mediamtx.yml they have no hand edits worth preserving, and the embedded URL changes with public_host/token). No `&` or `#` in these URLs (single hex-token query param), so a plain sed substitution is safe. obs-dock.html   -- Custom Browser Dock: holds the dashboard in an iframe with a "retrying…" screen while the server is unreachable (instead of OBS's bare "Couldn't load that page"). obs-source.html -- Browser Source (in a scene): standalone tracker that talks to /ws directly. Loading it as a local file:// keeps window.obsstudio reliable and lets the /ws URL be baked in (location.host is empty in file://).
sed \
  -e "s#__DASHBOARD_URL__#${DASHBOARD_URL}#g" \
  "${BASE_DIR}/controller/obs-dock.html.template" > "${BASE_DIR}/obs-dock.html"
sed \
  -e "s#__WS_URL__#${OBS_WS_URL}#g" \
  "${BASE_DIR}/controller/obs-source.html.template" > "${BASE_DIR}/obs-source.html"

# Restrict the secret-bearing files to the owner. data/ holds config.json
# (all passwords + token) and the generated mediamtx.yml (RTMP passwords), so
# lock the whole dir down; plus the two generated OBS files (URL/WS with the
# token). Harmless on a single-user VPS, important on a shared one. `|| true`
# -- never let a chmod failure abort the install.
chmod 700 "${DATA_DIR}" 2>/dev/null || true
chmod 600 \
  "${CONFIG}" \
  "${BASE_DIR}/obs-dock.html" \
  "${BASE_DIR}/obs-source.html" 2>/dev/null || true

if [ ! -f "${BASE_DIR}/backup/backup.mp4" ]; then
  echo "==> WARNING: ${BASE_DIR}/backup/backup.mp4 not found"
  echo "    Place a backup video there (any format ffmpeg can read), or change"
  echo "    the path in data/config.json (backup_file field)."
fi

# Читаємо obs-пароль напряму з config.json (єдине джерело правди, варіант Б),
# а не з OBS_PASS цього запуску -- інакше при вже наявному config.json тут не
# було б реального значення, яке можна скопіювати в OBS.
FINAL_OBS_PASS="$(python3 -c "import json; print(json.load(open('${CONFIG}')).get('obs_pass',''))" 2>/dev/null)"

BOLD="\033[1m"
CYAN="\033[36m"
RED="\033[31m"
RESET="\033[0m"

echo
echo "======================================================================"
echo "Installation complete."
echo
echo "Next, do this manually:"
echo "  1. Add your backup clip (ideally recorded in OBS with the same"
echo "     settings as your stream -- see README step 3) as:"
echo -e "       ${BOLD}${CYAN}${BASE_DIR}/backup/backup.mp4${RESET}"
echo "  2. Start the server:"
echo -e "       ${BOLD}${CYAN}./restreamctl.sh start${RESET}"
echo "  3. Add the dashboard as an OBS -> Docks -> Custom Browser Dock."
echo "     Copy this generated file to the OBS machine and point the dock"
echo "     at it via a local file path (see README step 7 for details):"
echo -e "       ${BOLD}${CYAN}${BASE_DIR}/obs-dock.html${RESET}"
echo "     On its Settings tab, set the primary platform's RTMP URL + stream"
echo "     key (e.g. Twitch: Creator Dashboard -> Settings -> Stream), add any"
echo "     extra restream platforms, and hit Apply. Use the Control tab to"
echo "     toggle each platform on/off live."
echo "  4. Configure OBS -> Settings -> Stream -> Service: \"Custom...\":"
echo -e "       Server:      ${BOLD}${CYAN}rtmp://${GEN_PUBLIC_HOST}:1935/live${RESET}"
echo -e "       Stream Key:  ${BOLD}${CYAN}main?user=obs&pass=${FINAL_OBS_PASS}${RESET}"
echo "     (This is the default pipeline. Extra pipelines are created in the dashboard.)"
echo "  5. Add a Browser Source (in a scene, size 32x32). Copy this"
echo "     generated file to the OBS machine and point the source at it"
echo "     via a local file path (see README step 7 for details):"
echo -e "       ${BOLD}${CYAN}${BASE_DIR}/obs-source.html${RESET}"
echo "     Required for correctly detecting Start/Stop Streaming clicks."
echo "     Set Page permission to \"Full access to OBS\" (recommended) so"
echo "     it can also stop the stream right away if something goes wrong"
echo "     at the start of the broadcast."
echo
echo -e "${BOLD}${RED}FIREWALL:${RESET}"
echo -e "${RED}  Ports you need to OPEN (ideally only to your own IP(s) -- the${RESET}"
echo -e "${RED}  token/RTMP password travel unencrypted over plain HTTP/RTMP):${RESET}"
echo -e "${RED}    ${BOLD}1935/tcp${RESET}${RED}  RTMP ingest from OBS${RESET}"
echo -e "${RED}    ${BOLD}8790/tcp${RESET}${RED}  dashboard + WebSocket (OBS dock/source, browser)${RESET}"
echo -e "${RED}  Checklist of ports that must NOT be open (already disabled in config):${RESET}"
echo -e "${RED}    ${BOLD}8554${RESET}${RED} RTSP   ${BOLD}8888${RESET}${RED} HLS   ${BOLD}8889${RESET}${RED} WebRTC   ${BOLD}8890${RESET}${RED} SRT   ${BOLD}8892${RESET}${RED} MoQ   ${BOLD}9997${RESET}${RED} control API${RESET}"
echo -e "${RED}  ufw: ${BOLD}ufw allow from <your-ip> to any port 1935,8790 proto tcp${RESET}"
echo
echo "(./restreamctl.sh check/status/logs are there if you need them later.)"
echo "======================================================================"
