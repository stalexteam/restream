#!/usr/bin/env bash
#
# Встановлення restream-controller: ffmpeg, MediaMTX, конфігурація з
# автогенерованими паролями, (опційно) systemd-юніти.
#
# Розрахований на Debian/Ubuntu (apt). Ідемпотентний: повторний запуск
# не перезаписує вже створені config.json / mediamtx.yml, щоб не
# затерти ваші ручні правки (twitch_url, backup_file тощо).

set -euo pipefail

MEDIAMTX_VERSION="v1.19.3"
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Project directory: ${BASE_DIR}"

echo "==> Installing system packages (ffmpeg, python3, curl, openssl)"
sudo apt-get update -qq
sudo apt-get install -y -qq ffmpeg python3 curl ca-certificates openssl

mkdir -p "${BASE_DIR}/bin" "${BASE_DIR}/backup" "${BASE_DIR}/controller"
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
  openssl rand -hex 16
}

if [ ! -f "${BASE_DIR}/mediamtx.yml" ]; then
  echo "==> Creating mediamtx.yml with new passwords"
  OBS_PASS="$(gen_secret)"
  INTERNAL_PASS="$(gen_secret)"
  sed \
    -e "s/__OBS_PASS__/${OBS_PASS}/g" \
    -e "s/__INTERNAL_PASS__/${INTERNAL_PASS}/g" \
    "${BASE_DIR}/mediamtx.yml.template" > "${BASE_DIR}/mediamtx.yml"
else
  echo "==> mediamtx.yml already exists, skipping (delete the file to regenerate)"
  INTERNAL_PASS="(already set in mediamtx.yml)"
fi

if [ ! -f "${BASE_DIR}/controller/config.json" ]; then
  echo "==> Creating controller/config.json"
  DASHBOARD_TOKEN="$(gen_secret)"
  # INTERNAL_PASS має збігатися з тим, що потрапив у mediamtx.yml саме
  # цього запуску — якщо mediamtx.yml вже існував, беремо пароль звідти.
  if [ "${INTERNAL_PASS}" = "(already set in mediamtx.yml)" ]; then
    INTERNAL_PASS="$(grep -A1 'user: internal' "${BASE_DIR}/mediamtx.yml" | grep 'pass:' | awk '{print $2}')"
  fi
  sed \
    -e "s/__DASHBOARD_TOKEN__/${DASHBOARD_TOKEN}/g" \
    -e "s/__INTERNAL_PASS__/${INTERNAL_PASS}/g" \
    -e "s/__PUBLIC_HOST__/YOUR_VPS_IP/g" \
    -e "s#__BASE_DIR__#${BASE_DIR}#g" \
    "${BASE_DIR}/controller/config.example.json" > "${BASE_DIR}/controller/config.json"
else
  echo "==> controller/config.json already exists, skipping"
  DASHBOARD_TOKEN="(already set in controller/config.json)"
fi

# public_host -- операційна адреса, не секрет (на відміну від
# паролів/токена вище): могла змінитись (переїзд на інший VPS, зміна
# DNS), тож питаємо ЩОРАЗУ, а не лише при першому створенні
# config.json. Оновлюємо ЛИШЕ це одне поле в уже наявному файлі --
# не перегенеровуємо його з шаблону повністю, щоб не затерти ручні
# правки twitch_url/backup_file/тощо.
CURRENT_PUBLIC_HOST="$(python3 -c "import json; print(json.load(open('${BASE_DIR}/controller/config.json')).get('public_host','') or '')" 2>/dev/null)"
[ "${CURRENT_PUBLIC_HOST}" = "YOUR_VPS_IP" ] && CURRENT_PUBLIC_HOST=""
# `|| true` -- без stdin (напр. `curl ... | bash`) read повертає
# ненульовий код на EOF, і `set -e` обірвав би install.sh на цьому
# місці; порожній ввід і так коректно падає до попереднього/дефолтного
# значення нижче.
read -rp "Public IP or hostname of this server [${CURRENT_PUBLIC_HOST:-leave empty to fill in later}]: " PUBLIC_HOST_INPUT || true
PUBLIC_HOST="${PUBLIC_HOST_INPUT:-${CURRENT_PUBLIC_HOST:-YOUR_VPS_IP}}"
python3 -c "
import json, os
path = '${BASE_DIR}/controller/config.json'
with open(path) as f:
    cfg = json.load(f)
cfg['public_host'] = '${PUBLIC_HOST}'
tmp = path + '.tmp'
with open(tmp, 'w') as f:
    json.dump(cfg, f, indent=2)
    f.write('\n')
os.replace(tmp, path)
"

# Значення для друку в кінці -- читаємо тепер, а не перегенеровуємо
# жодних файлів (obs-dock.html/file://-міст видалено, панель
# моніторингу й browser-source для ручного стопу віддаються прямо з
# HTTP-сервера контролера, окремих локальних файлів більше не треба).
read -r GEN_PUBLIC_HOST GEN_DASHBOARD_TOKEN GEN_PORT <<< "$(python3 -c "
import json
c = json.load(open('${BASE_DIR}/controller/config.json'))
print(c.get('public_host', 'YOUR_VPS_IP'), c.get('dashboard_token', ''), c.get('listen_port', 8790))
")"
DASHBOARD_URL="http://${GEN_PUBLIC_HOST}:${GEN_PORT}/dashboard?token=${GEN_DASHBOARD_TOKEN}"
OBS_SOURCE_URL="http://${GEN_PUBLIC_HOST}:${GEN_PORT}/obs-source?token=${GEN_DASHBOARD_TOKEN}"

if [ ! -f "${BASE_DIR}/backup/backup.mp4" ]; then
  echo "==> WARNING: ${BASE_DIR}/backup/backup.mp4 not found"
  echo "    Place a backup video there (any format ffmpeg can read), or change"
  echo "    the path in controller/config.json (backup_file field)."
fi

# Читаємо пароль напряму з mediamtx.yml (той самий підхід, що вже й
# так у restreamctl.sh credentials), а не з OBS_PASS цього запуску --
# інакше при вже наявному mediamtx.yml тут був би лише текст-заглушка
# "see mediamtx.yml", а не реальне значення, яке можна скопіювати.
FINAL_OBS_PASS="$(grep -A2 'user: obs' "${BASE_DIR}/mediamtx.yml" | grep 'pass:' | awk '{print $2}')"

BOLD="\033[1m"
CYAN="\033[36m"
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
echo "  3. Add the dashboard as an OBS -> Docks -> Custom Browser Dock"
echo "     (or just open it in any browser -- keep the URL private):"
echo -e "       ${BOLD}${CYAN}${DASHBOARD_URL}${RESET}"
echo "     On its Settings tab, set the Twitch RTMP URL + your Stream Key"
echo "     (Twitch Creator Dashboard -> Settings -> Stream -> Primary Stream"
echo "     Key) and hit Apply & Restart (nothing is streaming yet, so a"
echo "     restart here is free; plain Apply alone won't take effect until"
echo "     the next restart)."
echo "  4. Configure OBS -> Settings -> Stream -> Service: \"Custom...\":"
echo -e "       Server:      ${BOLD}${CYAN}rtmp://${GEN_PUBLIC_HOST}:1935/live${RESET}"
echo -e "       Stream Key:  ${BOLD}${CYAN}main?user=obs&pass=${FINAL_OBS_PASS}${RESET}"
echo "  5. Configure OBS -> add a Browser Source pointing at:"
echo -e "       ${BOLD}${CYAN}${OBS_SOURCE_URL}${RESET}"
echo "     Required for correctly detecting Start/Stop Streaming clicks."
echo "     Set Page permission to \"Full access to OBS\" (recommended) so"
echo "     it can also stop the stream right away if something goes wrong"
echo "     at the start of the broadcast."
echo
echo "(./restreamctl.sh check/status/logs are there if you need them later.)"
echo "======================================================================"
