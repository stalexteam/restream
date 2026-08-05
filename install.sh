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
  OBS_PASS="(already set in mediamtx.yml)"
  INTERNAL_PASS="(already set in mediamtx.yml)"
fi

if [ ! -f "${BASE_DIR}/controller/config.json" ]; then
  echo "==> Creating controller/config.json"
  WEBHOOK_TOKEN="$(gen_secret)"
  # INTERNAL_PASS має збігатися з тим, що потрапив у mediamtx.yml саме
  # цього запуску — якщо mediamtx.yml вже існував, беремо пароль звідти.
  if [ "${INTERNAL_PASS}" = "(already set in mediamtx.yml)" ]; then
    INTERNAL_PASS="$(grep -A1 'user: internal' "${BASE_DIR}/mediamtx.yml" | grep 'pass:' | awk '{print $2}')"
  fi
  sed \
    -e "s/__OBS_WEBHOOK_TOKEN__/${WEBHOOK_TOKEN}/g" \
    -e "s/__INTERNAL_PASS__/${INTERNAL_PASS}/g" \
    -e "s#__BASE_DIR__#${BASE_DIR}#g" \
    "${BASE_DIR}/controller/config.example.json" > "${BASE_DIR}/controller/config.json"
else
  echo "==> controller/config.json already exists, skipping"
  WEBHOOK_TOKEN="(already set in controller/config.json)"
fi

if [ ! -f "${BASE_DIR}/backup/backup.mp4" ]; then
  echo "==> WARNING: ${BASE_DIR}/backup/backup.mp4 not found"
  echo "    Place a backup video there (H.264 + AAC), or change the path"
  echo "    in controller/config.json (backup_file field)."
fi

echo
echo "======================================================================"
echo "Installation complete."
echo
echo "Next, do this manually:"
echo "  1. Edit controller/config.json:"
echo "       - twitch_url   -> your real Twitch RTMP URL with the stream key"
echo "       - backup_file  -> path to the backup video (H.264 + AAC)"
echo "  2. Configure OBS:"
echo "       Server:      rtmp://YOUR_VPS_IP:1935/live/main"
echo "       Stream Key:  (empty -- auth is via login/password below)"
echo "       Login:       obs"
if [ "${OBS_PASS}" != "(already set in mediamtx.yml)" ]; then
  echo "       Password:    ${OBS_PASS}"
else
  echo "       Password:    see mediamtx.yml (user: obs)"
fi
echo "  3. Install obs-plugin/obs_graceful_stop.py in OBS (Tools -> Scripts)"
echo "     and fill in the controller URL and token:"
echo "       URL:   http://YOUR_VPS_IP:8790/obs/graceful-stop"
if [ "${WEBHOOK_TOKEN}" != "(already set in controller/config.json)" ]; then
  echo "       Token: ${WEBHOOK_TOKEN}"
else
  echo "       Token: see controller/config.json (obs_webhook_token)"
fi
echo "  4. Add backup/backup.mp4 (H.264 + AAC)."
echo
echo "Then:"
echo "  ./restreamctl.sh check   -- check that everything is ready to start"
echo "  ./restreamctl.sh start   -- start MediaMTX and the controller"
echo "  ./restreamctl.sh status  -- check the current state"
echo "======================================================================"
