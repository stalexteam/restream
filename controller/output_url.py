"""
Складання фінального push-URL виходу з двох полів, які площадка дає
користувачу окремо: `server` (RTMP/RTMPS URL) + `key` (stream key).
Єдине джерело правди — використовується і бекендом (state_machine при
побудові ffmpeg-команди, валідація, net_probe), і дашбордом (превʼю).
Тому логіка тут, а не в UI: правка `config.json` руками й плейсхолдер
install.sh проходять через ту саму збірку.

Нормалізація Kick/AWS IVS: панель Kick показує Server без порту й без
шляху `/app` (`rtmps://<id>.global-contribute.live-video.net/`), а
IVS-ingest вимагає `rtmps://<id>...:443/app/<key>`. Користувач це навряд
чи допише правильно — тож для хостів `*.live-video.net` підставляємо
`:443/app` самі. Ідемпотентно: вже правильний URL не змінюється.
"""

from urllib.parse import urlsplit, urlunsplit


def build_push_url(server: str, key: str) -> str:
    """Зібрати фінальний URL із server+key. Порожній key -> server це вже повний URL."""
    server = (server or "").strip()
    key = (key or "").strip()
    if not server:
        return ""
    url = server.rstrip("/") + "/" + key.lstrip("/") if key else server
    return _normalize_ivs(url)


def _normalize_ivs(url: str) -> str:
    parts = urlsplit(url)
    host = parts.hostname or ""
    if parts.scheme != "rtmps" or not (host == "live-video.net" or host.endswith(".live-video.net")):
        return url
    netloc = parts.netloc if parts.port is not None else f"{host}:443"
    path = parts.path
    if path != "/app" and not path.startswith("/app/"):
        path = "/app" + (path if path.startswith("/") else "/" + path)
    return urlunsplit((parts.scheme, netloc, path, parts.query, parts.fragment))
