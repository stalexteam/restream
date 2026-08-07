"""
HTTP-шар контролера: хуки MediaMTX (`/hooks/*`), локальний `/status`
для `restreamctl.sh`, і дашборд (`/dashboard`, `/obs-source`, статичні
асети, `/ws` — push-канал стану й канал команд).
"""

import hmac
import json
import logging
import select
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import mediamtx_control
import settings_store
import ws
from state_machine import STATE_OFFLINE

_STATIC_CONTENT_TYPES = {
    "dashboard.css": "text/css; charset=utf-8",
    "dashboard.js": "application/javascript; charset=utf-8",
}


def make_handler(controller, config: dict, hub, config_path: Path):
    dashboard_dir = controller.base_dir / "controller" / "dashboard"

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            logging.debug("http: " + fmt, *args)

        def _send(self, code: int, body: dict):
            payload = json.dumps(body).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _send_file(self, path, content_type: str, extra_headers: dict | None = None):
            try:
                data = path.read_bytes()
            except OSError:
                self._send(404, {"error": "not found"})
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            for name, value in (extra_headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(data)

        def _is_localhost(self) -> bool:
            return self.client_address[0] in ("127.0.0.1", "::1")

        def _check_dashboard_token(self, query: dict) -> bool:
            provided = query.get("token", [""])[0]
            return hmac.compare_digest(provided, config["dashboard_token"])

        def do_POST(self):
            # Зчитуємо і відкидаємо тіло запиту (хуки MediaMTX шлють form-дані,
            # нам вони не потрібні — весь контекст ми й так знаємо з конфігу).
            length = int(self.headers.get("Content-Length", 0))
            if length:
                self.rfile.read(length)

            path = urlsplit(self.path).path

            if path == "/hooks/available":
                if not self._is_localhost():
                    self._send(403, {"error": "forbidden"})
                    return
                controller.on_available()
                self._send(200, {"ok": True})
                return

            if path == "/hooks/unavailable":
                if not self._is_localhost():
                    self._send(403, {"error": "forbidden"})
                    return
                controller.on_unavailable()
                self._send(200, {"ok": True})
                return

            self._send(404, {"error": "not found"})

        def do_GET(self):
            parsed = urlsplit(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)

            if path == "/status":
                if not self._is_localhost():
                    self._send(403, {"error": "forbidden"})
                    return
                self._send(200, controller.status())
                return

            if path == "/dashboard":
                if not self._check_dashboard_token(query):
                    self._send(401, {"error": "invalid token"})
                    return
                self._send_file(
                    dashboard_dir / "index.html",
                    "text/html; charset=utf-8",
                    # Токен їде в query-рядку самого URL цієї сторінки --
                    # не даємо йому піти назовні через Referer, якщо
                    # колись з'явиться будь-яке зовнішнє посилання.
                    {"Referrer-Policy": "no-referrer"},
                )
                return

            if path == "/obs-source":
                # Компактна сторінка для OBS Browser Source в сцені --
                # лише статус-пігулка й кнопка ручного стопу, без вкладки
                # Settings. Той самий токен, що й /dashboard/-ws (одна
                # спільна межа авторизації для всього дашборд-шару).
                if not self._check_dashboard_token(query):
                    self._send(401, {"error": "invalid token"})
                    return
                self._send_file(
                    dashboard_dir / "obs-source.html",
                    "text/html; charset=utf-8",
                    {"Referrer-Policy": "no-referrer"},
                )
                return

            if path in ("/dashboard.css", "/dashboard.js"):
                # Без токен-гейту навмисно: самі по собі ці файли не
                # несуть ані стану, ані секретів, лише розмітку/логіку —
                # звичайна практика для статичних асетів authenticated-
                # сторінки. Захищені ресурси -- сама HTML-сторінка й /ws.
                filename = path.lstrip("/")
                self._send_file(dashboard_dir / filename, _STATIC_CONTENT_TYPES[filename])
                return

            if path == "/ws":
                if not self._check_dashboard_token(query):
                    self._send(401, {"error": "invalid token"})
                    return
                self._serve_ws()
                return

            self._send(404, {"error": "not found"})

        def _serve_ws(self):
            if not ws.handshake(self):
                self._send(400, {"error": "expected a WebSocket upgrade"})
                return

            # Ми беремо на себе весь подальший обмін цим з'єднанням
            # напряму (WS-фрейми) -- HTTP-шар більше не повинен чекати
            # на ньому наступний запит.
            self.close_connection = True

            write_lock = hub.register(self)
            # settimeout лишається для ЗАПИСІВ (PONG, push з hub-а).
            # Для читання свідомо НЕ покладаємось на цей тайм-аут --
            # повторний тайм-аут на тому самому BufferedReader псує
            # його внутрішній стан (io.BufferedReader.read() після
            # timeout кидає "cannot read from timed out object" на
            # наступному виклику) -- тому select() перед кожним
            # read(), а не except TimeoutError.
            self.connection.settimeout(1.0)
            try:
                while True:
                    ready, _, _ = select.select([self.connection], [], [], 1.0)
                    if not ready:
                        continue

                    try:
                        frame = ws.recv_frame(self)
                    except OSError:
                        break

                    if frame is None:
                        break

                    opcode, payload = frame
                    if opcode == ws.OPCODE_CLOSE:
                        break
                    if opcode == ws.OPCODE_PING:
                        try:
                            ws.send_pong(self, payload, write_lock)
                        except OSError:
                            break
                        continue
                    if opcode == ws.OPCODE_TEXT:
                        self._handle_command(payload, write_lock)
            finally:
                hub.unregister(self)
                ws.send_close(self, write_lock)

        def _handle_command(self, payload: bytes, write_lock):
            try:
                message = json.loads(payload.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                logging.warning("dashboard: malformed /ws command payload")
                return

            command = message.get("command") if isinstance(message, dict) else None
            if command == "stop_broadcast":
                controller.on_manual_stop()
            elif command == "obs_streaming_started":
                controller.on_obs_streaming_started()
            elif command == "get_settings":
                self._handle_get_settings(write_lock)
            elif command == "save_settings":
                self._handle_save_settings(message, write_lock)
            else:
                logging.warning("dashboard: unknown /ws command: %r", command)

        def _handle_get_settings(self, write_lock):
            data = settings_store.load_editable(config_path)
            ws.send_text(self, json.dumps({"type": "settings", "data": data}), write_lock)

        def _handle_save_settings(self, message: dict, write_lock):
            values = message.get("settings")
            if not isinstance(values, dict):
                ws.send_text(self, json.dumps({
                    "type": "settings_saved", "ok": False,
                    "errors": {"_": "malformed settings payload"},
                }), write_lock)
                return

            errors = settings_store.validate(values, controller.base_dir)
            if errors:
                ws.send_text(self, json.dumps({
                    "type": "settings_saved", "ok": False, "errors": errors,
                }), write_lock)
                return

            settings_store.save(config_path, values, controller.base_dir)
            logging.info("dashboard: settings saved (twitch_url/offline_timeout_sec/backup_file/connect_timeout_ms/read_timeout_ms)")
            ws.send_text(self, json.dumps({"type": "settings_saved", "ok": True}), write_lock)

            if message.get("restart"):
                current_state = controller.status()["state"]
                if current_state != STATE_OFFLINE:
                    logging.warning(
                        "dashboard: restarting while state=%s -- this ends the current broadcast",
                        current_state,
                    )
                # MediaMTX -- ЗАВЖДИ, незалежно від того, які саме з
                # 5 полів змінились: Apply & Restart і так уже означає
                # "трансляція зараз обірветься", тож зайвий bounce
                # MediaMTX нічого не змінює для глядача, а умовна
                # логіка "лише якщо connect/read_timeout_ms
                # змінились" -- складність, яку нема чим виправдати.
                # Синхронно, ДО рестарту самого контролера -- інакше
                # нове значення readTimeout ніколи не дійде до вже
                # запущеного MediaMTX.
                try:
                    mediamtx_control.sync_read_timeout(
                        controller.base_dir / "mediamtx.yml",
                        int(values["connect_timeout_ms"]),
                        int(values["read_timeout_ms"]),
                    )
                    mediamtx_control.restart_mediamtx(controller.base_dir)
                except Exception:
                    # Не вдалось перезапустити MediaMTX -- контролер
                    # усе одно продовжує свій рестарт: краще мати
                    # живий, досяжний контролер (з чіткою помилкою в
                    # логах про MediaMTX), ніж обидва процеси мертві
                    # без жодної можливості діагностики через веб.
                    logging.exception("dashboard: failed to restart mediamtx alongside the controller")

                if controller.request_restart is not None:
                    controller.request_restart()

    return Handler
