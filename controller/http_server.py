"""
HTTP-шар контролера: хуки MediaMTX (`/hooks/*`), локальний `/status`
для `restreamctl.sh`, і дашборд (`/dashboard`, статичні асети,
`/ws` — push-канал стану й канал команд). OBS-трекер (obs-source.html)
більше не віддається сервером -- це автономний локальний файл, що
підключається напряму до `/ws` (див. controller/obs-source.html.template).
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

_STATIC_CONTENT_TYPES = {
    "dashboard.css": "text/css; charset=utf-8",
    "dashboard.js": "application/javascript; charset=utf-8",
}

# Ліміт тіла POST-запиту. Хуки MediaMTX шлють крихітні form-дані; усе
# більше -- зловживання. Без цього ліміту зовнішній клієнт міг би
# оголосити величезний Content-Length і змусити сервер читати його в
# пам'ять ЩЕ ДО перевірки маршруту/localhost (memory-DoS).
_MAX_REQUEST_BODY = 65536


def make_handler(controller, config: dict, hub, config_path: Path):
    dashboard_dir = controller.base_dir / "controller" / "dashboard"

    class Handler(BaseHTTPRequestHandler):
        # Обмежує читання самого запиту (рядок запиту + заголовки, і наше
        # читання тіла нижче). Без нього з'єднання, яке відкрилось і нічого
        # (або дуже повільно) не шле, тримало б потік ThreadingHTTPServer
        # вічно -- дешевий неавтентифікований slowloris-DoS. Для /ws після
        # хендшейку керування таймінгом бере на себе _serve_ws (select).
        timeout = 15

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
            # Спершу перевіряємо/обмежуємо розмір, і лише потім читаємо в
            # пам'ять -- захист від memory-DoS оголошеним велетенським тілом.
            try:
                length = int(self.headers.get("Content-Length", 0) or 0)
            except ValueError:
                self._send(400, {"error": "bad Content-Length"})
                return
            if length < 0 or length > _MAX_REQUEST_BODY:
                self._send(413, {"error": "request body too large"})
                return
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
            if command == "register_source":
                # obs-source.html представляється при коннекті (з id своєї
                # сесії OBS) -> hub рахує це з'єднання для індикатора Source,
                # контролер оновлює "останню відому сесію". Якщо саме ця
                # сесія заглушена (HALT) -- точково кажемо цьому джерелу
                # зупинити OBS (кейс: source повернувся ПІСЛЯ обриву).
                obs_id = message.get("obs_id")
                hub.mark_source(self)
                controller.report_obs_session(obs_id)
                if controller.is_session_halted(obs_id):
                    ws.send_text(self, json.dumps({"type": "control", "action": "stop_streaming"}), write_lock)
            elif command == "stop_broadcast":
                controller.on_manual_stop()
            elif command == "halt":
                controller.on_dashboard_halt()
            elif command == "obs_streaming_started":
                # Реальний старт стриму OBS -> нова сесія з новим id.
                controller.report_obs_session(message.get("obs_id"))
                controller.on_obs_streaming_started()
            elif command == "get_settings":
                self._handle_get_settings(write_lock)
            elif command == "save_settings":
                self._handle_save_settings(message, write_lock)
            elif command == "enable_output":
                name = message.get("name")
                if isinstance(name, str):
                    controller.enable_destination(name)
            elif command == "disable_output":
                name = message.get("name")
                if isinstance(name, str):
                    controller.disable_destination(name)
            elif command == "add_output":
                self._handle_add_output(message, write_lock)
            elif command == "update_output":
                self._handle_update_output(message, write_lock)
            elif command == "remove_output":
                self._handle_remove_output(message, write_lock)
            else:
                logging.warning("dashboard: unknown /ws command: %r", command)

        def _output_names(self):
            return [p["name"] for p in controller.outputs_for_settings()]

        def _reply_output(self, ok: bool, errors: dict, write_lock):
            ws.send_text(self, json.dumps({"type": "output_result", "ok": ok, "errors": errors}), write_lock)

        def _handle_get_settings(self, write_lock):
            # System-поля + список площадок (server/key для модалки Modify,
            # зібраний url для маскованого показу в списку).
            data = settings_store.load_editable(config_path)
            data["platforms"] = controller.outputs_for_settings()
            ws.send_text(self, json.dumps({"type": "settings", "data": data}), write_lock)

        def _handle_add_output(self, message: dict, write_lock):
            name = (message.get("name") or "").strip()
            server = (message.get("server") or "").strip()
            key = (message.get("key") or "").strip()
            errors = settings_store.validate_output(name, server, key, self._output_names())
            if errors:
                self._reply_output(False, errors, write_lock)
                return
            controller.add_destination(name, server, key)
            self._reply_output(True, {}, write_lock)
            self._handle_get_settings(write_lock)

        def _handle_update_output(self, message: dict, write_lock):
            old = message.get("name")
            new_name = (message.get("new_name") or "").strip()
            server = (message.get("server") or "").strip()
            key = (message.get("key") or "").strip()
            if not isinstance(old, str) or old not in self._output_names():
                self._reply_output(False, {"_": "unknown platform"}, write_lock)
                return
            # rename дозволено на будь-яке вільне ім'я -> виключаємо власне старе
            existing = [n for n in self._output_names() if n != old]
            errors = settings_store.validate_output(new_name, server, key, existing)
            if errors:
                self._reply_output(False, errors, write_lock)
                return
            controller.update_destination(old, new_name, server, key)
            self._reply_output(True, {}, write_lock)
            self._handle_get_settings(write_lock)

        def _handle_remove_output(self, message: dict, write_lock):
            name = message.get("name")
            if isinstance(name, str):
                controller.remove_destination(name)
            self._handle_get_settings(write_lock)

        def _handle_save_settings(self, message: dict, write_lock):
            values = message.get("settings")
            if not isinstance(values, dict):
                ws.send_text(self, json.dumps({
                    "type": "settings_saved", "ok": False,
                    "errors": {"_": "malformed settings payload"},
                }), write_lock)
                return

            errors = settings_store.validate_system(values, controller.base_dir)
            if errors:
                ws.send_text(self, json.dumps({
                    "type": "settings_saved", "ok": False, "errors": errors,
                }), write_lock)
                return

            # Тайминги MediaMTX (connect/read) неможливо застосувати без
            # рестарту самого MediaMTX -- фіксуємо, чи вони змінились, ДО
            # apply_settings (яке оновлює in-memory config). backup_file/
            # offline_timeout apply_settings застосовує точково, живцем.
            # (Площадки тут не при чому -- окремі negайні команди.)
            old_connect = controller.config.get("connect_timeout_ms")
            old_read = controller.config.get("read_timeout_ms")

            controller.apply_settings(values)
            logging.info("dashboard: system settings saved and applied (timeouts/backup)")
            ws.send_text(self, json.dumps({"type": "settings_saved", "ok": True}), write_lock)

            new_connect = int(values["connect_timeout_ms"])
            new_read = int(values["read_timeout_ms"])
            if new_connect != old_connect or new_read != old_read:
                current_state = controller.status()["state"]
                if current_state != "OFFLINE":
                    logging.warning(
                        "dashboard: applying connect/read timeout while state=%s -- "
                        "restarting MediaMTX ends the current broadcast",
                        current_state,
                    )
                try:
                    mediamtx_control.sync_read_timeout(
                        controller.base_dir / "mediamtx.yml", new_connect, new_read,
                    )
                    mediamtx_control.restart_mediamtx(controller.base_dir)
                except Exception:
                    # Не вдалось перезапустити MediaMTX -- краще живий,
                    # досяжний контролер (з чіткою помилкою в логах про
                    # MediaMTX), ніж обидва процеси мертві без діагностики.
                    logging.exception("dashboard: failed to restart mediamtx after a timeout change")

    return Handler
