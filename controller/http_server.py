"""HTTP-шар контролера: хуки MediaMTX, webhook OBS-скрипта, /status."""

import json
import logging
from http.server import BaseHTTPRequestHandler


def make_handler(controller, config: dict):
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

        def _is_localhost(self) -> bool:
            return self.client_address[0] in ("127.0.0.1", "::1")

        def _check_obs_token(self) -> bool:
            expected = f"Bearer {config['obs_webhook_token']}"
            return self.headers.get("Authorization") == expected

        def do_POST(self):
            # Зчитуємо і відкидаємо тіло запиту (хуки MediaMTX шлють form-дані,
            # нам вони не потрібні — весь контекст ми й так знаємо з конфігу).
            length = int(self.headers.get("Content-Length", 0))
            if length:
                self.rfile.read(length)

            if self.path == "/hooks/available":
                if not self._is_localhost():
                    self._send(403, {"error": "forbidden"})
                    return
                controller.on_available()
                self._send(200, {"ok": True})
                return

            if self.path == "/hooks/unavailable":
                if not self._is_localhost():
                    self._send(403, {"error": "forbidden"})
                    return
                controller.on_unavailable()
                self._send(200, {"ok": True})
                return

            if self.path == "/obs/graceful-stop":
                if not self._check_obs_token():
                    self._send(401, {"error": "invalid token"})
                    return
                controller.on_graceful_stop_signal()
                self._send(200, {"ok": True})
                return

            self._send(404, {"error": "not found"})

        def do_GET(self):
            if self.path == "/status":
                if not self._is_localhost():
                    self._send(403, {"error": "forbidden"})
                    return
                self._send(200, controller.status())
                return
            self._send(404, {"error": "not found"})

    return Handler
