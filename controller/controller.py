#!/usr/bin/env python3
"""
Контролер безперервного рестриму OBS -> Twitch. Точка входу: читає
конфіг, піднімає HTTP-сервер хуків і делегує всю логіку Controller
(state_machine.py). Сама логіка розписана по модулях цього ж каталогу
(state_machine/ffmpeg_proc/switcher/flv/probe/backup_prep/http_server).

Використовує лише стандартну бібліотеку Python (без pip-залежностей),
щоб встановлення на VPS зводилося до system-пакету python3.
"""

import json
import logging
import signal
import sys
from http.server import ThreadingHTTPServer
from pathlib import Path

from http_server import make_handler
from state_machine import Controller


def load_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    base_dir = Path(__file__).resolve().parent.parent
    config_path = Path(sys.argv[1]) if len(sys.argv) > 1 else base_dir / "controller" / "config.json"

    if not config_path.exists():
        print(f"Config file not found: {config_path}", file=sys.stderr)
        print("Copy config.example.json to config.json and fill in the values "
              "(or run install.sh, which does this automatically).", file=sys.stderr)
        sys.exit(1)

    config = load_config(config_path)

    log_dir = base_dir / "controller"
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(config.get("log_file", log_dir / "controller.log"), encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    controller = Controller(config, base_dir)

    def handle_signal(signum, _frame):
        logging.info("received termination signal (%s), stopping ffmpeg processes", signum)
        controller.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    handler_cls = make_handler(controller, config)
    server = ThreadingHTTPServer((config["listen_host"], config["listen_port"]), handler_cls)

    logging.info(
        "controller started on %s:%s (state=%s)",
        config["listen_host"], config["listen_port"], controller.state,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        controller.shutdown()


if __name__ == "__main__":
    main()
