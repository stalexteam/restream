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
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from dashboard_hub import DashboardHub
from http_server import make_handler
from mediamtx_log_watch import watch as watch_mediamtx_log
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

    controller = Controller(config, base_dir, config_path)
    # DashboardHub потребує controller (щоб будувати знімки стану),
    # а Controller відповідно сповіщає hub про кожну зміну стану --
    # взаємна залежність, тому hub створюється вже ПІСЛЯ controller,
    # і колбек підключається постфактум простим присвоєнням атрибута.
    hub = DashboardHub(controller, base_dir)
    controller.on_change = hub.notify
    controller.on_event = hub.push_event
    controller.on_control = hub.push_control

    threading.Thread(
        target=watch_mediamtx_log,
        args=(base_dir / "mediamtx.log", controller.on_mediamtx_connect_timeout),
        daemon=True,
    ).start()

    def handle_signal(signum, _frame):
        logging.info("received termination signal (%s), stopping ffmpeg processes", signum)
        controller.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    handler_cls = make_handler(controller, config, hub, config_path)
    server = ThreadingHTTPServer((config["listen_host"], config["listen_port"]), handler_cls)

    logging.info(
        "controller started on %s:%s (state=%s)",
        config["listen_host"], config["listen_port"], controller.state,
    )
    # Дашборд застосовує зміни налаштувань точково, живцем (bounce лише
    # зачепленого виходу / рестарт лише MediaMTX при зміні таймінгів) --
    # самоперезапуску контролера через os.execv більше немає потреби.
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        controller.shutdown()


if __name__ == "__main__":
    main()
