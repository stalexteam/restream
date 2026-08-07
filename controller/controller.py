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
import os
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

    controller = Controller(config, base_dir)
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

    # ThreadingHTTPServer.daemon_threads = True -- кожен /ws-хендлер
    # (звідки й приходить запит на рестарт, Settings -> Apply &
    # Restart) виконується в daemon-потоці. Виконати сам os.execv()
    # ПРЯМО звідти небезпечно: щойно server.shutdown() відпускає
    # serve_forever() у головному потоці, той одразу добігає до кінця
    # main() і завершує процес -- а разом з ним миттєво вбиваються всі
    # daemon-потоки, включно з тим самим /ws-хендлером, який у цей
    # момент, можливо, ще не встиг дійти до execv. Тому хендлер лише
    # ПРОСИТЬ про рестарт (виставляє прапор і будить serve_forever())
    # -- сам execv виконує ГОЛОВНИЙ потік, останньою дією перед тим,
    # як він і так природно завершив би процес.
    restart_requested = threading.Event()

    def restart_process():
        logging.info("restarting the controller process (settings applied via dashboard)")
        hub.close_all()
        restart_requested.set()
        server.shutdown()

    controller.request_restart = restart_process

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

    if restart_requested.is_set():
        # Порт треба звільнити ДО execv -- інакше новий образ процесу
        # (той самий PID) отримає "Address already in use": файлові
        # дескриптори переживають execv (без явного CLOEXEC), і без
        # цього виклику старий слухаючий сокет лишався б відкритим у
        # тому самому процесі, SO_REUSEADDR тут не рятує (це не
        # TIME_WAIT-кейс, а живий відкритий слухач).
        server.server_close()
        os.execv(sys.executable, [sys.executable] + sys.argv)


if __name__ == "__main__":
    main()
