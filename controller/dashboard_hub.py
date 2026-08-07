"""
DashboardHub -- серверна сторона push-каналу `/ws`. Одна фонова петля
закриває одразу дві потреби: негайно відреагувати на подію
(`Controller.on_change` -> `notify()`) і раз на секунду освіжити
CPU/mem/компоненти, які самі по собі не є дискретними подіями.

Розсилає лише дельту (змінені верхні ключі знімку) -- новому
з'єднанню при підключенні шлеться повний знімок (`register`), відтоді
воно отримує ті самі дельти, що й усі інші.
"""

import json
import logging
import os
import threading
from pathlib import Path

import proc_stats
import ws

TICK_SEC = 1.0


class DashboardHub:
    def __init__(self, controller, base_dir: Path):
        self._controller = controller
        self._base_dir = base_dir
        self._lock = threading.Lock()
        self._connections: dict[object, threading.Lock] = {}
        self._last_snapshot: dict | None = None
        self._event = threading.Event()
        threading.Thread(target=self._run, daemon=True).start()

    def notify(self) -> None:
        self._event.set()

    def push_event(self, level: str, text: str) -> None:
        """Toast для клієнта -- одноразова подія, не частина знімку/дельти стану."""
        self._broadcast_raw({"type": "event", "level": level, "text": text})

    def push_control(self, action: str) -> None:
        """
        Команда всім підключеним клієнтам -- наразі єдиний споживач --
        obs-source.html, дія "stop_streaming" (window.obsstudio.
        stopStreaming(), якщо Page permission це дозволяє). Дашборд
        просто ігнорує невідомий йому тип повідомлення.
        """
        with self._lock:
            count = len(self._connections)
        logging.info("dashboard: pushing control action=%s to %d connected /ws client(s)", action, count)
        self._broadcast_raw({"type": "control", "action": action})

    def _broadcast_raw(self, message: dict) -> None:
        text = json.dumps(message)
        with self._lock:
            dead = []
            for handler, write_lock in self._connections.items():
                if not self._send_raw(handler, write_lock, text):
                    dead.append(handler)
            for handler in dead:
                self._connections.pop(handler, None)

    def register(self, handler) -> threading.Lock:
        """
        Знімок будуємо ПОЗА self._lock -- _build_snapshot() викликає
        controller.status(), який бере Controller.lock, а Controller
        (_emit_event/on_control) сам викликає hub у зворотному порядку
        (тримаючи Controller.lock, бере self._lock). Тримати обидва
        локи одночасно в протилежних порядках із двох різних потоків
        -- deadlock, тож _build_snapshot() винесено назовні. Ціна:
        реєстрація вже не строго атомарна відносно паралельного
        `_broadcast()` (нове з'єднання теоретично може отримати ще й
        надлишкову, не хибну, дельту одразу після full) -- прийнятно.
        """
        write_lock = threading.Lock()
        snapshot = self._build_snapshot()
        with self._lock:
            self._last_snapshot = snapshot
            self._connections[handler] = write_lock
            self._send(handler, write_lock, {"type": "full", "data": snapshot})
        return write_lock

    def unregister(self, handler) -> None:
        """Ідемпотентно -- безпечно викликати і з read-циклу з'єднання, і з hub-а."""
        with self._lock:
            self._connections.pop(handler, None)

    def close_all(self) -> None:
        """
        Перед self-рестартом контролера (Settings -> Apply & Restart):
        шле CLOSE усім відкритим /ws-з'єднанням і закриває їхні сокети
        одразу, замість того, щоб лишити їх висіти на фактично
        мертвому fd після заміни образу процесу (`os.execv`) -- клієнт
        одразу бачить розрив і йде у вже готовий reconnect-з-бекофом.
        """
        with self._lock:
            for handler, write_lock in self._connections.items():
                try:
                    ws.send_close(handler, write_lock)
                except OSError:
                    pass
                try:
                    handler.connection.close()
                except OSError:
                    pass
            self._connections.clear()

    def _run(self) -> None:
        while True:
            self._event.wait(timeout=TICK_SEC)
            self._event.clear()
            self._broadcast()

    def _broadcast(self) -> None:
        # _build_snapshot() -- ПОЗА self._lock, з тієї ж причини, що й
        # у register() вище (уникнути lock-order deadlock із
        # Controller._emit_event()/on_control()).
        snapshot = self._build_snapshot()
        with self._lock:
            delta = self._diff(self._last_snapshot, snapshot)
            self._last_snapshot = snapshot
            if not delta or not self._connections:
                return
            message = json.dumps({"type": "delta", "data": delta})
            dead = []
            for handler, write_lock in self._connections.items():
                if not self._send_raw(handler, write_lock, message):
                    dead.append(handler)
            for handler in dead:
                self._connections.pop(handler, None)

    def _send(self, handler, write_lock, message: dict) -> bool:
        return self._send_raw(handler, write_lock, json.dumps(message))

    def _send_raw(self, handler, write_lock, text: str) -> bool:
        # Тайм-аут/помилка запису -- клієнт мертвий (переповнений
        # буфер, пропав зі мережі). Одне таке з'єднання не повинно
        # затримувати розсилку для решти -- обмежено `settimeout(1.0)`
        # на самому сокеті з'єднання (виставляється в http_server.py).
        try:
            ws.send_text(handler, text, write_lock)
            return True
        except (OSError, TimeoutError):
            logging.info("dashboard: dropping unresponsive /ws connection")
            return False

    @staticmethod
    def _diff(previous: dict | None, current: dict) -> dict:
        if previous is None:
            return dict(current)
        return {key: value for key, value in current.items() if previous.get(key) != value}

    def _build_snapshot(self) -> dict:
        status = self._controller.status()
        components = {
            "mediamtx": self._component(self._mediamtx_pid()),
            "controller": self._component(os.getpid()),
            "relay": self._component(status.pop("relay_pid")),
            "backup": self._component(status.pop("backup_pid")),
            "outbound": self._component(status.pop("outbound_pid")),
        }
        status["components"] = components
        return status

    @staticmethod
    def _component(pid: int | None) -> dict:
        running = pid is not None and DashboardHub._pid_alive(pid)
        stats = proc_stats.sample(pid) if running else None
        return {
            "running": running,
            "pid": pid if running else None,
            "cpu_percent": stats["cpu_percent"] if stats else None,
            "rss_mb": stats["rss_mb"] if stats else None,
        }

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def _mediamtx_pid(self) -> int | None:
        try:
            return int((self._base_dir / ".mediamtx.pid").read_text(encoding="ascii").strip())
        except (OSError, ValueError):
            return None
