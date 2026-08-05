"""
FfmpegProcess — обгортка над одним ffmpeg-процесом із супервізором
автоперезапуску. Нічого не знає про FLV/switcher — лише керує
процесом і, за потреби, віддає його stdin/stdout наружу через
callback-и (`on_start`/`on_exit`), щоб керуючий код (state_machine.py)
міг підключити reader-потоки чи switcher без того, щоб цей модуль
знав про їхнє існування.

Навіщо супервізор: ffmpeg-івські `-reconnect`-опції рятують лише
від обриву ВЖЕ встановленого з'єднання, а не від невдачі при самому
відкритті входу. Супервізор просто перезапускає процес, поки він
потрібен (desired=True) — закриває цю й будь-які інші тимчасові
збої ffmpeg.
"""

import logging
import subprocess
import threading
import time
from pathlib import Path
from typing import Callable

RESTART_BACKOFF_SEC = 1.5


class FfmpegProcess:
    def __init__(
        self,
        name: str,
        args,
        log_dir: Path,
        *,
        capture_stdout: bool = False,
        stdin_pipe: bool = False,
        on_start: Callable[[subprocess.Popen], None] | None = None,
        on_exit: Callable[[], None] | None = None,
    ):
        """
        `args` — або готовий список аргументів, або функція без
        параметрів, що повертає список (викликається заново перед
        КОЖНИМ (пере)запуском — дозволяє, наприклад, backup підхопити
        щойно підготовлену копію заглушки без явного рестарту ззовні).

        `capture_stdout` — stdout стає subprocess.PIPE (бінарні дані
        для читання наружу), stderr тоді ОБОВ'ЯЗКОВО йде в окремий
        файловий дескриптор (лог), а не туди ж, куди stdout: змішати
        бінарний FLV-потік із текстом логу в одному fd не можна.

        `stdin_pipe` — stdin стає subprocess.PIPE (для запису наружу),
        інакше DEVNULL, як і раніше.

        `on_start(proc)` викликається одразу після кожного успішного
        Popen (і на першому старті, і на кожному рестарті супервізора)
        — типове використання: підняти reader-потік на proc.stdout або
        підключити proc.stdin до switcher-а.

        `on_exit()` викликається щоразу, коли процес завершується —
        і природно (сам впав/вийшов), і через явний stop() — тому має
        бути безпечним для повторного виклику.
        """
        self.name = name
        self.args = args
        self.capture_stdout = capture_stdout
        self.stdin_pipe = stdin_pipe
        self._on_start = on_start
        self._on_exit = on_exit
        self._proc: subprocess.Popen | None = None
        self._log_path = log_dir / f"ffmpeg-{name}.log"
        self._lock = threading.Lock()
        self._desired = False
        self._monitor_thread: threading.Thread | None = None

    def _resolve_args(self) -> list[str]:
        return self.args() if callable(self.args) else self.args

    def is_running(self) -> bool:
        with self._lock:
            return self._proc is not None and self._proc.poll() is None

    def start(self):
        with self._lock:
            if self._desired:
                return
            self._desired = True
        self._monitor_thread = threading.Thread(target=self._run_supervised, daemon=True)
        self._monitor_thread.start()

    def stop(self, timeout: float = 5.0):
        with self._lock:
            self._desired = False
            proc = self._proc
        if proc is None:
            return

        logging.info("stopping ffmpeg[%s] (pid=%s)", self.name, proc.pid)
        self._fire_on_exit()

        if self.stdin_pipe and proc.stdin is not None:
            # Чистий EOF на pipe:0 замість SIGTERM: ffmpeg сам коректно
            # закриває вихідне з'єднання, не чекаючи сигналу під час
            # блокуючого читання.
            try:
                proc.stdin.close()
            except OSError:
                pass
        else:
            proc.terminate()

        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            logging.warning("ffmpeg[%s] did not exit within %.1fs -- terminate/kill", self.name, timeout)
            proc.terminate()
            try:
                proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5.0)

        with self._lock:
            self._proc = None
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=timeout + 2.0)

    def _fire_on_start(self, proc: subprocess.Popen):
        if self._on_start is None:
            return
        try:
            self._on_start(proc)
        except Exception:
            logging.exception("ffmpeg[%s]: error in on_start hook", self.name)

    def _fire_on_exit(self):
        if self._on_exit is None:
            return
        try:
            self._on_exit()
        except Exception:
            logging.exception("ffmpeg[%s]: error in on_exit hook", self.name)

    def _run_supervised(self):
        log_file = open(self._log_path, "ab")
        try:
            while True:
                with self._lock:
                    if not self._desired:
                        return
                    current_args = self._resolve_args()
                    logging.info("starting ffmpeg[%s]: %s", self.name, " ".join(current_args))
                    self._proc = subprocess.Popen(
                        current_args,
                        stdout=subprocess.PIPE if self.capture_stdout else log_file,
                        stderr=log_file if self.capture_stdout else subprocess.STDOUT,
                        stdin=subprocess.PIPE if self.stdin_pipe else subprocess.DEVNULL,
                    )
                    proc = self._proc
                self._fire_on_start(proc)

                proc.wait()

                with self._lock:
                    exited_desired = self._desired
                    if self._proc is proc:
                        self._proc = None
                self._fire_on_exit()

                if not exited_desired:
                    return
                logging.warning(
                    "ffmpeg[%s] exited unexpectedly, restarting in %.1fs",
                    self.name, RESTART_BACKOFF_SEC,
                )
                time.sleep(RESTART_BACKOFF_SEC)
        finally:
            log_file.close()
