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

# "Флепінг" -- процес помирає одразу після старту. on_flapping --
# сигнал ЗОВНІ про це, сам супервізор і далі продовжує пробувати
# нескінченно (за винятком never_succeeded=True, де викликач сам
# зупиняє все — дивись _run_supervised нижче).
#
# FLAPPING_COUNT_THRESHOLD стосується ЛИШЕ випадку "раніше вже
# з'єднувались, а тепер знову падає" (never_succeeded=False) — N
# падінь ПОСПІЛЬ, перш ніж бити на сполох, щоб не реагувати на
# одиничний мережевий блимок. Якщо ж з'єднання ЩЕ ЖОДНОГО разу не
# було успішним за цей start() — досить й ОДНІЄЇ невдачі (URL або
# валідний, або ні; повторні спроби нічого нового не скажуть).
FLAPPING_EXIT_THRESHOLD_SEC = 3.0
FLAPPING_COUNT_THRESHOLD = 3

# Twitch може відхиляти невалідний ключ не миттєво, а з помітною
# затримкою (асинхронна перевірка на їхній стороні) -- впритул до
# FLAPPING_EXIT_THRESHOLD_SEC. Один "довгий" запуск (>=
# FLAPPING_EXIT_THRESHOLD_SEC) сам по собі не доказ реального
# з'єднання -- лише скидає лічильник crash-loop'у для цієї спроби.
# Значно вищий, окремий поріг -- лише для ever_ran_long (проксі
# "справді з'єдналися").
EVER_SUCCEEDED_THRESHOLD_SEC = 10.0


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
        on_flapping: Callable[[bool], None] | None = None,
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

        `on_flapping(never_succeeded)` — процес помер несподівано.
        `never_succeeded=True` (жоден запуск від останнього `start()`
        ще не протримався довше `EVER_SUCCEEDED_THRESHOLD_SEC`) --
        викликається одразу на ПЕРШІЙ же невдачі. `never_succeeded=
        False` (хоч раз уже було стабільне з'єднання цієї трансляції)
        -- викликається лише після `FLAPPING_COUNT_THRESHOLD` швидких
        падінь ПОСПІЛЬ (`FLAPPING_EXIT_THRESHOLD_SEC`), щоб не
        реагувати на одиничний мережевий блимок.
        """
        self.name = name
        self.args = args
        self.capture_stdout = capture_stdout
        self.stdin_pipe = stdin_pipe
        self._on_start = on_start
        self._on_exit = on_exit
        self._on_flapping = on_flapping
        self._proc: subprocess.Popen | None = None
        self._log_path = log_dir / f"ffmpeg-{name}.log"
        self._lock = threading.Lock()
        self._desired = False
        self._monitor_thread: threading.Thread | None = None
        self._consecutive_early_exits = 0
        self._ever_ran_long = False

    def _resolve_args(self) -> list[str]:
        return self.args() if callable(self.args) else self.args

    def is_running(self) -> bool:
        with self._lock:
            return self._proc is not None and self._proc.poll() is None

    def pid(self) -> int | None:
        with self._lock:
            return self._proc.pid if self._proc is not None else None

    def ever_ran_long(self) -> bool:
        """Чи хоч один запуск від останнього start() протримався довше EVER_SUCCEEDED_THRESHOLD_SEC (проксі "успішно з'єднався")."""
        with self._lock:
            return self._ever_ran_long

    def start(self):
        with self._lock:
            if self._desired:
                return
            self._desired = True
            self._consecutive_early_exits = 0
            self._ever_ran_long = False
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

    def _fire_on_flapping(self, never_succeeded: bool):
        if self._on_flapping is None:
            return
        try:
            self._on_flapping(never_succeeded)
        except Exception:
            logging.exception("ffmpeg[%s]: error in on_flapping hook", self.name)

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

                started_at = time.monotonic()
                proc.wait()
                ran_for = time.monotonic() - started_at

                with self._lock:
                    exited_desired = self._desired
                    if self._proc is proc:
                        self._proc = None
                self._fire_on_exit()

                if not exited_desired:
                    return

                if not self._ever_ran_long:
                    # Жодного разу ще не було підтверджено робоче
                    # з'єднання за цей start() -- досить й ОДНІЄЇ
                    # невдачі, щоб визнати ключ/URL невалідним і
                    # здатися одразу; чекати кілька спроб поспіль тут
                    # безглуздо -- URL або валідний, або ні, повторні
                    # спроби нічого нового не скажуть.
                    self._fire_on_flapping(never_succeeded=True)
                elif ran_for < FLAPPING_EXIT_THRESHOLD_SEC:
                    # Уже було підтверджене робоче з'єднання цієї
                    # трансляції -- це більше схоже на тимчасовий
                    # мережевий збій, не на невалідний ключ, тож
                    # ретраїмо нескінченно (антиспам-тости), і тут
                    # усе ще потрібні кілька невдач ПОСПІЛЬ, щоб не
                    # піднімати тривогу через одиничний блимок.
                    self._consecutive_early_exits += 1
                    if self._consecutive_early_exits == FLAPPING_COUNT_THRESHOLD:
                        self._fire_on_flapping(never_succeeded=False)
                else:
                    self._consecutive_early_exits = 0

                if ran_for >= EVER_SUCCEEDED_THRESHOLD_SEC:
                    self._ever_ran_long = True

                with self._lock:
                    # on_flapping міг сам викликати stop() на цьому ж
                    # об'єкті (те саме, що дає self.desired -> False
                    # без спроби self-join, бо self._proc уже None на
                    # цей момент) -- перевіряємо ЗНОВУ, щоб не друкувати
                    # оманливе "restarting" й не чекати марно.
                    if not self._desired:
                        return

                logging.warning(
                    "ffmpeg[%s] exited unexpectedly, restarting in %.1fs",
                    self.name, RESTART_BACKOFF_SEC,
                )
                time.sleep(RESTART_BACKOFF_SEC)
        finally:
            log_file.close()
