"""
Controller — стейт-машина безперервного рестриму OBS -> Twitch.

Керує трьома ffmpeg-процесами (relay/backup/outbound) через
FfmpegProcess, перемиканням між ними — через FLVSwitcher, і відрізняє
свідоме завершення трансляції (сигнал від OBS-скрипта) від обриву
зв'язку — лише в другому випадку вмикає заглушку.
"""

import logging
import threading
import time
from pathlib import Path

from backup_prep import BackupPreparer
from ffmpeg_proc import FfmpegProcess
from flv import read_flv_tags
from switcher import FLVSwitcher

STATE_OFFLINE = "OFFLINE"    # OBS не публікує, на Twitch нічого не йде
STATE_LIVE = "LIVE"          # живий відеопотік від OBS іде на Twitch
STATE_FALLBACK = "FALLBACK"  # OBS відвалився, на Twitch крутиться заглушка

# Скільки секунд вважати сигнал "свідомого стопу" від OBS-скрипта чинним.
# Захист від застряглого прапорця, якщо після сигналу OBS не відключився.
GRACEFUL_STOP_TTL_SEC = 60

# Таймаут запису outbound -> Twitch (мкс): скільки чекати зависле
# з'єднання, перш ніж ffmpeg сам завершиться з помилкою.
OUTBOUND_RW_TIMEOUT_USEC = 15_000_000


class Controller:
    def __init__(self, config: dict, base_dir: Path):
        self.config = config
        self.base_dir = base_dir
        self.log_dir = base_dir / "controller"
        self.lock = threading.RLock()
        self.state = STATE_OFFLINE
        self._timeout_timer: threading.Timer | None = None
        self._graceful_stop_at: float | None = None

        mtx_host = config["mediamtx_rtmp_host"]
        mtx_port = config["mediamtx_rtmp_port"]
        user = config["internal_user"]
        password = config["internal_pass"]
        live_path = config["live_path"]

        # MediaMTX приймає логін/пароль для RTMP лише через query-параметри
        # (?user=...&pass=...), а НЕ через звичний rtmp://user:pass@host —
        # так влаштований сам протокол RTMP, userinfo в ньому не працює.
        live_url = f"rtmp://{mtx_host}:{mtx_port}/{live_path}?user={user}&pass={password}"
        self._live_probe_url = live_url

        self.switcher = FLVSwitcher()
        self._backup_preparer = BackupPreparer(Path(config["backup_file"]), config)

        self.relay = FfmpegProcess(
            "relay",
            [
                "ffmpeg", "-hide_banner", "-loglevel", "warning",
                "-i", live_url,
                "-c", "copy",
                "-f", "flv", "pipe:1",
            ],
            self.log_dir,
            capture_stdout=True,
            on_start=self._make_reader_hook("relay"),
        )

        self.backup = FfmpegProcess(
            "backup",
            # Функція, а не готовий список: щоразу перед (пере)запуском
            # обираємо найсвіжішу придатну копію заглушки (готова
            # заздалегідь копія, якщо є, інакше — оригінал користувача).
            lambda: [
                "ffmpeg", "-hide_banner", "-loglevel", "warning",
                "-stream_loop", "-1", "-re",
                "-i", str(self._backup_preparer.current_source()),
                "-c", "copy",
                "-f", "flv", "pipe:1",
            ],
            self.log_dir,
            capture_stdout=True,
            on_start=self._make_reader_hook("backup"),
        )

        self.outbound = FfmpegProcess(
            "outbound",
            [
                "ffmpeg", "-hide_banner", "-loglevel", "warning",
                # -y: для реального rtmp:// twitch_url — no-op (RTMP-публікація
                # не питає "перезаписати?"), а для файлового цілі (лише
                # тестування) рятує від "File already exists" на кожному
                # рестарті супервізора.
                "-y",
                "-i", "pipe:0",
                "-c", "copy",
                # -rw_timeout: якщо запис у Twitch зависає (з'єднання
                # живе, але дані не йдуть), а не розривається одразу —
                # ffmpeg сам завершиться з помилкою за цей час замість
                # блокування назавжди, супервізор перезапустить.
                "-rw_timeout", str(OUTBOUND_RW_TIMEOUT_USEC),
                "-f", "flv", config["twitch_url"],
            ],
            self.log_dir,
            stdin_pipe=True,
            on_start=lambda proc: self.switcher.attach_output(proc.stdin),
            on_exit=self.switcher.detach_output,
        )

    # --- обробники подій ---

    def on_available(self):
        with self.lock:
            self._cancel_timeout()

            if self.state == STATE_FALLBACK:
                # backup.stop() тут НЕ викликаємо — лишається активним,
                # поки switcher сам не перемкне через _on_switched_to_relay.
                logging.info(
                    "OBS reconnected -> waiting for the first live keyframe in the "
                    "background, backup video stays active until the seamless switch"
                )
                self.switcher.request_switch("relay", on_switched=self._on_switched_to_relay)
                self.relay.start()
            else:
                if self.state == STATE_OFFLINE:
                    logging.info("OBS started publishing -> starting the Twitch broadcast")
                    self.outbound.start()
                self.backup.stop()
                # set_active ДО relay.start(): свіжі seq-header-теги нового
                # процесу мають прилетіти вже під активним гейтом свіча,
                # інакше вони проскочать повз (source != active_source ще).
                self.switcher.set_active("relay")
                self.relay.start()
                self.state = STATE_LIVE

            self._graceful_stop_at = None

        # Поза self.lock: ffprobe + можливе перекодування можуть тривати
        # секунди, а то і хвилини на слабкому VPS — не тримати через них
        # стейт-машину заблокованою. Живий ефір це не зупиняє: заглушка
        # знадобиться лише якщо OBS відвалиться, а на той момент
        # перекодування, скоріш за все, уже встигне завершитись.
        self._backup_preparer.prepare_async(self._live_probe_url)

    def _on_switched_to_relay(self, params_changed: bool):
        """
        Callback від FLVSwitcher.request_switch: викликається з
        reader-потоку relay, поза внутрішніми локами свіча. Робота
        винесена в окремий потік, що бере self.lock — щоб не блокувати
        reader relay і не перегнатись із паралельним on_unavailable/
        on_available.

        `params_changed=True`: FLVSwitcher не виконав безшовний
        перехід (параметри кодека live змінились) — перезапускаємо
        outbound чистим з'єднанням замість безшовного cut.
        """
        def _finish():
            with self.lock:
                if self.state != STATE_FALLBACK:
                    # Стан уже змінився з іншої причини (напр. graceful
                    # stop чи повторний обрив) — нічого робити не треба.
                    return
                if params_changed:
                    logging.warning(
                        "live parameters changed while OBS was unavailable -> "
                        "restarting outbound with a clean connection instead of "
                        "a seamless switch"
                    )
                    self.switcher.set_active("relay")
                    self.outbound.stop()
                    self.outbound.start()
                else:
                    logging.info("live is ready (first keyframe received) -> seamless switch, stopping the backup video")
                self.state = STATE_LIVE
                self.backup.stop()

        threading.Thread(target=_finish, daemon=True).start()

    def on_unavailable(self):
        with self.lock:
            self.relay.stop()

            if self._is_graceful_stop_active():
                logging.info("OBS disconnected gracefully (Stop button) -> ending the broadcast")
                self.backup.stop()
                self.outbound.stop()
                self.state = STATE_OFFLINE
                self._graceful_stop_at = None
                self._cancel_timeout()
                return

            if self.state == STATE_OFFLINE:
                # unavailable без попереднього available — нема на що реагувати
                return

            logging.warning(
                "OBS disconnected WITHOUT a graceful-stop signal -> switching to "
                "backup video, waiting %s s for recovery",
                self.config["disconnect_timeout_sec"],
            )
            self.switcher.set_active("backup")
            self.backup.start()
            self.state = STATE_FALLBACK
            self._schedule_timeout()

    def on_graceful_stop_signal(self):
        with self.lock:
            logging.info("received graceful-stop signal from the OBS script")
            self._graceful_stop_at = time.monotonic()

    def status(self) -> dict:
        with self.lock:
            return {
                "state": self.state,
                "relay_running": self.relay.is_running(),
                "backup_running": self.backup.is_running(),
                "outbound_running": self.outbound.is_running(),
                "graceful_stop_pending": self._is_graceful_stop_active(),
            }

    def shutdown(self):
        with self.lock:
            self._cancel_timeout()
            self.relay.stop()
            self.backup.stop()
            self.outbound.stop()

    # --- внутрішнє ---

    def _make_reader_hook(self, source_name: str):
        def on_start(proc):
            threading.Thread(
                target=read_flv_tags,
                args=(proc.stdout, source_name, self.switcher.process),
                daemon=True,
            ).start()
        return on_start

    def _is_graceful_stop_active(self) -> bool:
        if self._graceful_stop_at is None:
            return False
        return (time.monotonic() - self._graceful_stop_at) <= GRACEFUL_STOP_TTL_SEC

    def _schedule_timeout(self):
        self._cancel_timeout()
        timeout_sec = self.config["disconnect_timeout_sec"]
        self._timeout_timer = threading.Timer(timeout_sec, self._on_timeout)
        self._timeout_timer.daemon = True
        self._timeout_timer.start()

    def _cancel_timeout(self):
        if self._timeout_timer is not None:
            self._timeout_timer.cancel()
            self._timeout_timer = None

    def _on_timeout(self):
        with self.lock:
            if self.state != STATE_FALLBACK:
                return
            logging.warning(
                "gave up waiting for OBS to recover after %s s -> ending the Twitch broadcast entirely",
                self.config["disconnect_timeout_sec"],
            )
            self.backup.stop()
            self.outbound.stop()
            self.state = STATE_OFFLINE
