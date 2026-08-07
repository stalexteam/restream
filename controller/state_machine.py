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
from typing import Callable

from backup_prep import BackupPreparer
from ffmpeg_proc import FfmpegProcess
from flv import read_flv_tags
from switcher import FLVSwitcher

STATE_OFFLINE = "OFFLINE"    # OBS не публікує, на Twitch нічого не йде
STATE_LIVE = "LIVE"          # живий відеопотік від OBS іде на Twitch
STATE_FALLBACK = "FALLBACK"  # OBS відвалився, на Twitch крутиться заглушка

# Таймаут запису outbound -> Twitch (мкс): скільки чекати зависле
# з'єднання, перш ніж ffmpeg сам завершиться з помилкою.
OUTBOUND_RW_TIMEOUT_USEC = 15_000_000

# Мінімальний інтервал між повторними toast-попередженнями про
# нестабільне з'єднання з Twitch (після того, як воно вже було
# успішним) -- без цього N хвилин обривів дали б N/1.5с тостів
# поспіль, які користувач фізично не встигає прочитати/закрити.
FLAPPING_TOAST_COOLDOWN_SEC = 30

# Той самий принцип для повторних спроб OBS<->MediaMTX (OBS зазвичай
# ретраїть кожні кілька секунд самостійно).
CONNECT_TIMEOUT_TOAST_COOLDOWN_SEC = 15


class Controller:
    def __init__(self, config: dict, base_dir: Path):
        self.config = config
        self.base_dir = base_dir
        self.log_dir = base_dir / "controller"
        self.lock = threading.RLock()
        self.state = STATE_OFFLINE
        self._state_since = time.time()
        self._timeout_timer: threading.Timer | None = None
        self._fallback_deadline: float | None = None
        self._last_flapping_toast_at = 0.0
        self._last_connect_timeout_toast_at = 0.0
        # OFFLINE через помилку (Twitch недосяжний), а не через свідомий
        # стоп/таймаут -- дашборд показує окремий червоний "Halt"
        # бейдж замість нейтрального "Offline". Скидається на старті
        # НАСТУПНОЇ трансляції (on_available), незалежно від того, чи
        # ця наступна спроба сама вдасться.
        self._halted = False
        # Викликається (без аргументів) при кожній зміні стану --
        # підключається ззовні (controller.py -> DashboardHub.notify),
        # сама Controller нічого не знає про HTTP/WS/дашборд.
        self.on_change: Callable[[], None] | None = None
        # Транзієнтні події (toast-повідомлення) -- на відміну від
        # on_change, це НЕ частина стану (не потрапляє в /ws full/delta),
        # підключається так само ззовні (controller.py -> DashboardHub.push_event).
        self.on_event: Callable[[str, str], None] | None = None
        # Команда клієнтам /ws (наразі лише obs-source.html) -- напр.
        # "stop_streaming", щоб той викликав window.obsstudio.
        # stopStreaming(). Підключається ззовні (controller.py ->
        # DashboardHub.push_control).
        self.on_control: Callable[[str], None] | None = None
        # Викликається, коли дашборд просить рестартнути процес
        # контролера (Settings -> Apply & Restart) -- підключається
        # ззовні (controller.py -> os.execv-механізм), Controller і
        # тут нічого не знає про сам механізм рестарту.
        self.request_restart: Callable[[], None] | None = None

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
            on_flapping=self._on_outbound_flapping,
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
                    self._emit_event("info", "Broadcast started")
                    self._last_flapping_toast_at = 0.0
                    self._halted = False
                    self.outbound.start()
                self.backup.stop()
                # set_active ДО relay.start(): свіжі seq-header-теги нового
                # процесу мають прилетіти вже під активним гейтом свіча,
                # інакше вони проскочать повз (source != active_source ще).
                self.switcher.set_active("relay")
                self.relay.start()
                self._set_state(STATE_LIVE)

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
                self._set_state(STATE_LIVE)
                self.backup.stop()

        threading.Thread(target=_finish, daemon=True).start()

    def on_unavailable(self):
        with self.lock:
            self.relay.stop()

            if self.state == STATE_OFFLINE:
                # unavailable без попереднього available — нема на що реагувати
                return

            if self.state == STATE_FALLBACK:
                # Вже в FALLBACK -- найімовірніше, власний read-timeout
                # детектор (_on_relay_stalled) устиг раніше за цей
                # хук MediaMTX. relay.stop() вище й так виконався
                # (тепер доречно -- MediaMTX підтвердив розрив), решту
                # (backup/таймер/стан) НЕ чіпаємо: безумовний повторний
                # _schedule_timeout() розтягнув би реальне очікування
                # довше за заявлений offline_timeout_sec.
                return

            if not self.outbound.ever_ran_long() and not self.outbound.is_running():
                # ever_ran_long() виставляється лише заднім числом, коли
                # процес уже завершився і протримався довше порогу --
                # для ще живого (успішно працюючого просто зараз, хай
                # навіть менше EVER_SUCCEEDED_THRESHOLD_SEC) outbound
                # лишається False, хоча зв'язок з Twitch насправді
                # справний. is_running() тут -- щоб такий випадок не
                # трактувався як недосяжний Twitch: здаємось лише якщо
                # outbound і ніколи не працював довго, і просто зараз
                # не працює.
                logging.error(
                    "OBS disconnected, and Twitch was never reached this broadcast -- "
                    "no point looping the backup video, stopping the broadcast entirely"
                )
                self._give_up_on_unreachable_twitch()
                return

            logging.warning(
                "OBS disconnected -> switching to backup video, waiting %s s for recovery",
                self.config["offline_timeout_sec"],
            )
            self.switcher.set_active("backup")
            self.backup.start()
            self._schedule_timeout()
            self._set_state(STATE_FALLBACK)

    def on_manual_stop(self) -> None:
        """
        Негайна зупинка на сигнал "OBS deliberately stopped streaming"
        від obs-source.html -- невидимого Browser Source-скрипта, що
        полить window.obsstudio.getStatus() і шле stop_broadcast у /ws
        сам, щойно streaming переходить true -> false. Ловить розрив
        РАНІШЕ, ніж MediaMTX<->OBS (не чекає on_unavailable()), тому
        трансляція завершується одразу, без заглушки й таймауту.

        window.obsstudio.getStatus() навмисно НЕ підключений до
        дашборда (Custom Browser Dock) -- відомий баг (з 2021) робить
        його там ненадійним; у Browser Source той самий API працює
        стабільно, звідси й окремий invisible-скрипт замість інтеграції
        в основний дашборд.
        """
        with self.lock:
            if self.state == STATE_OFFLINE:
                return
            logging.info("OBS reports streaming stopped (obs-source.html) -> ending the broadcast")
            self._emit_event("info", "Broadcast ended")
            self.relay.stop()
            self.backup.stop()
            self.outbound.stop()
            self._cancel_timeout()
            self._set_state(STATE_OFFLINE)

    def on_obs_streaming_started(self) -> None:
        # Лише лог/діагностика -- стан і так коректно виставляється
        # через runOnAvailable/on_available; це підтвердження зі
        # сторони OBS (obs-source.html), не тригер.
        logging.info("OBS reports streaming started (obs-source.html)")

    def on_mediamtx_connect_timeout(self) -> None:
        """
        MediaMTX закрив з'єднання OBS по readTimeout, так і не
        дочекавшись публікації -- runOnAvailable/runOnUnavailable у
        цьому разі жодного разу не спрацьовують, тож про це знає лише
        mediamtx_log_watch.py (звідси й приходить цей виклик).
        """
        with self.lock:
            if self.state != STATE_OFFLINE:
                # Уже є активна трансляція -- це стороннє/застаріле
                # з'єднання, не про невдалий старт поточного ефіру.
                return
            now = time.monotonic()
            if now - self._last_connect_timeout_toast_at < CONNECT_TIMEOUT_TOAST_COOLDOWN_SEC:
                return
            self._last_connect_timeout_toast_at = now
        logging.warning(
            "OBS failed to finish connecting to MediaMTX within the connect timeout -- "
            "consider raising it in Settings"
        )
        self._emit_event(
            "warning",
            "OBS didn't finish connecting in time -- try raising the connect timeout in Settings",
        )

    def status(self) -> dict:
        with self.lock:
            return {
                "state": self.state,
                "state_since": self._state_since,
                "halted": self._halted,
                "relay_running": self.relay.is_running(),
                "relay_pid": self.relay.pid(),
                "backup_running": self.backup.is_running(),
                "backup_pid": self.backup.pid(),
                "outbound_running": self.outbound.is_running(),
                "outbound_pid": self.outbound.pid(),
                "fallback_deadline": self._fallback_deadline,
            }

    def shutdown(self):
        with self.lock:
            self._cancel_timeout()
            self.relay.stop()
            self.backup.stop()
            self.outbound.stop()

    # --- внутрішнє ---

    def _make_reader_hook(self, source_name: str):
        # Read timeout -- лише для relay: він читає MediaMTX по
        # loopback, і пауза в тегах, які він віддає, надійно
        # відображає паузу в даних від OBS. backup -- локальний
        # файл-луп, детектор стагнації йому не потрібен.
        is_relay = source_name == "relay"

        def on_start(proc):
            kwargs = {}
            if is_relay:
                kwargs = {
                    "read_timeout_sec": self.config["read_timeout_ms"] / 1000,
                    "on_stall": self._on_relay_stalled,
                    "on_resume": self._on_relay_resumed,
                }
            threading.Thread(
                target=read_flv_tags,
                args=(proc.stdout, source_name, self.switcher.process),
                kwargs=kwargs,
                daemon=True,
            ).start()
        return on_start

    def _on_relay_stalled(self):
        with self.lock:
            if self.state != STATE_LIVE:
                return
            logging.warning(
                "no data from relay for %sms (network to OBS looks stalled) -> "
                "switching to backup video without dropping the relay connection",
                self.config["read_timeout_ms"],
            )
            self.switcher.set_active("backup")
            self.backup.start()
            self._schedule_timeout()
            self._set_state(STATE_FALLBACK)

    def _on_relay_resumed(self):
        with self.lock:
            if self.state != STATE_FALLBACK:
                return
            logging.info("data from relay resumed -> waiting for a keyframe for a seamless switch back")
            self.switcher.request_switch("relay", on_switched=self._on_switched_to_relay)
            # relay.start() тут НЕ потрібен -- на відміну від
            # on_available(), сам процес ніколи не зупинявся, він і
            # зараз ще читає MediaMTX.

    def _set_state(self, new_state: str) -> None:
        self.state = new_state
        self._state_since = time.time()
        self._notify()

    def _notify(self) -> None:
        if self.on_change is not None:
            self.on_change()

    def _emit_event(self, level: str, text: str) -> None:
        if self.on_event is not None:
            self.on_event(level, text)

    def _request_stop_streaming_in_obs(self) -> None:
        logging.info(
            "sending stop_streaming control to any connected obs-source.html "
            "(requires its Page permission set to \"Full access to OBS\" to take effect)"
        )
        if self.on_control is not None:
            self.on_control("stop_streaming")

    def _give_up_on_unreachable_twitch(self) -> None:
        """
        Спільний хвіст для двох шляхів, де Twitch жодного разу не був
        досяжний за цю трансляцію -- _on_outbound_flapping(never_
        succeeded=True) і on_unavailable(), коли outbound.ever_ran_long()
        -- False: зупиняє relay/backup/outbound на нашому боці, і, якщо
        obs-source.html підключений з Page permission "Full access to
        OBS", командує самому OBS зупинити стрім -- інакше OBS
        продовжував би публікувати в порожнечу, поки користувач не
        натисне Stop вручну. Виклик має відбуватись під self.lock.
        """
        self.relay.stop()
        self.backup.stop()
        self.outbound.stop()
        self._cancel_timeout()
        self._halted = True
        self._set_state(STATE_OFFLINE)
        self._emit_event(
            "error",
            "Failed to connect to Twitch -- check the Stream URL/key in Settings. Broadcast "
            "stopped, and a stop command was sent to the OBS browser-source. If OBS is still "
            "streaming, set its Page permission to \"Full access to OBS\".",
        )
        self._request_stop_streaming_in_obs()

    def _on_outbound_flapping(self, never_succeeded: bool) -> None:
        if never_succeeded:
            # Жодного успішного під'єднання від самого старту -- майже
            # напевно невалідний URL/ключ, а не тимчасовий мережевий
            # збій. Нескінченний crash-loop тут безглуздий (і спамить
            # тостами) -- зупиняємо всю трансляцію одразу, замість
            # мовчки довбитись у Twitch.
            logging.error(
                "outbound failed on every attempt since the broadcast started -- "
                "likely an invalid Twitch URL/stream key, stopping the broadcast"
            )
            with self.lock:
                if self.state == STATE_OFFLINE:
                    return
                self._give_up_on_unreachable_twitch()
            return

        # Було принаймні одне успішне з'єднання цієї трансляції --
        # схоже на тимчасовий мережевий збій, не на невалідний ключ.
        # Продовжуємо ретраїти нескінченно, лише обмежуємо частоту
        # тостів, щоб не спамити ними користувача.
        logging.warning(
            "outbound keeps failing to reach Twitch after a previously working "
            "connection -- possible network issue, still retrying"
        )
        now = time.monotonic()
        if now - self._last_flapping_toast_at < FLAPPING_TOAST_COOLDOWN_SEC:
            return
        self._last_flapping_toast_at = now
        self._emit_event("warning", "Connection to Twitch keeps failing -- still retrying...")

    def _schedule_timeout(self):
        self._cancel_timeout()
        timeout_sec = self.config["offline_timeout_sec"]
        self._fallback_deadline = time.time() + timeout_sec
        self._timeout_timer = threading.Timer(timeout_sec, self._on_timeout)
        self._timeout_timer.daemon = True
        self._timeout_timer.start()

    def _cancel_timeout(self):
        if self._timeout_timer is not None:
            self._timeout_timer.cancel()
            self._timeout_timer = None
        self._fallback_deadline = None

    def _on_timeout(self):
        with self.lock:
            if self.state != STATE_FALLBACK:
                return
            logging.warning(
                "gave up waiting for OBS to recover after %s s -> ending the Twitch broadcast entirely",
                self.config["offline_timeout_sec"],
            )
            self._emit_event("warning", "Broadcast ended -- OBS did not reconnect in time")
            self.backup.stop()
            self.outbound.stop()
            self._cancel_timeout()
            self._set_state(STATE_OFFLINE)
