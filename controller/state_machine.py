"""
Controller — стейт-машина безперервного рестриму OBS -> кілька платформ.

Дві сторони конвеєра:

- **Source** (`relay`/`backup` + `FLVSwitcher`): формує ЄДИНИЙ
  канонічний потік (живе відео від OBS або заглушка). Непрерывність
  (backup-відео при обриві OBS, `offline_timeout`) — властивість цього
  потоку, працює поки OBS публікує й увімкнено хоч один потребувач.
- **Destinations** (`Destination` = `FfmpegProcess` + `OutputSink`):
  однакові toggleable-виходи. `primary` (обов'язковий) + `restreams`
  (динамічний список). Кожен незалежно вмикається/вимикається на лету;
  повільна/впала площадка ніколи не чіпає інші (кожна має власну чергу
  й потік-писар у `OutputSink`).

Failsafe — агрегатний: жорсткий стоп усього ефіру (Halt + команда OBS
зупинити стрім) лише якщо ЖОДЕН увімкнений потребувач не піднявся.
Досить одного живого — упалі уходять у best-effort (тост + стоп цієї
площадки, якщо ключ невалідний; ретрай, якщо був сбій після успіху).
"""

import logging
import re
import threading
import time
from pathlib import Path
from typing import Callable

import net_probe
import output_url
import settings_store
from backup_prep import BackupPreparer
from ffmpeg_proc import FfmpegProcess
from flv import read_flv_tags
from switcher import FLVSwitcher, OutputSink

STATE_OFFLINE = "OFFLINE"    # OBS не публікує, на платформи нічого не йде
STATE_LIVE = "LIVE"          # живий відеопотік від OBS іде на платформи
STATE_FALLBACK = "FALLBACK"  # OBS відвалився, на платформи крутиться заглушка

# Таймаут запису destination -> платформа (мкс): скільки чекати зависле
# з'єднання, перш ніж ffmpeg сам завершиться з помилкою.
OUTBOUND_RW_TIMEOUT_USEC = 15_000_000

# Мінімальний інтервал між повторними toast-попередженнями про
# нестабільне з'єднання з платформою (після того, як воно вже було
# успішним) -- без цього N хвилин обривів дали б N/1.5с тостів поспіль.
FLAPPING_TOAST_COOLDOWN_SEC = 30

# Той самий принцип для повторних спроб OBS<->MediaMTX.
CONNECT_TIMEOUT_TOAST_COOLDOWN_SEC = 15

# Пауза між циклами пінгу увімкнених площадок (Control). Для ICMP-режиму
# кожен замір сам триває до ~кількох секунд (системний ping), тож
# фактична каденція = час замірів + ця пауза.
PING_INTERVAL_SEC = 3


def _safe_proc_name(name: str) -> str:
    # Ім'я площадки йде в ім'я лог-файлу ffmpeg (ffmpeg-out-<name>.log) --
    # прибираємо все, що не годиться для імені файлу.
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name) or "unnamed"


class Destination:
    """
    Одна платформа-виход. Обгортає власний ffmpeg (`-i pipe:0 -c copy
    -f flv <url>`) і `OutputSink` (черга + потік-писар). Зберігає
    `server`+`key` окремо (як їх дає площадка), а фінальний `url`
    складає `output_url.build_push_url` (join + IVS-нормалізація).
    Список аргументів — через callable, тож зміна `url` підхоплюється
    при наступному (пере)запуску без перестворення процесу.
    """

    def __init__(self, name: str, server: str, key: str, is_primary: bool, enabled: bool, controller, log_dir: Path):
        self.name = name
        self.server = server
        self.key = key
        self.url = output_url.build_push_url(server, key)
        self.is_primary = is_primary
        self.enabled = enabled
        # failed=True -- ключ/URL визнано невалідним (never_succeeded),
        # площадку зупинено; лишається enabled, поки користувач сам не
        # зніме галочку в Control. Скидається на старті наступного ефіру.
        self.failed = False
        self.rtt_ms: int | None = None

        self.sink = OutputSink(name, is_primary=is_primary)
        self._controller = controller
        self.proc = FfmpegProcess(
            f"out-{_safe_proc_name(name)}",
            self._build_args,
            log_dir,
            stdin_pipe=True,
            on_start=self._on_start,
            on_exit=self.sink.detach,
            on_flapping=self._on_flapping,
        )

    def _build_args(self):
        return [
            "ffmpeg", "-hide_banner", "-loglevel", "warning",
            "-y",
            "-i", "pipe:0",
            "-c", "copy",
            "-rw_timeout", str(OUTBOUND_RW_TIMEOUT_USEC),
            "-f", "flv", self.url,
        ]

    def _on_start(self, proc):
        self._controller.switcher.register_sink(self.sink)
        self.sink.attach(proc.stdin, self._controller.switcher.current_headers())

    def _on_flapping(self, never_succeeded: bool):
        self._controller._on_destination_flapping(self, never_succeeded)


class Controller:
    def __init__(self, config: dict, base_dir: Path, config_path: Path | None = None):
        self.config = config
        self.base_dir = base_dir
        self.config_path = config_path or (base_dir / "controller" / "config.json")
        self.log_dir = base_dir / "controller"
        self.lock = threading.RLock()
        self.state = STATE_OFFLINE
        self._state_since = time.time()
        self._timeout_timer: threading.Timer | None = None
        self._fallback_deadline: float | None = None
        self._last_flapping_toast_at = 0.0
        self._last_connect_timeout_toast_at = 0.0
        self._stopping = threading.Event()
        # OFFLINE через помилку (жодна площадка не досяжна), а не через
        # свідомий стоп/таймаут -- дашборд показує окремий "Halt" бейдж.
        self._halted = False

        # HALT із дашборда -- session-id латч (лише в пам'яті, без
        # персиста). obs-source.html генерить `OBSId` на реальному старті
        # OBS і шле його -> `_last_started_obs_id`. На HALT запам'ятовуємо
        # цю сесію в `_last_halted_obs_id`. Поки `_last_started ==
        # _last_halted`: (1) `on_available` ігнорує публікацію (не
        # рестартимо заглушену сесію -> без "вспышки" й без нескінченного
        # авто-рестарту, якщо в OBS немає прав на самостоп); (2) obs-source
        # цієї ж сесії при (пере)підключенні отримує `stop_streaming`.
        # Новий старт OBS -> новий id -> `_last_started != _last_halted` ->
        # латч знято, свіжий стрім не глушиться.
        self._last_started_obs_id: str | None = None
        self._last_halted_obs_id: str | None = None

        # Необов'язкові колбеки -- підключаються ззовні (controller.py),
        # сама Controller нічого не знає про HTTP/WS/дашборд.
        self.on_change: Callable[[], None] | None = None          # зміна стану -> hub.notify
        self.on_event: Callable[[str, str], None] | None = None   # toast -> hub.push_event
        self.on_control: Callable[[str], None] | None = None      # команда клієнтам /ws (напр. stop_streaming)

        mtx_host = config["mediamtx_rtmp_host"]
        mtx_port = config["mediamtx_rtmp_port"]
        user = config["internal_user"]
        password = config["internal_pass"]
        live_path = config["live_path"]

        # MediaMTX приймає логін/пароль для RTMP лише через query-параметри
        # (?user=...&pass=...), а НЕ через звичний rtmp://user:pass@host.
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

        # Виходи: primary (обов'язковий) першим, далі restreams. Кожен
        # тримає server+key окремо; back-compat зі старим єдиним полем
        # url/twitch_url -- читаємо його як server із порожнім key.
        self.destinations: dict[str, Destination] = {}
        primary_server = config.get("primary_server") or config.get("primary_url") or config.get("twitch_url", "")
        primary_key = config.get("primary_key", "")
        primary_name = config.get("primary_name", "primary")
        primary_enabled = config.get("primary_enabled", True)
        self._create_destination(primary_name, primary_server, primary_key, is_primary=True, enabled=primary_enabled)
        for item in config.get("restreams", []):
            if not isinstance(item, dict) or not item.get("name"):
                continue
            server = item.get("server") or item.get("url", "")
            if not server:
                continue
            self._create_destination(
                item["name"], server, item.get("key", ""),
                is_primary=False, enabled=bool(item.get("enabled", False)),
            )

        threading.Thread(target=self._ping_loop, name="ping", daemon=True).start()

    # --- обробники подій джерела (source) ---

    def on_available(self):
        with self.lock:
            if self._is_current_session_halted():
                # Ця сесія OBS заглушена з дашборда (HALT) -- не стартуємо
                # ефір на її (пере)публікацію. Стоп самому OBS шлеться, коли
                # його obs-source (пере)підключиться (register_source).
                logging.info(
                    "OBS is publishing, but this session was halted from the dashboard "
                    "-> ignoring (not restarting the broadcast)"
                )
                return

            self._cancel_timeout()

            if self.state == STATE_FALLBACK:
                # backup лишається активним, поки switcher не перемкне
                # безшовно через _on_switched_to_relay. Виходи не чіпаємо
                # -- вони весь час мирорять канонічний потік.
                logging.info(
                    "OBS reconnected -> waiting for the first live keyframe in the "
                    "background, backup video stays active until the seamless switch"
                )
                self.switcher.request_switch("relay", on_switched=self._on_switched_to_relay)
                self.relay.start()
            else:
                if self.state == STATE_OFFLINE:
                    logging.info("OBS started publishing -> starting the broadcast")
                    self._emit_event("info", "Broadcast started")
                    self._last_flapping_toast_at = 0.0
                    self._halted = False
                    for dest in self._enabled_destinations():
                        self._start_destination(dest)
                self.backup.stop()
                self.switcher.set_active("relay")
                self.relay.start()
                self._set_state(STATE_LIVE)

        # Поза self.lock: ffprobe + можливе перекодування можуть тривати
        # секунди -- не тримати через них стейт-машину заблокованою.
        self._backup_preparer.prepare_async(self._live_probe_url)

    def _on_switched_to_relay(self, params_changed: bool):
        """
        Callback від FLVSwitcher.request_switch (з reader-потоку relay,
        поза локами свіча). Робота винесена в окремий потік, що бере
        self.lock, щоб не блокувати reader і не перегнатись із
        паралельним on_unavailable/on_available.
        """
        def _finish():
            with self.lock:
                if self.state != STATE_FALLBACK:
                    return
                if params_changed:
                    logging.warning(
                        "live parameters changed while OBS was unavailable -> "
                        "reconnecting all platforms with a clean connection instead "
                        "of a seamless switch"
                    )
                    self.switcher.set_active("relay")
                    for dest in self._enabled_destinations():
                        if dest.failed:
                            continue
                        dest.proc.stop()
                        dest.proc.start()
                else:
                    logging.info("live is ready (first keyframe received) -> seamless switch, stopping the backup video")
                self._set_state(STATE_LIVE)
                self.backup.stop()

        threading.Thread(target=_finish, daemon=True).start()

    def on_unavailable(self):
        with self.lock:
            self.relay.stop()

            if self.state == STATE_OFFLINE:
                return

            if self.state == STATE_FALLBACK:
                # Уже в FALLBACK -- найімовірніше, власний read-timeout
                # детектор (_on_relay_stalled) устиг раніше за цей хук.
                # backup/таймер/стан не чіпаємо.
                return

            if self._enabled_destinations() and not self._any_enabled_destination_alive():
                logging.error(
                    "OBS disconnected, and no enabled platform was ever reached this "
                    "broadcast -- no point looping the backup video, stopping the broadcast"
                )
                self._give_up_on_unreachable()
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
        від obs-source.html (невидимий Browser Source-скрипт, що полить
        window.obsstudio.getStatus() і шле stop_broadcast у /ws на
        переході streaming true -> false). Ловить розрив РАНІШЕ за
        MediaMTX<->OBS, тому трансляція завершується одразу, без
        заглушки й таймауту.
        """
        with self.lock:
            # OBS реально зупинився -> сесія завершена, знімаємо латч (і
            # заглушену, і активну сесію), навіть якщо стан уже OFFLINE
            # (напр. після HALT, коли obs-source нарешті зупинив OBS).
            self._last_started_obs_id = None
            self._last_halted_obs_id = None
            if self.state == STATE_OFFLINE:
                return
            logging.info("OBS reports streaming stopped (obs-source.html) -> ending the broadcast")
            self._emit_event("info", "Broadcast ended")
            self.relay.stop()
            self.backup.stop()
            self._stop_all_destinations()
            self._cancel_timeout()
            self._set_state(STATE_OFFLINE)

    def on_dashboard_halt(self) -> None:
        """
        Ручний "HALT" з дашборда (червона кнопка в шапці): негайно
        зупиняє весь ефір (relay/backup/усі виходи) і командує OBS
        зупинити стрім (`stop_streaming` -> obs-source.html, якщо той
        має Page permission "Full access to OBS"). Сценарій: користувач
        відпав (OBS/ПК завис, крутиться backup) і заходить у дашборд з
        телефона, щоб заглушити трансляцію звідти. Це свідомий стоп ->
        стан OFFLINE (не FAILURE), `_halted` не виставляємо.
        """
        with self.lock:
            if self.state == STATE_OFFLINE:
                return
            logging.warning("HALT requested from the dashboard -> stopping the broadcast and asking OBS to stop")
            self._emit_event("warning", "Broadcast halted from the dashboard")
            self.relay.stop()
            self.backup.stop()
            self._stop_all_destinations()
            self._cancel_timeout()
            # Запамʼятовуємо поточну сесію OBS як заглушену -> латч (див.
            # on_available). Якщо id невідомий (obs-source жодного разу не
            # доповів) -- лишається None: HALT спрацює разово (без латча),
            # заглушити нема за чим.
            self._last_halted_obs_id = self._last_started_obs_id
            self._set_state(STATE_OFFLINE)
            # Одразу шлемо стоп підключеним obs-source (це і є заглушена
            # сесія); тим, хто підключиться пізніше, шле http_server на
            # register_source (точково, за збігом obs_id).
            self._request_stop_streaming_in_obs()

    def report_obs_session(self, obs_id) -> None:
        """
        obs-source.html доповів id поточної сесії стриму OBS (у
        `register_source` при коннекті або в `obs_streaming_started` на
        реальному старті). Порожній/None ігноруємо (сесія невідома --
        напр. сторінку джерела перезавантажили посеред стриму), щоб не
        затерти відомий id.
        """
        if not obs_id:
            return
        with self.lock:
            self._last_started_obs_id = obs_id

    def is_session_halted(self, obs_id) -> bool:
        """Чи саме ця сесія OBS заглушена (для точкового stop_streaming при register_source)."""
        with self.lock:
            return bool(obs_id) and obs_id == self._last_halted_obs_id

    def _is_current_session_halted(self) -> bool:
        # Під self.lock. Латч активний, поки остання відома сесія OBS
        # збігається із заглушеною.
        return self._last_halted_obs_id is not None and self._last_started_obs_id == self._last_halted_obs_id

    def on_obs_streaming_started(self) -> None:
        # Лише лог -- стан і так виставляється через runOnAvailable/
        # on_available; це підтвердження зі сторони OBS, не тригер.
        logging.info("OBS reports streaming started (obs-source.html)")

    def on_mediamtx_connect_timeout(self) -> None:
        """
        MediaMTX закрив з'єднання OBS по readTimeout, так і не
        дочекавшись публікації -- runOnAvailable/runOnUnavailable у
        цьому разі жодного разу не спрацьовують (звідси й приходить цей
        виклик з mediamtx_log_watch.py).
        """
        with self.lock:
            if self.state != STATE_OFFLINE:
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

    # --- керування виходами (Control / Settings) ---

    def enable_destination(self, name: str) -> None:
        with self.lock:
            dest = self.destinations.get(name)
            if dest is None or dest.enabled:
                return
            dest.enabled = True
            if self.state in (STATE_LIVE, STATE_FALLBACK):
                self._start_destination(dest)
                logging.info("enabled platform %s (started live)", name)
            else:
                logging.info("enabled platform %s (will start on next broadcast)", name)
            self._sync_outputs_config()
            self._persist()

    def disable_destination(self, name: str) -> None:
        with self.lock:
            dest = self.destinations.get(name)
            if dest is None or not dest.enabled:
                return
            dest.enabled = False
            dest.failed = False
            self._stop_destination(dest)
            logging.info("disabled platform %s", name)
            self._sync_outputs_config()
            self._persist()

    # --- негайний CRUD площадок (вкладка Settings) -- кожна дія
    # застосовується одразу й персиститься, як enable/disable; окремого
    # Apply тут нема (Apply лишився тільки для System-блоку). Викликач
    # (http_server) відповідає за попередню валідацію.

    def add_destination(self, name: str, server: str, key: str) -> None:
        with self.lock:
            # Нова площадка -- вимкнена; вмикає користувач у Control.
            self._create_destination(name, server, key, is_primary=False, enabled=False)
            logging.info("added platform %s", name)
            self._sync_outputs_config()
            self._persist()

    def update_destination(self, name: str, new_name: str, server: str, key: str) -> None:
        with self.lock:
            dest = self.destinations.get(name)
            if dest is None:
                return
            was_enabled = dest.enabled
            if new_name != name:
                # Перейменування -> чисте перестворення (sink/proc ключуються
                # іменем): знімаємо старий, ставимо новий зі збереженим enabled.
                is_primary = dest.is_primary
                self._remove_destination(dest)
                new_dest = self._create_destination(new_name, server, key, is_primary=is_primary, enabled=was_enabled)
                if was_enabled and self.state in (STATE_LIVE, STATE_FALLBACK):
                    self._start_destination(new_dest)
            else:
                changed = server != dest.server or key != dest.key
                dest.server = server
                dest.key = key
                dest.url = output_url.build_push_url(server, key)
                if changed and dest.enabled and self.state in (STATE_LIVE, STATE_FALLBACK):
                    dest.proc.stop()  # bounce лише цієї площадки з новим URL
                    dest.proc.start()
            logging.info("updated platform %s", new_name)
            self._sync_outputs_config()
            self._persist()

    def remove_destination(self, name: str) -> None:
        with self.lock:
            dest = self.destinations.get(name)
            if dest is None or dest.is_primary:
                return  # primary незнищенний
            self._remove_destination(dest)
            logging.info("removed platform %s", name)
            self._sync_outputs_config()
            self._persist()

    def apply_settings(self, values: dict) -> None:
        """
        System-блок вкладки Settings: `backup_file`, `offline_timeout_sec`,
        `connect_timeout_ms`, `read_timeout_ms`. Оновлює `self.config` у
        пам'яті й застосовує backup_file живцем (новий BackupPreparer).
        Тайминги MediaMTX застосовує викликач окремо (mediamtx_control).
        Платформи сюди НЕ входять -- ними керують add/update/remove/enable/
        disable, кожна негайно. Викликати без self.lock -- бере сам.
        """
        with self.lock:
            self.config["offline_timeout_sec"] = int(values["offline_timeout_sec"])
            self.config["connect_timeout_ms"] = int(values["connect_timeout_ms"])
            self.config["read_timeout_ms"] = int(values["read_timeout_ms"])
            self.config["icmp_ping"] = bool(values.get("icmp_ping", False))

            new_backup = str(settings_store.resolve_backup_path(values["backup_file"], self.base_dir))
            if new_backup != self.config.get("backup_file"):
                self.config["backup_file"] = new_backup
                self._backup_preparer = BackupPreparer(Path(new_backup), self.config)

            self._persist()

    def outputs_for_settings(self) -> list[dict]:
        """Дані площадок для вкладки Settings: server/key (префіл модалки) + фінальний url (маскований показ)."""
        with self.lock:
            out = [
                {
                    "name": dest.name,
                    "is_primary": dest.is_primary,
                    "enabled": dest.enabled,
                    "server": dest.server,
                    "key": dest.key,
                    "url": dest.url,
                }
                for dest in self.destinations.values()
            ]
            out.sort(key=lambda d: not d["is_primary"])  # primary завжди першим
            return out

    # --- статус / життєвий цикл ---

    def status(self) -> dict:
        with self.lock:
            obs = self.switcher.source_stats()
            live = self._backup_preparer.last_live_params()
            if live:
                for key in ("width", "height", "fps", "video_codec", "audio_codec"):
                    obs[key] = live.get(key)

            dests = []
            for dest in self.destinations.values():
                stats = dest.sink.stats()
                dests.append({
                    "name": dest.name,
                    "is_primary": dest.is_primary,
                    "enabled": dest.enabled,
                    "failed": dest.failed,
                    "running": dest.proc.is_running(),
                    "pid": dest.proc.pid(),
                    "up": dest.proc.ever_ran_long(),
                    "uptime_sec": round(dest.proc.uptime_sec()),
                    "restarts": dest.proc.restart_count(),
                    "rtt_ms": dest.rtt_ms,
                    "dropped": stats["dropped"],
                    "behind": stats["behind"],
                })
            dests.sort(key=lambda d: not d["is_primary"])  # primary завжди першим

            return {
                "state": self.state,
                "state_since": self._state_since,
                "halted": self._halted,
                "manual_halt": self._is_current_session_halted(),
                "obs": obs,
                "relay_running": self.relay.is_running(),
                "relay_pid": self.relay.pid(),
                "backup_running": self.backup.is_running(),
                "backup_pid": self.backup.pid(),
                "fallback_deadline": self._fallback_deadline,
                "destinations": dests,
            }

    def shutdown(self):
        with self.lock:
            self._stopping.set()
            self._cancel_timeout()
            self.relay.stop()
            self.backup.stop()
            for dest in self.destinations.values():
                dest.proc.stop()
                dest.sink.close()

    # --- внутрішнє: виходи ---

    def _create_destination(self, name: str, server: str, key: str, is_primary: bool, enabled: bool) -> Destination:
        dest = Destination(name, server, key, is_primary, enabled, self, self.log_dir)
        self.destinations[name] = dest
        if enabled and self.state in (STATE_LIVE, STATE_FALLBACK):
            self._start_destination(dest)
        return dest

    def _remove_destination(self, dest: Destination) -> None:
        dest.proc.stop()
        self.switcher.unregister_sink(dest.name)
        dest.sink.close()
        self.destinations.pop(dest.name, None)

    def _start_destination(self, dest: Destination) -> None:
        dest.failed = False
        self.switcher.register_sink(dest.sink)
        dest.proc.start()

    def _stop_destination(self, dest: Destination) -> None:
        dest.proc.stop()
        self.switcher.unregister_sink(dest.name)

    def _stop_all_destinations(self) -> None:
        for dest in self.destinations.values():
            self._stop_destination(dest)

    def _enabled_destinations(self) -> list[Destination]:
        return [d for d in self.destinations.values() if d.enabled]

    def _any_enabled_destination_alive(self) -> bool:
        for dest in self.destinations.values():
            if dest.enabled and not dest.failed and (dest.proc.ever_ran_long() or dest.proc.is_running()):
                return True
        return False

    def _primary_destination(self) -> Destination:
        for dest in self.destinations.values():
            if dest.is_primary:
                return dest
        raise RuntimeError("primary destination missing")  # інваріант: primary завжди існує

    def _on_destination_flapping(self, dest: Destination, never_succeeded: bool) -> None:
        if never_succeeded:
            # Жодного успішного під'єднання цієї площадки від старту --
            # майже напевно невалідний URL/ключ. Зупиняємо ЇЇ (повторні
            # спроби нічого не змінять), а весь ефір рубаємо лише якщо
            # це була остання жива площадка (агрегатний failsafe).
            with self.lock:
                if self.state == STATE_OFFLINE:
                    return
                dest.failed = True
                self._stop_destination(dest)
                if self._enabled_destinations() and not self._any_enabled_destination_alive():
                    logging.error(
                        "no enabled platform could be reached this broadcast -- stopping the broadcast"
                    )
                    self._give_up_on_unreachable()
                else:
                    logging.warning(
                        "%s failed to connect (likely invalid URL/key) -- other platforms keep streaming",
                        dest.name,
                    )
                    self._emit_event(
                        "warning",
                        f"{dest.name}: failed to connect -- check its URL/key. Other platforms keep streaming.",
                    )
            return

        # Було успішне з'єднання цієї трансляції -- схоже на тимчасовий
        # мережевий збій, не невалідний ключ. Ретраїмо нескінченно
        # (супервізор), лише антиспам тостів.
        logging.warning(
            "%s keeps failing after a previously working connection -- possible network issue, still retrying",
            dest.name,
        )
        now = time.monotonic()
        with self.lock:
            if now - self._last_flapping_toast_at < FLAPPING_TOAST_COOLDOWN_SEC:
                return
            self._last_flapping_toast_at = now
        self._emit_event("warning", f"{dest.name}: connection keeps failing -- still retrying...")

    def _give_up_on_unreachable(self) -> None:
        """
        Спільний хвіст, коли жодна увімкнена площадка не досяжна (обидва
        шляхи: агрегат never_succeeded і on_unavailable без жодної живої
        площадки). Зупиняє все на нашому боці й командує OBS зупинити
        стрім (якщо obs-source.html підключений із Page permission "Full
        access to OBS"). Викликати під self.lock.
        """
        self.relay.stop()
        self.backup.stop()
        self._stop_all_destinations()
        self._cancel_timeout()
        self._halted = True
        self._set_state(STATE_OFFLINE)
        self._emit_event(
            "error",
            "Couldn't connect to any enabled platform -- check the URLs/keys in Settings. "
            "Broadcast stopped, and a stop command was sent to the OBS browser-source. If OBS "
            "is still streaming, set its Page permission to \"Full access to OBS\".",
        )
        self._request_stop_streaming_in_obs()

    def _request_stop_streaming_in_obs(self) -> None:
        logging.info(
            "sending stop_streaming control to any connected obs-source.html "
            "(requires its Page permission set to \"Full access to OBS\" to take effect)"
        )
        if self.on_control is not None:
            self.on_control("stop_streaming")

    # --- внутрішнє: source-детектори ---

    def _make_reader_hook(self, source_name: str):
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

    # --- внутрішнє: стан/конфіг/таймер ---

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

    def _sync_outputs_config(self) -> None:
        # Дзеркалимо поточні площадки в self.config (server+key роздільно,
        # не зібраний url) -- звідси _persist() пише їх у config.json.
        # Прибираємо застарілі одинарні поля (primary_url/twitch_url), щоб
        # back-compat-читання не воскрешало старе значення після правки.
        primary = self._primary_destination()
        self.config["primary_name"] = primary.name
        self.config["primary_server"] = primary.server
        self.config["primary_key"] = primary.key
        self.config["primary_enabled"] = primary.enabled
        self.config.pop("primary_url", None)
        self.config.pop("twitch_url", None)
        self.config["restreams"] = [
            {"name": d.name, "server": d.server, "key": d.key, "enabled": d.enabled}
            for d in self.destinations.values() if not d.is_primary
        ]

    def _persist(self) -> None:
        try:
            settings_store.persist(self.config_path, self.config)
        except OSError:
            logging.exception("failed to persist config.json")

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
                "gave up waiting for OBS to recover after %s s -> ending the broadcast entirely",
                self.config["offline_timeout_sec"],
            )
            self._emit_event("warning", "Broadcast ended -- OBS did not reconnect in time")
            self.backup.stop()
            self._stop_all_destinations()
            self._cancel_timeout()
            self._set_state(STATE_OFFLINE)

    def _ping_loop(self):
        while not self._stopping.is_set():
            use_icmp = bool(self.config.get("icmp_ping", False))
            for dest in list(self.destinations.values()):
                if not dest.enabled:
                    dest.rtt_ms = None
                elif use_icmp:
                    dest.rtt_ms = net_probe.icmp_rtt_ms(dest.url)
                else:
                    dest.rtt_ms = net_probe.tcp_rtt_ms(dest.url)
            self._stopping.wait(PING_INTERVAL_SEC)
