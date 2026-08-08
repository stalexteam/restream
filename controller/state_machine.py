"""
Стейт-машина безперервного рестриму OBS -> кілька платформ, розрізана
на два рівні:

- **Pipeline** — ОДИН незалежний канонічний потік: власний ingest-шлях
  MediaMTX (`live_path`), власні `relay`/`backup`/`FLVSwitcher`, власний
  набір `Destination`-виходів і власна стейт-машина непрерывності
  (`OFFLINE`/`LIVE`/`FALLBACK` + `offline_timeout`). Кожен пайплайн
  тримає власний `Pipeline.lock`.
- **Manager** — те, що завʼязано на "один OBS": session-латч (HALT),
  оракул штатного стопу, право слати `stop_streaming` в OBS, роутинг
  хуків MediaMTX по шляху, CRUD пайплайнів і їхніх виходів, персист,
  ping-петля, колбеки в hub. Тримає `Manager.lock`.

Інваріант локів: **`Manager.lock` -> `Pipeline.lock`, ніколи навпаки
одночасно.** Прямий напрям: `on_available(path)` бере `Manager.lock`
(латч/роутинг) і делегує `pipeline.*` (бере `Pipeline.lock`). Зворотний
напрям (`Pipeline -> Manager -> інші пайплайни`) НЕ виконується
синхронно під локом-звонарем: рішення диспатчиться в окремий потік
(`Manager.on_pipeline_gave_up`), де менеджер заново бере свої локи.
Персист і доступ до реєстру пайплайнів — ЛИШЕ на рівні Manager під
`Manager.lock`; методи Pipeline самі `_persist` не зовуть.

Failsafe-асиметрія: жорсткий стоп OBS (`stop_streaming`) — прерогатива
ТІЛЬКИ дефолтного пайплайна АБО ситуації "усі пайплайни мертві".
Додатковий пайплайн, що втратив усі свої площадки, глушить лише себе.
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
from backup_prep import BackupCache, BackupPreparer
from ffmpeg_proc import FfmpegProcess
from flv import read_flv_tags
from probe import probe_stream_params
from switcher import FLVSwitcher, MergeSwitcher, OutputSink

STATE_OFFLINE = "OFFLINE"    # OBS не публікує, на платформи нічого не йде
STATE_LIVE = "LIVE"          # живий відеопотік від OBS іде на платформи
STATE_FALLBACK = "FALLBACK"  # OBS відвалився, на платформи крутиться заглушка

# Типи пайплайнів (поле `type` у pcfg). `restream` -- класичний потік зі
# своїм RTMP-входом і виходами (дефолт при відсутності поля, back-compat).
# `input` -- лише іменований ingest-шлях без виходів (джерело для remux).
# `remux` -- виходи + backup, але джерело — merge video з одного чужого
# входу + audio з іншого (свого live_path немає). Див. plan.md §2.
TYPE_RESTREAM = "restream"
TYPE_INPUT = "input"
TYPE_REMUX = "remux"
_PIPELINE_TYPES = (TYPE_RESTREAM, TYPE_INPUT, TYPE_REMUX)

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

# Вікно кореляції "штатний стоп OBS <-> обрив доп-пайплайна" (оракул,
# §7). Доп-пайплайн, що обірвався в межах цього вікна від штатного
# стопу дефолтного, трактує це як штатне завершення (без заглушки).
ORACLE_WINDOW_SEC = 1.5

# Поля старого "плоского" конфіга (до колекції pipelines), що
# переїхали ВСЕРЕДИНУ пайплайна. При першому _persist() менеджер
# мігрує їх у pipelines[] і прибирає з верхнього рівня.
# `offline_timeout_sec` тут НЕМА -- він лишається ГЛОБАЛЬНИМ top-level
# полем (один OBS -> одне вікно очікування повернення; таймер веде лише
# дефолтний пайплайн, його спрацювання гасить усі).
_FLAT_PIPELINE_KEYS = (
    "live_path", "backup_file",
    "output_video_bitrate_kbps", "output_audio_bitrate_kbps",
    "primary_name", "primary_server", "primary_key", "primary_enabled",
    "primary_url", "twitch_url", "restreams",
)


def _safe_proc_name(name: str) -> str:
    # Ім'я йде в ім'я лог-файлу ffmpeg (ffmpeg-<name>.log) --
    # прибираємо все, що не годиться для імені файлу.
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name) or "unnamed"


def _pipeline_from_flat(config: dict) -> dict:
    """Зібрати єдиний дефолтний пайплайн зі старих плоских top-level полів (back-compat)."""
    primary_server = config.get("primary_server") or config.get("primary_url") or config.get("twitch_url", "")
    return {
        "name": "main",
        "type": TYPE_RESTREAM,
        "is_default": True,
        "enabled": True,
        "live_path": config.get("live_path", "live/main"),
        "backup_file": config.get("backup_file", ""),
        "primary_name": config.get("primary_name", "primary"),
        "primary_server": primary_server,
        "primary_key": config.get("primary_key", ""),
        "primary_enabled": config.get("primary_enabled", True),
        "restreams": config.get("restreams", []),
    }


def normalize_pipelines(config: dict) -> list[dict]:
    """
    Список конфіг-словників пайплайнів. Якщо `pipelines` немає (старий
    плоский конфіг) -- мігруємо в один дефолтний пайплайн. Гарантуємо
    рівно один `is_default` (перший, якщо жоден не помічений).
    """
    raw = config.get("pipelines")
    if not isinstance(raw, list):
        return [_pipeline_from_flat(config)]
    pipelines = [p for p in raw if isinstance(p, dict) and p.get("name")]
    if not pipelines:
        return [_pipeline_from_flat(config)]
    for p in pipelines:
        # Відсутнє/невідоме `type` -> restream (back-compat зі старим
        # конфігом без поля типу). Дефолтний завжди restream (у нього є вхід).
        if p.get("type") not in _PIPELINE_TYPES:
            p["type"] = TYPE_RESTREAM
    if not any(p.get("is_default") for p in pipelines):
        pipelines[0]["is_default"] = True
    return pipelines


class Destination:
    """
    Одна платформа-виход. Обгортає власний ffmpeg (`-i pipe:0 -c copy
    -f flv <url>`) і `OutputSink` (черга + потік-писар). Зберігає
    `server`+`key` окремо (як їх дає площадка), а фінальний `url`
    складає `output_url.build_push_url` (join + IVS-нормалізація).
    Список аргументів — через callable, тож зміна `url` підхоплюється
    при наступному (пере)запуску без перестворення процесу.
    """

    def __init__(self, name: str, server: str, key: str, is_primary: bool, enabled: bool, pipeline, log_dir: Path):
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
        self._pipeline = pipeline
        # Ім'я процесу тегуємо пайплайном -- лог-файли й компоненти
        # дашборда не конфліктують між пайплайнами з однаковими іменами
        # площадок.
        self.proc = FfmpegProcess(
            f"out-{_safe_proc_name(pipeline.name)}-{_safe_proc_name(name)}",
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
        self._pipeline.switcher.register_sink(self.sink)
        self.sink.attach(proc.stdin, self._pipeline.switcher.current_headers())

    def _on_flapping(self, never_succeeded: bool):
        self._pipeline._on_destination_flapping(self, never_succeeded)


class Pipeline:
    """
    Один незалежний канонічний потік (див. докстринг модуля). `pcfg` --
    конфіг-словник саме цього пайплайна (name/is_default/enabled/
    live_path/backup_file/offline_timeout_sec/бітрейти/primary_*/
    restreams). `global_config` — спільний top-level конфіг (mediamtx-
    креди, read_timeout_ms тощо), читається живцем, тож live-apply
    глобальних налаштувань підхоплюється без перестворення пайплайна.
    """

    PIPELINE_TYPE = TYPE_RESTREAM

    def __init__(self, manager, pcfg: dict, global_config: dict, base_dir: Path, log_dir: Path):
        self._init_common(manager, pcfg, global_config, base_dir, log_dir)

        # --- source-половина: один relay на власному live_path ---
        self.live_path = pcfg["live_path"]
        mtx_host = global_config["mediamtx_rtmp_host"]
        mtx_port = global_config["mediamtx_rtmp_port"]
        user = global_config["internal_user"]
        password = global_config["internal_pass"]

        # MediaMTX приймає логін/пароль для RTMP лише через query-параметри
        # (?user=...&pass=...), а НЕ через звичний rtmp://user:pass@host.
        live_url = f"rtmp://{mtx_host}:{mtx_port}/{self.live_path}?user={user}&pass={password}"
        self.live_url = live_url
        self._live_probe_url = live_url

        tag = _safe_proc_name(self.name)
        self.relay = FfmpegProcess(
            f"relay-{tag}",
            [
                "ffmpeg", "-hide_banner", "-loglevel", "warning",
                "-i", live_url,
                "-c", "copy",
                "-f", "flv", "pipe:1",
            ],
            log_dir,
            capture_stdout=True,
            on_start=self._make_reader_hook("relay"),
        )

    def _init_common(self, manager, pcfg: dict, global_config: dict, base_dir: Path, log_dir: Path) -> None:
        """
        Спільна output-половина будь-якого пайплайна з виходами
        (restream + remux): стан/локи/switcher/backup/destinations. НЕ
        чіпає source-половину (relay для restream, два relay+merge для
        remux) -- її кожен тип будує сам після цього виклику.
        """
        self._manager = manager
        self.pcfg = pcfg
        self._gcfg = global_config
        self.base_dir = base_dir
        self.log_dir = log_dir

        self.name = pcfg["name"]
        self.is_default = bool(pcfg.get("is_default", False))
        self.enabled = bool(pcfg.get("enabled", True))

        self.lock = threading.RLock()
        self.state = STATE_OFFLINE
        self._state_since = time.time()
        self._timeout_timer: threading.Timer | None = None
        self._fallback_deadline: float | None = None
        # Оракул: відкладена перепроверка "чи це був штатний стоп" (§7).
        self._oracle_timer: threading.Timer | None = None
        self._last_flapping_toast_at = 0.0
        # OFFLINE через помилку (жодна площадка не досяжна), а не через
        # свідомий стоп/таймаут -- дашборд показує окремий "Failure" бейдж.
        self._halted = False

        self.switcher = self._make_switcher()
        backup_source = settings_store.resolve_backup_path(pcfg.get("backup_file", ""), base_dir)
        # Цільовий бітрейт заглушки автодетектиться з ВИМІРЯНОГО бітрейту
        # живого потоку (switcher.source_stats) -- ручного вводу немає.
        self._backup_preparer = BackupPreparer(
            backup_source, pcfg, manager._backup_cache, self.switcher.source_stats)

        tag = _safe_proc_name(self.name)
        self.backup = FfmpegProcess(
            f"backup-{tag}",
            lambda: [
                "ffmpeg", "-hide_banner", "-loglevel", "warning",
                "-stream_loop", "-1", "-re",
                "-i", str(self._backup_preparer.current_source()),
                "-c", "copy",
                "-f", "flv", "pipe:1",
            ],
            log_dir,
            capture_stdout=True,
            on_start=self._make_reader_hook("backup"),
        )

        # Виходи: primary (обов'язковий) першим, далі restreams. Кожен
        # тримає server+key окремо; back-compat зі старим єдиним полем
        # url/twitch_url -- читаємо його як server із порожнім key.
        self.destinations: dict[str, Destination] = {}
        primary_server = pcfg.get("primary_server") or pcfg.get("primary_url") or pcfg.get("twitch_url", "")
        primary_key = pcfg.get("primary_key", "")
        primary_name = pcfg.get("primary_name", "primary")
        primary_enabled = pcfg.get("primary_enabled", True)
        self._create_destination(primary_name, primary_server, primary_key, is_primary=True, enabled=primary_enabled)
        for item in pcfg.get("restreams", []):
            if not isinstance(item, dict) or not item.get("name"):
                continue
            server = item.get("server") or item.get("url", "")
            if not server:
                continue
            self._create_destination(
                item["name"], server, item.get("key", ""),
                is_primary=False, enabled=bool(item.get("enabled", False)),
            )

    # --- невеликі помічники подій (тегуються іменем пайплайна) ---

    def _emit(self, level: str, text: str) -> None:
        self._manager.emit_pipeline_event(self, level, text)

    def _notify(self) -> None:
        self._manager._notify()

    def subscriptions(self) -> list[tuple[str, str]]:
        """Хук-підписки (path, role) для роутингу MediaMTX (1:N). restream/input володіють своїм live_path."""
        return [(self.live_path, "owner")]

    # --- обробники подій джерела (source). Латч перевіряє менеджер ПЕРЕД
    # делегуванням, тож тут його вже немає. ---

    def on_available(self) -> None:
        with self.lock:
            self._cancel_timeout()
            self._cancel_oracle()

            if self.state == STATE_FALLBACK:
                # backup лишається активним, поки switcher не перемкне
                # безшовно через _on_switched_to_relay. Виходи не чіпаємо
                # -- вони весь час мирорять канонічний потік.
                logging.info(
                    "[%s] OBS reconnected -> waiting for the first live keyframe in the "
                    "background, backup video stays active until the seamless switch",
                    self.name,
                )
                self.switcher.request_switch("relay", on_switched=self._on_switched_to_relay)
                self.relay.start()
            else:
                if self.state == STATE_OFFLINE:
                    logging.info("[%s] OBS started publishing -> starting the broadcast", self.name)
                    self._emit("info", "Broadcast started")
                    self._last_flapping_toast_at = 0.0
                    self._halted = False
                    for dest in self._enabled_destinations():
                        self._start_destination(dest)
                self.backup.stop()
                self.switcher.set_active("relay")
                self.relay.start()
                self._set_state(STATE_LIVE)

            # prepare_async лише СПАВНИТЬ фоновий потік (probe/перекод у
            # ньому) -- виклик миттєвий, тримати лок безпечно.
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
                        "[%s] live parameters changed while OBS was unavailable -> "
                        "reconnecting all platforms with a clean connection instead "
                        "of a seamless switch",
                        self.name,
                    )
                    self.switcher.set_active("relay")
                    for dest in self._enabled_destinations():
                        if dest.failed:
                            continue
                        dest.proc.stop()
                        dest.proc.start()
                else:
                    logging.info("[%s] live is ready (first keyframe received) -> seamless switch, stopping the backup video", self.name)
                self._set_state(STATE_LIVE)
                self.backup.stop()

        threading.Thread(target=_finish, daemon=True).start()

    def on_unavailable(self) -> None:
        with self.lock:
            self.relay.stop()

            if self.state == STATE_OFFLINE:
                return

            if self.state == STATE_FALLBACK:
                # Уже в FALLBACK -- найімовірніше, власний read-timeout
                # детектор (_on_relay_stalled) устиг раніше за цей хук.
                # backup/таймер/стан не чіпаємо.
                return

            # Аукс-пайплайн НЕ вмикає заглушку, якщо немає «сесії», на яку
            # спертись: (а) обрив у вікні штатного стопу OBS (оракул §7), або
            # (б) головний (дефолтний) пайплайн зараз не в ефірі -- аукс
            # публікувався «сам по собі» (obs-multi-rtmp без старту головного
            # виходу OBS), його зупинку/обрив ловити нічим. Обидва -> чисто
            # завершуємо, без backup.
            if not self.is_default and (
                self._manager.is_graceful_recent() or not self._manager.is_main_session_live()
            ):
                logging.info("[%s] disconnected with no live main session to lean on -> clean end (no backup)", self.name)
                self._teardown_clean()
                return

            if self._enabled_destinations() and not self._any_enabled_destination_alive():
                logging.error(
                    "[%s] OBS disconnected, and no enabled platform was ever reached this "
                    "broadcast -- no point looping the backup video, stopping the broadcast",
                    self.name,
                )
                self._give_up_on_unreachable()
                return

            logging.warning(
                "[%s] OBS disconnected -> switching to backup video%s",
                self.name,
                f", waiting {self._offline_timeout()} s for recovery" if self.is_default else " (waiting on the default pipeline's timeout)",
            )
            self.switcher.set_active("backup")
            self.backup.start()
            if self.is_default:
                # Лише дефолтний веде offline-таймер (один OBS -> одне вікно
                # очікування повернення); його спрацювання гасить УСІ пайплайни.
                self._schedule_timeout()
            else:
                # Aux свого таймера НЕ веде -- лише перепровірка оракула
                # (штатний стоп міг прийти трохи ПІЗНІШЕ за обрив).
                self._schedule_oracle_recheck()
            self._set_state(STATE_FALLBACK)

    def _on_relay_stalled(self):
        with self.lock:
            if self.state != STATE_LIVE:
                return
            if not self.is_default and (
                self._manager.is_graceful_recent() or not self._manager.is_main_session_live()
            ):
                logging.info("[%s] relay stalled with no live main session to lean on -> clean end (no backup)", self.name)
                self._teardown_clean()
                return
            logging.warning(
                "[%s] no data from relay for %sms (network to OBS looks stalled) -> "
                "switching to backup video without dropping the relay connection",
                self.name, self._gcfg["read_timeout_ms"],
            )
            self.switcher.set_active("backup")
            self.backup.start()
            if self.is_default:
                self._schedule_timeout()
            else:
                self._schedule_oracle_recheck()
            self._set_state(STATE_FALLBACK)

    def _on_relay_resumed(self):
        with self.lock:
            if self.state != STATE_FALLBACK:
                return
            logging.info("[%s] data from relay resumed -> waiting for a keyframe for a seamless switch back", self.name)
            self.switcher.request_switch("relay", on_switched=self._on_switched_to_relay)

    # --- завершення (механічне, без OBS-впливу) ---

    def _make_switcher(self):
        """Фабрика switcher-а: restream -- FLVSwitcher; remux перевизначає на MergeSwitcher."""
        return FLVSwitcher()

    def _stop_sources(self) -> None:
        """Зупинити source-половину (для restream -- relay; remux перевизначає на два relay). Під self.lock."""
        self.relay.stop()

    def _teardown_clean(self) -> None:
        """Штатне чисте завершення (стоп vs обрив): усе стоп, стан OFFLINE, без заглушки/таймауту. Під self.lock."""
        self._stop_sources()
        self.backup.stop()
        self._stop_all_destinations()
        self._cancel_timeout()
        self._cancel_oracle()
        self._set_state(STATE_OFFLINE)

    def on_manual_stop(self) -> bool:
        """Штатний стоп OBS (obs-source.html). Повертає True, якщо був активний ефір. Під self.lock бере сам."""
        with self.lock:
            if self.state == STATE_OFFLINE:
                return False
            logging.info("[%s] OBS reports streaming stopped -> ending the broadcast", self.name)
            self._emit("info", "Broadcast ended")
            self._teardown_clean()
            return True

    def halt(self) -> bool:
        """Механічний стоп для HALT/CRUD (менеджер сам вирішує щодо OBS/подій). Повертає True, якщо був активний."""
        with self.lock:
            was_active = self.state != STATE_OFFLINE
            self._teardown_clean()
            return was_active

    def graceful_stop_if_fallback(self) -> None:
        """
        Оракул, штатний стоп при aux, УЖЕ сидячому в FALLBACK (§7): такий
        aux нового on_unavailable не отримає й досидів би весь offline_
        timeout. Менеджер зве це на всіх aux при on_manual_stop.
        """
        with self.lock:
            if self.state == STATE_FALLBACK:
                logging.info("[%s] graceful stop while in fallback -> clean end", self.name)
                self._teardown_clean()

    def set_master(self, active: bool) -> None:
        """
        Master AND-гейт над усіма площадками цього пайплайна (тумблер у
        шапці Control). Сам пайплайн (relay/стейт-машина) продовжує йти за
        публікацією OBS -- гейт лише вирішує, чи щось реально віддається
        назовні. OFF -> глушимо всі виходи; ON -> піднімаємо ті, що мають
        власну галочку, якщо ефір активний. Індивідуальні галочки площадок
        зберігаються (гейт їх лише пригнічує). Під self.lock бере сам.
        """
        with self.lock:
            if self.enabled == active:
                return
            self.enabled = active
            if not active:
                self._stop_all_destinations()
                logging.info("[%s] all platforms muted (pipeline keeps running)", self.name)
            else:
                if self.state in (STATE_LIVE, STATE_FALLBACK):
                    for dest in self._enabled_destinations():
                        self._start_destination(dest)
                logging.info("[%s] platforms un-muted", self.name)

    # --- керування виходами (механіка; персист/роутинг -- на менеджері) ---

    def enable_destination(self, name: str) -> None:
        with self.lock:
            dest = self.destinations.get(name)
            if dest is None or dest.enabled:
                return
            dest.enabled = True
            if self.enabled and self.state in (STATE_LIVE, STATE_FALLBACK):
                self._start_destination(dest)
                logging.info("[%s] enabled platform %s (started live)", self.name, name)
            elif not self.enabled:
                logging.info("[%s] enabled platform %s (pipeline muted -- starts when the pipeline toggle is on)", self.name, name)
            else:
                logging.info("[%s] enabled platform %s (will start on next broadcast)", self.name, name)

    def disable_destination(self, name: str) -> None:
        with self.lock:
            dest = self.destinations.get(name)
            if dest is None or not dest.enabled:
                return
            dest.enabled = False
            dest.failed = False
            self._stop_destination(dest)
            logging.info("[%s] disabled platform %s", self.name, name)

    def add_destination(self, name: str, server: str, key: str) -> None:
        with self.lock:
            # Нова площадка -- вимкнена; вмикає користувач у Control.
            self._create_destination(name, server, key, is_primary=False, enabled=False)
            logging.info("[%s] added platform %s", self.name, name)

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
                if was_enabled and self.enabled and self.state in (STATE_LIVE, STATE_FALLBACK):
                    self._start_destination(new_dest)
            else:
                changed = server != dest.server or key != dest.key
                dest.server = server
                dest.key = key
                dest.url = output_url.build_push_url(server, key)
                if changed and dest.enabled and self.enabled and self.state in (STATE_LIVE, STATE_FALLBACK):
                    dest.proc.stop()  # bounce лише цієї площадки з новим URL
                    dest.proc.start()
            logging.info("[%s] updated platform %s", self.name, new_name)

    def remove_destination(self, name: str) -> None:
        with self.lock:
            dest = self.destinations.get(name)
            if dest is None or dest.is_primary:
                return  # primary незнищенний
            self._remove_destination(dest)
            logging.info("[%s] removed platform %s", self.name, name)

    def apply_local_settings(self, backup_file: str | None) -> None:
        """Per-pipeline поля живцем (backup_file -> новий BackupPreparer). offline_timeout/бітрейти тут нема (глобальний / автодетект). Під self.lock бере сам."""
        with self.lock:
            if backup_file is not None:
                new_backup = str(settings_store.resolve_backup_path(backup_file, self.base_dir))
                if new_backup != str(settings_store.resolve_backup_path(self.pcfg.get("backup_file", ""), self.base_dir)):
                    self.pcfg["backup_file"] = backup_file
                    self._backup_preparer = BackupPreparer(
                        Path(new_backup), self.pcfg, self._manager._backup_cache, self.switcher.source_stats)

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

    def destination_names(self) -> list[str]:
        with self.lock:
            return [d.name for d in self.destinations.values()]

    def list_destinations(self) -> list[Destination]:
        with self.lock:
            return list(self.destinations.values())

    # --- статус / конфіг / життєвий цикл ---

    def _destinations_status(self) -> list[dict]:
        # Під self.lock. Спільний для restream і remux (успадковує).
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
        return dests

    def status(self) -> dict:
        with self.lock:
            obs = self.switcher.source_stats()
            live = self._backup_preparer.last_live_params()
            if live:
                for key in ("width", "height", "fps", "video_codec", "audio_codec"):
                    obs[key] = live.get(key)

            dests = self._destinations_status()

            return {
                "name": self.name,
                "type": self.PIPELINE_TYPE,
                "is_default": self.is_default,
                "enabled": self.enabled,
                "state": self.state,
                "state_since": self._state_since,
                "halted": self._halted,
                "obs": obs,
                "relay_running": self.relay.is_running(),
                "relay_pid": self.relay.pid(),
                "backup_running": self.backup.is_running(),
                "backup_pid": self.backup.pid(),
                "fallback_deadline": self._fallback_deadline,
                "destinations": dests,
            }

    def to_config(self) -> dict:
        with self.lock:
            primary = self._primary_destination()
            return {
                "name": self.name,
                "type": self.PIPELINE_TYPE,
                "is_default": self.is_default,
                "enabled": self.enabled,
                "live_path": self.live_path,
                "backup_file": self.pcfg.get("backup_file", ""),
                "primary_name": primary.name,
                "primary_server": primary.server,
                "primary_key": primary.key,
                "primary_enabled": primary.enabled,
                "restreams": [
                    {"name": d.name, "server": d.server, "key": d.key, "enabled": d.enabled}
                    for d in self.destinations.values() if not d.is_primary
                ],
            }

    def shutdown(self) -> None:
        with self.lock:
            self._cancel_timeout()
            self._cancel_oracle()
            self._stop_sources()
            self.backup.stop()
            for dest in self.destinations.values():
                dest.proc.stop()
                dest.sink.close()

    # --- внутрішнє: виходи ---

    def _create_destination(self, name: str, server: str, key: str, is_primary: bool, enabled: bool) -> Destination:
        dest = Destination(name, server, key, is_primary, enabled, self, self.log_dir)
        self.destinations[name] = dest
        if enabled and self.enabled and self.state in (STATE_LIVE, STATE_FALLBACK):
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
        # Master-гейт (self.enabled) закритий -> назовні не віддаємо нічого,
        # хоч би які були індивідуальні галочки площадок (AND-семантика).
        if not self.enabled:
            return []
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
            # спроби нічого не змінять), а весь пайплайн рубаємо лише якщо
            # це була остання жива площадка (агрегатний failsafe).
            with self.lock:
                if self.state == STATE_OFFLINE:
                    return
                dest.failed = True
                self._stop_destination(dest)
                if self._enabled_destinations() and not self._any_enabled_destination_alive():
                    logging.error(
                        "[%s] no enabled platform could be reached this broadcast -- stopping this pipeline",
                        self.name,
                    )
                    self._give_up_on_unreachable()
                else:
                    logging.warning(
                        "[%s] %s failed to connect (likely invalid URL/key) -- other platforms keep streaming",
                        self.name, dest.name,
                    )
                    self._emit(
                        "warning",
                        f"{dest.name}: failed to connect -- check its URL/key. Other platforms keep streaming.",
                    )
            return

        # Було успішне з'єднання цієї трансляції -- схоже на тимчасовий
        # мережевий збій, не невалідний ключ. Ретраїмо нескінченно
        # (супервізор), лише антиспам тостів.
        logging.warning(
            "[%s] %s keeps failing after a previously working connection -- possible network issue, still retrying",
            self.name, dest.name,
        )
        now = time.monotonic()
        with self.lock:
            if now - self._last_flapping_toast_at < FLAPPING_TOAST_COOLDOWN_SEC:
                return
            self._last_flapping_toast_at = now
        self._emit("warning", f"{dest.name}: connection keeps failing -- still retrying...")

    def _give_up_on_unreachable(self) -> None:
        """
        Жодна увімкнена площадка цього пайплайна не досяжна. Зупиняє все
        на нашому боці й делегує менеджеру рішення про OBS-стоп
        (failsafe-асиметрія §8: OBS глушиться лише якщо це дефолтний
        пайплайн АБО померли всі). Викликати під self.lock.
        """
        self._stop_sources()
        self.backup.stop()
        self._stop_all_destinations()
        self._cancel_timeout()
        self._cancel_oracle()
        self._halted = True
        self._set_state(STATE_OFFLINE)
        self._manager.on_pipeline_gave_up(self)

    # --- внутрішнє: source-детектори ---

    def _make_reader_hook(self, source_name: str):
        is_relay = source_name == "relay"

        def on_start(proc):
            kwargs = {}
            if is_relay:
                kwargs = {
                    "read_timeout_sec": self._gcfg["read_timeout_ms"] / 1000,
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

    # --- внутрішнє: стан/таймер/оракул ---

    def _offline_timeout(self) -> int:
        # Глобальне поле (один OBS -> одне вікно очікування). Веде таймер
        # лише дефолтний пайплайн; його спрацювання гасить усі.
        return int(self._gcfg.get("offline_timeout_sec", 1800))

    def _set_state(self, new_state: str) -> None:
        self.state = new_state
        self._state_since = time.time()
        self._notify()

    def _schedule_timeout(self):
        self._cancel_timeout()
        timeout_sec = self._offline_timeout()
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
        # Лише дефолтний пайплайн заводить цей таймер (один OBS -> одне
        # вікно). OBS не повернувся -> кінець усієї сесії: гасимо УСІ
        # пайплайни через менеджер. Викликаємо ПОЗА self.lock (з Timer-
        # потоку) -> без інверсії Pipeline->Manager.
        with self.lock:
            if self.state != STATE_FALLBACK:
                return
            self._cancel_timeout()
        logging.warning(
            "gave up waiting for OBS to recover after %s s -> ending the broadcast (all pipelines)",
            self._offline_timeout(),
        )
        self._manager.on_offline_timeout()

    def _schedule_oracle_recheck(self):
        # Під self.lock. Доп-пайплайн у FALLBACK: перепровірити, чи не
        # прийшов штатний стоп трохи пізніше за обрив (§7).
        self._cancel_oracle()
        self._oracle_timer = threading.Timer(ORACLE_WINDOW_SEC, self._oracle_recheck)
        self._oracle_timer.daemon = True
        self._oracle_timer.start()

    def _cancel_oracle(self):
        if self._oracle_timer is not None:
            self._oracle_timer.cancel()
            self._oracle_timer = None

    def _oracle_recheck(self):
        with self.lock:
            # No-op, якщо OBS уже повернувся (реконнект почав безшовний
            # cut) або стан уже не FALLBACK -- не затираємо початий
            # _on_relay_resumed/_on_switched_to_relay.
            if self.state != STATE_FALLBACK:
                return
            if self.switcher.pending_source is not None:
                return
            if self._manager.is_graceful_recent():
                logging.info("[%s] graceful stop confirmed after the drop -> clean end (no backup)", self.name)
                self._teardown_clean()


class RemuxPipeline(Pipeline):
    """
    Пайплайн-remux (plan.md §5.6): output-половина (destinations/backup/
    стан/failsafe/оракул) успадкована від `Pipeline`, а source -- це
    merge video з одного чужого входу + audio з іншого (свого live_path
    немає). Continuity -- AND-gate по двох входах (§5.3): LIVE лише коли
    обидва течуть, будь-який обрив -> FALLBACK (backup).

    **Фаза 1: реального merge ще немає (MergeSwitcher -- Фаза 2).** Тут
    лише стейт-машина за доступністю входів: обидва вгору -> LIVE (у
    Фазі 2 сюди підключаться два relay + MergeSwitcher), будь-який вниз
    -> backup. remux ніколи не default -> веде себе як aux (оракул,
    таймер offline веде дефолтний).
    """

    PIPELINE_TYPE = TYPE_REMUX

    def __init__(self, manager, pcfg: dict, global_config: dict, base_dir: Path, log_dir: Path):
        self._init_common(manager, pcfg, global_config, base_dir, log_dir)
        # source-половина: посилання на два чужі входи по live_path
        # (стабільні при rename джерела), власного live_path у remux немає.
        self.live_path = None
        self.video_src_path = pcfg.get("video_src_path", "")
        self.audio_src_path = pcfg.get("audio_src_path", "")
        self.audio_trim_ms = int(pcfg.get("audio_trim_ms", 0) or 0)
        # Готовність (хук available/unavailable) і стагнація (read-timeout)
        # кожної ролі. "Тече" = ready AND not stalled.
        self._src_ready: dict[str, bool] = {"video": False, "audio": False}
        self._src_stalled: dict[str, bool] = {"video": False, "audio": False}

        mtx_host = global_config["mediamtx_rtmp_host"]
        mtx_port = global_config["mediamtx_rtmp_port"]
        user = global_config["internal_user"]
        password = global_config["internal_pass"]
        self._video_probe_url = f"rtmp://{mtx_host}:{mtx_port}/{self.video_src_path}?user={user}&pass={password}"
        self._audio_probe_url = f"rtmp://{mtx_host}:{mtx_port}/{self.audio_src_path}?user={user}&pass={password}"

        tag = _safe_proc_name(self.name)
        self._relays: dict[str, FfmpegProcess] = {
            "video": self._make_source_relay(f"relay-{tag}-video", self._video_probe_url, "video"),
            "audio": self._make_source_relay(f"relay-{tag}-audio", self._audio_probe_url, "audio"),
        }

    def _make_switcher(self):
        return MergeSwitcher(
            reanchor=self._gcfg.get("remux_reanchor"),
            audio_trim_ms=int(self.pcfg.get("audio_trim_ms", 0) or 0),
        )

    def _make_source_relay(self, name: str, url: str, role: str) -> FfmpegProcess:
        return FfmpegProcess(
            name,
            ["ffmpeg", "-hide_banner", "-loglevel", "warning", "-i", url,
             "-c", "copy", "-f", "flv", "pipe:1"],
            self.log_dir,
            capture_stdout=True,
            on_start=self._make_source_reader(role),
        )

    def _make_source_reader(self, role: str):
        def on_start(proc):
            threading.Thread(
                target=read_flv_tags,
                args=(proc.stdout, role, self.switcher.process),
                kwargs={
                    "read_timeout_sec": self._gcfg["read_timeout_ms"] / 1000,
                    "on_stall": lambda: self._on_source_stalled(role),
                    "on_resume": lambda: self._on_source_resumed(role),
                },
                daemon=True,
            ).start()
        return on_start

    def subscriptions(self) -> list[tuple[str, str]]:
        subs = []
        if self.video_src_path:
            subs.append((self.video_src_path, "video"))
        if self.audio_src_path:
            subs.append((self.audio_src_path, "audio"))
        return subs

    def _stop_sources(self) -> None:
        for relay in self._relays.values():
            relay.stop()
        self._src_ready = {"video": False, "audio": False}
        self._src_stalled = {"video": False, "audio": False}

    def _flowing(self, role: str) -> bool:
        return self._src_ready.get(role, False) and not self._src_stalled.get(role, False)

    def _both_flowing(self) -> bool:
        return self._flowing("video") and self._flowing("audio")

    # remux не володіє власним шляхом -- owner-хука в нього нема; життєвий
    # цикл ведуть on_source_available/unavailable (роутинг Manager по ролі).
    def on_available(self) -> None:
        logging.warning("[%s] remux has no owner path -- on_available ignored", self.name)

    def on_unavailable(self) -> None:
        logging.warning("[%s] remux has no owner path -- on_unavailable ignored", self.name)

    def on_source_available(self, role: str) -> None:
        with self.lock:
            if role in self._src_ready:
                self._src_ready[role] = True
                self._src_stalled[role] = False
            relay = self._relays.get(role)
            if relay is not None:
                relay.start()
            self._reconsider_up()

    def on_source_unavailable(self, role: str) -> None:
        with self.lock:
            if role in self._src_ready:
                self._src_ready[role] = False
                self._src_stalled[role] = False
            relay = self._relays.get(role)
            if relay is not None:
                relay.stop()
            self._reconsider_down(role)

    def _on_source_stalled(self, role: str) -> None:
        # read-timeout на relay ролі: дані просіли (з'єднання може ще жити,
        # relay НЕ зупиняємо -- як restream). Трактуємо як «вхід просів».
        with self.lock:
            self._src_stalled[role] = True
            self._reconsider_down(role)

    def _on_source_resumed(self, role: str) -> None:
        with self.lock:
            self._src_stalled[role] = False
            self._reconsider_up()

    def _reconsider_up(self) -> None:
        # Під self.lock. Обидва входи течуть -> LIVE (cold-start з OFFLINE,
        # безшовний повернення з FALLBACK). Merge живиться reader-потоками
        # обох relay; тут лише перемикаємо активне джерело switcher-а.
        if not self._both_flowing():
            return
        self._cancel_timeout()
        self._cancel_oracle()
        if self.state == STATE_LIVE:
            return
        if self.state == STATE_FALLBACK:
            logging.info("[%s] both remux sources flowing again -> waiting for a live keyframe for a seamless switch", self.name)
            self.switcher.request_switch("live", on_switched=self._on_switched_to_live)
        else:  # OFFLINE
            logging.info("[%s] both remux sources up -> starting the broadcast", self.name)
            self._emit("info", "Broadcast started")
            self._last_flapping_toast_at = 0.0
            self._halted = False
            self.backup.stop()
            # Свіжа сесія -> чистий merge-стан (інакше протухлі last_out/offset
            # з попередньої сесії ламають нове аудіо/затримку -- див. reset()).
            self.switcher.reset()
            self.switcher.set_active("live")
            for dest in self._enabled_destinations():
                self._start_destination(dest)
            self._set_state(STATE_LIVE)
        self._backup_preparer.prepare_async_remux(self._video_probe_url, self._audio_probe_url)

    def _reconsider_down(self, role: str) -> None:
        # Під self.lock. Якийсь вхід перестав текти -> AND-gate у backup.
        if self.state == STATE_OFFLINE or self._both_flowing():
            return
        if self.state == STATE_FALLBACK:
            return
        # був LIVE. remux -- завжди aux: без живої головної сесії тушимо чисто.
        if self._manager.is_graceful_recent() or not self._manager.is_main_session_live():
            logging.info("[%s] remux %s source down with no live main session to lean on -> clean end (no backup)", self.name, role)
            self._teardown_clean()
            return
        if self._enabled_destinations() and not self._any_enabled_destination_alive():
            logging.error(
                "[%s] remux %s source down, and no enabled platform was ever reached -- stopping this pipeline",
                self.name, role,
            )
            self._give_up_on_unreachable()
            return
        logging.warning("[%s] remux %s source down -> switching to backup video (waiting on the default pipeline's timeout)", self.name, role)
        self.switcher.set_active("backup")
        self.backup.start()
        self._schedule_oracle_recheck()
        self._set_state(STATE_FALLBACK)

    def _on_switched_to_live(self, params_changed: bool):
        """Callback MergeSwitcher.request_switch (з reader-потоку). Робота -- в окремому потоці під self.lock (як Pipeline._on_switched_to_relay)."""
        def _finish():
            with self.lock:
                if self.state != STATE_FALLBACK:
                    return
                if params_changed:
                    logging.warning(
                        "[%s] remux source parameters changed while down -> reconnecting all platforms cleanly",
                        self.name,
                    )
                    self.switcher.set_active("live")
                    for dest in self._enabled_destinations():
                        if dest.failed:
                            continue
                        dest.proc.stop()
                        dest.proc.start()
                else:
                    logging.info("[%s] remux live is ready (first keyframe) -> seamless switch, stopping the backup video", self.name)
                self._set_state(STATE_LIVE)
                self.backup.stop()
        threading.Thread(target=_finish, daemon=True).start()

    def set_audio_trim(self, ms: int) -> None:
        """Ручний триммер аудіо (§5.2), live-apply: зсуває audio_offset у merge без реконнекту."""
        with self.lock:
            self.audio_trim_ms = int(ms)
            self.pcfg["audio_trim_ms"] = int(ms)
            self.switcher.set_audio_trim(int(ms))
            logging.info("[%s] audio_trim_ms set to %s", self.name, ms)

    def status(self) -> dict:
        with self.lock:
            return {
                "name": self.name,
                "type": self.PIPELINE_TYPE,
                "is_default": self.is_default,
                "enabled": self.enabled,
                "state": self.state,
                "state_since": self._state_since,
                "halted": self._halted,
                "obs": self.switcher.source_stats(),
                "backup_running": self.backup.is_running(),
                "backup_pid": self.backup.pid(),
                "fallback_deadline": self._fallback_deadline,
                "destinations": self._destinations_status(),
                # remux-специфіка для дашборда: статус обох входів (тече = ready
                # AND not stalled) + поточна оцінена A/V-Δ (skew).
                "video_src_path": self.video_src_path,
                "audio_src_path": self.audio_src_path,
                "audio_trim_ms": self.audio_trim_ms,
                "sources": {r: self._flowing(r) for r in ("video", "audio")},
                "skew_ms": self.switcher.skew_ms(),
            }

    def to_config(self) -> dict:
        with self.lock:
            primary = self._primary_destination()
            return {
                "name": self.name,
                "type": self.PIPELINE_TYPE,
                "is_default": self.is_default,
                "enabled": self.enabled,
                "video_src_path": self.video_src_path,
                "audio_src_path": self.audio_src_path,
                "audio_trim_ms": self.audio_trim_ms,
                "backup_file": self.pcfg.get("backup_file", ""),
                "primary_name": primary.name,
                "primary_server": primary.server,
                "primary_key": primary.key,
                "primary_enabled": primary.enabled,
                "restreams": [
                    {"name": d.name, "server": d.server, "key": d.key, "enabled": d.enabled}
                    for d in self.destinations.values() if not d.is_primary
                ],
            }


class InputPipeline:
    """
    Іменований ingest-шлях (plan.md §5.5): свій `live_path` для OBS-виходу,
    служить джерелом для remux. БЕЗ backup/destinations/стейт-машини
    непрерывності. Стан LIVE/OFFLINE веде публікація OBS (хуки MediaMTX).
    Щоб показувати реальну аудіо/відео-статистику вхідного потоку (а не
    лише boolean), тримає ЛЕГКИЙ stats-relay: власний ffmpeg-читач шляху
    -> `FLVSwitcher` БЕЗ жодного sink (лише source_stats: kbps/flowing) +
    разовий probe геометрії. Це окремий читач MediaMTX (незалежний від
    того, що вхід ще й читає remux).
    """

    PIPELINE_TYPE = TYPE_INPUT

    def __init__(self, manager, pcfg: dict, global_config: dict, base_dir: Path, log_dir: Path):
        self._manager = manager
        self.pcfg = pcfg
        self._gcfg = global_config
        self.base_dir = base_dir
        self.log_dir = log_dir
        self.name = pcfg["name"]
        self.is_default = False  # input ніколи не дефолтний
        self.enabled = True      # немає площадок -> master-гейт незастосовний
        self.live_path = pcfg["live_path"]
        self.lock = threading.RLock()
        self.state = STATE_OFFLINE
        self._state_since = time.time()

        mtx_host = global_config["mediamtx_rtmp_host"]
        mtx_port = global_config["mediamtx_rtmp_port"]
        user = global_config["internal_user"]
        password = global_config["internal_pass"]
        live_url = f"rtmp://{mtx_host}:{mtx_port}/{self.live_path}?user={user}&pass={password}"
        self._live_probe_url = live_url
        self._last_params: dict | None = None  # геометрія з probe (для тултипу)

        self.switcher = FLVSwitcher()  # без sink-ів -- лише лічильник source_stats
        tag = _safe_proc_name(self.name)
        self.relay = FfmpegProcess(
            f"input-{tag}",
            ["ffmpeg", "-hide_banner", "-loglevel", "warning", "-i", live_url,
             "-c", "copy", "-f", "flv", "pipe:1"],
            log_dir,
            capture_stdout=True,
            on_start=self._on_relay_start,
        )

    def _on_relay_start(self, proc):
        threading.Thread(
            target=read_flv_tags,
            args=(proc.stdout, "relay", self.switcher.process),
            daemon=True,
        ).start()

    def _probe_async(self) -> None:
        def run():
            params = probe_stream_params(self._live_probe_url)
            if params:
                self._last_params = params
        threading.Thread(target=run, daemon=True).start()

    def subscriptions(self) -> list[tuple[str, str]]:
        return [(self.live_path, "owner")]

    def on_available(self) -> None:
        with self.lock:
            if self.state != STATE_LIVE:
                logging.info("[%s] input is publishing -> LIVE", self.name)
                self.state = STATE_LIVE
                self._state_since = time.time()
                self.relay.start()      # почати вимірювати вхідний потік
                self._probe_async()     # геометрія для тултипу
        self._manager._notify()

    def on_unavailable(self) -> None:
        with self.lock:
            if self.state != STATE_OFFLINE:
                logging.info("[%s] input stopped publishing -> OFFLINE", self.name)
                self.state = STATE_OFFLINE
                self._state_since = time.time()
                self.relay.stop()
                self._last_params = None
        self._manager._notify()

    # --- no-op хуки життєвого циклу (input не має виходів/сесії) ---

    def halt(self) -> bool:
        # Стан входу веде виключно публікація OBS; HALT сам вхід не «зупиняє».
        return False

    def graceful_stop_if_fallback(self) -> None:
        pass

    def set_master(self, active: bool) -> None:
        pass

    def shutdown(self) -> None:
        with self.lock:
            self.relay.stop()

    def list_destinations(self) -> list:
        return []

    def outputs_for_settings(self) -> list[dict]:
        return []

    def destination_names(self) -> list[str]:
        return []

    def status(self) -> dict:
        with self.lock:
            obs = self.switcher.source_stats()
            if self._last_params:
                for key in ("width", "height", "fps", "video_codec", "audio_codec"):
                    obs[key] = self._last_params.get(key)
            return {
                "name": self.name,
                "type": self.PIPELINE_TYPE,
                "is_default": False,
                "enabled": True,
                "state": self.state,
                "state_since": self._state_since,
                "halted": False,
                "live_path": self.live_path,
                "obs": obs,
                "destinations": [],
            }

    def to_config(self) -> dict:
        with self.lock:
            return {
                "name": self.name,
                "type": self.PIPELINE_TYPE,
                "is_default": False,
                "enabled": True,
                "live_path": self.live_path,
            }


class Manager:
    """
    Верхній рівень: те, що завʼязано на "один OBS". Створює пайплайни з
    `config["pipelines"]` (з back-compat міграцією плоского конфіга),
    роутить хуки MediaMTX по шляху, тримає session-латч і оракул,
    персистить конфіг, крутить ping-петлю. Колбеки в hub
    (on_change/on_event/on_control) підключаються ззовні (controller.py).
    """

    def __init__(self, config: dict, base_dir: Path, config_path: Path | None = None):
        self.config = config
        self.base_dir = base_dir
        self.config_path = config_path or (base_dir / "data" / "config.json")
        self.log_dir = base_dir / "logs"
        self.lock = threading.RLock()
        self._stopping = threading.Event()

        # Для показу готового OBS Stream Key кожного пайплайна в дашборді
        # (динамічні шляхи -> замість комбобокса показуємо, КУДИ публікувати).
        self._public_host = config.get("public_host", "")
        self._rtmp_port = config.get("mediamtx_rtmp_port", 1935)
        self._obs_ingest_pass = config.get("obs_pass", "")

        # HALT із дашборда -- session-id латч (лише в пам'яті, без
        # персиста). obs-source.html генерить `OBSId` на реальному старті
        # OBS і шле його -> `_last_started_obs_id`. На HALT запам'ятовуємо
        # цю сесію в `_last_halted_obs_id`. Латч ГЛОБАЛЬНИЙ (один OBS) --
        # діє на всі пайплайни цієї OBS-сесії.
        self._last_started_obs_id: str | None = None
        self._last_halted_obs_id: str | None = None
        self._last_connect_timeout_toast_at = 0.0

        # Оракул штатного стопу (§7): monotonic() останнього штатного
        # стопу OBS. Читається доп-пайплайнами БЕЗ Manager.lock (простий
        # read float під GIL) -- щоб уникнути інверсії Pipeline->Manager.
        self._graceful_stop_at: float | None = None

        # Колбеки в hub -- підключаються ззовні (controller.py).
        self.on_change: Callable[[], None] | None = None
        self.on_event: Callable[[str, str], None] | None = None
        self.on_control: Callable[[str], None] | None = None

        # Спільний контент-адресуемий кэш готових заглушок (§6.9): один
        # вихідний файл, спільний для кількох пайплайнів, готується під
        # СВОЇ параметри кожного, без thrashing; однакові (джерело+
        # параметри) -- рівно один транскод.
        self._backup_cache = BackupCache(self.base_dir / "data" / "backup-cache")

        self.pipelines: dict[str, Pipeline] = {}
        # 1:N роутинг хуків (§5.4): один ingest-шлях може годувати кілька
        # пайплайнів. Значення -- список підписників (pipeline, role), де
        # role ∈ {"owner","video","audio"}. owner -> on_available/
        # on_unavailable; video/audio -> remux.on_source_(un)available.
        self._by_path: dict[str, list[tuple]] = {}
        # Пряме посилання на дефолтний пайплайн (оновлюється в
        # _instantiate_pipeline під Manager.lock). Аукс-пайплайни читають
        # його стан БЕЗ Manager.lock (is_main_session_live) -- щоб не було
        # інверсії Pipeline->Manager; тому кеш, а не пошук по dict.
        self._default: Pipeline | None = None
        for pcfg in normalize_pipelines(config):
            self._instantiate_pipeline(pcfg)

        threading.Thread(target=self._ping_loop, name="ping", daemon=True).start()

    def _instantiate_pipeline(self, pcfg: dict):
        ptype = pcfg.get("type", TYPE_RESTREAM)
        if ptype == TYPE_INPUT:
            pipeline = InputPipeline(self, pcfg, self.config, self.base_dir, self.log_dir)
        elif ptype == TYPE_REMUX:
            pipeline = RemuxPipeline(self, pcfg, self.config, self.base_dir, self.log_dir)
        else:
            pipeline = Pipeline(self, pcfg, self.config, self.base_dir, self.log_dir)
        self.pipelines[pipeline.name] = pipeline
        self._register_subscriptions(pipeline)
        if pipeline.is_default:
            self._default = pipeline
        return pipeline

    def _register_subscriptions(self, pipeline) -> None:
        # Під Manager.lock. Кожна (path, role)-підписка пайплайна ->
        # список підписників цього шляху (1:N).
        for path, role in pipeline.subscriptions():
            self._by_path.setdefault(path, []).append((pipeline, role))

    def _unregister_subscriptions(self, pipeline) -> None:
        # Під Manager.lock. Знімає всі підписки пайплайна з усіх шляхів.
        for path in list(self._by_path):
            remaining = [s for s in self._by_path[path] if s[0] is not pipeline]
            if remaining:
                self._by_path[path] = remaining
            else:
                self._by_path.pop(path, None)

    def _default_pipeline(self) -> Pipeline:
        for p in self.pipelines.values():
            if p.is_default:
                return p
        # інваріант normalize_pipelines: рівно один is_default; але про
        # всяк -- перший.
        return next(iter(self.pipelines.values()))

    def _subscribers_for_path(self, path: str | None) -> list[tuple]:
        if path is None:
            # Back-compat: хук без ?path (одношляхова конфігурація) ->
            # дефолтний пайплайн як owner.
            default = self._default_pipeline()
            return [(default, "owner")] if default is not None else []
        return list(self._by_path.get(path, ()))

    # --- хуки MediaMTX (роутинг по шляху, 1:N §5.4) ---

    def on_available(self, path: str | None = None) -> None:
        with self.lock:
            subscribers = self._subscribers_for_path(path)
            if not subscribers:
                logging.warning("available hook for unknown path %r -- ignoring", path)
                return
            # Життєвий цикл пайплайна керується ЛИШЕ публікацією OBS на його
            # шлях (немає ручного disable-пайплайна). Тумблер у шапці Control
            # -- це master AND-гейт над площадками (Pipeline.enabled), а не
            # гейт запуску самого пайплайна: тут його НЕ перевіряємо.
            if self._is_current_session_halted():
                # Ця сесія OBS заглушена з дашборда (HALT) -- не стартуємо
                # ефір на її (пере)публікацію. Латч глобальний: діє на всі
                # пайплайни (owner+remux) цієї сесії. Стоп самому OBS шлеться,
                # коли його obs-source (пере)підключиться (register_source).
                logging.info(
                    "OBS is publishing (path=%s), but this session was halted from the dashboard "
                    "-> ignoring (not restarting the broadcast)", path,
                )
                return
            for pipeline, role in subscribers:
                if role == "owner":
                    pipeline.on_available()
                else:
                    pipeline.on_source_available(role)

    def on_unavailable(self, path: str | None = None) -> None:
        with self.lock:
            subscribers = self._subscribers_for_path(path)
            if not subscribers:
                logging.warning("unavailable hook for unknown path %r -- ignoring", path)
                return
            for pipeline, role in subscribers:
                if role == "owner":
                    pipeline.on_unavailable()
                else:
                    pipeline.on_source_unavailable(role)

    # --- сигнали OBS / латч / оракул ---

    def on_manual_stop(self) -> None:
        """
        Штатний стоп OBS (obs-source.html). Негайно чисто завершує
        дефолтний пайплайн (він наблюдаемий), ставить оракул і чистить
        латч. Доп-пайплайни: ті, що вже в FALLBACK, теж чисто завершуємо
        (нового on_unavailable вони не отримають, §7); решта доловить
        оракул на своєму наступному обриві.
        """
        with self.lock:
            self._last_started_obs_id = None
            self._last_halted_obs_id = None
            self._graceful_stop_at = time.monotonic()
            default = self._default_pipeline()
            default.on_manual_stop()
            for pipeline in self.pipelines.values():
                if pipeline is not default:
                    pipeline.graceful_stop_if_fallback()

    def on_dashboard_halt(self) -> None:
        """
        Ручний "HALT" з дашборда: негайно зупиняє ВСІ пайплайни й
        командує OBS зупинити стрім. Це свідомий стоп -> стан OFFLINE
        (не FAILURE). Латч глобальний.
        """
        with self.lock:
            any_active = False
            for pipeline in self.pipelines.values():
                if pipeline.halt():
                    any_active = True
            if not any_active:
                return
            logging.warning("HALT requested from the dashboard -> stopping all pipelines and asking OBS to stop")
            self._emit_event("warning", "Broadcast halted from the dashboard")
            # Запамʼятовуємо поточну сесію OBS як заглушену -> латч.
            self._last_halted_obs_id = self._last_started_obs_id
            # Одразу шлемо стоп підключеним obs-source; тим, хто
            # підключиться пізніше, шле http_server на register_source.
            self._request_stop_streaming_in_obs()

    def on_pipeline_gave_up(self, pipeline: Pipeline) -> None:
        """
        Пайплайн заглушив себе (жодна площадка не піднялась). Рішення про
        OBS-стоп (failsafe-асиметрія §8) виконуємо НЕ синхронно під локом-
        звонарем (pipeline.lock, іноді супервізорний потік): диспатчимо в
        окремий потік, де беремо Manager.lock і опитуємо інші пайплайни --
        так уникаємо інверсії Pipeline->Manager проти Manager->Pipeline.
        """
        def _decide():
            with self.lock:
                if pipeline.is_default:
                    logging.error("default pipeline failed -- asking OBS to stop the whole stream")
                    self._emit_event(
                        "error",
                        "Couldn't connect to any enabled platform on the main pipeline -- check the "
                        "URLs/keys in Settings. Broadcast stopped, and a stop command was sent to the "
                        "OBS browser-source. If OBS is still streaming, set its Page permission to "
                        "\"Full access to OBS\".",
                    )
                    self._request_stop_streaming_in_obs()
                    return
                if not self._any_pipeline_alive():
                    logging.error("all pipelines are down -- asking OBS to stop the whole stream")
                    self._emit_event(
                        "error",
                        "Every pipeline is down -- check the URLs/keys in Settings. Broadcast stopped, "
                        "and a stop command was sent to the OBS browser-source.",
                    )
                    self._request_stop_streaming_in_obs()
                    return
                # Хоч один пайплайн ще живий -- глушимо лише цей, OBS не чіпаємо.
                logging.warning(
                    "pipeline %s gave up -- other pipelines keep streaming, OBS not touched", pipeline.name,
                )
                self._emit_event(
                    "warning",
                    f"[{pipeline.name}] couldn't connect to any platform -- this pipeline stopped. "
                    "The main broadcast keeps going.",
                )

        threading.Thread(target=_decide, daemon=True).start()

    def _any_pipeline_alive(self) -> bool:
        # Під Manager.lock. "Живий" = має виходи (не input), увімкнений і не
        # в OFFLINE. Input не бродкастить -> не рахуємо його «живим» для
        # failsafe-рішення про OBS-стоп.
        for p in self.pipelines.values():
            if p.PIPELINE_TYPE == TYPE_INPUT:
                continue
            if p.enabled and p.state != STATE_OFFLINE:
                return True
        return False

    def is_graceful_recent(self) -> bool:
        # Читається пайплайнами БЕЗ Manager.lock (уникаємо інверсії).
        at = self._graceful_stop_at
        return at is not None and (time.monotonic() - at) < ORACLE_WINDOW_SEC

    def is_main_session_live(self) -> bool:
        """
        Чи ГОЛОВНИЙ (дефолтний) пайплайн зараз у живій сесії (не OFFLINE).
        Аукс-пайплайн спирається на це, вирішуючи, чи вмикати заглушку:
        його continuity піггібечить на сесію дефолта (аукс навіть чекає на
        offline-таймер дефолта). Якщо дефолт не в ефірі -- аукс публікувався
        «сам по собі» (obs-multi-rtmp без старту головного виходу OBS), його
        зупинку/обрив ловити нічим (немає оракула) -> заглушку не вмикаємо.
        Читається БЕЗ Manager.lock (простий read рядка-стану під GIL;
        посилання на дефолт кешоване) -- уникаємо інверсії Pipeline->Manager.
        """
        d = self._default
        return d is not None and d.state != STATE_OFFLINE

    def report_obs_session(self, obs_id) -> None:
        """
        obs-source.html доповів id поточної сесії стриму OBS. Порожній/
        None ігноруємо (сесія невідома), щоб не затерти відомий id.
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
        # Під Manager.lock. Латч активний, поки остання відома сесія OBS
        # збігається із заглушеною.
        return self._last_halted_obs_id is not None and self._last_started_obs_id == self._last_halted_obs_id

    def on_obs_streaming_started(self) -> None:
        logging.info("OBS reports streaming started (obs-source.html)")

    def on_mediamtx_connect_timeout(self) -> None:
        """
        MediaMTX закрив з'єднання OBS по readTimeout, так і не дочекавшись
        публікації. `readTimeout` глобальний -- атрибутувати таймаут
        конкретному пайплайну з логу не можна (лог має лише conn IP:port,
        не шлях), тож лишаємо це глобальним попередженням.
        """
        with self.lock:
            if self._any_pipeline_active():
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

    def _any_pipeline_active(self) -> bool:
        for p in self.pipelines.values():
            if p.state != STATE_OFFLINE:
                return True
        return False

    def _request_stop_streaming_in_obs(self) -> None:
        logging.info(
            "sending stop_streaming control to any connected obs-source.html "
            "(requires its Page permission set to \"Full access to OBS\" to take effect)"
        )
        if self.on_control is not None:
            self.on_control("stop_streaming")

    # --- CRUD виходів (роутинг у пайплайн + персист на рівні менеджера) ---

    def enable_output(self, name: str, pipeline: str | None = None) -> None:
        with self.lock:
            p = self._resolve_output_pipeline(pipeline)
            if p is None:
                return
            p.enable_destination(name)
            self._persist_locked()

    def disable_output(self, name: str, pipeline: str | None = None) -> None:
        with self.lock:
            p = self._resolve_output_pipeline(pipeline)
            if p is None:
                return
            p.disable_destination(name)
            self._persist_locked()

    def add_output(self, name: str, server: str, key: str, pipeline: str | None = None) -> None:
        with self.lock:
            p = self._resolve_output_pipeline(pipeline)
            if p is None:
                return
            p.add_destination(name, server, key)
            self._persist_locked()

    def update_output(self, name: str, new_name: str, server: str, key: str, pipeline: str | None = None) -> None:
        with self.lock:
            p = self._resolve_output_pipeline(pipeline)
            if p is None:
                return
            p.update_destination(name, new_name, server, key)
            self._persist_locked()

    def remove_output(self, name: str, pipeline: str | None = None) -> None:
        with self.lock:
            p = self._resolve_output_pipeline(pipeline)
            if p is None:
                return
            p.remove_destination(name)
            self._persist_locked()

    def _resolve_pipeline(self, name: str | None):
        if name is None:
            return self._default_pipeline()
        return self.pipelines.get(name)

    def _resolve_output_pipeline(self, name: str | None):
        # Площадки є лише в output-типів (restream/remux); input їх не має.
        p = self._resolve_pipeline(name)
        if p is None or p.PIPELINE_TYPE == TYPE_INPUT:
            return None
        return p

    def outputs_for_settings(self, pipeline: str | None = None) -> list[dict]:
        with self.lock:
            p = self._resolve_pipeline(pipeline)
            return p.outputs_for_settings() if p else []

    def output_names(self, pipeline: str | None = None) -> list[str]:
        with self.lock:
            p = self._resolve_pipeline(pipeline)
            return p.destination_names() if p else []

    # --- CRUD пайплайнів (немедленно + персист, БЕЗ рестарту MediaMTX --
    # шляхи пре-провизионені §5.1). Валідацію робить викликач (http_server
    # -> settings_store.validate_pipeline). ---

    def _default_backup_file(self) -> str:
        """backup_file дефолтного (main) пайплайна -- дефолт для нових (спільна заглушка -- частий кейс)."""
        d = self._default
        return d.pcfg.get("backup_file", "") if d is not None and hasattr(d, "pcfg") else ""

    def add_pipeline(self, name: str, backup_file: str) -> None:
        """Restream-пайплайн (свій вхід + виходи). Тип за замовчуванням."""
        with self.lock:
            if name in self.pipelines:
                return
            live_path = self._assign_path(name)  # авто-призначення (без комбобокса)
            backup_file = backup_file or self._default_backup_file()
            pcfg = {
                "name": name, "type": TYPE_RESTREAM, "is_default": False, "enabled": False,
                "live_path": live_path, "backup_file": backup_file,
                # Плейсхолдер-primary (обов'язковий інваріант пайплайна) --
                # вимкнений і порожній; користувач заповнить у модалці площадки.
                "primary_name": "primary", "primary_server": "", "primary_key": "",
                "primary_enabled": False, "restreams": [],
            }
            self._instantiate_pipeline(pcfg)
            logging.info("added restream pipeline %s on auto-assigned path %s", name, live_path)
            self._persist_locked()

    def add_input_pipeline(self, name: str) -> None:
        """Іменований ingest-вхід без виходів (джерело для remux)."""
        with self.lock:
            if name in self.pipelines:
                return
            live_path = self._assign_path(name)
            pcfg = {"name": name, "type": TYPE_INPUT, "is_default": False,
                    "enabled": True, "live_path": live_path}
            self._instantiate_pipeline(pcfg)
            logging.info("added input pipeline %s on auto-assigned path %s", name, live_path)
            self._persist_locked()

    def add_remux_pipeline(self, name: str, video_src_path: str, audio_src_path: str, backup_file: str) -> None:
        """Remux-пайплайн: video з одного чужого входу + audio з іншого; свого шляху не має."""
        with self.lock:
            if name in self.pipelines:
                return
            backup_file = backup_file or self._default_backup_file()
            pcfg = {
                "name": name, "type": TYPE_REMUX, "is_default": False, "enabled": False,
                "video_src_path": video_src_path, "audio_src_path": audio_src_path,
                "audio_trim_ms": 0, "backup_file": backup_file,
                "primary_name": "primary", "primary_server": "", "primary_key": "",
                "primary_enabled": False, "restreams": [],
            }
            self._instantiate_pipeline(pcfg)
            logging.info("added remux pipeline %s (video=%s audio=%s)", name, video_src_path, audio_src_path)
            self._persist_locked()

    def source_candidates(self) -> list[dict]:
        """Пайплайни, придатні як джерело для remux (restream/input, мають live_path)."""
        with self.lock:
            return [
                {"name": p.name, "live_path": p.live_path, "type": p.PIPELINE_TYPE}
                for p in self._ordered_pipelines()
                if p.PIPELINE_TYPE in (TYPE_RESTREAM, TYPE_INPUT)
            ]

    def remux_referencing(self, live_path: str) -> list[str]:
        """Імена remux-пайплайнів, що посилаються на цей live_path (для guard видалення)."""
        with self.lock:
            names = []
            for p in self.pipelines.values():
                if p.PIPELINE_TYPE == TYPE_REMUX and live_path in (p.video_src_path, p.audio_src_path):
                    names.append(p.name)
            return names

    def _assign_path(self, name: str) -> str:
        # Дружній slug з імені -> live/<slug>; гарантуємо унікальність
        # (не збігтись із зайнятим шляхом, зокрема з дефолтним live/main).
        # Шлях фіксується при створенні й НЕ міняється при перейменуванні
        # (щоб не ламати вже налаштований OBS-вихід). Charset збігається з
        # regex у mediamtx.yml (`[A-Za-z0-9_-]+`).
        slug = re.sub(r"[^A-Za-z0-9_-]+", "-", name).strip("-").lower() or "pipeline"
        candidate = f"live/{slug}"
        i = 2
        while candidate in self._by_path:
            candidate = f"live/{slug}-{i}"
            i += 1
        return candidate

    def update_pipeline(self, name: str, new_name: str, backup_file: str,
                        video_src_path: str | None = None, audio_src_path: str | None = None) -> None:
        with self.lock:
            old = self.pipelines.get(name)
            if old is None:
                return
            is_remux = old.PIPELINE_TYPE == TYPE_REMUX
            # Структурна зміна (rename АБО зміна джерел remux) -> чисте
            # перестворення: імена процесів/логів тегуються іменем, а підписки
            # remux залежать від source-шляхів, тож їх треба перереєструвати.
            # Шлях (live_path) СТАБІЛЬНИЙ при rename (інакше зламався б уже
            # налаштований OBS-вихід) -- переноситься через to_config().
            sources_changed = is_remux and (
                (video_src_path is not None and video_src_path != old.video_src_path)
                or (audio_src_path is not None and audio_src_path != old.audio_src_path)
            )
            if new_name != name or sources_changed:
                new_pcfg = old.to_config()
                new_pcfg["name"] = new_name
                if old.PIPELINE_TYPE != TYPE_INPUT:
                    new_pcfg["backup_file"] = backup_file
                if is_remux:
                    if video_src_path is not None:
                        new_pcfg["video_src_path"] = video_src_path
                    if audio_src_path is not None:
                        new_pcfg["audio_src_path"] = audio_src_path
                old.shutdown()
                self.pipelines.pop(name, None)
                self._unregister_subscriptions(old)
                self._instantiate_pipeline(new_pcfg)
                logging.info("recreated pipeline %s -> %s", name, new_name)
            else:
                # input не має backup -- apply_local_settings лише в output-типів.
                if hasattr(old, "apply_local_settings"):
                    old.apply_local_settings(backup_file=backup_file)
                logging.info("updated pipeline %s (backup)", name)
            self._persist_locked()

    def remove_pipeline(self, name: str) -> None:
        with self.lock:
            p = self.pipelines.get(name)
            if p is None or p.is_default:
                return  # дефолтний незнищенний
            p.shutdown()
            self.pipelines.pop(name, None)
            self._unregister_subscriptions(p)
            logging.info("removed pipeline %s", name)
            self._persist_locked()

    def enable_pipeline(self, name: str) -> None:
        # Master AND-гейт (тумблер у шапці Control), НЕ керування життєвим
        # циклом: пайплайн і далі йде за публікацією OBS. ON -> піднімаємо
        # площадки з власною галочкою (якщо ефір уже активний).
        with self.lock:
            p = self.pipelines.get(name)
            if p is None:
                return
            p.set_master(True)
            self._persist_locked()

    def disable_pipeline(self, name: str) -> None:
        # Master-гейт OFF -> глушимо ВСІ площадки цього пайплайна (relay/стан
        # лишаються, пайплайн просто нікуди не віддає). Діє й на дефолтний.
        with self.lock:
            p = self.pipelines.get(name)
            if p is None:
                return
            p.set_master(False)
            self._persist_locked()

    def set_audio_trim(self, name: str, ms: int) -> None:
        """Ручний аудіо-триммер remux (§5.2), live-apply + персист."""
        with self.lock:
            p = self.pipelines.get(name)
            if p is None or p.PIPELINE_TYPE != TYPE_REMUX:
                return
            p.set_audio_trim(ms)
            self._persist_locked()

    # --- аксессори для валідації/Settings ---

    def pipeline_names(self) -> list[str]:
        with self.lock:
            return list(self.pipelines.keys())

    def pipeline_type(self, name: str) -> str | None:
        with self.lock:
            p = self.pipelines.get(name)
            return p.PIPELINE_TYPE if p else None

    def blocking_remux_for(self, name: str) -> list[str]:
        """Remux-пайплайни, для яких `name` є джерелом (не даємо видалити джерело з-під живого remux, §4.4)."""
        with self.lock:
            p = self.pipelines.get(name)
            live_path = getattr(p, "live_path", None) if p else None
            if not live_path:
                return []
            return self.remux_referencing(live_path)

    def _ingest_key(self, live_path: str) -> str:
        # Готовий OBS Stream Key для цього пайплайна: "<sub>?user=obs&pass=
        # <obspass>", де sub -- шлях без префікса "live/". Порожній, якщо
        # obs-пароль не вдалось прочитати з mediamtx.yml.
        if not self._obs_ingest_pass or not live_path:
            return ""
        sub = live_path[len("live/"):] if live_path.startswith("live/") else live_path
        return f"{sub}?user=obs&pass={self._obs_ingest_pass}"

    def _ordered_pipelines(self) -> list:
        # Canonical display order -- default pipeline first, the rest keep
        # their existing order (stable sort). Both status() and
        # pipelines_for_settings() use it so the dashboard lists pipelines
        # identically in the Control and Settings tabs. Call under self.lock.
        return sorted(self.pipelines.values(), key=lambda p: not p.is_default)

    def pipelines_for_settings(self) -> list[dict]:
        with self.lock:
            pipelines = self._ordered_pipelines()
        server = f"rtmp://{self._public_host}:{self._rtmp_port}/live" if self._public_host else ""
        # outputs_for_settings кожного пайплайна бере власний lock окремо.
        result = []
        for p in pipelines:
            entry = {
                "name": p.name,
                "type": p.PIPELINE_TYPE,
                "is_default": p.is_default,
                "enabled": p.enabled,
                "live_path": p.live_path,
                "backup_file": p.pcfg.get("backup_file", ""),
                # Готова інфа для налаштування OBS-виходу (замість комбобокса
                # шляхів): куди публікувати цей пайплайн. Для remux -- порожня
                # (свого входу немає, публікувати нема куди).
                "ingest_server": server if p.live_path else "",
                "ingest_key": self._ingest_key(p.live_path),
                "platforms": p.outputs_for_settings(),
            }
            if p.PIPELINE_TYPE == TYPE_REMUX:
                entry["video_src_path"] = p.video_src_path
                entry["audio_src_path"] = p.audio_src_path
                entry["audio_trim_ms"] = p.audio_trim_ms
            result.append(entry)
        return result  # вже впорядковано (_ordered_pipelines: дефолтний першим)

    # --- глобальні System-налаштування ---

    def apply_settings(self, values: dict) -> None:
        """
        Глобальний System-блок вкладки Settings: `connect_timeout_ms`,
        `read_timeout_ms` (обидва -> `readTimeout` MediaMTX, один на
        інстанс), `offline_timeout_sec` (один OBS -> одне вікно очікування;
        веде дефолтний пайплайн, гасить усі), `icmp_ping`. Per-pipeline
        `backup_file`/бітрейти -- через CRUD пайплайна (`update_pipeline`).
        Тайминги MediaMTX застосовує викликач окремо (mediamtx_control).
        Викликати без self.lock -- бере сам.
        """
        with self.lock:
            self.config["connect_timeout_ms"] = int(values["connect_timeout_ms"])
            self.config["read_timeout_ms"] = int(values["read_timeout_ms"])
            self.config["offline_timeout_sec"] = int(values["offline_timeout_sec"])
            self.config["icmp_ping"] = bool(values.get("icmp_ping", False))
            self._persist_locked()

    def on_offline_timeout(self) -> None:
        """
        Дефолтний пайплайн вичерпав offline-таймер (OBS не повернувся) ->
        кінець усієї сесії: гасимо ВСІ пайплайни. Викликається з Timer-
        потоку дефолтного пайплайна ПОЗА його локом -> тут беремо свій
        Manager.lock і локи пайплайнів по черзі (коректний порядок).
        """
        with self.lock:
            for pipeline in self.pipelines.values():
                pipeline.halt()
        self._emit_event("warning", "Broadcast ended -- OBS did not reconnect in time")

    # --- статус / персист / життєвий цикл ---

    def status(self) -> dict:
        with self.lock:
            pipelines = self._ordered_pipelines()
            manual_halt = self._is_current_session_halted()
        # status() кожного пайплайна бере власний lock окремо (взяв/
        # відпустив) -- НЕ вкладено з Manager.lock і не з hub-локом
        # (hub будує snapshot поза своїм локом).
        return {
            "pipelines": [p.status() for p in pipelines],
            "manual_halt": manual_halt,
        }

    def _persist_locked(self) -> None:
        # Під Manager.lock. to_config() кожного пайплайна бере власний
        # lock (Manager->Pipeline, коректний порядок).
        self.config["pipelines"] = [p.to_config() for p in self.pipelines.values()]
        for key in _FLAT_PIPELINE_KEYS:
            self.config.pop(key, None)
        try:
            settings_store.persist(self.config_path, self.config)
        except OSError:
            logging.exception("failed to persist config.json")

    def _notify(self) -> None:
        if self.on_change is not None:
            self.on_change()

    def _emit_event(self, level: str, text: str) -> None:
        if self.on_event is not None:
            self.on_event(level, text)

    def emit_pipeline_event(self, pipeline: Pipeline, level: str, text: str) -> None:
        # Дефолтний пайплайн -- без префікса (той самий текст, що й у
        # одношляховій конфігурації); доп-пайплайни тегуються іменем.
        if pipeline.is_default:
            self._emit_event(level, text)
        else:
            self._emit_event(level, f"[{pipeline.name}] {text}")

    def shutdown(self) -> None:
        with self.lock:
            self._stopping.set()
            for pipeline in self.pipelines.values():
                pipeline.shutdown()

    def _ping_loop(self):
        while not self._stopping.is_set():
            use_icmp = bool(self.config.get("icmp_ping", False))
            for pipeline in list(self.pipelines.values()):
                pipeline_enabled = pipeline.enabled
                for dest in pipeline.list_destinations():
                    if not dest.enabled or not pipeline_enabled:
                        dest.rtt_ms = None
                    elif use_icmp:
                        dest.rtt_ms = net_probe.icmp_rtt_ms(dest.url)
                    else:
                        dest.rtt_ms = net_probe.tcp_rtt_ms(dest.url)
            self._stopping.wait(PING_INTERVAL_SEC)
