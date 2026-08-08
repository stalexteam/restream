"""
Підготовка заглушки під параметри живого потоку, щоб `-c copy` на всіх
плечах (relay/backup/outbound) не ламався через розбіжну роздільність/
fps/канали/кодек між live і backup.

Два рівні (мульти-пайплайн):

- **BackupCache** — РОЗДІЛЮВАНИЙ на рівні Manager контент-адресуемий кэш
  готових артефактів + реєстр воркерів "in-flight". Один вихідний файл
  МОЖЕ бути спільним для кількох пайплайнів, але цільові параметри
  (роздільність/fps/канали/бітрейт) у кожного СВОЇ -> різні артефакти.
  Ключ = hash(ідентичність_джерела + target_params); тому 1080p і 720p
  з одного джерела -- два різні артефакти, не конфліктують. Однакові
  (джерело+параметри) готуються рівно ОДИН раз: перший воркер транскодить,
  решта чекають його Event і реюзять результат.

- **BackupPreparer** — per-pipeline обгортка: probe живого потоку,
  визначення target_params (геометрія з live + бітрейт цього пайплайна),
  делегування в кэш. Якщо джерело і так матчить live -- віддаємо оригінал
  без транскоду.

Конкурентність: `BackupCache._lock` короткоживучий (лише перевірка/
реєстрація Event). Очікування чужого Event -- ПОЗА всіма локами
(підготовка йде у фоновому потоці, Pipeline.lock/Manager.lock не
тримаються). Ключовий запрет: не чекати чужий Event, тримаючи
Pipeline.lock/Manager.lock.
"""

import hashlib
import json
import logging
import subprocess
import threading
import time
from pathlib import Path

from probe import probe_stream_params

# Автодетект цільового бітрейту заглушки: беремо ВИМІРЯНИЙ бітрейт живого
# потоку і КВАНТУЄМО (округлення ВГОРУ до кроку), щоб ключ кэша був
# стабільним між запусками (5.9 і 6.05 Мбіт -> той самий 6000), а не
# перетранскодувати щоразу через дрібне коливання виміру.
_VIDEO_BITRATE_STEP_KBPS = 500
_AUDIO_BITRATE_STEP_KBPS = 32
_DEFAULT_VIDEO_BITRATE_KBPS = 6000
_DEFAULT_AUDIO_BITRATE_KBPS = 160
# Скільки чекати першого ненульового виміру бітрейту (switcher набирає
# ~2с семплів після старту relay; probe геометрії теж кілька секунд).
_BITRATE_MEASURE_TIMEOUT_SEC = 4.0


def _quantize_up(kbps, step: int, default: int) -> int:
    if not kbps or kbps <= 0:
        return default
    return max(step, -(-int(kbps) // step) * step)


class BackupCache:
    """Спільний контент-адресуемий кэш готових заглушок + дедуп воркерів."""

    def __init__(self, cache_dir: Path):
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._inflight: dict[str, threading.Event] = {}

    def get_or_build(self, source: Path, target_params: dict) -> Path | None:
        """
        Повертає готовий артефакт для (джерело+target_params), збудувавши
        його рівно один раз. None -- джерело зникло або транскод упав
        (викликач фолбекне на оригінал). Викликати ПОЗА Pipeline/Manager
        локами (може блокуюче чекати чужий Event).
        """
        source_id = self._source_identity(source)
        if source_id is None:
            logging.warning("backup source %s is missing -- cannot prepare", source)
            return None
        key = self._key(source_id, target_params)
        artifact = self._cache_dir / f"{key}.mp4"
        meta = self._cache_dir / f"{key}.json"

        # (a) уже готове на диску -> реюз без ffmpeg.
        if self._valid(artifact, meta, source_id, target_params):
            return artifact

        with self._lock:
            if self._valid(artifact, meta, source_id, target_params):
                return artifact
            event = self._inflight.get(key)
            if event is None:
                # (c) ми -- воркер цього ключа.
                event = threading.Event()
                self._inflight[key] = event
                owner = True
            else:
                # (b) ключ уже готує інший воркер.
                owner = False

        if not owner:
            event.wait()  # ПОЗА локом
            return artifact if self._valid(artifact, meta, source_id, target_params) else None

        try:
            ok = self._transcode(source, artifact, meta, source_id, target_params)
            return artifact if ok else None
        finally:
            with self._lock:
                self._inflight.pop(key, None)
            event.set()

    @staticmethod
    def _source_identity(source: Path) -> dict | None:
        try:
            st = source.stat()
        except OSError:
            return None
        return {"path": str(source.resolve()), "mtime": int(st.st_mtime), "size": st.st_size}

    @staticmethod
    def _key(source_id: dict, target_params: dict) -> str:
        blob = json.dumps({"source": source_id, "target": target_params}, sort_keys=True)
        return hashlib.sha1(blob.encode("utf-8")).hexdigest()

    @staticmethod
    def _valid(artifact: Path, meta: Path, source_id: dict, target_params: dict) -> bool:
        if not artifact.exists() or not meta.exists():
            return False
        try:
            cached = json.loads(meta.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return False
        return cached.get("source") == source_id and cached.get("target") == target_params

    def _transcode(self, source: Path, artifact: Path, meta: Path,
                   source_id: dict, target_params: dict) -> bool:
        w, h, fps = target_params["width"], target_params["height"], target_params["fps"]
        vbitrate = target_params["video_bitrate_kbps"]
        abitrate = target_params["audio_bitrate_kbps"]
        tmp_path = artifact.with_suffix(".tmp" + artifact.suffix)

        logging.warning(
            "preparing backup artifact from %s -> %s (%sx%s@%s, v=%skbps a=%skbps)",
            source, artifact.name, w, h, fps, vbitrate, abitrate,
        )
        cmd = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
            "-i", str(source),
            "-vf", (
                f"scale={w}:{h}:force_original_aspect_ratio=decrease,"
                f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2,fps={fps}"
            ),
            "-ac", str(target_params["channels"]), "-ar", str(target_params["sample_rate"]),
            # -bf 0: без B-кадрів -- decode-порядок заглушки завжди
            # збігається з порядком показу.
            "-c:v", "libx264", "-preset", "veryfast", "-bf", "0",
            "-g", str(fps * 2), "-keyint_min", str(fps * 2),
            "-b:v", f"{vbitrate}k", "-maxrate", f"{vbitrate}k",
            "-bufsize", f"{vbitrate * 2}k",
            "-c:a", "aac", "-b:a", f"{abitrate}k",
            str(tmp_path),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logging.error("failed to transcode the backup video: %s", result.stderr.strip()[-2000:])
            tmp_path.unlink(missing_ok=True)
            return False

        tmp_path.replace(artifact)
        meta.write_text(json.dumps({"source": source_id, "target": target_params}), encoding="utf-8")
        logging.warning("backup artifact ready: %s", artifact)
        return True


class BackupPreparer:
    """
    Per-pipeline підготовка: probe live -> target_params -> спільний кэш.
    Цільовий бітрейт АВТОДЕТЕКТИТЬСЯ з виміряного бітрейту живого потоку
    (`bitrate_provider` = `switcher.source_stats`), квантований для
    стабільності ключа кэша; ручного вводу немає. Геометрія (роздільність/
    fps/канали) береться з probe живого потоку, як і раніше.
    """

    def __init__(self, backup_source: Path, config: dict, cache: BackupCache, bitrate_provider=None):
        self._source = backup_source
        self._config = config
        self._cache = cache
        self._bitrate_provider = bitrate_provider
        # Артефакт для backup-ffmpeg: None -> віддаємо оригінал (поки не
        # підготували під live). Присвоєння/читання атомарні під GIL.
        self._artifact: Path | None = None
        # Останні визначені параметри живого потоку -- дашборд показує їх
        # у тултипі OBS-індикатора. None, поки не було успішного probe.
        self._last_live_params: dict | None = None

    def current_source(self) -> Path:
        return self._artifact or self._source

    def last_live_params(self) -> dict | None:
        return self._last_live_params

    def prepare_async(self, live_probe_url: str) -> None:
        """
        Запускає перевірку/підготовку у фоновому потоці. Живий ефір це не
        зупиняє: заглушка знадобиться лише якщо OBS відвалиться, а на той
        момент підготовка, скоріш за все, уже встигне завершитись.
        """
        threading.Thread(target=self._prepare, args=(live_probe_url,), daemon=True).start()

    def _prepare(self, live_probe_url: str) -> None:
        live = probe_stream_params(live_probe_url)
        if live is None:
            logging.warning(
                "could not determine live stream parameters -- skipping backup "
                "check/preparation for this stream start"
            )
            return
        self._last_live_params = live

        source_params = probe_stream_params(str(self._source))
        if source_params == live:
            # Оригінал і так підходить під -c copy -- транскод не потрібен.
            self._artifact = self._source
            logging.info("backup source already matches live stream parameters, no transcode needed")
            return

        vbitrate, abitrate = self._detect_target_bitrates()
        target_params = {
            "width": live["width"], "height": live["height"], "fps": live["fps"],
            "channels": live["channels"], "sample_rate": live["sample_rate"],
            "video_bitrate_kbps": vbitrate,
            "audio_bitrate_kbps": abitrate,
        }
        # ПОЗА будь-якими локами -- може блокуюче чекати чужий воркер.
        artifact = self._cache.get_or_build(self._source, target_params)
        if artifact is not None:
            self._artifact = artifact

    def _detect_target_bitrates(self) -> tuple[int, int]:
        """
        Виміряний бітрейт живого потоку (з switcher), квантований. Якщо
        вимір ще не готовий (немає провайдера/потік щойно почався) -- деф.
        Явний override в конфізі (`output_*_bitrate_kbps`) має пріоритет.
        """
        cfg_v = self._config.get("output_video_bitrate_kbps")
        cfg_a = self._config.get("output_audio_bitrate_kbps")
        measured = self._measure() if self._bitrate_provider is not None else {}
        vbitrate = int(cfg_v) if cfg_v else _quantize_up(
            measured.get("video_kbps"), _VIDEO_BITRATE_STEP_KBPS, _DEFAULT_VIDEO_BITRATE_KBPS)
        abitrate = int(cfg_a) if cfg_a else _quantize_up(
            measured.get("audio_kbps"), _AUDIO_BITRATE_STEP_KBPS, _DEFAULT_AUDIO_BITRATE_KBPS)
        return vbitrate, abitrate

    def _measure(self) -> dict:
        # switcher набирає ~2с семплів після старту relay -- чекаємо перший
        # ненульовий вимір (probe геометрії вище й так з'їв кілька секунд).
        deadline = time.monotonic() + _BITRATE_MEASURE_TIMEOUT_SEC
        stats: dict = {}
        while time.monotonic() < deadline:
            stats = self._bitrate_provider() or {}
            if stats.get("video_kbps"):
                return stats
            time.sleep(0.3)
        return stats
