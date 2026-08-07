"""
BackupPreparer — одноразова підготовка заглушки (backup.mp4) під
параметри живого потоку, щоб `-c copy` на всіх плечах (relay/backup/
outbound) не ламався через розбіжну роздільність/fps/канали/кодек між
live і backup.
"""

import json
import logging
import subprocess
import threading
from pathlib import Path

from probe import probe_stream_params


class BackupPreparer:
    def __init__(self, backup_source: Path, config: dict):
        self._backup_source = backup_source
        self._config = config
        self._prepared = backup_source.with_name(backup_source.stem + ".prepared" + backup_source.suffix)
        self._prepared_meta = backup_source.with_name(backup_source.stem + ".prepared.meta.json")
        self._lock = threading.Lock()
        # Останні визначені параметри живого потоку (width/height/fps/
        # кодеки) -- дашборд показує їх у тултипі OBS-індикатора. None,
        # поки не було жодного успішного probe цього ефіру.
        self._last_live_params: dict | None = None

    def current_source(self) -> Path:
        """Готова (перекодована) копія заглушки, якщо є, інакше оригінал."""
        if self._prepared.exists():
            return self._prepared
        return self._backup_source

    def last_live_params(self) -> dict | None:
        return self._last_live_params

    def prepare_async(self, live_probe_url: str) -> None:
        """
        Запускає перевірку/підготовку у фоновому потоці. Поза
        Controller.lock (виклик асинхронний): ffprobe + можливе
        перекодування можуть тривати секунди, а то й хвилини на
        слабкому VPS — не тримати через них стейт-машину заблокованою.
        Живий ефір це не зупиняє: заглушка знадобиться лише якщо OBS
        відвалиться, а на той момент перекодування, скоріш за все, уже
        встигне завершитись.
        """
        threading.Thread(target=self._prepare, args=(live_probe_url,), daemon=True).start()

    def _prepare(self, live_probe_url: str) -> None:
        live_params = probe_stream_params(live_probe_url)
        if live_params is None:
            logging.warning(
                "could not determine live stream parameters -- skipping backup "
                "check/preparation for this stream start"
            )
            return
        self._last_live_params = live_params
        self._ensure_matches_live(live_params)

    def _ensure_matches_live(self, live_params: dict) -> None:
        with self._lock:
            cached = None
            if self._prepared_meta.exists():
                try:
                    cached = json.loads(self._prepared_meta.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    cached = None

            if cached == live_params and self._prepared.exists():
                logging.info("backup video already matches live stream parameters, no transcode needed")
                return

            backup_params = probe_stream_params(str(self._backup_source))
            if backup_params == live_params:
                # Оригінал і так підходить під -c copy — окрема копія не потрібна.
                if self._prepared.exists():
                    self._prepared.unlink(missing_ok=True)
                self._prepared_meta.write_text(json.dumps(live_params), encoding="utf-8")
                logging.info("backup video (original file) already matches live stream parameters")
                return

            logging.warning(
                "backup video %s (%s) does not match live stream parameters (%s) -- "
                "transcoding in the background into %s",
                self._backup_source, backup_params, live_params, self._prepared,
            )

            out_vbitrate = self._config.get("output_video_bitrate_kbps", 6000)
            out_abitrate = self._config.get("output_audio_bitrate_kbps", 160)
            w, h, fps = live_params["width"], live_params["height"], live_params["fps"]
            tmp_path = self._prepared.with_suffix(".tmp" + self._prepared.suffix)

            cmd = [
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
                "-i", str(self._backup_source),
                "-vf", (
                    f"scale={w}:{h}:force_original_aspect_ratio=decrease,"
                    f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2,fps={fps}"
                ),
                "-ac", str(live_params["channels"]), "-ar", str(live_params["sample_rate"]),
                # -bf 0: без B-кадрів — decode-порядок заглушки завжди
                # збігається з порядком показу.
                "-c:v", "libx264", "-preset", "veryfast", "-bf", "0",
                "-g", str(fps * 2), "-keyint_min", str(fps * 2),
                "-b:v", f"{out_vbitrate}k", "-maxrate", f"{out_vbitrate}k",
                "-bufsize", f"{out_vbitrate * 2}k",
                "-c:a", "aac", "-b:a", f"{out_abitrate}k",
                str(tmp_path),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logging.error(
                    "failed to transcode the backup video: %s",
                    result.stderr.strip()[-2000:],
                )
                tmp_path.unlink(missing_ok=True)
                return

            tmp_path.replace(self._prepared)
            # Кешуємо РЕАЛЬНІ параметри готового файлу (перепробувавши його),
            # а не сліпо те, що просили в -vf/-c:v — про всяк випадок, якщо
            # кодування дало щось трохи інше (напр. live не H.264/AAC, хоча
            # для Twitch/RTMP-джерела так майже завжди). Якщо перепроба не
            # вдалась — файл все одно робочий, просто кешуємо запитані
            # параметри як найкраще наближення.
            actual_params = probe_stream_params(str(self._prepared)) or live_params
            self._prepared_meta.write_text(json.dumps(actual_params), encoding="utf-8")
            logging.warning("backup video transcoded and ready to use: %s", self._prepared)
