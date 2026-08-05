"""ffprobe-обгортка для визначення параметрів відео/аудіо джерела."""

import json
import logging
import subprocess

PROBE_TIMEOUT_SEC = 15


def probe_stream_params(target: str) -> dict | None:
    """
    Повертає {video_codec, width, height, fps, audio_codec, channels,
    sample_rate} для першого відео- та аудіотреку джерела (файл або
    rtmp-URL), або None, якщо не вдалось визначити (джерело
    недоступне/без потрібних треків).

    Кодеки — навмисно частина порівняння: `-c copy` вимагає не лише
    збіжної роздільності/fps/каналів, а й самого кодека (H.264/AAC).
    Якщо порівнювати лише "геометрію", файл з правильним розміром
    кадру, але, наприклад, VP9-відео, здався б "уже відповідним" і
    ніколи не перекодувався б — а потім `-c copy` для нього просто
    впав би.
    """
    try:
        video = subprocess.run(
            [
                "ffprobe", "-v", "error", "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,width,height,r_frame_rate",
                "-of", "json", target,
            ],
            capture_output=True, text=True, timeout=PROBE_TIMEOUT_SEC,
        )
        audio = subprocess.run(
            [
                "ffprobe", "-v", "error", "-select_streams", "a:0",
                "-show_entries", "stream=codec_name,channels,sample_rate",
                "-of", "json", target,
            ],
            capture_output=True, text=True, timeout=PROBE_TIMEOUT_SEC,
        )
        vstreams = json.loads(video.stdout).get("streams", [])
        astreams = json.loads(audio.stdout).get("streams", [])
        if not vstreams or not astreams:
            return None

        v, a = vstreams[0], astreams[0]
        num, _, den = v["r_frame_rate"].partition("/")
        fps = round(int(num) / int(den)) if int(den or 0) else 0

        return {
            "video_codec": v["codec_name"],
            "width": int(v["width"]),
            "height": int(v["height"]),
            "fps": fps,
            "audio_codec": a["codec_name"],
            "channels": int(a["channels"]),
            "sample_rate": int(a["sample_rate"]),
        }
    except (subprocess.TimeoutExpired, KeyError, ValueError, json.JSONDecodeError, OSError) as error:
        logging.warning("could not determine parameters for '%s': %s", target, error)
        return None
