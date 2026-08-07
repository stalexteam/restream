"""
Читання/валідація/запис "day-2" полів `config.json`, які редагуються
через вкладку Settings у дашборді: `twitch_url`,
`offline_timeout_sec`, `backup_file`, `connect_timeout_ms`,
`read_timeout_ms`. Решта полів файлу (токени, внутрішня авторизація
MediaMTX, порти, `output_*_bitrate_kbps`) цей модуль не торкається --
і на читанні, і на записі.
"""

import json
from pathlib import Path

from probe import probe_stream_params

EDITABLE_FIELDS = (
    "twitch_url",
    "offline_timeout_sec",
    "backup_file",
    "connect_timeout_ms",
    "read_timeout_ms",
)

# Хардкоджені мінімуми -- нижче них або ефект нестабільний (RTMP-
# рукостискання не встигає), або детектор стагнації ловив би нормальний
# джиттер потоку як хибний обрив.
MIN_CONNECT_TIMEOUT_MS = 2500
MIN_READ_TIMEOUT_MS = 300
MIN_OFFLINE_TIMEOUT_SEC = 60


def load_editable(config_path: Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        full = json.load(f)
    return {key: full.get(key) for key in EDITABLE_FIELDS}


def validate(values: dict, base_dir: Path) -> dict[str, str]:
    """
    `{поле: причина}` для невалідних значень, порожній словник --
    усе ок. Все-або-нічого перевіряє викликач (`save()` не викликати,
    якщо тут щось повернулось). Значення нижче хардкодженого мінімуму
    -- ВІДХИЛЯЄМО з поясненням, а не мовчки підганяємо до мінімуму:
    той самий принцип, що й для решти полів -- непомітна підміна
    введеного користувачем числа гірша за чітку помилку.
    """
    errors: dict[str, str] = {}

    twitch_url = values.get("twitch_url", "")
    if not isinstance(twitch_url, str) or not twitch_url.startswith("rtmp://"):
        errors["twitch_url"] = "must be an rtmp:// URL"

    _validate_number(values, errors, "offline_timeout_sec", MIN_OFFLINE_TIMEOUT_SEC, "seconds")
    _validate_number(values, errors, "connect_timeout_ms", MIN_CONNECT_TIMEOUT_MS, "ms")
    _validate_number(values, errors, "read_timeout_ms", MIN_READ_TIMEOUT_MS, "ms")

    backup_file = values.get("backup_file", "")
    if not isinstance(backup_file, str) or not backup_file:
        errors["backup_file"] = "path is required"
    else:
        resolved = _resolve_backup_path(backup_file, base_dir)
        if not resolved.is_file():
            errors["backup_file"] = f"file not found: {resolved}"
        elif probe_stream_params(str(resolved)) is None:
            errors["backup_file"] = "no readable video/audio track in this file (ffprobe failed)"

    return errors


def _validate_number(values: dict, errors: dict, field: str, minimum: float, unit: str) -> None:
    value = values.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < minimum:
        errors[field] = f"must be a number, at least {minimum} {unit}"


def save(config_path: Path, values: dict, base_dir: Path) -> None:
    """
    Читає повний `config.json`, перезаписує ЛИШЕ `EDITABLE_FIELDS`,
    пише назад через тимчасовий файл + атомарний `Path.replace()` --
    той самий патерн, що вже є в `backup_prep.py`
    (`tmp_path.replace(self._prepared)`), тут з тієї самої причини:
    не лишити `config.json` напівзаписаним, якщо процес впаде рівно
    посеред запису. Викликач відповідає за попередній виклик
    `validate()` -- цей метод сам нічого не перевіряє.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        full = json.load(f)

    full["twitch_url"] = values["twitch_url"]
    full["offline_timeout_sec"] = int(values["offline_timeout_sec"])
    full["backup_file"] = str(_resolve_backup_path(values["backup_file"], base_dir))
    full["connect_timeout_ms"] = int(values["connect_timeout_ms"])
    full["read_timeout_ms"] = int(values["read_timeout_ms"])

    tmp_path = config_path.with_suffix(".tmp" + config_path.suffix)
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(full, f, indent=2)
        f.write("\n")
    tmp_path.replace(config_path)


def _resolve_backup_path(backup_file: str, base_dir: Path) -> Path:
    # Відносний шлях -- відносно кореня проєкту (той самий інваріант,
    # що вже й так тримає install.sh, підставляючи __BASE_DIR__ в
    # config.example.json), а не відносно cwd процесу контролера,
    # яке залежить від того, звідки саме він був запущений.
    path = Path(backup_file)
    return path if path.is_absolute() else (base_dir / path)
