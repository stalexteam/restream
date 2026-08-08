"""
Валідація/збереження "day-2" полів `config.json`. Дві групи:

- **System-блок** (вкладка Settings, за кнопкою Apply): `backup_file`,
  `offline_timeout_sec`, `connect_timeout_ms`, `read_timeout_ms` --
  `load_editable` + `validate_system`.
- **Площадки** (список на вкладці Settings, кожна дія негайна) --
  `validate_output` перевіряє одну площадку (name+server+key). Самі
  дані площадок бере/пише `state_machine` (server+key роздільно), тут
  лише валідація.

`persist()` пише переданий in-memory config цілком (зберігає й поля,
яких Settings не торкається, включно з `enabled`-прапорцями).
"""

import json
from pathlib import Path

import output_url
from probe import probe_stream_params

# Хардкоджені мінімуми -- нижче них або ефект нестабільний (RTMP-
# рукостискання не встигає), або детектор стагнації ловив би нормальний
# джиттер потоку як хибний обрив.
MIN_CONNECT_TIMEOUT_MS = 2500
MIN_READ_TIMEOUT_MS = 300
MIN_OFFLINE_TIMEOUT_SEC = 60


def load_editable(config_path: Path) -> dict:
    """
    Глобальні System-поля для вкладки Settings. Per-pipeline поля
    (`offline_timeout_sec`/`backup_file`) тепер живуть усередині
    пайплайна -- їх додає http_server через `manager.default_local_
    settings()`; площадки віддає менеджер окремо.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        full = json.load(f)
    return {
        "connect_timeout_ms": full.get("connect_timeout_ms"),
        "read_timeout_ms": full.get("read_timeout_ms"),
        "offline_timeout_sec": full.get("offline_timeout_sec"),
        "icmp_ping": bool(full.get("icmp_ping", False)),
    }


def validate_system(values: dict, base_dir: Path) -> dict[str, str]:
    """
    `{поле: причина}` для невалідних ГЛОБАЛЬНИХ System-полів: connect/read
    timeout (`readTimeout` MediaMTX один на інстанс) + `offline_timeout_
    sec` (один OBS -> одне вікно очікування повернення). Порожній словник
    -- усе ок. Per-pipeline `backup_file`/бітрейти валідує
    `validate_pipeline` окремо. Все-або-нічого; значення нижче мінімуму
    ВІДХИЛЯЄМО з поясненням.
    """
    errors: dict[str, str] = {}
    _validate_number(values, errors, "connect_timeout_ms", MIN_CONNECT_TIMEOUT_MS, "ms")
    _validate_number(values, errors, "read_timeout_ms", MIN_READ_TIMEOUT_MS, "ms")
    _validate_number(values, errors, "offline_timeout_sec", MIN_OFFLINE_TIMEOUT_SEC, "seconds")
    return errors


def validate_output(name: str, server: str, key: str, existing_names) -> dict[str, str]:
    """
    Валідація однієї площадки (add/update). `existing_names` -- імена, з
    якими не можна збігтись (для update викликач виключає власне старе
    ім'я). Перевіряємо зібраний фінальний URL, а не лише server -- бо
    саме він піде у ffmpeg.
    """
    errors: dict[str, str] = {}

    clean_name = (name or "").strip()
    if not clean_name:
        errors["name"] = "name is required"
    elif clean_name in set(existing_names):
        errors["name"] = f"a platform named '{clean_name}' already exists"

    if not _is_rtmp(output_url.build_push_url(server or "", key or "")):
        errors["server"] = "server must be an rtmp:// or rtmps:// URL"

    return errors


def validate_pipeline(name: str, backup_file: str, existing_names, base_dir: Path) -> dict[str, str]:
    """
    Валідація одного пайплайна (add/update). `existing_names` -- імена, з
    якими не можна збігтись (для update викликач виключає власне старе).
    Ingest-шлях НЕ валідуємо -- він призначається автоматично контролером
    (динамічні regex-шляхи MediaMTX). `offline_timeout_sec` глобальний
    (System-блок). All-or-nothing.
    """
    errors: dict[str, str] = {}

    clean_name = (name or "").strip()
    if not clean_name:
        errors["name"] = "name is required"
    elif clean_name in set(existing_names):
        errors["name"] = f"a pipeline named '{clean_name}' already exists"

    if not isinstance(backup_file, str) or not backup_file:
        errors["backup_file"] = "path is required"
    else:
        resolved = resolve_backup_path(backup_file, base_dir)
        if not resolved.is_file():
            errors["backup_file"] = f"file not found: {resolved}"
        elif probe_stream_params(str(resolved)) is None:
            errors["backup_file"] = "no readable video/audio track in this file (ffprobe failed)"

    return errors


def persist(config_path: Path, config: dict) -> None:
    """
    Атомарний запис усього in-memory config назад у файл (тимчасовий
    файл + `Path.replace()`) -- щоб не лишити `config.json`
    напівзаписаним при падінні процесу рівно посеред запису.
    """
    tmp_path = config_path.with_suffix(".tmp" + config_path.suffix)
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
        f.write("\n")
    tmp_path.replace(config_path)


def resolve_backup_path(backup_file: str, base_dir: Path) -> Path:
    # Відносний шлях -- відносно кореня проєкту (той самий інваріант,
    # що тримає install.sh, підставляючи __BASE_DIR__), а не відносно
    # cwd процесу контролера.
    path = Path(backup_file)
    return path if path.is_absolute() else (base_dir / path)


def _is_rtmp(url) -> bool:
    # rtmps:// теж -- напр. Kick (AWS IVS) віддає лише RTMPS-ingest;
    # ffmpeg вміє rtmps через tls, а вихід у нас і так FLV (-c copy).
    return isinstance(url, str) and (url.startswith("rtmp://") or url.startswith("rtmps://"))


def _validate_number(values: dict, errors: dict, field: str, minimum: float, unit: str) -> None:
    value = values.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < minimum:
        errors[field] = f"must be a number, at least {minimum} {unit}"
