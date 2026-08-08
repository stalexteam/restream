"""
Рендер `data/mediamtx.yml` з шаблону `controller/mediamtx.yml.template` +
`data/config.json`. Єдине джерело правди для секретів -- config.json
(варіант Б): паролі `obs`/`internal` і таймаути живуть ТАМ, а mediamtx.yml
-- згенерований артефакт, який перезаписується перед КОЖНИМ стартом
MediaMTX (і `restreamctl.sh`, і `mediamtx_control.restart_mediamtx`
викликають цей рендер). Тож config.json і mediamtx.yml не можуть
розійтись у паролях -- клас багу "internal_pass рознесло" зникає.

Підстановки: `__OBS_PASS__`/`__INTERNAL_PASS__` -> паролі з config.json;
рядок `readTimeout:` -> `connect_timeout_ms + read_timeout_ms`
(КЛАМП до мінімумів -- config міг редагуватись руками повз валідацію
Settings; занижена цифра не має ламати старт). Проєкт свідомо без
PyYAML -- підстановка текстова (`.replace` + regex на один рядок).

CLI (для restreamctl.sh): `python3 mediamtx_config.py <config.json>
<template> <out.yml>`.
"""

import json
import re
import sys
from pathlib import Path

from settings_store import MIN_CONNECT_TIMEOUT_MS, MIN_READ_TIMEOUT_MS

_READ_TIMEOUT_LINE = re.compile(r"^readTimeout:[ \t]*\S+[ \t]*$", re.MULTILINE)


def _read_timeout_ms(config: dict) -> int:
    # Клампимо до мінімумів (ті самі, що в settings_store) -- не відмова:
    # значення могли потрапити в config.json ручним редагуванням повз
    # валідацію дашборда, а одна занижена цифра не повинна блокувати старт.
    connect = config.get("connect_timeout_ms")
    read = config.get("read_timeout_ms")
    connect = max(int(connect), MIN_CONNECT_TIMEOUT_MS) if isinstance(connect, int) else MIN_CONNECT_TIMEOUT_MS
    read = max(int(read), MIN_READ_TIMEOUT_MS) if isinstance(read, int) else MIN_READ_TIMEOUT_MS
    return connect + read


def render(template_path: Path, config: dict, out_path: Path) -> None:
    """
    Рендерить mediamtx.yml із шаблону + config у пам'яті. Пише атомарно
    (temp + replace). Кидає, якщо рядок `readTimeout:` у шаблоні відсутній
    (краще голосно, ніж мовчки лишити файл без таймауту).
    """
    text = template_path.read_text(encoding="utf-8")
    text = text.replace("__OBS_PASS__", config.get("obs_pass", ""))
    text = text.replace("__INTERNAL_PASS__", config.get("internal_pass", ""))
    total_ms = _read_timeout_ms(config)
    text, count = _READ_TIMEOUT_LINE.subn(f"readTimeout: {total_ms}ms", text, count=1)
    if count == 0:
        raise RuntimeError(f"'readTimeout:' line not found in {template_path} -- refusing to render")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(".tmp" + out_path.suffix)
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(out_path)


def render_from_files(config_path: Path, template_path: Path, out_path: Path) -> None:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    render(template_path, config, out_path)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("usage: mediamtx_config.py <config.json> <template> <out.yml>", file=sys.stderr)
        sys.exit(2)
    render_from_files(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]))
