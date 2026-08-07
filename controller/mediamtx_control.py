"""
Керування MediaMTX-процесом з боку контролера -- потрібне ЛИШЕ для
Settings -> Apply & Restart у дашборді (значення `connect_timeout_ms`/
`read_timeout_ms` живуть у `controller/config.json`, а MediaMTX читає
свій власний `readTimeout` з окремого `mediamtx.yml`, який ніхто,
крім `restreamctl.sh`/цього модуля, не перезаписує). Ручний шлях через
SSH (`restreamctl.sh start`/`restart`) лишається за `restreamctl.sh` --
цей модуль його не замінює.
"""

import logging
import os
import re
import signal
import subprocess
import time
from pathlib import Path

_READ_TIMEOUT_LINE = re.compile(r"^readTimeout:[ \t]*\S+[ \t]*$", re.MULTILINE)

_STOP_TIMEOUT_SEC = 5.0
_STARTUP_CHECK_DELAY_SEC = 1.0


def sync_read_timeout(mediamtx_yml_path: Path, connect_timeout_ms: int, read_timeout_ms: int) -> None:
    """
    Підміняє значення `readTimeout:` у вже наявному `mediamtx.yml` на
    суму `connect_timeout_ms + read_timeout_ms` -- звичайним текстовим
    заміщенням рядка (проєкт свідомо уникає `PyYAML`, той самий
    принцип, що й для `controller/config.json`). Захисно: якщо рядок
    не знайдено -- кидає виняток і НІЧОГО не пише, краще голосно
    зупинити рестарт, ніж мовчки лишити файл без зміни чи зіпсованим.
    """
    text = mediamtx_yml_path.read_text(encoding="utf-8")
    total_ms = connect_timeout_ms + read_timeout_ms
    new_text, count = _READ_TIMEOUT_LINE.subn(f"readTimeout: {total_ms}ms", text, count=1)
    if count == 0:
        raise RuntimeError(f"'readTimeout:' line not found in {mediamtx_yml_path} -- refusing to touch the file")

    tmp_path = mediamtx_yml_path.with_suffix(".tmp" + mediamtx_yml_path.suffix)
    tmp_path.write_text(new_text, encoding="utf-8")
    tmp_path.replace(mediamtx_yml_path)


def restart_mediamtx(base_dir: Path) -> None:
    """
    Той самий макет каталогів, що й `restreamctl.sh` (`bin/mediamtx`,
    `mediamtx.yml`, `mediamtx.log`, `.mediamtx.pid` у корені проєкту) --
    щоб `restreamctl.sh status`/`stop` лишались коректними незалежно
    від того, хто останній рестартнув MediaMTX.
    """
    pid_file = base_dir / ".mediamtx.pid"
    _stop_existing(pid_file)
    _start_new(
        pid_file,
        mediamtx_bin=base_dir / "bin" / "mediamtx",
        mediamtx_yml=base_dir / "mediamtx.yml",
        log_path=base_dir / "mediamtx.log",
    )


def _stop_existing(pid_file: Path) -> None:
    pid = _read_pid(pid_file)
    if pid is None or not _is_alive(pid):
        return

    logging.info("stopping mediamtx (pid=%s) for restart", pid)
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return

    deadline = time.monotonic() + _STOP_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if not _is_alive(pid):
            return
        time.sleep(0.2)

    logging.warning("mediamtx (pid=%s) did not exit within %.1fs -- killing it", pid, _STOP_TIMEOUT_SEC)
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def _start_new(pid_file: Path, mediamtx_bin: Path, mediamtx_yml: Path, log_path: Path) -> None:
    with open(log_path, "ab") as log_file:
        proc = subprocess.Popen(
            [str(mediamtx_bin), str(mediamtx_yml)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,  # інакше наступний os.execv() контролера міг би зачепити цей процес
        )
    pid_file.write_text(str(proc.pid), encoding="ascii")
    logging.info("mediamtx restarted (pid=%s)", proc.pid)

    time.sleep(_STARTUP_CHECK_DELAY_SEC)
    if not _is_alive(proc.pid):
        logging.error("mediamtx failed to start after restart (pid=%s) -- check %s", proc.pid, log_path)


def _read_pid(pid_file: Path) -> int | None:
    try:
        return int(pid_file.read_text(encoding="ascii").strip())
    except (OSError, ValueError):
        return None


def _is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True
