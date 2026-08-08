"""
Керування MediaMTX-процесом з боку контролера -- потрібне ЛИШЕ для
Settings -> Apply & Restart у дашборді. `data/mediamtx.yml` -- це
згенерований артефакт: перед рестартом рендеримо його наново з
`controller/mediamtx.yml.template` + `data/config.json` (єдине джерело
правди для паролів і таймаутів, варіант Б; той самий рендер, що робить
`restreamctl.sh` через `mediamtx_config.py`). Ручний шлях через SSH
(`restreamctl.sh start`/`restart`) лишається за `restreamctl.sh` --
цей модуль його не замінює.
"""

import logging
import os
import signal
import subprocess
import time
from pathlib import Path

import mediamtx_config

_STOP_TIMEOUT_SEC = 5.0
_STARTUP_CHECK_DELAY_SEC = 1.0


def restart_mediamtx(base_dir: Path, config: dict) -> None:
    """
    Той самий макет каталогів, що й `restreamctl.sh` (`bin/mediamtx`,
    `data/mediamtx.yml`, `logs/mediamtx.log`, `data/.mediamtx.pid`) --
    щоб `restreamctl.sh status`/`stop` лишались коректними незалежно
    від того, хто останній рестартнув MediaMTX. `data/mediamtx.yml`
    рендериться заново з config перед стартом.
    """
    data_dir = base_dir / "data"
    mediamtx_yml = data_dir / "mediamtx.yml"
    mediamtx_config.render(base_dir / "controller" / "mediamtx.yml.template", config, mediamtx_yml)
    pid_file = data_dir / ".mediamtx.pid"
    _stop_existing(pid_file)
    _start_new(
        pid_file,
        mediamtx_bin=base_dir / "bin" / "mediamtx",
        mediamtx_yml=mediamtx_yml,
        log_path=base_dir / "logs" / "mediamtx.log",
        cwd=data_dir,
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


def _start_new(pid_file: Path, mediamtx_bin: Path, mediamtx_yml: Path, log_path: Path, cwd: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "ab") as log_file:
        proc = subprocess.Popen(
            [str(mediamtx_bin), str(mediamtx_yml)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            cwd=str(cwd),  # будь-які відносні артефакти MediaMTX (auto.crt/key) -> data/
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
