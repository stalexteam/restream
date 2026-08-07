"""
Стежить за mediamtx.log у фоновому потоці -- єдиний спосіб дізнатись
про обрив OBS<->MediaMTX ще ДО того, як шлях стає "available": якщо
`readTimeout` вбиває з'єднання раніше, ніж OBS встиг опублікувати
потік, `runOnAvailable`/`runOnUnavailable` жодного разу не
спрацьовують -- ця подія іншим шляхом до контролера не долітає.
"""

import re
import time
from pathlib import Path
from typing import Callable

_PUBLISHING_RE = re.compile(r"\[conn ([\d.]+:\d+)\] is publishing to path")
_TIMEOUT_CLOSE_RE = re.compile(r"\[conn ([\d.]+:\d+)\] closed: read tcp .*: i/o timeout")
_CLOSED_RE = re.compile(r"\[conn ([\d.]+:\d+)\] closed:")

_POLL_INTERVAL_SEC = 0.5


def watch(log_path: Path, on_connect_timeout: Callable[[], None]) -> None:
    """
    Читає лише НОВІ рядки (з моменту старту потоку, не всю історію
    файлу). `publishing` -- набір conn-ідентифікаторів, що вже дійшли
    до "is publishing" -- readTimeout-закриття такого з'єднання це вже
    не невдалий конект (той випадок ловить власний read_timeout_ms
    детектор контролера, швидше й точніше), а щось інше -- не
    репортимо тут.
    """
    publishing: set[str] = set()
    while not log_path.exists():
        time.sleep(_POLL_INTERVAL_SEC)

    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        f.seek(0, 2)
        while True:
            line = f.readline()
            if not line:
                time.sleep(_POLL_INTERVAL_SEC)
                continue

            m = _PUBLISHING_RE.search(line)
            if m:
                publishing.add(m.group(1))
                continue

            m = _TIMEOUT_CLOSE_RE.search(line)
            if m:
                conn = m.group(1)
                if conn not in publishing:
                    on_connect_timeout()
                publishing.discard(conn)
                continue

            m = _CLOSED_RE.search(line)
            if m:
                publishing.discard(m.group(1))
