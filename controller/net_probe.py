"""
Ping до ingest-сервера платформи -- для колонки Ping у вкладці Control.
Це проксі затримки до платформи, НЕ здоров'я самого потоку: `-c copy`-
конвеєр однаково віддає всім платформам той самий потік, тож реальне
"встигає/не встигає" показує лічильник дропів черги
(switcher.OutputSink), а Ping лише орієнтир щодо мережевого шляху.

Два режими (перемикач "Use ICMP ping" у Settings, деф. вимкнено):
- **TCP-connect** (деф.): час рукостискання до RTMP/RTMPS-порту. Без
  привілеїв, порт заведомо відкритий (туди й стримимо).
- **ICMP** (`icmp_rtt_ms`): системний `ping` (avg RTT). Ближче до
  "класичного" пінгу, але ICMP може бути зарізаний файрволом або
  вимагати прав -- тоді повертаємо None (у UI "–").
"""

import re
import socket
import subprocess
import time
from urllib.parse import urlsplit

_DEFAULT_RTMP_PORT = 1935
_DEFAULT_RTMPS_PORT = 443  # rtmps (напр. Kick/AWS IVS) слухає TLS на 443

_ICMP_COUNT = 3
_ICMP_DEADLINE_SEC = 3


def rtmp_host_port(url: str) -> tuple[str | None, int]:
    parts = urlsplit(url)
    default = _DEFAULT_RTMPS_PORT if parts.scheme == "rtmps" else _DEFAULT_RTMP_PORT
    return parts.hostname, (parts.port or default)


def tcp_rtt_ms(url: str, timeout: float = 3.0) -> int | None:
    """Час встановлення TCP-з'єднання до ingest-хоста в мілісекундах, або None при невдачі."""
    host, port = rtmp_host_port(url)
    if not host:
        return None
    try:
        start = time.monotonic()
        with socket.create_connection((host, port), timeout=timeout):
            pass
        return round((time.monotonic() - start) * 1000)
    except OSError:
        return None


def icmp_rtt_ms(url: str) -> int | None:
    """
    Середній ICMP-RTT (мс) через системний `ping`, або None при
    невдачі/блокуванні ICMP/відсутності `ping`. Один виклик; парсимо
    рядок-підсумок `... = min/avg/max[/mdev] ms` і беремо avg. `-n`
    (без reverse-DNS) швидше; `-w` -- загальний дедлайн у секундах.
    """
    host, _ = rtmp_host_port(url)
    if not host:
        return None
    try:
        proc = subprocess.run(
            ["ping", "-n", "-c", str(_ICMP_COUNT), "-w", str(_ICMP_DEADLINE_SEC), host],
            capture_output=True, text=True, timeout=_ICMP_DEADLINE_SEC + _ICMP_COUNT + 2,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    # iputils: "rtt min/avg/max/mdev = 5.1/6.2/7.3/0.8 ms"; busybox:
    # "round-trip min/avg/max = 5.1/6.2/7.3 ms" -- у обох avg це [1].
    m = re.search(r"=\s*[\d.]+/([\d.]+)/", proc.stdout)
    if not m:
        return None
    try:
        return round(float(m.group(1)))
    except ValueError:
        return None
