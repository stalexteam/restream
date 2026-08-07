"""
CPU%/RSS-статистика процесів для дашборда — напряму через /proc, без
psutil (проєкт лишається stdlib-only). Лише Linux, як і решта
проєкту (Debian/Ubuntu VPS).
"""

import os
import time

_CLK_TCK = os.sysconf("SC_CLK_TCK")

# pid -> (monotonic час семплу, сумарні cpu-тіки utime+stime на той момент)
_prev_samples: dict[int, tuple[float, int]] = {}


def sample(pid: int | None) -> dict | None:
    """
    `None`, якщо pid відсутній або процес не читається (завершився).
    Перший виклик для нового pid не має з чим порахувати дельту --
    поверне `cpu_percent: 0.0` (не помилка, просто ще немає історії).
    """
    if pid is None:
        return None

    try:
        with open(f"/proc/{pid}/stat", "r", encoding="ascii") as f:
            stat = f.read()
        with open(f"/proc/{pid}/status", "r", encoding="ascii") as f:
            status_text = f.read()
    except OSError:
        # Процес зник -- прибираємо застарілий запис, інакше словник
        # росте сміттям на кожному рестарті relay/backup/outbound
        # (щоразу новий pid) за довгий час роботи контролера.
        _prev_samples.pop(pid, None)
        return None

    # /proc/pid/stat: поле comm у дужках може містити пробіли/дужки --
    # безпечно ділити лише по ОСТАННЬОМУ ")". Після нього індекс 0 --
    # це поле 3 (state), тож utime (поле 14) -- індекс 11, stime
    # (поле 15) -- індекс 12.
    after_comm = stat.rsplit(")", 1)[1].split()
    cpu_ticks = int(after_comm[11]) + int(after_comm[12])

    rss_kb = 0
    for line in status_text.splitlines():
        if line.startswith("VmRSS:"):
            rss_kb = int(line.split()[1])
            break

    now = time.monotonic()
    prev = _prev_samples.get(pid)
    _prev_samples[pid] = (now, cpu_ticks)

    cpu_percent = 0.0
    if prev is not None:
        prev_time, prev_ticks = prev
        elapsed = now - prev_time
        if elapsed > 0:
            cpu_percent = max(0.0, (cpu_ticks - prev_ticks) / _CLK_TCK / elapsed * 100)

    return {"cpu_percent": round(cpu_percent, 1), "rss_mb": round(rss_kb / 1024, 1)}
