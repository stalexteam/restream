"""
FLVSwitcher — формує ЄДИНИЙ канонічний FLV-таймлайн з активного
джерела (live/backup) і роздає його всім підключеним виходам
(OutputSink) без розриву RTMP-з'єднання кожного з них.

Дві відповідальності свідомо розділені (це головна причина рефактора
під мульти-платформність):

- **Shared source-таймлайн** (сам FLVSwitcher): читає FLV-теги з
  активного джерела, коригує timestamp під єдину вихідну шкалу,
  на межі relay<->backup чекає keyframe й reinject-ить seq-header.
  Результат — послідовність канонічних `(tag_type, out_ts, payload)`,
  однакова для ВСІХ виходів. Ця шкала НЕ скидається, коли окремий
  вихід (пере)підключається — інакше таймстемпи вже підключених
  платформ стрибнули б назад.

- **Per-destination вихід** (OutputSink): кожна платформа має власну
  чергу + потік-писар. `process()` лише кладе канонічний тег у чергу
  кожного sink (`offer`, неблокуюче) — повільна платформа НІКОЛИ не
  стопорить reader-потоки джерела чи інші платформи. Кожен sink сам
  гейтиться на першому keyframe й інжектить свій FLV-заголовок +
  seq-header (щоб платформа, підключена посеред ефіру, коректно
  почала декодувати).

`request_switch` додатково не виконує перехід, якщо seq-header
джерела змінився з часу його останньої активності (інша роздільність/
бітрейт) — сигналізує про це викликачу через `on_switched(True)`.
"""

import collections
import heapq
import logging
import math
import threading
import time

import flv

_SWITCH_TS_STEP_MS = 33

# Скільки тримати вимірювання байт для розрахунку бітрейту джерела,
# і поріг "дані з OBS зараз течуть" (для OBS-індикатора дашборда).
_BITRATE_WINDOW_SEC = 2.0
_DATA_PRESENT_SEC = 1.0

# Глибина черги виходу (у тегах, ~70 тег/с при 30fps+AAC). Primary
# терпиміший до заторів (хочемо по можливості без втрат), restream —
# коротший, щоб при заторі швидше ресинкнутись на свіжий keyframe, а
# не тягти застарілий хвіст.
PRIMARY_QUEUE_MAX = 900
RESTREAM_QUEUE_MAX = 300

# Скільки секунд після останнього дропу вважати вихід "відстаючим"
# (для health-індикатора в Control).
_BEHIND_WINDOW_SEC = 3.0


class OutputSink:
    """
    Один вихід (одна платформа). Власна черга + потік-писар. `offer()`
    ніколи не блокує викликача; при переповненні черги (платформа не
    встигає) — чистимо чергу й ресинкаємось на наступному keyframe,
    рахуючи дроп (health-метрика). FLV-заголовок і seq-header кожен
    sink інжектить собі сам на першому keyframe свого поточного
    з'єднання — тому підключення/відключення виходу не чіпає ні
    спільний таймлайн, ні інші виходи.
    """

    def __init__(self, name: str, is_primary: bool = False):
        self.name = name
        self.is_primary = is_primary
        self._maxlen = PRIMARY_QUEUE_MAX if is_primary else RESTREAM_QUEUE_MAX
        self._cv = threading.Condition()
        self._queue: collections.deque = collections.deque()
        self._out = None
        self._seed_headers: dict[str, bytes] = {}
        # Кожне (пере)підключення виходу піднімає _gen — писар помічає
        # це й починає з чистого аркуша (новий stdin, знову чекає
        # keyframe, свіжий снімок seq-header).
        self._gen = 0
        self._overflow = False
        self._dropped = 0
        self._last_drop_at = 0.0
        self._closed = False
        self._thread = threading.Thread(target=self._run, name=f"sink-{name}", daemon=True)
        self._thread.start()

    def attach(self, out, seed_headers: dict[str, bytes]) -> None:
        """Підключити свіжий вихідний потік (stdin нового ffmpeg-процесу)."""
        with self._cv:
            self._out = out
            self._seed_headers = dict(seed_headers)
            self._gen += 1
            self._queue.clear()
            self._cv.notify()

    def detach(self) -> None:
        """Ідемпотентно: процес виходу завершився — більше нічого не пишемо."""
        with self._cv:
            self._out = None
            self._gen += 1
            self._queue.clear()
            self._cv.notify()

    def close(self) -> None:
        """Остаточно зупинити потік-писар (вихід видаляють зовсім)."""
        with self._cv:
            self._closed = True
            self._cv.notify()

    def offer(self, tag_type: int, ts: int, payload: bytes) -> None:
        with self._cv:
            if self._out is None:
                return
            if len(self._queue) >= self._maxlen:
                # Платформа не встигає — скидаємо накопичене й ресинкнемось
                # на наступному keyframe (тримати застарілий хвіст немає
                # сенсу, тільки збільшує відставання).
                self._queue.clear()
                self._overflow = True
                self._dropped += 1
                self._last_drop_at = time.monotonic()
            self._queue.append((tag_type, ts, payload))
            self._cv.notify()

    def stats(self) -> dict:
        with self._cv:
            behind = self._last_drop_at > 0 and (time.monotonic() - self._last_drop_at) < _BEHIND_WINDOW_SEC
            return {"dropped": self._dropped, "behind": behind}

    def _run(self) -> None:
        started = False
        headers: dict[str, bytes] = {}
        local_gen = 0
        out = None
        while True:
            with self._cv:
                while True:
                    if self._closed:
                        return
                    if self._gen != local_gen:
                        # (Пере)підключення виходу. Чергу тут НЕ чистимо:
                        # attach/detach уже зробили це під локом у момент
                        # події -- а теги, що надійшли ПІСЛЯ attach (той
                        # самий gen), валідні й чекають на обробку; повторна
                        # чистка тут вимила б їх (race: offer встигає між
                        # attach і першим пробудженням писаря).
                        local_gen = self._gen
                        out = self._out
                        headers = dict(self._seed_headers)
                        started = False
                    if self._overflow:
                        self._overflow = False
                        started = False
                    if self._queue:
                        item = self._queue.popleft()
                        break
                    self._cv.wait()
                out_now = out
            if out_now is None:
                continue
            started, headers = self._forward(out_now, item, started, headers)

    def _forward(self, out, item, started: bool, headers: dict[str, bytes]):
        tag_type, ts, payload = item
        seq = flv.is_seq_header(tag_type, payload)
        is_meta = tag_type == flv.TAG_TYPE_SCRIPT
        if seq:
            headers["video" if tag_type == flv.TAG_TYPE_VIDEO else "audio"] = payload
        elif is_meta:
            headers["meta"] = payload

        if seq or is_meta:
            # Заголовки кодека — не дані для відтворення; поки вихід ще
            # не стартував (чекає keyframe), лише кешуємо, інжектимо
            # перед першим keyframe нижче. Уже стартованому — форвардимо
            # одразу (напр. зміна параметрів на стику джерел).
            if started:
                self._safe_write(out, tag_type, ts, payload)
            return started, headers

        is_kf = tag_type == flv.TAG_TYPE_VIDEO and flv.is_video_keyframe(payload)
        if not started:
            if not is_kf:
                return started, headers  # дропаємо реальні дані до першого keyframe
            self._safe_write_header(out)
            if headers.get("meta"):
                self._safe_write(out, flv.TAG_TYPE_SCRIPT, ts, headers["meta"])
            if headers.get("video"):
                self._safe_write(out, flv.TAG_TYPE_VIDEO, ts, headers["video"])
            if headers.get("audio"):
                self._safe_write(out, flv.TAG_TYPE_AUDIO, ts, headers["audio"])
            started = True
        self._safe_write(out, tag_type, ts, payload)
        return started, headers

    @staticmethod
    def _safe_write(out, tag_type: int, ts: int, payload: bytes) -> None:
        try:
            flv.write_flv_tag(out, tag_type, ts, payload)
        except (BrokenPipeError, OSError, ValueError):
            pass

    @staticmethod
    def _safe_write_header(out) -> None:
        try:
            out.write(flv.FLV_HEADER)
        except (BrokenPipeError, OSError, ValueError):
            pass


class FLVSwitcher:
    def __init__(self):
        self._active_lock = threading.Lock()
        self._timeline_lock = threading.Lock()
        self._stats_lock = threading.Lock()

        self.active_source: str | None = None
        self.pending_source: str | None = None
        self._pending_callback = None
        self._pending_prior_headers: dict[str, bytes] = {}

        self._sinks: dict[str, OutputSink] = {}
        self._output_source: str | None = None
        self.last_out_timestamp = 0
        self.offset = 0
        self.wait_for_keyframe = False
        self.switch_out_timestamp = 0
        self._seq_headers: dict[str, dict[str, bytes]] = {}

        self._byte_samples: collections.deque = collections.deque()
        self._last_relay_data_at: float | None = None

    # --- керування виходами ---

    def register_sink(self, sink: OutputSink) -> None:
        with self._timeline_lock:
            self._sinks[sink.name] = sink

    def unregister_sink(self, name: str) -> None:
        with self._timeline_lock:
            self._sinks.pop(name, None)

    def current_headers(self) -> dict[str, bytes]:
        """Снімок seq-header джерела, що зараз іде у вихід — для сідування нового sink при attach."""
        with self._timeline_lock:
            return dict(self._seq_headers.get(self._output_source, {}))

    # --- перемикання джерела ---

    def set_active(self, source: str | None) -> None:
        """Негайне перемикання. Скасовує будь-який очікуваний `request_switch`."""
        with self._active_lock:
            self.active_source = source
            self.pending_source = None
            self._pending_callback = None
            self._pending_prior_headers = {}

    def request_switch(self, source: str, on_switched=None) -> None:
        """
        Відкладене перемикання: `source` стає активним лише коли від
        нього прийде перший keyframe, поточне джерело продовжує йти на
        вихід без змін до того моменту. `on_switched(params_changed)`
        викликається один раз, одразу після вирішення (поза внутрішніми
        локами свіча).
        """
        with self._active_lock:
            self.pending_source = source
            self._pending_callback = on_switched
            self._pending_prior_headers = dict(self._seq_headers.get(source, {}))

    def _get_active(self) -> str | None:
        with self._active_lock:
            return self.active_source

    # --- метрики джерела (для OBS-індикатора дашборда) ---

    def source_stats(self) -> dict:
        now = time.monotonic()
        with self._stats_lock:
            cutoff = now - _BITRATE_WINDOW_SEC
            while self._byte_samples and self._byte_samples[0][0] < cutoff:
                self._byte_samples.popleft()
            vbytes = sum(size for _, tt, size in self._byte_samples if tt == flv.TAG_TYPE_VIDEO)
            abytes = sum(size for _, tt, size in self._byte_samples if tt == flv.TAG_TYPE_AUDIO)
            flowing = (
                self._last_relay_data_at is not None
                and (now - self._last_relay_data_at) < _DATA_PRESENT_SEC
            )
        return {
            "flowing": flowing,
            "video_kbps": round(vbytes * 8 / _BITRATE_WINDOW_SEC / 1000),
            "audio_kbps": round(abytes * 8 / _BITRATE_WINDOW_SEC / 1000),
        }

    def _record_relay_bitrate(self, tag_type: int, size: int) -> None:
        now = time.monotonic()
        with self._stats_lock:
            self._byte_samples.append((now, tag_type, size))
            self._last_relay_data_at = now

    # --- основний конвеєр ---

    def process(self, source: str, tag_type: int, ts: int, payload: bytes) -> None:
        seq_header = flv.is_seq_header(tag_type, payload)
        is_meta = tag_type == flv.TAG_TYPE_SCRIPT
        if seq_header:
            key = "video" if tag_type == flv.TAG_TYPE_VIDEO else "audio"
            self._seq_headers.setdefault(source, {})[key] = payload
        elif is_meta:
            self._seq_headers.setdefault(source, {})["meta"] = payload

        # Бітрейт/присутність даних рахуємо саме по relay (= потік від
        # OBS), незалежно від того, активний він зараз чи pending —
        # OBS-індикатор має показувати "дані стримера знову йдуть" ще
        # до безшовного перемикання назад на live.
        if source == "relay" and not seq_header and not is_meta:
            self._record_relay_bitrate(tag_type, len(payload))

        is_ready_keyframe = (
            not seq_header and tag_type == flv.TAG_TYPE_VIDEO and flv.is_video_keyframe(payload)
        )

        with self._active_lock:
            callback = None
            params_changed = False
            if self.pending_source is not None and source == self.pending_source and is_ready_keyframe:
                new_headers = self._seq_headers.get(source, {})
                prior = self._pending_prior_headers
                params_changed = bool(prior) and prior != new_headers
                if not params_changed:
                    self.active_source = self.pending_source
                self.pending_source = None
                self._pending_prior_headers = {}
                callback = self._pending_callback
                self._pending_callback = None
            active = self.active_source

        if callback is not None:
            callback(params_changed)

        if source != active:
            return

        with self._timeline_lock:
            source_changed = self._output_source != source
            if source_changed:
                self._output_source = source
                self.last_out_timestamp += _SWITCH_TS_STEP_MS
                self.switch_out_timestamp = self.last_out_timestamp
                self.wait_for_keyframe = True

            if self.wait_for_keyframe:
                if seq_header or is_meta or not (tag_type == flv.TAG_TYPE_VIDEO and flv.is_video_keyframe(payload)):
                    return
                self.wait_for_keyframe = False
                self.offset = self.last_out_timestamp - ts + _SWITCH_TS_STEP_MS
                # Метадані й заголовки штовхаємо ЩОЙНО тут, впритул перед
                # самим keyframe (тим самим out_ts) — а не раніше: інакше
                # вихід оголошує нову конфігурацію без жодних даних одразу
                # за нею на час очікування keyframe, що плутає декодер на
                # приймаючій стороні. Уже стартовані sink отримають ці
                # заголовки; ще не стартовані закешують і інжектнуть собі
                # самі на своєму першому keyframe.
                hdrs = self._seq_headers.get(source, {})
                if hdrs.get("meta"):
                    self._emit(flv.TAG_TYPE_SCRIPT, self.switch_out_timestamp, hdrs["meta"])
                if hdrs.get("video"):
                    self._emit(flv.TAG_TYPE_VIDEO, self.switch_out_timestamp, hdrs["video"])
                if hdrs.get("audio"):
                    self._emit(flv.TAG_TYPE_AUDIO, self.switch_out_timestamp, hdrs["audio"])

            out_ts = ts + self.offset
            if out_ts < 0:
                out_ts = 0
            if out_ts > self.last_out_timestamp:
                self.last_out_timestamp = out_ts
            self._emit(tag_type, out_ts, payload)

    def _emit(self, tag_type: int, ts: int, payload: bytes) -> None:
        # Викликається під self._timeline_lock. offer() неблокуючий —
        # тримати лок тут безпечно (сам запис у платформу відбувається
        # у власному потоці кожного sink, не тут).
        for sink in self._sinks.values():
            sink.offer(tag_type, ts, payload)


# --- MergeSwitcher (remux: відео з одного джерела + аудіо з іншого) ---

# Вікно реордер-буфера: video й audio приходять із ДВОХ reader-потоків,
# тримаємо їх у купі, впорядкованій за out_ts, і флашимо теги старші за
# (max_seen - вікно) -- так muxer/площадка бачать приблизно монотонне
# чергування доріжок (всередині доріжки порядок і так монотонний).
_REORDER_WINDOW_MS = 300
# Скільки тримати історію (wall, ts) main-audio/video для оцінки Δ.
_DELTA_HISTORY_SEC = 2.0
# Вікно збору кандидатів для стартової медіанної фіксації audio_offset.
_START_FIX_WINDOW_SEC = 0.4
# Дефолт re-anchor, якщо в конфізі немає (rig-specific, plan §5.2).
_DEFAULT_REANCHOR = {"enabled": True, "ema_sec": 300, "deadband_ms": 12, "step_ms": 5}

# Детект «живого краю» доставки для чесної фіксації Δ. Коли джерело
# (пере)підключається, MediaMTX віддає новому читачу бэклог (останній GOP) --
# старі кадри одним сплеском, де ts біжить ШВИДШЕ за реальний час. Якщо
# зафіксувати Δ на цьому бэклозі, аудіо намертво відстане на глибину бэклогу.
# Тому Δ фіксуємо лише коли темп доставки ~реальний (rate < поріг), а сплеск
# дропаємо. Виняток -- коли й video-джерело саме в сплеску (холодний старт,
# обидва свіжі й синхронні): там Δ валідний і на сплеску.
_BURST_RATE_THRESH = 1.5       # ts-мс на 1 мс wall; >1.5 == бэклог-сплеск
_RATE_MIN_WINDOW_SEC = 0.25    # мінімум семплів (за wall) для оцінки темпу
_DELTA_FIX_GRACE_SEC = 2.5     # запобіжник: зафіксувати Δ навіть без «живого краю»

# Само-відновлення (coarse re-sync) поверх fine-дрейфу: якщо СГЛАЖЕНА
# коротким вікном оцінка Δ УСТІЙЛИВО (>= HOLD с) розходиться з застосованим
# audio_offset більше за поріг -- це не дрейф, а погана стартова фіксація ->
# один разовий ре-синк (короткий глітч, зате далі синк вірний). Fine-дрейф
# великі помилки виправити не встигає (deadband/крок -- одиниці мс).
_COARSE_WINDOW_SEC = 3.0       # коротке вікно для стійкої оцінки Δ
_COARSE_RESYNC_MS = 300        # поріг «це вже не дрейф, а зсув»
_COARSE_HOLD_SEC = 4.0         # скільки розбіжність має протриматись до ре-синку


class MergeSwitcher:
    """
    Варіант `FLVSwitcher` для remux (plan.md §5.1): формує ОДИН
    канонічний таймлайн із ДВОХ живих джерел -- відео з `"video"`-relay,
    аудіо з `"audio"`-relay -- і роздає його тим самим `OutputSink`
    (sink-шар не змінюється). У FALLBACK -- один самодостатній `"backup"`.

    Ключові відмінності від `FLVSwitcher`:
    - **Роздільні оффсети** video та audio (жодної спільної накопичувальної
      дельти -- вона ламає A/V). `audio_offset = video_offset + Δ +
      audio_trim`, де Δ оцінюється мостом через main-audio (§5.2).
    - **Монотонність ПО ДОРІЖЦІ** (два лічильники `last_out_video/audio`);
      спільний кламп на обидві доріжки НЕ застосовуємо.
    - **Перехід на single-source (backup) і назад** рахує ОДИН offset на
      обидва треки, якорений на `max(last_out_video, last_out_audio)`.
    - **Reorder-буфер** для міжпоточного чергування двох доріжок.

    Інтерфейс сумісний із `FLVSwitcher` там, де його використовує
    `Pipeline`/`Destination`: register_sink/unregister_sink/current_headers/
    set_active/request_switch/source_stats/process/`pending_source`.
    Токени активного джерела -- `"live"` (композит) та `"backup"`.
    """

    def __init__(self, reanchor: dict | None = None, audio_trim_ms: int = 0):
        self._lock = threading.Lock()          # timeline/offsets/reorder/estimator/headers
        self._active_lock = threading.Lock()   # active/pending
        self._stats_lock = threading.Lock()

        self.active_source: str | None = None   # "live" | "backup" | None
        self.pending_source: str | None = None
        self._pending_callback = None
        self._pending_prior_headers: dict[str, bytes] = {}

        self._sinks: dict[str, OutputSink] = {}
        # seq-headers/meta per relay-джерело ("video"/"audio"/"backup").
        self._seq_headers: dict[str, dict[str, bytes]] = {}
        self._have_audio_seq = False  # чи вже маємо AAC seq header із "audio"

        self._output_mode: str | None = None   # що зараз реально емітимо
        self._wait_kf = False                   # чекаємо перший keyframe для (пере)встановлення
        self._last_out_video = 0
        self._last_out_audio = 0
        self._video_offset = 0
        self._video_offset_set = False
        self._audio_offset = 0
        self._audio_offset_set = False
        self._audio_started = False  # чи вже інжектнули audio-seq у поточному live
        # single-source (backup) -- один спільний offset на обидва треки.
        self._backup_offset = 0
        self._backup_offset_set = False

        # reorder-буфер: min-heap (out_ts, seq, tag_type, payload).
        self._reorder: list = []
        self._reorder_seq = 0
        self._max_seen_out = 0

        # --- Δ-оцінювач (§5.2) ---
        self._main_audio_hist: collections.deque = collections.deque()  # (wall, ts) main-audio
        self._video_hist: collections.deque = collections.deque()       # (wall, ts) main-video (фолбек)
        self._clean_audio_hist: collections.deque = collections.deque() # (wall, ts) clean-audio (детект живого краю)
        self._clean_first_seen_at: float | None = None                  # перший clean-audio цього live-епізоду
        self._have_main_audio = False
        self._audio_trim_ms = int(audio_trim_ms or 0)
        self._start_fix_until = 0.0
        self._start_candidates: list[float] = []
        self._offset_ema: float | None = None
        self._ema_last_wall: float | None = None
        # coarse re-sync (само-відновлення від поганої стартової фіксації)
        self._recent_candidates: collections.deque = collections.deque()  # (wall, candidate)
        self._coarse_since: float | None = None
        self._reanchor = dict(_DEFAULT_REANCHOR)
        if isinstance(reanchor, dict):
            self._reanchor.update(reanchor)

        self._byte_samples: collections.deque = collections.deque()
        self._last_video_data_at: float | None = None

    # --- керування виходами (як у FLVSwitcher) ---

    def register_sink(self, sink: OutputSink) -> None:
        with self._lock:
            self._sinks[sink.name] = sink

    def unregister_sink(self, name: str) -> None:
        with self._lock:
            self._sinks.pop(name, None)

    def current_headers(self) -> dict[str, bytes]:
        """Комбінований снімок seq/meta для сідування нового sink: у live -- video-seq з `"video"`, audio-seq з `"audio"`, meta з `"video"`; у backup -- усе з `"backup"`."""
        with self._lock:
            if self._output_mode == "backup":
                return dict(self._seq_headers.get("backup", {}))
            return self._live_headers_locked()

    def _live_headers_locked(self) -> dict[str, bytes]:
        vid = self._seq_headers.get("video", {})
        aud = self._seq_headers.get("audio", {})
        headers: dict[str, bytes] = {}
        if vid.get("video"):
            headers["video"] = vid["video"]
        if aud.get("audio"):
            headers["audio"] = aud["audio"]
        # onMetaData беремо з main (video-джерело): video-поля коректні, а
        # аудіо-поля інформаційні -- реальний AAC-конфіг несе audio-seq
        # header. (Синтез AMF з аудіо-полів clean -- TODO, plan §5.1.)
        if vid.get("meta"):
            headers["meta"] = vid["meta"]
        return headers

    # --- перемикання джерела (як у FLVSwitcher, токени live/backup) ---

    def set_active(self, source: str | None) -> None:
        with self._active_lock:
            self.active_source = source
            self.pending_source = None
            self._pending_callback = None
            self._pending_prior_headers = {}

    def request_switch(self, source: str, on_switched=None) -> None:
        with self._active_lock:
            self.pending_source = source
            self._pending_callback = on_switched
            # Порівнюємо seq-headers джерела до/після паузи -- зміна
            # параметрів -> НЕ безшовно (як у FLVSwitcher).
            with self._lock:
                self._pending_prior_headers = dict(self._live_headers_locked()) if source == "live" \
                    else dict(self._seq_headers.get(source, {}))

    def set_audio_trim(self, ms: int) -> None:
        """Live-правка ручного триммера (§5.2): миттєво зсуває audio_offset на дельту, без реконнекту."""
        with self._lock:
            delta = int(ms) - self._audio_trim_ms
            self._audio_trim_ms = int(ms)
            if self._audio_offset_set:
                self._audio_offset += delta

    def reset(self) -> None:
        """
        Повне скидання канонічного таймлайну + оцінювача Δ + reorder-буфера.
        Викликається на ХОЛОДНОМУ старті нової сесії remux (OFFLINE->LIVE):
        інакше свіжа сесія успадкувала б протухлі `last_out_*`/offset-и з
        попередньої (напр. коли ts джерел розʼїхались через несинхронний
        плагін) -> нове аудіо вічно клампилось би (нема звуку), а великий
        протухлий `last_out` давав би величезну затримку. Безшовний возврат
        із FALLBACK reset НЕ робить (там навпаки треба тяглість).
        """
        with self._active_lock:
            self.active_source = None
            self.pending_source = None
            self._pending_callback = None
            self._pending_prior_headers = {}
        with self._lock:
            self._seq_headers = {}
            self._have_audio_seq = False
            self._output_mode = None
            self._wait_kf = False
            self._last_out_video = 0
            self._last_out_audio = 0
            self._video_offset = 0
            self._video_offset_set = False
            self._audio_offset = 0
            self._audio_offset_set = False
            self._audio_started = False
            self._backup_offset = 0
            self._backup_offset_set = False
            self._reorder = []
            self._reorder_seq = 0
            self._max_seen_out = 0
            self._main_audio_hist.clear()
            self._video_hist.clear()
            self._clean_audio_hist.clear()
            self._clean_first_seen_at = None
            self._have_main_audio = False
            self._start_fix_until = 0.0
            self._start_candidates = []
            self._offset_ema = None
            self._ema_last_wall = None
            self._recent_candidates.clear()
            self._coarse_since = None
        with self._stats_lock:
            self._byte_samples.clear()
            self._last_video_data_at = None
        logging.info("[merge] state reset for a fresh session")

    # --- метрики джерела (video) для OBS-індикатора ---

    def source_stats(self) -> dict:
        now = time.monotonic()
        with self._stats_lock:
            cutoff = now - _BITRATE_WINDOW_SEC
            while self._byte_samples and self._byte_samples[0][0] < cutoff:
                self._byte_samples.popleft()
            vbytes = sum(size for _, tt, size in self._byte_samples if tt == flv.TAG_TYPE_VIDEO)
            abytes = sum(size for _, tt, size in self._byte_samples if tt == flv.TAG_TYPE_AUDIO)
            flowing = (
                self._last_video_data_at is not None
                and (now - self._last_video_data_at) < _DATA_PRESENT_SEC
            )
        return {
            "flowing": flowing,
            "video_kbps": round(vbytes * 8 / _BITRATE_WINDOW_SEC / 1000),
            "audio_kbps": round(abytes * 8 / _BITRATE_WINDOW_SEC / 1000),
        }

    def skew_ms(self) -> int:
        """Поточна оцінена A/V-різниця (для діагностики в дашборді): audio_offset − video_offset − trim = Δ."""
        with self._lock:
            if not (self._video_offset_set and self._audio_offset_set):
                return 0
            return int(self._audio_offset - self._video_offset - self._audio_trim_ms)

    # --- основний конвеєр ---

    def process(self, source: str, tag_type: int, ts: int, payload: bytes) -> None:
        seq = flv.is_seq_header(tag_type, payload)
        is_meta = tag_type == flv.TAG_TYPE_SCRIPT
        is_audio = tag_type == flv.TAG_TYPE_AUDIO
        is_video = tag_type == flv.TAG_TYPE_VIDEO

        with self._lock:
            if seq:
                key = "video" if is_video else "audio"
                self._seq_headers.setdefault(source, {})[key] = payload
                if source == "audio" and is_audio:
                    self._have_audio_seq = True
            elif is_meta:
                self._seq_headers.setdefault(source, {})["meta"] = payload

        now = time.monotonic()
        # Бітрейт/присутність -- по video-джерелу (OBS-індикатор).
        if source == "video" and not seq and not is_meta:
            with self._stats_lock:
                self._byte_samples.append((now, tag_type, len(payload)))
                self._last_video_data_at = now

        # Історія прибуття для оцінки Δ (тільки дані, не seq).
        if source == "video" and not seq:
            with self._lock:
                if is_audio:
                    self._have_main_audio = True
                    self._push_hist(self._main_audio_hist, now, ts)
                elif is_video:
                    self._push_hist(self._video_hist, now, ts)
        elif source == "audio" and is_audio and not seq:
            with self._lock:
                self._push_hist(self._clean_audio_hist, now, ts)
                if self._clean_first_seen_at is None:
                    self._clean_first_seen_at = now

        ready_kf = (not seq and is_video and flv.is_video_keyframe(payload))

        # Розвʼязання pending -> active (тригер: video-keyframe для "live"
        # + наявність audio-seq; backup-keyframe для "backup").
        with self._active_lock:
            callback = None
            params_changed = False
            pending = self.pending_source
            if pending is not None:
                trigger = "video" if pending == "live" else "backup"
                gate_ok = ready_kf and (pending != "live" or self._have_audio_seq)
                if source == trigger and gate_ok:
                    with self._lock:
                        new_headers = self._live_headers_locked() if pending == "live" \
                            else self._seq_headers.get(pending, {})
                    prior = self._pending_prior_headers
                    params_changed = bool(prior) and prior != new_headers
                    if not params_changed:
                        self.active_source = pending
                    self.pending_source = None
                    self._pending_prior_headers = {}
                    callback = self._pending_callback
                    self._pending_callback = None
            active = self.active_source

        if callback is not None:
            callback(params_changed)

        # Маршрутизація тега за активним режимом. Строго: з "video" беремо
        # ЛИШЕ відео+meta (будь-яке аудіо -- data чи seq -- це main-audio, лише
        # для Δ, НЕ форвардимо); з "audio" -- ЛИШЕ аудіо (відео статичної
        # картинки й її meta -- дропаємо). Інакше leak чужого seq-header у потік.
        if active == "live":
            if source == "video":
                if is_audio:
                    return  # main-audio (data/seq) -- лише для оцінки Δ
                self._feed_live(True, "video", tag_type, ts, payload, seq, is_meta, now)
            elif source == "audio":
                if is_video or is_meta:
                    return  # статична картинка clean (video/seq) + її meta -- дроп
                self._feed_live(False, "audio", tag_type, ts, payload, seq, is_meta, now)
            # backup-теги при active==live ігноруємо
        elif active == "backup":
            if source == "backup":
                self._feed_backup(tag_type, ts, payload, seq, is_meta)

    # --- live-гілка (два джерела, роздільні оффсети) ---

    def _feed_live(self, is_video, role, tag_type, ts, payload, seq, is_meta, now):
        with self._lock:
            # Зміна режиму виводу -> флашимо буфер і чекаємо video-keyframe.
            if self._output_mode != "live":
                self._flush_all_locked()
                self._output_mode = "live"
                self._wait_kf = True
                self._video_offset_set = False
                self._audio_offset_set = False
                self._audio_started = False
                # Fresh Δ estimation for this live entry (don't carry a stale
                # EMA/start-fix from before a backup episode).
                self._start_fix_until = 0.0
                self._start_candidates = []
                self._offset_ema = None
                self._ema_last_wall = None
                self._recent_candidates.clear()
                self._coarse_since = None
                self._clean_first_seen_at = None  # regate the live-edge check
                self._clean_audio_hist.clear()

            if self._wait_kf:
                # Стартуємо ТІЛЬКИ на video-keyframe (audio до нього дропаємо).
                if not (role == "video" and not seq and not is_meta
                        and tag_type == flv.TAG_TYPE_VIDEO and flv.is_video_keyframe(payload)):
                    return
                self._wait_kf = False
                anchor = max(self._last_out_video, self._last_out_audio) + _SWITCH_TS_STEP_MS
                self._video_offset = anchor - ts
                self._video_offset_set = True
                logging.info("[merge] live start: video_offset=%d (anchor=%d, ts_kf=%d)", self._video_offset, anchor, ts)
                # meta + video-seq впритул перед keyframe (той самий out_ts).
                # audio-seq НЕ тут: він іде на СВОЇЙ доріжці перед першим audio
                # (out_ts аудіо може відставати від video-якоря -> інакше зворотний
                # ts у аудіо-доріжці на стику).
                hdrs = self._live_headers_locked()
                if hdrs.get("meta"):
                    self._push(anchor, flv.TAG_TYPE_SCRIPT, hdrs["meta"], header=True)
                if hdrs.get("video"):
                    self._push(anchor, flv.TAG_TYPE_VIDEO, hdrs["video"], header=True)
                # сам keyframe:
                self._last_out_video = anchor
                self._push(anchor, tag_type, payload, header=False)
                self._flush_locked()
                return

            if seq or is_meta:
                # Уже стартовані sink отримують оновлений seq/meta одразу
                # (напр. зміна параметрів на льоту); out_ts -- поточний рівень
                # своєї доріжки, без просування лічильника.
                out_ts = self._last_out_video if is_video else self._last_out_audio
                self._push(out_ts, tag_type, payload, header=True)
                self._flush_locked()
                return

            if role == "video":
                out_ts = ts + self._video_offset
                if out_ts < self._last_out_video:
                    out_ts = self._last_out_video
                self._last_out_video = out_ts
                self._push(out_ts, tag_type, payload, header=False)
            else:  # role == "audio", clean-audio
                self._estimate_audio_offset_locked(now, ts)
                if not self._audio_offset_set:
                    # Ще не змогли оцінити Δ (немає історії main) -- рідкісний
                    # стартовий випадок; дропаємо кілька тегів, доки оцінимо.
                    return
                out_ts = ts + self._audio_offset
                if out_ts < self._last_out_audio:
                    out_ts = self._last_out_audio
                if not self._audio_started:
                    # audio-seq впритул перед першим audio-пакетом на ЙОГО out_ts
                    # (не на video-якорі) -- гарантує монотонність аудіо-доріжки
                    # через стик backup<->live.
                    aud_seq = self._seq_headers.get("audio", {}).get("audio")
                    if aud_seq:
                        self._push(out_ts, flv.TAG_TYPE_AUDIO, aud_seq, header=True)
                    self._audio_started = True
                self._last_out_audio = out_ts
                self._push(out_ts, tag_type, payload, header=False)
            self._flush_locked()

    # --- backup-гілка (одне джерело, спільний offset на max-якорі) ---

    def _feed_backup(self, tag_type, ts, payload, seq, is_meta):
        with self._lock:
            if self._output_mode != "backup":
                self._flush_all_locked()
                self._output_mode = "backup"
                self._wait_kf = True
                self._backup_offset = 0
                self._backup_offset_set = False

            is_kf = (not seq and tag_type == flv.TAG_TYPE_VIDEO and flv.is_video_keyframe(payload))
            if self._wait_kf:
                if seq or is_meta or not is_kf:
                    return
                self._wait_kf = False
                anchor = max(self._last_out_video, self._last_out_audio) + _SWITCH_TS_STEP_MS
                self._backup_offset = anchor - ts
                self._backup_offset_set = True
                hdrs = self._seq_headers.get("backup", {})
                if hdrs.get("meta"):
                    self._push(anchor, flv.TAG_TYPE_SCRIPT, hdrs["meta"], header=True)
                if hdrs.get("video"):
                    self._push(anchor, flv.TAG_TYPE_VIDEO, hdrs["video"], header=True)
                if hdrs.get("audio"):
                    self._push(anchor, flv.TAG_TYPE_AUDIO, hdrs["audio"], header=True)
                self._last_out_video = anchor
                self._push(anchor, tag_type, payload, header=False)
                self._flush_locked()
                return

            if seq or is_meta:
                out_ts = max(self._last_out_video, self._last_out_audio)
                self._push(out_ts, tag_type, payload, header=True)
                self._flush_locked()
                return

            out_ts = ts + self._backup_offset
            if tag_type == flv.TAG_TYPE_VIDEO:
                if out_ts < self._last_out_video:
                    out_ts = self._last_out_video
                self._last_out_video = out_ts
            else:
                if out_ts < self._last_out_audio:
                    out_ts = self._last_out_audio
                self._last_out_audio = out_ts
            self._push(out_ts, tag_type, payload, header=False)
            self._flush_locked()

    # --- Δ-оцінка (§5.2), під self._lock ---

    def _estimate_audio_offset_locked(self, now: float, ts_ca: int) -> None:
        if not self._video_offset_set:
            return
        candidate = self._bridge_candidate_locked(now, ts_ca)
        if candidate is None:
            return
        if not self._audio_offset_set:
            if not self._ready_to_fix_delta_locked(now):
                return  # ще бэклог-сплеск clean -- дропаємо, не фіксуємо Δ
            # Стартова фіксація: медіана кандидатів за перші ~400 мс.
            self._start_candidates.append(candidate)
            if self._start_fix_until == 0.0:
                self._start_fix_until = now + _START_FIX_WINDOW_SEC
            med = _median(self._start_candidates)
            self._audio_offset = med + self._audio_trim_ms
            self._audio_offset_set = True
            self._offset_ema = candidate
            self._ema_last_wall = now
            waited = (now - self._clean_first_seen_at) if self._clean_first_seen_at else 0.0
            logging.info(
                "[merge] live start: audio_offset=%d (delta=%d, video_offset=%d, trim=%d, ts_ca=%d, "
                "clean_rate=%.2f, waited_for_live_edge=%.2fs)",
                int(self._audio_offset), int(candidate - self._video_offset),
                self._video_offset, self._audio_trim_ms, ts_ca,
                (self._delivery_rate(self._clean_audio_hist) or -1.0), waited,
            )
            return
        # Ще в стартовому вікні -- уточнюємо медіаною (кілька дрібних кроків).
        if now < self._start_fix_until:
            self._start_candidates.append(candidate)
            self._audio_offset = _median(self._start_candidates) + self._audio_trim_ms
        # Re-anchor (тільки з main-audio мостом; на video-фолбеку надто шумно).
        self._reanchor_locked(now, candidate)

    def _reanchor_locked(self, now: float, candidate: float) -> None:
        if not (self._reanchor.get("enabled") and self._have_main_audio):
            return

        # --- coarse: само-відновлення від поганої стартової фіксації ---
        # Тримаємо коротке вікно кандидатів; якщо їхня медіана УСТІЙЛИВО далеко
        # від застосованого audio_offset -- це не дрейф (той в межах deadband),
        # а зсув, який fine-крок не наздожене -> один разовий ре-синк.
        self._recent_candidates.append((now, candidate))
        cutoff = now - _COARSE_WINDOW_SEC
        while self._recent_candidates and self._recent_candidates[0][0] < cutoff:
            self._recent_candidates.popleft()
        if len(self._recent_candidates) >= 3:
            short_med = _median([c for _, c in self._recent_candidates])
            target_coarse = short_med + self._audio_trim_ms
            if abs(target_coarse - self._audio_offset) > _COARSE_RESYNC_MS:
                if self._coarse_since is None:
                    self._coarse_since = now
                elif (now - self._coarse_since) >= _COARSE_HOLD_SEC:
                    old = self._audio_offset
                    self._audio_offset = target_coarse
                    self._offset_ema = short_med
                    self._coarse_since = None
                    logging.warning(
                        "[merge] audio re-sync: audio_offset %d -> %d (%+d ms, recovering a bad Δ fix)",
                        int(old), int(self._audio_offset), int(self._audio_offset - old),
                    )
                    return
            else:
                self._coarse_since = None  # у межах порогу -> скидаємо таймер

        # --- fine: плавний дрейф (EMA + deadband/крок), неслышний ---
        tau = max(1.0, float(self._reanchor.get("ema_sec", 300)))
        if self._offset_ema is None:
            self._offset_ema = candidate
            self._ema_last_wall = now
            return
        dt = now - (self._ema_last_wall or now)
        self._ema_last_wall = now
        alpha = 1.0 - math.exp(-max(0.0, dt) / tau)
        self._offset_ema += alpha * (candidate - self._offset_ema)
        target = self._offset_ema + self._audio_trim_ms
        deadband = float(self._reanchor.get("deadband_ms", 12))
        step = float(self._reanchor.get("step_ms", 5))
        diff = target - self._audio_offset
        if abs(diff) > deadband:
            self._audio_offset += math.copysign(min(step, abs(diff)), diff)

    def _ready_to_fix_delta_locked(self, now: float) -> bool:
        """
        Чи можна ЗАРАЗ фіксувати стартову Δ. Не можна, поки clean ще ллє
        бэклог-сплеск (ts біжить швидше за реальний час) І main при цьому
        вже на живому краю -- інакше залочимо відставання на глибину бэклогу
        (див. лог: seamless-возврат давав delta≈58с при ts_ca=0). Дозволяємо
        фіксацію коли: (а) clean вийшов на живий край, АБО (б) main сам зараз
        у сплеску (холодний старт -- обидва свіжі й симетричні), АБО (в) сплив
        запобіжний grace від першого clean-пакета.
        """
        seen = self._clean_first_seen_at
        if seen is not None and (now - seen) >= _DELTA_FIX_GRACE_SEC:
            return True
        rate_ca = self._delivery_rate(self._clean_audio_hist)
        rate_ma = self._delivery_rate(self._main_audio_hist)
        main_live = rate_ma is not None and rate_ma < _BURST_RATE_THRESH
        if not main_live:
            # main сам ще свіжий/у сплеску -> обидва джерела симетричні, Δ
            # валідний і на сплеску (як холодний старт).
            return True
        clean_live = rate_ca is not None and rate_ca < _BURST_RATE_THRESH
        return clean_live

    @staticmethod
    def _delivery_rate(hist: collections.deque) -> float | None:
        """Темп доставки: приріст ts (мс) на 1 мс wall. ~1.0 у реальному часі, >>1 на бэклог-сплеску. None -- замало семплів."""
        if len(hist) < 2:
            return None
        w0, t0 = hist[0]
        w1, t1 = hist[-1]
        dw = w1 - w0
        if dw < _RATE_MIN_WINDOW_SEC:
            return None
        return (t1 - t0) / (dw * 1000.0)

    def _bridge_candidate_locked(self, now: float, ts_ca: int) -> float | None:
        # Міст: canonical(clean_audio) має збігтись з canonical(main_audio) в
        # ту саму мить приходу -> audio_offset = video_offset + (ts_ma - ts_ca).
        ts_ref = self._interp(self._main_audio_hist, now)
        if ts_ref is None:
            # Фолбек: немає main-audio -> мостимо до main-video (грубіше,
            # re-anchor у цьому режимі вимкнено, _have_main_audio=False).
            ts_ref = self._interp(self._video_hist, now)
        if ts_ref is None:
            return None
        return self._video_offset + (ts_ref - ts_ca)

    @staticmethod
    def _push_hist(hist: collections.deque, now: float, ts: int) -> None:
        # Джерело (пере)підключилось -> ts стрибнув НАЗАД (обнулення). Стара
        # шкала більше не валідна -- чистимо, щоб темп/інтерполяція не рахували
        # по мішанці старих і нових ts.
        if hist and ts + 1000 < hist[-1][1]:
            hist.clear()
        hist.append((now, ts))
        cutoff = now - _DELTA_HISTORY_SEC
        while hist and hist[0][0] < cutoff:
            hist.popleft()

    @staticmethod
    def _interp(hist: collections.deque, wall: float) -> float | None:
        if not hist:
            return None
        if wall <= hist[0][0]:
            return float(hist[0][1])
        if wall >= hist[-1][0]:
            w_last, ts_last = hist[-1]
            return float(ts_last) + (wall - w_last) * 1000.0  # ts у мс ~ wall*1000
        prev = hist[0]
        for cur in hist:
            if cur[0] >= wall:
                w0, t0 = prev
                w1, t1 = cur
                if w1 == w0:
                    return float(t1)
                frac = (wall - w0) / (w1 - w0)
                return float(t0) + frac * (t1 - t0)
            prev = cur
        return float(hist[-1][1])

    # --- reorder-буфер + емісія, під self._lock ---

    def _push(self, out_ts: int, tag_type: int, payload: bytes, header: bool) -> None:
        out_ts = int(out_ts)
        if out_ts < 0:
            out_ts = 0
        self._reorder_seq += 1
        heapq.heappush(self._reorder, (out_ts, self._reorder_seq, tag_type, payload))
        if out_ts > self._max_seen_out:
            self._max_seen_out = out_ts

    def _flush_locked(self) -> None:
        horizon = self._max_seen_out - _REORDER_WINDOW_MS
        while self._reorder and self._reorder[0][0] <= horizon:
            out_ts, _, tag_type, payload = heapq.heappop(self._reorder)
            self._emit(tag_type, out_ts, payload)

    def _flush_all_locked(self) -> None:
        while self._reorder:
            out_ts, _, tag_type, payload = heapq.heappop(self._reorder)
            self._emit(tag_type, out_ts, payload)

    def flush(self) -> None:
        """Скинути залишок reorder-буфера у sink-и (коректне завершення/тести)."""
        with self._lock:
            self._flush_all_locked()

    def _emit(self, tag_type: int, ts: int, payload: bytes) -> None:
        for sink in self._sinks.values():
            sink.offer(tag_type, ts, payload)


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0
