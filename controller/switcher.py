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
