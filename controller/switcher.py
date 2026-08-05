"""
FLVSwitcher — перемикання джерела (live/backup) у виході на Twitch без
розриву RTMP-з'єднання.

Контракт:
- `process()` читає FLV-теги з активного джерела й пише їх у
  `out_stream`, коригуючи timestamp під єдину вихідну шкалу часу.
- Перемикання джерела (`set_active`/`request_switch`) не рве вихідний
  потік: на межі reinject-иться sequence header нового джерела, і вихід
  чекає на перший keyframe, перш ніж продовжити писати реальні дані.
- `request_switch` додатково не виконує перехід, якщо seq-header
  джерела змінився з часу його останньої активності (інша роздільність/
  бітрейт) — сигналізує про це викликачу через `on_switched(True)`.
"""

import threading

import flv

_SWITCH_TS_STEP_MS = 33


class FLVSwitcher:
    def __init__(self):
        self._active_lock = threading.Lock()
        self._write_lock = threading.Lock()

        self.active_source: str | None = None
        self.pending_source: str | None = None
        self._pending_callback = None
        self._pending_prior_headers: dict[str, bytes] = {}
        self.out_stream = None
        self.current_output_source: str | None = None
        self.last_out_timestamp = 0
        self.offset = 0
        self.wait_for_keyframe = False
        self.switch_out_timestamp = 0
        self._seq_headers: dict[str, dict[str, bytes]] = {}

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

    def attach_output(self, stream) -> None:
        """
        Підключає нового вихідного writer-а (новий stdin outbound) —
        скидає стан переходу. Заголовки активного джерела НЕ штовхаємо
        тут одразу — `process()` сам відправить їх разом із першим
        keyframe, коли той прийде (див. коментар у `process()`).
        """
        with self._write_lock:
            self.out_stream = stream
            self.current_output_source = None
            self.wait_for_keyframe = True
            self.last_out_timestamp = 0
            self.offset = 0
            self.switch_out_timestamp = 0
            self._write_header_locked()

    def detach_output(self) -> None:
        """Ідемпотентно: безпечно викликати повторно з різних місць."""
        with self._write_lock:
            self.out_stream = None

    def process(self, source: str, tag_type: int, ts: int, payload: bytes) -> None:
        seq_header = flv.is_seq_header(tag_type, payload)
        is_meta = tag_type == flv.TAG_TYPE_SCRIPT
        if seq_header:
            key = "video" if tag_type == flv.TAG_TYPE_VIDEO else "audio"
            self._seq_headers.setdefault(source, {})[key] = payload
        elif is_meta:
            self._seq_headers.setdefault(source, {})["meta"] = payload

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

        with self._write_lock:
            if self.out_stream is None:
                return

            source_changed = self.current_output_source != source
            if source_changed:
                self.current_output_source = source
                self.last_out_timestamp += _SWITCH_TS_STEP_MS
                self.switch_out_timestamp = self.last_out_timestamp
                self.wait_for_keyframe = True

            if self.wait_for_keyframe:
                if seq_header or is_meta or not (tag_type == flv.TAG_TYPE_VIDEO and flv.is_video_keyframe(payload)):
                    return
                self.wait_for_keyframe = False
                self.offset = self.last_out_timestamp - ts + _SWITCH_TS_STEP_MS
                # Метадані й заголовки штовхаємо ЩОЙНО тут, впритул перед
                # самим keyframe (тим самим out_ts) — а не раніше, як
                # тільки прийшли: інакше вихід оголошує нову конфігурацію
                # без жодних даних одразу за нею на час очікування
                # keyframe (до кількох секунд), що плутає декодер/рендер
                # на приймаючій стороні.
                hdrs = self._seq_headers.get(source, {})
                if hdrs.get("meta"):
                    self._write_locked(flv.TAG_TYPE_SCRIPT, self.switch_out_timestamp, hdrs["meta"])
                if hdrs.get("video"):
                    self._write_locked(flv.TAG_TYPE_VIDEO, self.switch_out_timestamp, hdrs["video"])
                if hdrs.get("audio"):
                    self._write_locked(flv.TAG_TYPE_AUDIO, self.switch_out_timestamp, hdrs["audio"])

            out_ts = ts + self.offset
            if out_ts < 0:
                out_ts = 0
            if out_ts > self.last_out_timestamp:
                self.last_out_timestamp = out_ts
            self._write_locked(tag_type, out_ts, payload)

    def _write_locked(self, tag_type: int, ts: int, payload: bytes) -> None:
        try:
            flv.write_flv_tag(self.out_stream, tag_type, ts, payload)
        except (BrokenPipeError, OSError, ValueError):
            pass

    def _write_header_locked(self) -> None:
        try:
            self.out_stream.write(flv.FLV_HEADER)
        except (BrokenPipeError, OSError, ValueError):
            pass
