"""
Мінімальний парсер/писар FLV-тегів (лише те, що потрібне switcher-у):
читання тегів зі stdout ffmpeg (relay/backup), запис тегів у stdin
ffmpeg (outbound). Без залежностей поза stdlib.

Формат FLV-тегу: 11-байтний заголовок (тип, розмір payload, 24-бітний
timestamp + 8-бітний TimestampExtended, StreamID=0) + payload +
4-байтний PreviousTagSize. Дивись специфікацію Adobe FLV, розділ
"The FLV File Format" — тут нічого специфічного для проєкту нема,
самé декодування контейнера.
"""

import struct

FLV_HEADER = b"FLV\x01\x05\x00\x00\x00\x09" + struct.pack(">I", 0)

TAG_TYPE_AUDIO = 8
TAG_TYPE_VIDEO = 9
TAG_TYPE_SCRIPT = 18  # onMetaData: width/height/framerate/бітрейти для приймаючої сторони

_TAG_HEADER_SIZE = 11
_PREV_TAG_SIZE_SIZE = 4


def read_exact(stream, n: int) -> bytes | None:
    """Читає рівно n байт або повертає None на EOF (навіть частковому)."""
    buf = b""
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def is_video_keyframe(payload: bytes) -> bool:
    return len(payload) >= 1 and (payload[0] >> 4) == 1


def is_avc_seq_header(payload: bytes) -> bool:
    return len(payload) >= 2 and payload[1] == 0


def is_aac_seq_header(payload: bytes) -> bool:
    return len(payload) >= 2 and payload[1] == 0


def is_seq_header(tag_type: int, payload: bytes) -> bool:
    if tag_type == TAG_TYPE_VIDEO:
        return is_avc_seq_header(payload)
    if tag_type == TAG_TYPE_AUDIO:
        return is_aac_seq_header(payload)
    return False


def write_flv_tag(stream, tag_type: int, timestamp: int, payload: bytes) -> None:
    ts = timestamp & 0xFFFFFFFF
    ts_bytes = struct.pack(">I", ts)
    ts_low = ts_bytes[1:]   # 3 молодші байти timestamp
    ts_ext = ts_bytes[0:1]  # старший байт (TimestampExtended)
    header = (
        bytes([tag_type])
        + struct.pack(">I", len(payload))[1:]
        + ts_low + ts_ext
        + b"\x00\x00\x00"  # StreamID, завжди 0
    )
    stream.write(header)
    stream.write(payload)
    stream.write(struct.pack(">I", _TAG_HEADER_SIZE + len(payload)))


def read_flv_tags(stream, source: str, on_tag) -> None:
    """
    Читає FLV-теги зі stream, поки не EOF, і викликає
    on_tag(source, tag_type, timestamp, payload) для кожного
    аудіо/відео/script-data-тегу (тип 8/9/18).

    Завершується сама на EOF: коли ffmpeg-процес-джерело
    завершується/падає, ОС закриває його кінець pipe на запис, і
    read() тут одразу повертає b'' — жодного зовнішнього
    stop-сигналу не треба.
    """
    header = read_exact(stream, len(FLV_HEADER) - 4)
    if header is None or header[:3] != b"FLV":
        return
    if read_exact(stream, _PREV_TAG_SIZE_SIZE) is None:  # PreviousTagSize0
        return

    while True:
        tag_header = read_exact(stream, _TAG_HEADER_SIZE)
        if tag_header is None:
            return
        tag_type = tag_header[0]
        data_size = int.from_bytes(tag_header[1:4], "big")
        ts = int.from_bytes(tag_header[4:7], "big") | (tag_header[7] << 24)
        payload = read_exact(stream, data_size)
        if payload is None:
            return
        if read_exact(stream, _PREV_TAG_SIZE_SIZE) is None:
            return
        if tag_type in (TAG_TYPE_AUDIO, TAG_TYPE_VIDEO, TAG_TYPE_SCRIPT):
            on_tag(source, tag_type, ts, payload)
