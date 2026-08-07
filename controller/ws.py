"""
Мінімальна серверна реалізація WebSocket (RFC 6455), stdlib-only —
лише те, що потрібно дашборду: хендшейк, текстові фрейми в обидва
боки, PING/PONG, CLOSE. Без фрагментації (наші повідомлення — короткий
JSON, влазять в один фрейм) і без розширень (compression тощо).

Свідомо працюємо через `handler.rfile`/`handler.wfile`
(`BaseHTTPRequestHandler`), а не напряму через сокет: `rfile` —
буферизований читач, і паралельний сирий `socket.recv()` ризикує
загубити байти, які вже осіли в його внутрішньому буфері.
"""

import base64
import hashlib
import struct

_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

OPCODE_CONTINUATION = 0x0
OPCODE_TEXT = 0x1
OPCODE_BINARY = 0x2
OPCODE_CLOSE = 0x8
OPCODE_PING = 0x9
OPCODE_PONG = 0xA

# Стеля розміру вхідного фрейму. Наші клієнти шлють лише крихітний JSON
# (команди дашборда); фрейм більший за це -- зловживання (клієнт оголошує
# довжину до 2^64 і змушує сервер прочитати її в пам'ять + XOR-розмаскувати).
# Понад ліміт -- розриваємо з'єднання, а не виділяємо пам'ять.
_MAX_FRAME_PAYLOAD = 1 << 20  # 1 MiB


def handshake(handler) -> bool:
    """
    Перевіряє upgrade-заголовки й шле відповідь 101 через
    `handler.wfile`. Повертає False (без запису), якщо заголовки не
    схожі на WebSocket-хендшейк — виклик коду сам вирішує, як
    відповісти (звичайний HTTP-код помилки).
    """
    upgrade = handler.headers.get("Upgrade", "").lower()
    connection = handler.headers.get("Connection", "").lower()
    key = handler.headers.get("Sec-WebSocket-Key")
    if upgrade != "websocket" or "upgrade" not in connection or not key:
        return False

    accept = base64.b64encode(hashlib.sha1((key + _WS_GUID).encode("ascii")).digest()).decode("ascii")
    response = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\n"
        "\r\n"
    )
    handler.wfile.write(response.encode("ascii"))
    handler.wfile.flush()
    return True


def _write_frame(handler, lock, opcode: int, payload: bytes) -> None:
    length = len(payload)
    if length < 126:
        header = struct.pack("!BB", 0x80 | opcode, length)
    elif length < 65536:
        header = struct.pack("!BBH", 0x80 | opcode, 126, length)
    else:
        header = struct.pack("!BBQ", 0x80 | opcode, 127, length)

    with lock:
        handler.wfile.write(header + payload)
        handler.wfile.flush()


def send_text(handler, text: str, lock) -> None:
    """Сервер -> клієнт, неmasked фрейм (RFC 6455 вимагає це саме так)."""
    _write_frame(handler, lock, OPCODE_TEXT, text.encode("utf-8"))


def send_pong(handler, payload: bytes, lock) -> None:
    _write_frame(handler, lock, OPCODE_PONG, payload)


def send_close(handler, lock) -> None:
    try:
        _write_frame(handler, lock, OPCODE_CLOSE, b"")
    except OSError:
        pass


def recv_frame(handler) -> tuple[int, bytes] | None:
    """
    Читає один фрейм від клієнта через `handler.rfile`. `None` —
    з'єднання закрито (EOF на першому байті заголовка).

    Викликач мусить спершу перевірити `select()` на сирому сокеті, що
    дані вже є -- НЕ покладатись на `socket.settimeout()` для переривання
    саме цього читання: повторний тайм-аут на тому самому
    `io.BufferedReader` псує його внутрішній стан (наступний `read()`
    кидає "cannot read from timed out object").
    """
    header = handler.rfile.read(2)
    if not header or len(header) < 2:
        return None

    first, second = header[0], header[1]
    opcode = first & 0x0F
    masked = bool(second & 0x80)
    length = second & 0x7F

    if length == 126:
        length = struct.unpack("!H", handler.rfile.read(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", handler.rfile.read(8))[0]

    # Завеликий фрейм -- не читаємо payload у пам'ять, сигналимо закриття
    # (викликач трактує None як закрите з'єднання й розриває клієнта).
    if length > _MAX_FRAME_PAYLOAD:
        return None

    mask_key = handler.rfile.read(4) if masked else b""

    payload = handler.rfile.read(length) if length else b""
    if masked and payload:
        payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))

    return opcode, payload
