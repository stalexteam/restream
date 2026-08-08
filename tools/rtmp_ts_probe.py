#!/usr/bin/env python3
"""
rtmp_ts_probe.py -- minimal RTMP ingest server for a single purpose:
log the ON-WIRE RTMP timestamps that OBS (main output) and the
obs-multi-rtmp plugin send, so we can decide whether the two streams
share OBS's master clock or carry independent per-connection timelines.

Why a raw server and not MediaMTX/ffmpeg: MediaMTX may re-base or
normalize timestamps on republish, and ffmpeg copies them but hides
them. Here we read the RTMP chunk timestamps directly from the socket,
before anything can touch them.

It is a DIAGNOSTIC TOOL, not part of the controller. It accepts any
publish without checking auth (we only care about timestamps).

Usage (run INSTEAD of MediaMTX -- it binds the same port 1935):
    python3 tools/rtmp_ts_probe.py            # listens on 0.0.0.0:1935

Then in OBS:
    Settings -> Stream -> Service: Custom
      Server:     rtmp://127.0.0.1:1935/live
      Stream Key: main?user=obs&pass=<whatever>
    obs-multi-rtmp output:
      URL:        rtmp://127.0.0.1:1935/live
      Key:        no-licensed-audio?user=obs&pass=<whatever>

The single most diagnostic line is "FIRST video" / "FIRST audio" per
connection: its rtmp_ts tells the story.

  * If the SECOND stream, started N seconds after the first, reports
    FIRST rtmp_ts ~= 0  -> timestamps are INDEPENDENT (each output zeroes
    at its own start). Remux sync from stream data alone is unreliable.
  * If it reports FIRST rtmp_ts ~= N*1000 ms -> the outputs SHARE OBS's
    master clock. Remux is directly synchronizable on our side.

The periodic "sample" lines let you check for DRIFT over time: for each
stream, (rtmp_ts - wall_since_publish_ms) should stay ~constant.
"""

import os
import socket
import struct
import sys
import threading
import time

# ---------------------------------------------------------------- logging

_LOG_LOCK = threading.Lock()
_T0 = time.time()  # server start, for a common wall-clock axis across streams


def log(conn, msg):
    now = time.time()
    stamp = time.strftime("%H:%M:%S", time.localtime(now)) + f".{int((now % 1) * 1000):03d}"
    axis = now - _T0
    with _LOG_LOCK:
        print(f"[{stamp}] [+{axis:8.3f}] [{conn:>18}] {msg}", flush=True)


# Registry of currently-publishing streams -> publish wall-clock, so a new
# publish can report the gap since the others' starts.
_PUBLISHES_LOCK = threading.Lock()
_PUBLISHES = {}  # name -> publish_wall

# Optional CSV sink for drift analysis: one row per media tag
# (arrival_monotonic, stream, type, rtmp_ts). Enabled by passing a csv path
# as the 2nd CLI arg. Point of this mode: resolve relative clock drift by
# slope regression over a long dense run (point-wise ts comparison is buried
# under frame quantization; see tools/drift_analyze.py).
_CSV_LOCK = threading.Lock()
_CSV = None


def csv_row(mono: float, stream: str, kind: str, ts: int) -> None:
    if _CSV is None:
        return
    with _CSV_LOCK:
        _CSV.write(f"{mono:.6f},{stream},{kind},{ts}\n")


# ------------------------------------------------------------------- amf0

def amf_number(x):
    return b"\x00" + struct.pack(">d", float(x))


def amf_bool(b):
    return b"\x01" + (b"\x01" if b else b"\x00")


def amf_string(s):
    e = s.encode("utf-8")
    return b"\x02" + struct.pack(">H", len(e)) + e


def amf_null():
    return b"\x05"


def amf_object(d):
    out = b"\x03"
    for k, v in d.items():
        ke = k.encode("utf-8")
        out += struct.pack(">H", len(ke)) + ke + v
    out += b"\x00\x00\x09"
    return out


def amf_read_value(data, i):
    """Return (value, next_index). Handles the AMF0 subset OBS uses."""
    t = data[i]
    i += 1
    if t == 0x00:  # number
        return struct.unpack(">d", data[i:i + 8])[0], i + 8
    if t == 0x01:  # boolean
        return data[i] != 0, i + 1
    if t == 0x02:  # string
        ln = struct.unpack(">H", data[i:i + 2])[0]
        i += 2
        return data[i:i + ln].decode("utf-8", "replace"), i + ln
    if t in (0x03, 0x08):  # object / ecma-array
        if t == 0x08:
            i += 4  # skip declared count -- we read until the end marker anyway
        obj = {}
        while True:
            ln = struct.unpack(">H", data[i:i + 2])[0]
            i += 2
            if ln == 0:
                i += 1  # object end marker 0x09
                break
            key = data[i:i + ln].decode("utf-8", "replace")
            i += ln
            val, i = amf_read_value(data, i)
            obj[key] = val
        return obj, i
    if t in (0x05, 0x06):  # null / undefined
        return None, i
    raise ValueError(f"unsupported AMF0 type 0x{t:02x} at {i - 1}")


def amf_read_all(data):
    vals = []
    i = 0
    while i < len(data):
        try:
            v, i = amf_read_value(data, i)
        except (ValueError, struct.error, IndexError):
            break
        vals.append(v)
    return vals


# --------------------------------------------------------------- rtmp i/o

def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed")
        buf += chunk
    return bytes(buf)


def handshake(sock):
    """Simple RTMP handshake (the nginx-rtmp style OBS accepts)."""
    c0 = recv_exact(sock, 1)  # version
    c1 = recv_exact(sock, 1536)
    s0 = b"\x03"
    s1 = b"\x00" * 8 + os.urandom(1528)
    s2 = c1  # echo C1
    sock.sendall(s0 + s1 + s2)
    recv_exact(sock, 1536)  # C2, ignored


def send_message(sock, csid, type_id, stream_id, payload, out_chunk):
    """Send one RTMP message, splitting into chunks of out_chunk bytes."""
    bh0 = bytes([(0 << 6) | csid])  # fmt 0, csid < 64
    ts3 = struct.pack(">I", 0)[1:]
    len3 = struct.pack(">I", len(payload))[1:]
    mh = ts3 + len3 + bytes([type_id]) + struct.pack("<I", stream_id)
    sock.sendall(bh0 + mh + payload[:out_chunk])
    rest = payload[out_chunk:]
    bh3 = bytes([(3 << 6) | csid])
    while rest:
        sock.sendall(bh3 + rest[:out_chunk])
        rest = rest[out_chunk:]


# ---------------------------------------------------------------- session

TYPE_SET_CHUNK_SIZE = 1
TYPE_ACK = 3
TYPE_WINDOW_ACK = 5
TYPE_SET_PEER_BW = 6
TYPE_AUDIO = 8
TYPE_VIDEO = 9
TYPE_DATA_AMF0 = 18
TYPE_CMD_AMF0 = 20

SAMPLE_EVERY_SEC = 2.0
ACK_EVERY_BYTES = 1_000_000


class Session:
    def __init__(self, sock, peer):
        self.sock = sock
        self.peer = peer
        self.name = f"{peer[0]}:{peer[1]}"  # until we learn the stream name
        self.in_chunk = 128
        self.out_chunk = 128
        self.csid = {}  # per-csid parse state
        self.total_recv = 0
        self.last_ack = 0

        self.publish_wall = None
        self.first_video = False
        self.first_audio = False
        self.last_video_ts = None
        self.last_audio_ts = None
        self._last_sample = 0.0

    # -- chunk reader -----------------------------------------------------

    def _st(self, csid):
        return self.csid.setdefault(
            csid,
            {"ts": 0, "delta": 0, "len": 0, "type": 0, "stream": 0,
             "buf": bytearray(), "remaining": 0},
        )

    def read_chunk(self):
        """Read one chunk; return a completed (type, ts, payload) or None."""
        b0 = recv_exact(self.sock, 1)[0]
        self.total_recv += 1
        fmt = b0 >> 6
        csid = b0 & 0x3F
        if csid == 0:
            csid = 64 + recv_exact(self.sock, 1)[0]
            self.total_recv += 1
        elif csid == 1:
            ext = recv_exact(self.sock, 2)
            self.total_recv += 2
            csid = 64 + ext[0] + ext[1] * 256

        st = self._st(csid)

        if fmt == 0:
            h = recv_exact(self.sock, 11)
            self.total_recv += 11
            ts = int.from_bytes(h[0:3], "big")
            mlen = int.from_bytes(h[3:6], "big")
            mtype = h[6]
            stream = int.from_bytes(h[7:11], "little")
            if ts == 0xFFFFFF:
                ts = int.from_bytes(recv_exact(self.sock, 4), "big")
                self.total_recv += 4
            st.update(ts=ts, delta=0, len=mlen, type=mtype, stream=stream)
            st["remaining"] = mlen
            st["buf"] = bytearray()
        elif fmt == 1:
            h = recv_exact(self.sock, 7)
            self.total_recv += 7
            delta = int.from_bytes(h[0:3], "big")
            mlen = int.from_bytes(h[3:6], "big")
            mtype = h[6]
            if delta == 0xFFFFFF:
                delta = int.from_bytes(recv_exact(self.sock, 4), "big")
                self.total_recv += 4
            st["ts"] += delta
            st.update(delta=delta, len=mlen, type=mtype)
            st["remaining"] = mlen
            st["buf"] = bytearray()
        elif fmt == 2:
            h = recv_exact(self.sock, 3)
            self.total_recv += 3
            delta = int.from_bytes(h[0:3], "big")
            if delta == 0xFFFFFF:
                delta = int.from_bytes(recv_exact(self.sock, 4), "big")
                self.total_recv += 4
            st["ts"] += delta
            st["delta"] = delta
            st["remaining"] = st["len"]
            st["buf"] = bytearray()
        else:  # fmt == 3
            if st["remaining"] == 0:
                # New message repeating the previous header -> advance by the
                # last delta. (Extended-timestamp on fmt3 is ignored: it only
                # matters past ~4.6h of runtime, irrelevant for this probe.)
                st["ts"] += st["delta"]
                st["remaining"] = st["len"]
                st["buf"] = bytearray()
            # else: continuation of the current message, ts unchanged.

        want = min(st["remaining"], self.in_chunk)
        data = recv_exact(self.sock, want)
        self.total_recv += want
        st["buf"] += data
        st["remaining"] -= want

        self._maybe_ack()

        if st["remaining"] == 0 and st["len"] > 0:
            return st["type"], st["ts"], bytes(st["buf"])
        return None

    def _maybe_ack(self):
        if self.total_recv - self.last_ack >= ACK_EVERY_BYTES:
            self.last_ack = self.total_recv
            send_message(self.sock, 2, TYPE_ACK, 0,
                         struct.pack(">I", self.total_recv & 0xFFFFFFFF),
                         self.out_chunk)

    # -- message handling -------------------------------------------------

    def handle(self, mtype, ts, payload):
        if mtype == TYPE_SET_CHUNK_SIZE:
            self.in_chunk = int.from_bytes(payload[:4], "big")
            log(self.name, f"peer set chunk size -> {self.in_chunk}")
        elif mtype == TYPE_CMD_AMF0:
            self._handle_command(payload)
        elif mtype == TYPE_DATA_AMF0:
            self._handle_data(payload)
        elif mtype == TYPE_AUDIO:
            self._on_media("audio", ts)
        elif mtype == TYPE_VIDEO:
            self._on_media("video", ts)
        # control messages from peer (ack, window ack) -> ignore

    def _handle_command(self, payload):
        vals = amf_read_all(payload)
        if not vals:
            return
        cmd = vals[0]
        transid = vals[1] if len(vals) > 1 and isinstance(vals[1], float) else 0.0
        if cmd == "connect":
            app = ""
            if len(vals) > 2 and isinstance(vals[2], dict):
                app = vals[2].get("app", "")
            log(self.name, f"connect  app={app!r}")
            self._reply_connect(transid)
        elif cmd == "createStream":
            send_message(self.sock, 3, TYPE_CMD_AMF0, 0,
                         amf_string("_result") + amf_number(transid) +
                         amf_null() + amf_number(1.0), self.out_chunk)
        elif cmd == "publish":
            stream_name = ""
            for v in vals[3:]:
                if isinstance(v, str):
                    stream_name = v
                    break
            self._on_publish(stream_name)
        elif cmd in ("releaseStream", "FCPublish", "FCUnpublish", "deleteStream"):
            pass  # OBS is happy without explicit replies to these

    def _reply_connect(self, transid):
        send_message(self.sock, 2, TYPE_WINDOW_ACK, 0,
                     struct.pack(">I", 250_000_000), self.out_chunk)
        send_message(self.sock, 2, TYPE_SET_PEER_BW, 0,
                     struct.pack(">I", 250_000_000) + bytes([2]), self.out_chunk)
        send_message(self.sock, 2, TYPE_SET_CHUNK_SIZE, 0,
                     struct.pack(">I", 4096), self.out_chunk)
        self.out_chunk = 4096
        props = amf_object({"fmsVer": amf_string("FMS/3,0,1,123"),
                            "capabilities": amf_number(31)})
        info = amf_object({"level": amf_string("status"),
                           "code": amf_string("NetConnection.Connect.Success"),
                           "description": amf_string("Connection succeeded.")})
        send_message(self.sock, 3, TYPE_CMD_AMF0, 0,
                     amf_string("_result") + amf_number(transid) + props + info,
                     self.out_chunk)

    def _on_publish(self, stream_name):
        clean = stream_name.split("?", 1)[0] or stream_name
        self.name = clean or self.name
        self.publish_wall = time.time()

        # Report the gap versus every other stream already publishing.
        with _PUBLISHES_LOCK:
            others = dict(_PUBLISHES)
            _PUBLISHES[self.name] = self.publish_wall
        if others:
            gaps = ", ".join(
                f"{n}:+{self.publish_wall - t:.3f}s" for n, t in others.items())
            log(self.name, f"PUBLISH start (key={stream_name!r})  "
                           f"gap since other publishes -> {gaps}")
        else:
            log(self.name, f"PUBLISH start (key={stream_name!r})  "
                           f"(first stream publishing)")

        send_message(self.sock, 5, TYPE_CMD_AMF0, 1,
                     amf_string("onStatus") + amf_number(0) + amf_null() +
                     amf_object({"level": amf_string("status"),
                                 "code": amf_string("NetStream.Publish.Start"),
                                 "description": amf_string("Start publishing")}),
                     self.out_chunk)

    def _handle_data(self, payload):
        vals = amf_read_all(payload)
        meta = next((v for v in vals if isinstance(v, dict)), None)
        if meta:
            keys = ("width", "height", "framerate", "fps", "videocodecid",
                    "audiosamplerate", "audiocodecid", "videodatarate",
                    "audiodatarate")
            shown = {k: meta[k] for k in keys if k in meta}
            log(self.name, f"onMetaData {shown}")

    def _on_media(self, kind, ts):
        csv_row(time.monotonic(), self.name, kind, ts)
        since = (time.time() - self.publish_wall) if self.publish_wall else 0.0
        if kind == "video":
            self.last_video_ts = ts
            if not self.first_video:
                self.first_video = True
                log(self.name, f">>> FIRST video  rtmp_ts={ts:>8} ms   "
                               f"wall_since_publish={since:6.3f}s   "
                               f"<-- key number: 0 => independent, "
                               f"~gap => shared clock")
        else:
            self.last_audio_ts = ts
            if not self.first_audio:
                self.first_audio = True
                log(self.name, f">>> FIRST audio  rtmp_ts={ts:>8} ms   "
                               f"wall_since_publish={since:6.3f}s")

        now = time.time()
        if now - self._last_sample >= SAMPLE_EVERY_SEC and self.publish_wall:
            self._last_sample = now
            v = self.last_video_ts if self.last_video_ts is not None else -1
            a = self.last_audio_ts if self.last_audio_ts is not None else -1
            wall_ms = since * 1000.0
            # For a stream that zeroed at its own publish, (ts - wall_ms)
            # stays ~const (no drift). Compare this offset between streams.
            log(self.name, f"sample  video_ts={v:>8}  audio_ts={a:>8}  "
                           f"wall_since_publish={wall_ms:8.0f}ms  "
                           f"a/v_skew={ (a - v) if (v>=0 and a>=0) else 0:>6}ms  "
                           f"ts-wall(video)={ (v - wall_ms):8.0f}ms")

    # -- lifecycle --------------------------------------------------------

    def run(self):
        try:
            handshake(self.sock)
            log(self.name, "handshake ok")
            while True:
                msg = self.read_chunk()
                if msg is not None:
                    self.handle(*msg)
        except (ConnectionError, OSError) as e:
            log(self.name, f"disconnected ({e})")
        finally:
            if self.publish_wall is not None:
                dur = time.time() - self.publish_wall
                log(self.name, f"PUBLISH end   duration={dur:.3f}s   "
                               f"last video_ts={self.last_video_ts}  "
                               f"last audio_ts={self.last_audio_ts}")
            with _PUBLISHES_LOCK:
                if _PUBLISHES.get(self.name) == self.publish_wall:
                    _PUBLISHES.pop(self.name, None)
            try:
                self.sock.close()
            except OSError:
                pass


def main():
    global _CSV
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 1935
    if len(sys.argv) > 2:
        _CSV = open(sys.argv[2], "w", buffering=1)
        _CSV.write("arrival_monotonic,stream,type,rtmp_ts\n")
        log("server", f"CSV drift log -> {sys.argv[2]} (one row per media tag)")
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(8)
    log("server", f"listening on 0.0.0.0:{port}  (start OBS main first, then "
                  f"the plugin a few seconds later)")
    try:
        while True:
            sock, peer = srv.accept()
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            threading.Thread(target=Session(sock, peer).run, daemon=True).start()
    except KeyboardInterrupt:
        log("server", "shutting down")
    finally:
        srv.close()


if __name__ == "__main__":
    main()
