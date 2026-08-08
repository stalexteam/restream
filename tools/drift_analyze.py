#!/usr/bin/env python3
"""
drift_analyze.py -- resolve RELATIVE clock drift between the two OBS outputs
from a dense per-tag CSV produced by rtmp_ts_probe.py in CSV mode
(`rtmp_ts_probe.py <port> <csv>`).

Why not compare timestamps point-wise: each rtmp_ts is quantized to its frame
cadence (~16 ms video@60, ~21 ms AAC), so a point-wise cross-stream skew has
tens of ms of noise -- it cannot distinguish 0 drift from ~1 ms/s. Fitting a
slope `rtmp_ts(ms) ~ arrival(s)` over a long dense run averages that noise down
as ~noise/(span*sqrt(N)); the relative drift is then the DIFFERENCE of the two
streams' slopes (the server's own wall-clock rate cancels in the difference).

Handles reconnects: each (re)publish restarts rtmp_ts at 0, so a (stream,type)
series is split into segments at ts resets / arrival gaps; the longest segment
per series is used, with the first seconds dropped (encoder warmup bias).

Usage:
    python3 tools/drift_analyze.py <csv_path> [--warmup SEC] [--min-overlap SEC]

Interpretation:
    relative drift = slope(video source) - slope(audio source), in ms per wall
    second. Over a stream of length H hours the accumulated A/V desync a fixed
    audio_offset cannot fix is  drift_ms_per_s * 3600 * H.
    Gate example: keep desync < 50 ms over 6 h  =>  |drift| < ~0.0023 ms/s (~2 ppm).
"""

import bisect
import math
import statistics
import sys

RESET_DROP_MS = 1000     # ts going back by more than this => new segment (reconnect)
GAP_SEC = 3.0            # arrival gap larger than this => new segment
MIN_POINTS = 100


def ols(xs, ys):
    n = len(xs)
    sx = sum(xs); sy = sum(ys)
    sxx = sum(x * x for x in xs); sxy = sum(x * y for x, y in zip(xs, ys))
    d = n * sxx - sx * sx
    if d == 0:
        return 0.0, 0.0
    slope = (n * sxy - sx * sy) / d
    inter = (sy - slope * sx) / n
    return slope, inter


def fit_robust(pts):
    """pts: list of (t_sec, ts_ms). Returns (slope_ms_per_s, resid_std_ms, n)."""
    xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
    slope, inter = ols(xs, ys)
    res = [y - (slope * x + inter) for x, y in zip(xs, ys)]
    sd = statistics.pstdev(res) if len(res) > 2 else 0.0
    if sd > 0:
        keep = [(x, y) for x, y, r in zip(xs, ys, res) if abs(r) <= 5 * sd]
        if 2 < len(keep) < len(xs):
            xs = [p[0] for p in keep]; ys = [p[1] for p in keep]
            slope, inter = ols(xs, ys)
            res = [y - (slope * x + inter) for x, y in zip(xs, ys)]
            sd = statistics.pstdev(res) if len(res) > 2 else 0.0
    return slope, sd, len(xs)


def make_interp(seg):
    """seg: sorted [(t, ts)] -> f(t)->ts (linear interp), plus [t0, t1]."""
    ts_at = [p[0] for p in seg]
    val = [p[1] for p in seg]

    def f(tq):
        if tq <= ts_at[0]:
            return val[0]
        if tq >= ts_at[-1]:
            return val[-1]
        i = bisect.bisect_right(ts_at, tq)
        t0, t1 = ts_at[i - 1], ts_at[i]
        v0, v1 = val[i - 1], val[i]
        return v0 if t1 == t0 else v0 + (v1 - v0) * (tq - t0) / (t1 - t0)

    return f, ts_at[0], ts_at[-1]


def offset_series(vseg, aseg, bucket):
    """
    Direct offset(t) = ts_audio - ts_video (video interpolated at each audio
    arrival). Fit the secular trend (accumulating drift) with its uncertainty,
    then bucket-average the residual to reveal bounded low-frequency wander
    (does the offset swing +/- periodically?). Bucket-averaging kills frame
    quantization, so ~13 ms wander is resolvable.
    """
    fV, v0, v1 = make_interp(vseg)
    lo, hi = max(v0, aseg[0][0]), min(v1, aseg[-1][0])
    xs, ys = [], []
    for t, ts in aseg:
        if lo <= t <= hi:
            xs.append(t - lo)
            ys.append(ts - fV(t))
    if len(xs) < 50:
        return None
    slope, inter = ols(xs, ys)                       # ms per s = secular drift
    resid = [y - (slope * x + inter) for x, y in zip(xs, ys)]
    sd = statistics.pstdev(resid)
    xmean = statistics.fmean(xs)
    ssx = sum((x - xmean) ** 2 for x in xs)
    se = sd / math.sqrt(ssx) if ssx > 0 else 0.0     # ms/s std error of slope
    buckets = {}
    for x, r in zip(xs, resid):
        buckets.setdefault(int(x // bucket), []).append(r)
    series = [(b * bucket, statistics.fmean(v)) for b, v in sorted(buckets.items())]
    return slope, se, series, sd, xs[-1]


def segment(points):
    """points sorted by arrival -> list of segments split at ts reset / gap."""
    segs = []; cur = []
    for t, ts in points:
        if cur:
            pt, pts = cur[-1]
            if ts < pts - RESET_DROP_MS or t - pt > GAP_SEC:
                segs.append(cur); cur = []
        cur.append((t, ts))
    if cur:
        segs.append(cur)
    return segs


def longest_segment(points, warmup):
    best = None; best_span = -1.0
    for seg in segment(points):
        seg = [(t, ts) for t, ts in seg if t >= seg[0][0] + warmup]
        if len(seg) < MIN_POINTS:
            continue
        span = seg[-1][0] - seg[0][0]
        if span > best_span:
            best_span = span; best = seg
    return best


def main():
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(1)
    csv_path = sys.argv[1]
    warmup = 10.0
    min_overlap = 300.0
    offset_bucket = None
    args = sys.argv[2:]
    for i, a in enumerate(args):
        if a == "--warmup" and i + 1 < len(args):
            warmup = float(args[i + 1])
        elif a == "--min-overlap" and i + 1 < len(args):
            min_overlap = float(args[i + 1])
        elif a == "--offset-series":
            offset_bucket = 30.0
            if i + 1 < len(args):
                try:
                    offset_bucket = float(args[i + 1])
                except ValueError:
                    pass

    groups = {}  # (stream, type) -> list[(t, ts)]
    with open(csv_path) as f:
        header = f.readline()
        for line in f:
            parts = line.rstrip("\n").split(",")
            if len(parts) != 4:
                continue
            t, stream, kind, ts = parts
            try:
                groups.setdefault((stream, kind), []).append((float(t), int(ts)))
            except ValueError:
                continue

    # Per-series slope over its longest segment.
    fits = {}  # key -> (slope, sd, n, seg)
    print("=== per-series slope (longest segment, warmup-trimmed) ===")
    print(f"{'stream':>20} {'type':>6} {'N':>8} {'span_s':>8} "
          f"{'slope_ms/s':>12} {'dev_ppm':>9} {'resid_ms':>9}")
    for key in sorted(groups):
        pts = sorted(groups[key])
        seg = longest_segment(pts, warmup)
        if seg is None:
            print(f"{key[0]:>20} {key[1]:>6}   (no segment with >= {MIN_POINTS} pts)")
            continue
        slope, sd, n = fit_robust(seg)
        span = seg[-1][0] - seg[0][0]
        dev_ppm = (slope / 1000.0 - 1.0) * 1e6  # vs ideal 1000 ms per s
        fits[key] = (slope, sd, n, seg)
        print(f"{key[0]:>20} {key[1]:>6} {n:>8} {span:>8.0f} "
              f"{slope:>12.4f} {dev_ppm:>9.1f} {sd:>9.1f}")

    # Measurement floor from internal A/V: a single OBS output keeps its own
    # video and audio in sync (true internal drift ~= 0), so whatever we measure
    # there IS the method's noise floor (arrival jitter / bursty video vs steady
    # audio / buffer breathing). ms per wall second.
    internal = {}  # stream -> drift ms/s
    for s in sorted({k[0] for k in fits}):
        if (s, "video") in fits and (s, "audio") in fits:
            internal[s] = fits[(s, "video")][0] - fits[(s, "audio")][0]
    floor_ppm = max((abs(v) / 1000.0 * 1e6 for v in internal.values()), default=0.0)

    # Relative drift: video-series vs audio-series of a DIFFERENT stream
    # (the remux case: video from one, audio from the other).
    print("\n=== relative drift (video source vs audio source) ===")
    vids = [k for k in fits if k[1] == "video"]
    auds = [k for k in fits if k[1] == "audio"]
    any_pair = False
    for vk in vids:
        for ak in auds:
            if vk[0] == ak[0]:
                continue
            any_pair = True
            vs = fits[vk]; as_ = fits[ak]
            ov0 = max(vs[3][0][0], as_[3][0][0])
            ov1 = min(vs[3][-1][0], as_[3][-1][0])
            overlap = max(0.0, ov1 - ov0)
            drift = vs[0] - as_[0]            # ms per wall second
            ppm = drift / 1000.0 * 1e6
            per_hour = drift * 3600.0
            per_6h = drift * 3600.0 * 6.0 / 1000.0
            notes = []
            if overlap < min_overlap:
                notes.append("LOW OVERLAP")
            if floor_ppm and abs(ppm) <= 2 * floor_ppm:
                notes.append(f"within ~2x floor (~{floor_ppm:.0f}ppm): "
                             f"indistinguishable from 0, true |drift| <= floor")
            note = ("  <-- " + "; ".join(notes)) if notes else ""
            print(f"video[{vk[0]}] - audio[{ak[0]}]: "
                  f"drift = {drift:+.5f} ms/s  ({ppm:+.2f} ppm)  "
                  f"= {per_hour:+.1f} ms/h  = {per_6h:+.2f} s over 6h   "
                  f"[overlap {overlap:.0f}s]{note}")
    if not any_pair:
        print("  (need one 'video' series and one 'audio' series from different "
              "streams -- run main + clean concurrently)")

    # Sanity / floor.
    print("\n=== sanity: internal video-vs-audio drift (true ~=0 => this IS the floor) ===")
    for s, drift in internal.items():
        print(f"{s}: internal drift = {drift:+.5f} ms/s "
              f"({drift * 3600:+.1f} ms/h, {drift / 1000.0 * 1e6:+.1f} ppm)")
    if internal:
        print(f"=> measurement floor ~= {floor_ppm:.0f} ppm. Goal is to EXCLUDE gross "
              f"drift (~1000 ppm = 1 ms/s), NOT to prove 0.\n"
              f"   Residual below/near floor is handled by slow re-anchoring "
              f"(remux_reanchor).")

    # Optional: does the offset swing +/- over time (bounded wander) vs
    # accumulate (secular drift)? Direct offset(t), detrended, bucket-averaged.
    if offset_bucket:
        print(f"\n=== offset(t) wander: audio - video, {offset_bucket:.0f}s buckets ===")
        best = None; best_ov = -1.0
        for vk in vids:
            for ak in auds:
                if vk[0] == ak[0]:
                    continue
                vseg = fits[vk][3]; aseg = fits[ak][3]
                ov = min(vseg[-1][0], aseg[-1][0]) - max(vseg[0][0], aseg[0][0])
                if ov > best_ov:
                    best_ov = ov; best = (vk, ak)
        if best is None:
            print("  (need video + audio from different streams)")
        else:
            vk, ak = best
            r = offset_series(fits[vk][3], fits[ak][3], offset_bucket)
            if r is None:
                print("  (not enough overlapping samples)")
            else:
                slope, se, series, sd, span = r
                sec_ppm = slope / 1000.0 * 1e6
                vals = [v for _, v in series]
                lo, hi = min(vals), max(vals)
                amp = hi - lo
                sec_change = abs(slope * span)  # ms accumulated over the window
                # Formal OLS se assumes independent residuals; the low-frequency
                # wander makes them autocorrelated, so se is over-optimistic.
                # Honest test: is the secular accumulation over the window bigger
                # than the wander amplitude? If not, they can't be separated here.
                if sec_change < amp:
                    verdict = ("secular NOT separable from wander on this window "
                               "(need a run >> wander period); both tiny")
                else:
                    verdict = "secular component exceeds wander (likely real accumulation)"
                print(f"  pair: audio[{ak[0]}] - video[{vk[0]}], span {span:.0f}s, "
                      f"resid_sd {sd:.1f} ms")
                print(f"  secular drift = {sec_ppm:+.2f} ppm ({slope * 3600:+.1f} ms/h, "
                      f"= {sec_change:.1f} ms over this window)  -> {verdict}")
                print(f"  bucketed wander amplitude = {amp:.1f} ms "
                      f"[{lo:+.1f} .. {hi:+.1f}]  (sign flips => oscillation)")
                width = 46
                rng = (hi - lo) or 1.0
                zero = int((0 - lo) / rng * (width - 1))
                for t, v in series:
                    pos = int((v - lo) / rng * (width - 1))
                    row = [" "] * width
                    if 0 <= zero < width:
                        row[zero] = "|"
                    row[pos] = "#"
                    print(f"   t={t:6.0f}s {v:+7.2f} ms {''.join(row)}")


if __name__ == "__main__":
    main()
