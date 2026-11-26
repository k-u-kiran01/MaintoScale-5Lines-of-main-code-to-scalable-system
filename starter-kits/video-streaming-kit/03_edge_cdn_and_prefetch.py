"""Beat 4 & Beat 5: Global Edge CDN caching + +2 Chunk Prefetch Buffer.

Connects to Redis at localhost:6379, or automatically falls back to in-memory
fakeredis if Redis is not running (zero Docker required).

Run:
    python 03_edge_cdn_and_prefetch.py
"""

from __future__ import annotations

import sys
from collections import deque

import fakeredis
import redis

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

CACHE_CONTROL_HEADER = "Cache-Control: public, max-age=86400"
EDGE_TTL_SECONDS = 86400


def get_redis_client() -> tuple[redis.Redis, str]:
    client = redis.Redis(host="localhost", port=6379, decode_responses=True, socket_connect_timeout=0.5)
    try:
        client.ping()
        return client, "redis://localhost:6379"
    except redis.RedisError:
        print("[Redis] localhost:6379 not reachable -> using in-memory fakeredis (no Docker needed!)")
        return fakeredis.FakeRedis(decode_responses=True), "fakeredis (in-memory)"


def demo_global_edge_cdn() -> None:
    print("\n=== Beat 4: Global Edge CDN (Cache-Control: public, max-age=86400) ===\n")
    r, mode = get_redis_client()
    print(f"Edge store backend: {mode}")

    # Clear sample keys so counts stay deterministic across runs
    for pop in ("us", "eu", "asia"):
        for seg in ("seg_001.ts", "seg_002.ts", "seg_003.ts"):
            r.delete(f"cdn:{pop}:720p:{seg}")

    pops = ("us", "eu", "asia")
    hot_segments = ("seg_001.ts", "seg_002.ts", "seg_003.ts")

    # 999 worldwide requests across US / EU / ASIA for the same 3 hit chunks
    origin_hits = 0
    edge_hits = 0
    total_latency_ms = 0.0

    for req_idx in range(999):
        pop = pops[req_idx % len(pops)]
        seg = hot_segments[(req_idx // len(pops)) % len(hot_segments)]
        cache_key = f"cdn:{pop}:720p:{seg}"

        if r.get(cache_key) is not None:
            edge_hits += 1
            total_latency_ms += 14.0  # Local Edge POP round-trip
        else:
            origin_hits += 1
            total_latency_ms += 860.0  # Cross-ocean origin fetch
            r.set(cache_key, f"BYTES({seg})", ex=EDGE_TTL_SECONDS)

    total_reqs = origin_hits + edge_hits
    hit_rate = (edge_hits / total_reqs) * 100.0
    avg_latency = total_latency_ms / total_reqs

    print(f"Policy applied        : {CACHE_CONTROL_HEADER}")
    print(f"Worldwide requests    : {total_reqs:,} across US / EU / ASIA")
    print(f"Origin-only baseline  : {total_reqs:,} origin hits | avg latency ~860.0 ms")
    print(
        f"With Global Edge CDN  : {origin_hits} origin fills | {edge_hits:,} edge hits "
        f"({hit_rate:.1f}% HIT) | avg latency {avg_latency:.1f} ms\n"
    )


def demo_prefetch_playhead_buffer() -> None:
    print("=== Beat 5: +2 Chunk Prefetch Buffer Across a Dead Zone ===\n")

    segs = [f"seg_{i:03d}.ts" for i in range(1, 10)]
    # Signal schedule per 4-second playback step (step 3 & 4 enter a 6-8s dead zone)
    signal_up = [True, True, False, False, True, True]

    def fetch(seg_name: str, online: bool) -> str | None:
        return seg_name if online else None

    # 1) Without prefetch: fetches only on-demand right when playhead needs it
    no_prefetch_stalls = 0
    for step, online in enumerate(signal_up):
        seg = fetch(segs[step], online)
        if seg is None:
            no_prefetch_stalls += 1

    # 2) With exact Beat 5 +2 prefetch loop
    buffer: deque[str] = deque()
    i = 0
    # Warm buffer before playback starts: holds seg_001.ts, seg_002.ts
    buffer.append(segs[0])
    buffer.append(segs[1])
    i = 1

    played: list[str] = []
    prefetch_stalls = 0

    for step, online in enumerate(signal_up, start=1):
        # --- Exact Beat 5 prefetch pattern ---
        while len(buffer) < 3 and i + 1 < len(segs):
            if not online:
                break
            next_seg = fetch(segs[i + 1], online)
            if next_seg is None:
                break
            buffer.append(next_seg)
            i += 1

        if buffer:
            current = buffer.popleft()
            played.append(current)
            state_str = f"PLAY {current} | buffer ahead: {list(buffer)}"
        else:
            prefetch_stalls += 1
            state_str = "STALL (buffer empty)"

        sig_label = "ONLINE   " if online else "DEAD ZONE"
        print(f"Step {step} [{sig_label}] -> {state_str}")

    print(
        f"\nDead-zone summary: No-Prefetch stalled {no_prefetch_stalls} steps | "
        f"+2 Prefetch Buffer stalled {prefetch_stalls} steps (0 ms screen freeze!).\n"
    )


if __name__ == "__main__":
    demo_global_edge_cdn()
    demo_prefetch_playhead_buffer()
