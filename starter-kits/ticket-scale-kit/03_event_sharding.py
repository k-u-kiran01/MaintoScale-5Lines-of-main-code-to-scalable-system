"""Beat 4: Event-ID Sharding Across 8 Postgres Shards & Redis Cluster Hash Tags.

Demonstrates:
- Single shared node: One massive stadium drop (Event A) pegs Redis CPU and
  primary disk I/O at 100%, dragging down unrelated shows (Event B & Event C).
- 8 Event-ID Shards (`shard.py`):
    shard = db_shards[event_id % 8]
    rkey = '{%d}:seats' % event_id
    return shard, redis.get(rkey)
  plus CRC16 Redis Cluster slot verification proving `{event_id}:*` keys stay
  co-located on the same Redis shard slot with zero noisy-neighbor contention.

Run:
    python 03_event_sharding.py
"""

from __future__ import annotations

import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import fakeredis
import redis
from sqlalchemy import create_engine, text

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

NUM_SHARDS = 8
EVENT_A = 101  # Mega stadium drop (90% of load)
EVENT_B = 102  # Comedy theater show
EVENT_C = 103  # Indie concert


def connect_sync_redis() -> tuple[redis.Redis, str]:
    """Connect to localhost:6379, or fall back cleanly to in-memory fakeredis."""
    client = redis.Redis(host="localhost", port=6379, decode_responses=True, socket_connect_timeout=0.5)
    try:
        client.ping()
        return client, "redis://localhost:6379"
    except (OSError, redis.RedisError):
        client.close()
        return fakeredis.FakeRedis(decode_responses=True), "fakeredis (in-memory)"


def crc16_ccitt(data: bytes) -> int:
    """Official Redis Cluster CRC16-CCITT (XMODEM) implementation."""
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


def redis_cluster_slot(key: str) -> int:
    """Compute Redis Cluster slot (0..16383), honoring `{hash_tag}` co-location."""
    start = key.find("{")
    if start != -1:
        end = key.find("}", start + 1)
        if end != -1 and end > start + 1:
            return crc16_ccitt(key[start + 1 : end].encode("utf-8")) % 16384
    return crc16_ccitt(key.encode("utf-8")) % 16384


def verify_hash_tag_colocation() -> None:
    print("[Redis Cluster Slot Verification]")
    for label, eid in (("Event A", EVENT_A), ("Event B", EVENT_B), ("Event C", EVENT_C)):
        k_seats = "{%d}:seats" % eid
        k_locks = "{%d}:locks" % eid
        k_queue = "{%d}:queue" % eid
        s1 = redis_cluster_slot(k_seats)
        s2 = redis_cluster_slot(k_locks)
        s3 = redis_cluster_slot(k_queue)
        assert s1 == s2 == s3
        print(f"  {label} (event_id={eid}) -> DB shard #{eid % NUM_SHARDS} | Redis keys {k_seats}, {k_locks} -> SAME slot #{s1}")
    print()


def percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    idx = min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[idx]


def build_interleaved_workload() -> list[int]:
    """Mix Event B & Event C arrivals throughout Event A's stadium spike."""
    workload: list[int] = []
    for i in range(20):
        workload.extend([EVENT_A] * 8)
        workload.append(EVENT_B if i % 2 == 0 else EVENT_C)
    return workload


def run_monolith_benchmark(r: redis.Redis, tmp: Path) -> dict[int, float]:
    """Before: One shared DB + one serialized hot queue pegged by Event A."""
    shared_db = create_engine(f"sqlite:///{tmp / 'shared.db'}", connect_args={"check_same_thread": False})
    with shared_db.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS event_seats (event_id INTEGER PRIMARY KEY, remaining INTEGER)"))
        for eid in (EVENT_A, EVENT_B, EVENT_C):
            conn.execute(text("INSERT INTO event_seats VALUES (:e, 50000)"), {"e": eid})
    shared_gate = threading.Semaphore(1)  # Single shared Postgres + single-node Redis contention

    latencies: dict[int, list[float]] = {EVENT_A: [], EVENT_B: [], EVENT_C: []}
    lat_lock = threading.Lock()
    workload = build_interleaved_workload()
    burst_start = time.perf_counter()

    def req(eid: int) -> None:
        with shared_gate:
            r.get(f"shared:{eid}:seats")
            with shared_db.connect() as conn:
                conn.execute(text("SELECT remaining FROM event_seats WHERE event_id = :e"), {"e": eid}).scalar()
            time.sleep(0.003)
        dt = (time.perf_counter() - burst_start) * 1000.0
        with lat_lock:
            latencies[eid].append(dt)

    with ThreadPoolExecutor(max_workers=len(workload)) as pool:
        list(pool.map(req, workload))
    shared_db.dispose()

    return {eid: percentile(vals, 95) for eid, vals in latencies.items()}


def run_sharded_benchmark(r: redis.Redis, tmp: Path) -> dict[int, float]:
    """After: 8 isolated DB shards (`db_shards[event_id % 8]`) + Redis hash-tagged slices."""
    db_shards = [
        create_engine(f"sqlite:///{tmp / f'shard_{s}.db'}", connect_args={"check_same_thread": False})
        for s in range(NUM_SHARDS)
    ]
    shard_gates = [threading.Semaphore(1) for _ in range(NUM_SHARDS)]

    for eng in db_shards:
        with eng.begin() as conn:
            conn.execute(text("CREATE TABLE IF NOT EXISTS event_seats (event_id INTEGER PRIMARY KEY, remaining INTEGER)"))
    for eid in (EVENT_A, EVENT_B, EVENT_C):
        r.set("{%d}:seats" % eid, "50000")
        with db_shards[eid % NUM_SHARDS].begin() as conn:
            conn.execute(text("INSERT INTO event_seats VALUES (:e, 50000)"), {"e": eid})

    # --- Exact Beat 4 (`shard.py`) route function --------------------------------------------
    def route(event_id: int):
        shard = db_shards[event_id % 8]
        rkey = "{%d}:seats" % event_id
        return shard, r.get(rkey)

    latencies: dict[int, list[float]] = {EVENT_A: [], EVENT_B: [], EVENT_C: []}
    lat_lock = threading.Lock()
    workload = build_interleaved_workload()
    burst_start = time.perf_counter()

    def req(eid: int) -> None:
        gate = shard_gates[eid % NUM_SHARDS]
        with gate:
            shard, cached_seats = route(eid)
            with shard.connect() as conn:
                _ = conn.execute(text("SELECT remaining FROM event_seats WHERE event_id = :e"), {"e": eid}).scalar()
            _ = cached_seats
            time.sleep(0.003)
        dt = (time.perf_counter() - burst_start) * 1000.0
        with lat_lock:
            latencies[eid].append(dt)

    with ThreadPoolExecutor(max_workers=len(workload)) as pool:
        list(pool.map(req, workload))

    for eng in db_shards:
        eng.dispose()

    return {eid: percentile(vals, 95) for eid, vals in latencies.items()}


def main() -> None:
    print("=== Beat 4: Event-ID Sharding Across 8 DB Shards + Redis Hash Tags ===\n")
    r, backend = connect_sync_redis()
    print(f"Redis backend: {backend}\n")

    try:
        verify_hash_tag_colocation()
        with tempfile.TemporaryDirectory(prefix="mts_beat4_") as tmpdir:
            tmp = Path(tmpdir)
            before_p95 = run_monolith_benchmark(r, tmp)
            after_p95 = run_sharded_benchmark(r, tmp)
    finally:
        r.close()

    print(f"{'Show':<24} | {'Shared Node p95':>16} | {'Sharded (id%8) p95':>18} | {'Isolation Gain':>14}")
    print("-" * 80)
    for label, eid in (
        ("Event A (Stadium Spike)", EVENT_A),
        ("Event B (Normal Show)", EVENT_B),
        ("Event C (Indie Show)", EVENT_C),
    ):
        b = before_p95[eid]
        a = after_p95[eid]
        gain = b / max(0.01, a)
        print(f"{label:<24} | {b:>13.1f} ms | {a:>15.1f} ms | {gain:>12.1f}x")

    print("\nTakeaway: Sharding by `event_id % 8` + `{%d}:seats` isolates stadium spikes so other shows never feel the load.")


if __name__ == "__main__":
    main()
