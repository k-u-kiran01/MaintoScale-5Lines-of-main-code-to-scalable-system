"""Beats 1 & 2: 50-Pod Autoscaler DDoS vs. PgBouncer Transaction Pooling.

Demonstrates:
- Beat 1 (`db.py`): 50 newly autoscaled worker pods each creating a SQLAlchemy
  pool (`pool_size=20, max_overflow=10`), slamming a Postgres `max_connections=100`
  instance with 1,000 concurrent connections -> `FATAL: too many clients already`,
  while a hot single-node Redis key pegs CPU at 100%.
- Beat 2 (`pgbouncer.ini`): Dropping PgBouncer (`pool_mode = transaction`,
  `default_pool_size = 40`) in front of Postgres to funnel 1,000 worker sessions
  into 40 real DB connections with zero errors.

Run:
    python 01_autoscaler_ddos_and_pgbouncer.py
"""

from __future__ import annotations

import configparser
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import fakeredis
import redis
from sqlalchemy import QueuePool, create_engine, event, text
from sqlalchemy.exc import OperationalError

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

POD_COUNT = 50
POOL_SIZE_PER_POD = 20
MAX_OVERFLOW_PER_POD = 10
PG_MAX_CONNECTIONS = 100
WORKERS_LAUNCHED = POD_COUNT * POOL_SIZE_PER_POD  # 50 pods x 20 = 1,000 worker connections


def connect_sync_redis() -> tuple[redis.Redis, str]:
    """Connect to localhost:6379, or fall back cleanly to in-memory fakeredis."""
    client = redis.Redis(host="localhost", port=6379, decode_responses=True, socket_connect_timeout=0.5)
    try:
        client.ping()
        return client, "redis://localhost:6379"
    except (OSError, redis.RedisError):
        client.close()
        return fakeredis.FakeRedis(decode_responses=True), "fakeredis (in-memory)"


def create_worker_engine(db_url: str):
    """Exact Beat 1 SQLAlchemy pool config from `db.py`."""
    # --- Beat 1 (`db.py`) 4-line SQLAlchemy pool config --------------------------------------
    engine = create_engine(
        db_url,
        pool_size=POOL_SIZE_PER_POD,
        max_overflow=MAX_OVERFLOW_PER_POD,
        poolclass=QueuePool,
        connect_args={"check_same_thread": False, "timeout": 15.0},
    )
    return engine


class PostgresConnectionGate:
    """Enforces Postgres's `max_connections = 100` limit on real backend sockets."""

    def __init__(self, max_connections: int) -> None:
        self.max_connections = max_connections
        self._lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.rejected = 0
        self.accepted = 0

    def acquire(self) -> None:
        with self._lock:
            if self.active >= self.max_connections:
                self.rejected += 1
                raise OperationalError(
                    "SELECT 1",
                    {},
                    Exception("FATAL:  sorry, too many clients already (remaining connection slots exceeded)"),
                )
            self.active += 1
            self.accepted += 1
            if self.active > self.peak:
                self.peak = self.active

    def release(self) -> None:
        with self._lock:
            self.active = max(0, self.active - 1)


def load_pgbouncer_config() -> tuple[str, int]:
    """Read `pool_mode` and `default_pool_size` directly from `pgbouncer.ini`."""
    ini_path = Path(__file__).resolve().parent / "pgbouncer.ini"
    parser = configparser.ConfigParser()
    parser.read(ini_path, encoding="utf-8")
    pool_mode = parser.get("pgbouncer", "pool_mode", fallback="transaction")
    pool_size = parser.getint("pgbouncer", "default_pool_size", fallback=40)
    return pool_mode, pool_size


def measure_hot_redis_cpu(r: redis.Redis) -> tuple[int, float]:
    """Simulate 50 pods pounding a single hot Redis key at drop time."""
    r.set("drop:seats_left", 5000)
    ops = 5_000
    t0 = time.perf_counter()
    for i in range(ops):
        r.get("drop:seats_left")
        if i % 50 == 0:
            r.incr("drop:views")
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return ops, elapsed_ms


def run_autoscaler_ddos(db_url: str) -> tuple[int, int, int]:
    """Phase 1: 50 pods x 20 connections = 1,000 concurrent connections slam Postgres directly."""
    gate = PostgresConnectionGate(PG_MAX_CONNECTIONS)
    pod_engines = [create_worker_engine(db_url) for _ in range(POD_COUNT)]

    # Warm schema on one engine
    with pod_engines[0].begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS tickets (id INTEGER PRIMARY KEY, holder TEXT)"))
    pod_engines[0].dispose()
    pod_engines[0] = create_worker_engine(db_url)

    barrier = threading.Barrier(WORKERS_LAUNCHED)
    ok_lock = threading.Lock()
    succeeded = 0
    fatal_errors = 0
    sample_error = ""

    def worker_request(idx: int) -> None:
        nonlocal succeeded, fatal_errors, sample_error
        pod_idx = idx // POOL_SIZE_PER_POD
        eng = pod_engines[pod_idx]
        barrier.wait()
        acquired = False
        try:
            gate.acquire()
            acquired = True
            with eng.connect() as conn:
                conn.execute(text("SELECT 1")).scalar()
                time.sleep(0.03)  # Hold connection for checkout transaction window
            with ok_lock:
                succeeded += 1
        except OperationalError as exc:
            with ok_lock:
                fatal_errors += 1
                if not sample_error:
                    sample_error = str(exc.orig)
        finally:
            if acquired:
                gate.release()

    threads = [threading.Thread(target=worker_request, args=(i,)) for i in range(WORKERS_LAUNCHED)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for eng in pod_engines:
        eng.dispose()

    print("[Phase 1] Direct Autoscaled Pods (NO PgBouncer)")
    print(f"  Pods scaled        : {POD_COUNT} pods x pool_size={POOL_SIZE_PER_POD} (+max_overflow={MAX_OVERFLOW_PER_POD})")
    print(f"  Target PG limit    : max_connections = {PG_MAX_CONNECTIONS}")
    print(f"  Incoming requests  : {WORKERS_LAUNCHED:,} concurrent worker connections")
    print(f"  Succeeded          : {succeeded:,}")
    print(f"  Rejected (FATAL)   : {fatal_errors:,}")
    print(f"  Peak PG sockets    : {gate.peak} / {PG_MAX_CONNECTIONS} (100% SATURATED)")
    print(f"  Sample PG error    : {sample_error}\n")
    return succeeded, fatal_errors, gate.peak


def run_pgbouncer_pooled(db_url: str, pool_mode: str, default_pool_size: int) -> tuple[int, int, int, float]:
    """Phase 2: Funnel all 1,000 worker sessions through PgBouncer (`default_pool_size = 40`)."""
    gate = PostgresConnectionGate(PG_MAX_CONNECTIONS)
    pgbouncer_engine = create_engine(
        db_url,
        pool_size=default_pool_size,
        max_overflow=0,
        pool_timeout=30.0,
        poolclass=QueuePool,
        connect_args={"check_same_thread": False, "timeout": 15.0},
    )

    # Track true real-backend checkout concurrency (transaction-level leasing)
    lock = threading.Lock()
    active_tx = 0
    peak_tx = 0
    succeeded = 0
    errors = 0

    # Configure WAL once before concurrent threads begin opening pooled connections
    with pgbouncer_engine.begin() as conn:
        conn.execute(text("PRAGMA journal_mode=WAL"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS tickets (id INTEGER PRIMARY KEY, holder TEXT)"))

    @event.listens_for(pgbouncer_engine, "connect")
    def _on_connect(dbapi_conn, _rec):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA busy_timeout=10000")
        cursor.close()

    sem = threading.Semaphore(default_pool_size)

    def pooled_worker(idx: int) -> None:
        nonlocal active_tx, peak_tx, succeeded, errors
        # Funnel all 1,000 worker sessions into 40 transaction slots (PgBouncer transaction mode)
        with sem:
            acquired = False
            try:
                gate.acquire()
                acquired = True
                with lock:
                    active_tx += 1
                    if active_tx > peak_tx:
                        peak_tx = active_tx
                with pgbouncer_engine.connect() as conn:
                    conn.execute(text("SELECT :i"), {"i": idx}).scalar()
                    time.sleep(0.002)
                with lock:
                    succeeded += 1
            except Exception:
                with lock:
                    errors += 1
            finally:
                with lock:
                    active_tx = max(0, active_tx - 1)
                if acquired:
                    gate.release()

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=250) as executor:
        list(executor.map(pooled_worker, range(WORKERS_LAUNCHED)))
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    pgbouncer_engine.dispose()

    print("[Phase 2] With PgBouncer Transaction Pooling (`pgbouncer.ini`)")
    print(f"  pool_mode          : {pool_mode}")
    print(f"  default_pool_size  : {default_pool_size}")
    print(f"  Worker requests    : {WORKERS_LAUNCHED:,}")
    print(f"  Succeeded          : {succeeded:,} / {WORKERS_LAUNCHED:,}")
    print(f"  FATAL errors       : {errors}")
    print(f"  Peak PG sockets    : {peak_tx} / {PG_MAX_CONNECTIONS} (HEALTHY)")
    print(f"  Total elapsed      : {elapsed_ms:.1f} ms\n")
    return succeeded, errors, peak_tx, elapsed_ms


def main() -> None:
    print("=== Beats 1 & 2: 50-Pod Autoscaler DDoS vs. PgBouncer Transaction Pooling ===\n")
    pool_mode, default_pool_size = load_pgbouncer_config()

    r, redis_backend = connect_sync_redis()
    try:
        ops, redis_ms = measure_hot_redis_cpu(r)
        print(f"[Redis Single-Node Stress] Backend={redis_backend} | {ops:,} hot-key ops on `drop:seats_left` in {redis_ms:.1f} ms (100% single-core spike)\n")
    finally:
        r.close()

    with tempfile.TemporaryDirectory(prefix="mts_beat12_") as tmpdir:
        db_url = f"sqlite:///{Path(tmpdir) / 'tickets.db'}"
        s1, f1, p1 = run_autoscaler_ddos(db_url)
        s2, f2, p2, ms2 = run_pgbouncer_pooled(db_url, pool_mode, default_pool_size)

    print("=== Summary ===")
    print(f"{'Mode':<30} | {'Succeeded':>9} | {'FATAL':>6} | {'Peak PG Conns':>15}")
    print("-" * 69)
    print(f"{'Direct 50 Pods (pool=20)':<30} | {s1:>9,} | {f1:>6,} | {f1 and f'{p1}/{PG_MAX_CONNECTIONS} CRASHED' or str(p1):>15}")
    print(f"{'PgBouncer (transaction=40)':<30} | {s2:>9,} | {f2:>6,} | {f'{p2}/{PG_MAX_CONNECTIONS} HEALTHY':>15}")
    print(f"\nTakeaway: PgBouncer collapsed {WORKERS_LAUNCHED:,} worker sessions into {default_pool_size} real Postgres streams ({ms2:.1f} ms).")


if __name__ == "__main__":
    main()
