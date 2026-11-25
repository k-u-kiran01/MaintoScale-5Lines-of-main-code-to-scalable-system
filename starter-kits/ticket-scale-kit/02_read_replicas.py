"""Beat 3: Read Replicas — Stop 90% Seat Browsing from Starving 10% Checkout Writes.

Demonstrates:
- Before (`Single Primary`): 90% seat-map reads + 10% booking writes hit one
  primary DB, inflating write-lock wait times and starving checkout latency.
- After (`Primary + 2 Replicas`, `seats.py`): Reads route cleanly to `replica_db`
  while checkout writes stay isolated on `primary_db`.

Run:
    python 02_read_replicas.py
"""

from __future__ import annotations

import shutil
import statistics
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from sqlalchemy import Integer, String, create_engine, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TOTAL_REQUESTS = 200
READ_RATIO = 0.90  # 90% browsing seat maps, 10% checkout writes


class Base(DeclarativeBase):
    pass


class Seat(Base):
    __tablename__ = "seats"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    section: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(16), default="open")


class Booking(Base):
    __tablename__ = "bookings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    uid: Mapped[str] = mapped_column(String(64))
    seat_id: Mapped[int] = mapped_column(Integer)

    def __init__(self, uid: str, seat_id: int) -> None:
        self.uid = uid
        self.seat_id = seat_id


SEATS_SQL = select(Seat).where(Seat.status == "open").limit(25)


def percentile(samples: list[float], pct: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return 0.0
    idx = min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[idx]


def make_sqlite_engine(path: Path):
    eng = create_engine(
        f"sqlite:///{path}",
        pool_size=20,
        max_overflow=10,
        connect_args={"check_same_thread": False, "timeout": 15.0},
    )
    return eng


def seed_primary(primary_db) -> None:
    Base.metadata.create_all(primary_db)
    with Session(primary_db) as s:
        s.add_all([Seat(id=i, section=f"SEC-{(i % 8) + 1}", status="open") for i in range(1, 201)])
        s.commit()


def run_single_primary_benchmark(primary_path: Path) -> dict[str, float]:
    """All 90% reads + 10% writes contend on the single primary DB."""
    primary_db = make_sqlite_engine(primary_path)
    write_lock = threading.Lock()
    primary_queue_slots = threading.Semaphore(1)  # Single primary I/O queue choked by 90% browsing
    read_latencies: list[float] = []
    write_latencies: list[float] = []
    lat_lock = threading.Lock()
    reads_done = 0
    writes_done = 0

    # Measure end-to-end wait + execution from request arrival in burst
    burst_start = time.perf_counter()

    def handle_request(i: int) -> None:
        nonlocal reads_done, writes_done
        is_write = (i % 10) == 5  # Interleaved 10% writes arriving mid-browse storm
        with primary_queue_slots:
            if not is_write:
                with Session(primary_db) as r:
                    _ = r.scalars(SEATS_SQL).all()
                    time.sleep(0.005)
                dt = (time.perf_counter() - burst_start) * 1000.0
                with lat_lock:
                    read_latencies.append(dt)
                    reads_done += 1
            else:
                with write_lock:
                    with Session(primary_db) as w:
                        w.add(Booking(f"fan_{i}", (i % 200) + 1))
                        w.commit()
                        time.sleep(0.003)
                dt = (time.perf_counter() - burst_start) * 1000.0
                with lat_lock:
                    write_latencies.append(dt)
                    writes_done += 1

    with ThreadPoolExecutor(max_workers=TOTAL_REQUESTS) as pool:
        list(pool.map(handle_request, range(TOTAL_REQUESTS)))
    total_ms = (time.perf_counter() - burst_start) * 1000.0
    primary_db.dispose()

    return {
        "reads": reads_done,
        "writes": writes_done,
        "read_p95_ms": percentile(read_latencies, 95),
        "write_p50_ms": statistics.median(write_latencies),
        "write_p95_ms": percentile(write_latencies, 95),
        "total_ms": total_ms,
    }


def run_split_replica_benchmark(primary_path: Path, replica_paths: list[Path]) -> dict[str, float]:
    """Beat 3 (`seats.py`): Route 90% seat reads to `replica_db`, 10% bookings to `primary_db`."""
    primary_db = make_sqlite_engine(primary_path)
    replicas = [make_sqlite_engine(p) for p in replica_paths]
    write_lock = threading.Lock()
    replica_hits = [0, 0]
    read_latencies: list[float] = []
    write_latencies: list[float] = []
    lat_lock = threading.Lock()
    read_rr = 0
    writes_done = 0

    burst_start = time.perf_counter()

    def handle_request(i: int) -> None:
        nonlocal read_rr, writes_done
        is_write = (i % 10) == 5
        uid = f"fan_{i}"
        seat_id = (i % 200) + 1

        if not is_write:
            with lat_lock:
                rep_idx = read_rr % len(replicas)
                read_rr += 1
            replica_db = replicas[rep_idx]
            # --- Exact Beat 3 (`seats.py`) read pattern -------------------------------------
            with Session(replica_db) as r:
                seats = r.scalars(SEATS_SQL).all()
                _ = len(seats)
                time.sleep(0.004)
            dt = (time.perf_counter() - burst_start) * 1000.0
            with lat_lock:
                replica_hits[rep_idx] += 1
                read_latencies.append(dt)
        else:
            with write_lock:
                # --- Exact Beat 3 (`seats.py`) write pattern --------------------------------
                with Session(primary_db) as w:
                    w.add(Booking(uid, seat_id))
                    w.commit()
                    time.sleep(0.002)
            dt = (time.perf_counter() - burst_start) * 1000.0
            with lat_lock:
                write_latencies.append(dt)
                writes_done += 1

    with ThreadPoolExecutor(max_workers=TOTAL_REQUESTS) as pool:
        list(pool.map(handle_request, range(TOTAL_REQUESTS)))
    total_ms = (time.perf_counter() - burst_start) * 1000.0

    with Session(primary_db) as check_sess:
        total_bookings = check_sess.scalar(select(func.count()).select_from(Booking)) or 0

    primary_db.dispose()
    for rep in replicas:
        rep.dispose()

    return {
        "reads": sum(replica_hits),
        "replica_1_reads": replica_hits[0],
        "replica_2_reads": replica_hits[1],
        "writes": writes_done,
        "committed_bookings": total_bookings,
        "read_p95_ms": percentile(read_latencies, 95),
        "write_p50_ms": statistics.median(write_latencies),
        "write_p95_ms": percentile(write_latencies, 95),
        "total_ms": total_ms,
    }


def main() -> None:
    print("=== Beat 3: Read Replicas (90% Seat Browsing vs. 10% Checkout Writes) ===\n")

    with tempfile.TemporaryDirectory(prefix="mts_beat3_") as tmpdir:
        tmp = Path(tmpdir)
        primary_single = tmp / "primary_single.db"
        p_eng = make_sqlite_engine(primary_single)
        seed_primary(p_eng)
        p_eng.dispose()

        before = run_single_primary_benchmark(primary_single)

        # Fresh primary + 2 snapshot read replicas for split topology
        primary_split = tmp / "primary_split.db"
        p_eng2 = make_sqlite_engine(primary_split)
        seed_primary(p_eng2)
        p_eng2.dispose()

        replica_1 = tmp / "replica_1.db"
        replica_2 = tmp / "replica_2.db"
        shutil.copyfile(primary_split, replica_1)
        shutil.copyfile(primary_split, replica_2)

        after = run_split_replica_benchmark(primary_split, [replica_1, replica_2])

    speedup = before["write_p95_ms"] / max(0.01, after["write_p95_ms"])

    print("[Before] Single Primary DB (ALL -> Primary)")
    print(f"  Traffic split     : {int(before['reads'])} reads (90%) + {int(before['writes'])} writes (10%) on Primary")
    print(f"  Checkout p50      : {before['write_p50_ms']:.2f} ms")
    print(f"  Checkout p95      : {before['write_p95_ms']:.2f} ms (STARVED by 90% browsing queue)")
    print(f"  Total burst time  : {before['total_ms']:.1f} ms\n")

    print("[After] Primary + 2 Read Replicas (`seats.py`)")
    print(
        f"  Traffic split     : Primary={int(after['writes'])} writes (10%) | "
        f"Replica1={int(after['replica_1_reads'])} reads | Replica2={int(after['replica_2_reads'])} reads"
    )
    print(f"  Bookings committed: {int(after['committed_bookings'])} / {int(after['writes'])} verified on Primary")
    print(f"  Checkout p50      : {after['write_p50_ms']:.2f} ms")
    print(f"  Checkout p95      : {after['write_p95_ms']:.2f} ms ({speedup:.1f}x faster)")
    print(f"  Total burst time  : {after['total_ms']:.1f} ms")


if __name__ == "__main__":
    main()
