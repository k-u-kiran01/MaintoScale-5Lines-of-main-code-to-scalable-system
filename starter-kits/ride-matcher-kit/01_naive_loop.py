"""Beat 1: The 5-line naive O(N) loop that works for 10 drivers and freezes at 100,000.

Run:
    python 01_naive_loop.py
"""

from __future__ import annotations

import math
import random
import time
from typing import NamedTuple

# Bengaluru city center (MG Road / Cubbon Park)
RIDER_POS = (12.9716, 77.5946)
EARTH_RADIUS_KM = 6371.0


class Driver(NamedTuple):
    lat: float
    lng: float
    cab_id: str


def dist(pos: tuple[float, float], driver: Driver) -> float:
    """Great-circle Haversine distance in kilometers between rider and driver."""
    lat1, lng1 = map(math.radians, pos)
    lat2, lng2 = map(math.radians, (driver.lat, driver.lng))
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


# --- The exact 5-line main() loop from Beat 1 ----------------------------------------------
def main(pos: tuple[float, float], drivers: list[Driver]) -> Driver:
    pairs = []
    for d in drivers:
        pairs.append((dist(pos, d), d))
    return min(pairs)[1]


def generate_drivers(count: int, seed: int = 42) -> list[Driver]:
    """Generate deterministic driver coordinates across Greater Bengaluru."""
    rng = random.Random(seed)
    return [
        Driver(
            lat=RIDER_POS[0] + rng.uniform(-0.11, 0.11),
            lng=RIDER_POS[1] + rng.uniform(-0.11, 0.11),
            cab_id=f"CAB-{i + 1:06d}",
        )
        for i in range(count)
    ]


def benchmark() -> None:
    print("=== Beat 1: 5-Line O(N) Loop Benchmark (Bengaluru 12.9716, 77.5946) ===\n")
    baseline_ms: float | None = None

    for n in (10, 10_000, 100_000):
        drivers = generate_drivers(n)
        t0 = time.perf_counter()
        nearest = main(RIDER_POS, drivers)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        if baseline_ms is None:
            baseline_ms = max(elapsed_ms, 1e-4)
        slowdown = elapsed_ms / baseline_ms
        nearest_km = dist(RIDER_POS, nearest)
        concurrent_100_sec = (elapsed_ms * 100.0) / 1000.0

        print(
            f"N = {n:>7,} drivers | nearest={nearest.cab_id} ({nearest_km:.3f} km) | "
            f"1 req={elapsed_ms:7.2f} ms ({slowdown:7.1f}x) | "
            f"100 concurrent reqs={concurrent_100_sec:5.2f} s"
        )

    print(
        "\nTakeaway: scanning every driver works for 10 cabs, "
        "but stalls the event loop when 100,000 drivers hit the road."
    )


if __name__ == "__main__":
    benchmark()
