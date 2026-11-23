"""Beats 2 & 5: Uber H3 hexagonal grid indexing + rush-hour k-ring expansion.

Uses H3 v4 APIs (`h3.latlng_to_cell` and `h3.grid_disk`) at resolution 9 around Bengaluru.

Run:
    python 02_h3_hex_and_kring.py
"""

from __future__ import annotations

import math
import random
import time
from collections import defaultdict
from typing import NamedTuple

import h3

RIDER_LAT, RIDER_LNG = 12.9716, 77.5946
H3_RESOLUTION = 9  # ~0.105 km^2 per hex (~174 m edge length)


class Driver(NamedTuple):
    lat: float
    lng: float
    cab_id: str


def dist_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle Haversine distance in kilometers."""
    r1, g1, r2, g2 = map(math.radians, (lat1, lng1, lat2, lng2))
    dlat = r2 - r1
    dlng = g2 - g1
    a = math.sin(dlat / 2) ** 2 + math.cos(r1) * math.cos(r2) * math.sin(dlng / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def nearest(pos: tuple[float, float], candidates: list[Driver]) -> Driver:
    return min(candidates, key=lambda d: dist_km(pos[0], pos[1], d.lat, d.lng))


def build_hex_index(count: int = 100_000, seed: int = 42) -> tuple[list[Driver], dict[str, list[Driver]]]:
    """Generate 100,000 Bengaluru drivers and bucket them by H3 resolution-9 cell."""
    rng = random.Random(seed)
    all_drivers: list[Driver] = []
    cell_drivers: dict[str, list[Driver]] = defaultdict(list)

    for i in range(count):
        lat = RIDER_LAT + rng.uniform(-0.11, 0.11)
        lng = RIDER_LNG + rng.uniform(-0.11, 0.11)
        driver = Driver(lat=lat, lng=lng, cab_id=f"CAB-{i + 1:06d}")
        all_drivers.append(driver)
        cell = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
        cell_drivers[cell].append(driver)

    return all_drivers, cell_drivers


def find_in_cells(cells: set[str] | list[str], cell_drivers: dict[str, list[Driver]]) -> list[Driver]:
    found: list[Driver] = []
    for c in cells:
        found.extend(cell_drivers.get(c, ()))
    return found


def demo() -> None:
    print(f"=== Beats 2 & 5: Uber H3 v{h3.__version__} Spatial Indexing (Res {H3_RESOLUTION}) ===\n")
    all_drivers, cell_drivers = build_hex_index(100_000)
    pos = (RIDER_LAT, RIDER_LNG)

    # --- Beat 2: Slice the city into H3 hex cells and scan only nearby drivers ---------------
    t_brute0 = time.perf_counter()
    brute_cab = nearest(pos, all_drivers)
    brute_ms = (time.perf_counter() - t_brute0) * 1000.0

    t_h3_0 = time.perf_counter()
    cell = h3.latlng_to_cell(RIDER_LAT, RIDER_LNG, H3_RESOLUTION)
    nearby = cell_drivers[cell]
    hex_cab = nearest(pos, nearby)
    h3_ms = (time.perf_counter() - t_h3_0) * 1000.0

    print("[Beat 2] Single-Cell H3 Lookup vs Full 100K City Scan")
    print(f"  Rider hex cell (res {H3_RESOLUTION}): {cell}")
    print(f"  Brute-force O(N) : scanned {len(all_drivers):>7,} drivers -> {brute_cab.cab_id} in {brute_ms:6.2f} ms")
    print(
        f"  H3 cell lookup   : scanned {len(nearby):>7,} drivers -> {hex_cab.cab_id} in {h3_ms:6.3f} ms "
        f"({brute_ms / max(h3_ms, 1e-4):,.0f}x faster)"
    )

    # --- Beat 5: Rush hour empties the rider's hexagon -> expand outward via grid_disk -------
    print("\n[Beat 5] Rush-Hour Empty Hex -> h3.grid_disk(cell, k) Expansion")
    for empty_cell in h3.grid_disk(cell, 1):
        cell_drivers.pop(empty_cell, None)
    print(f"  Simulated rush hour: emptied center cell + Ring 1 ({len(h3.grid_disk(cell, 1))} cells = 0 cabs).")

    matched_cab: Driver | None = None
    for k in range(0, 4):
        cells = h3.grid_disk(cell, k)
        cabs = find_in_cells(cells, cell_drivers)
        print(f"  k={k}: scanned {len(cells):>2} hex cells -> found {len(cabs):>3} cabs")
        if cabs:
            matched_cab = nearest(pos, cabs)
            d_km = dist_km(RIDER_LAT, RIDER_LNG, matched_cab.lat, matched_cab.lng)
            print(
                f"  -> Matched {matched_cab.cab_id} from ring k={k} at {d_km:.3f} km "
                f"({matched_cab.lat:.4f}, {matched_cab.lng:.4f})"
            )
            break

    if matched_cab is None:
        print("  No cabs available within k=3 rings.")


if __name__ == "__main__":
    demo()
