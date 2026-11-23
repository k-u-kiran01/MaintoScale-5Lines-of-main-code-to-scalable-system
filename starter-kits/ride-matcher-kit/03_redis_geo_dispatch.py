"""Beats 3 & 6: In-memory Redis Geo index + 15-second atomic dispatch lock.

Automatically falls back to in-memory `fakeredis` if Redis is not running on localhost:6379.

Run:
    python 03_redis_geo_dispatch.py
"""

from __future__ import annotations

import asyncio
import random

import fakeredis.aioredis as fake_aioredis
import redis.asyncio as aioredis
from redis.exceptions import RedisError

RIDER_LAT, RIDER_LNG = 12.9716, 77.5946
GEO_KEY = "cabs:bengaluru"


async def connect_redis() -> tuple[aioredis.Redis, str]:
    """Connect to localhost:6379, or fall back cleanly to in-memory fakeredis."""
    client = aioredis.Redis(
        host="localhost",
        port=6379,
        decode_responses=True,
        socket_connect_timeout=0.5,
    )
    try:
        await client.ping()
        return client, "redis://localhost:6379"
    except (OSError, RedisError):
        await client.aclose()
        print("[Redis] localhost:6379 not reachable -> using in-memory fakeredis (no Docker needed!)")
        return fake_aioredis.FakeRedis(decode_responses=True), "fakeredis (in-memory)"


async def seed_live_gps_pings(r: aioredis.Redis, count: int = 25) -> None:
    """Beat 3: Stream 3-second GPS pings into Redis Geo instead of melting a disk DB."""
    rng = random.Random(42)
    await r.delete(GEO_KEY)
    for i in range(count):
        lat = RIDER_LAT + rng.uniform(-0.025, 0.025)
        lng = RIDER_LNG + rng.uniform(-0.025, 0.025)
        cab_id = f"CAB-{i + 1:02d}"
        await r.geoadd(GEO_KEY, (lng, lat, cab_id))


async def naive_claim_cab(r: aioredis.Redis, cab_id: str, rider_id: str) -> str:
    """BROKEN check-then-set dispatch (susceptible to race conditions)."""
    key = f"naive_lock:{cab_id}"
    if await r.get(key) is None:
        await asyncio.sleep(0)  # Simulated network / event-loop yield
        await r.set(key, rider_id, ex=15)
        return "matched"
    return "taken"


async def claim_cab(r: aioredis.Redis, cab_id: str, rider_id: str) -> str:
    """Beat 6: Atomic 15-second Redis lock (`nx=True, ex=15`) prevents double dispatch."""
    ok = await r.set(f"lock:{cab_id}", rider_id, nx=True, ex=15)
    return "matched" if ok else "taken"


async def main() -> None:
    print("=== Beats 3 & 6: Redis Geo Index + 15s Atomic Lock ===\n")
    r, backend = await connect_redis()
    print(f"Connected backend: {backend}\n")

    try:
        # --- Beat 3: GEOADD + GEOSEARCH in RAM ---------------------------------------------
        await seed_live_gps_pings(r, count=25)
        nearby = await r.geosearch(
            GEO_KEY,
            longitude=RIDER_LNG,
            latitude=RIDER_LAT,
            radius=2.5,
            unit="km",
            withdist=True,
            withcoord=True,
            sort="ASC",
        )
        print(f"[Beat 3] GEOSEARCH within 2.5 km of ({RIDER_LAT}, {RIDER_LNG}) -> {len(nearby)} cabs:")
        for cab_id, dist_km, (lng, lat) in nearby[:5]:
            print(f"  {cab_id} | {dist_km:5.3f} km away | ({lat:.4f}, {lng:.4f})")

        target_cab = nearby[0][0]

        # --- Beat 6a: Broken unlocked race condition ---------------------------------------
        await r.delete(f"naive_lock:{target_cab}", f"lock:{target_cab}")
        broken_results = await asyncio.gather(
            naive_claim_cab(r, target_cab, "Rider-A"),
            naive_claim_cab(r, target_cab, "Rider-B"),
        )
        print(f"\n[Beat 6a] WITHOUT atomic lock -> racing Rider-A & Rider-B for {target_cab}:")
        print(f"  Rider-A={broken_results[0]}, Rider-B={broken_results[1]}  <-- DOUBLE DISPATCH BUG!")

        # --- Beat 6b: Fixed 15-second atomic Redis lock (`nx=True, ex=15`) ------------------
        await r.delete(f"lock:{target_cab}")
        fixed_results = await asyncio.gather(
            claim_cab(r, target_cab, "Rider-A"),
            claim_cab(r, target_cab, "Rider-B"),
        )
        ttl_sec = await r.ttl(f"lock:{target_cab}")
        holder = await r.get(f"lock:{target_cab}")
        print(f"\n[Beat 6b] WITH Redis nx=True, ex=15 -> racing Rider-A & Rider-B for {target_cab}:")
        print(f"  Rider-A={fixed_results[0]}, Rider-B={fixed_results[1]} | holder={holder} | TTL={ttl_sec}s")

        # 5-rider stress test on the next cab
        burst_cab = nearby[1][0]
        await r.delete(f"lock:{burst_cab}")
        riders = [f"Rider-{i + 1}" for i in range(5)]
        burst = await asyncio.gather(*(claim_cab(r, burst_cab, rid) for rid in riders))
        winners = [rid for rid, status in zip(riders, burst) if status == "matched"]
        print(f"\n[Stress] 5 simultaneous riders race {burst_cab} -> exactly {len(winners)} winner: {winners[0]}")
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
