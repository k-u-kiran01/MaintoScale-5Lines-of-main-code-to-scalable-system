"""Beat 4 (+ full pipeline): FastAPI WebSocket live ride-dispatch server around Bengaluru.

Combines:
- Uber H3 v4 spatial indexing (`h3.latlng_to_cell` + `h3.grid_disk` at resolution 9)
- Redis Geo live GPS pings (`GEOADD` / `GEOSEARCH`) with automatic `fakeredis` fallback
- WebSocket coordinate push (`await ws.send_json`) with zero HTTP polling
- Atomic 15-second Redis dispatch lock (`nx=True, ex=15`)

Run:
    python 04_live_ws_server.py
    # or: uvicorn 04_live_ws_server:app --reload
Then open http://127.0.0.1:8000 in your browser.
"""

from __future__ import annotations

import asyncio
import math
import random
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fakeredis.aioredis as fake_aioredis
import h3
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from redis.exceptions import RedisError

RIDER_LAT, RIDER_LNG = 12.9716, 77.5946  # Bengaluru center (MG Road / Cubbon Park)
H3_RES = 9
GEO_KEY = "cabs:live:bengaluru"
LOCK_TTL_SEC = 15
TICK_SEC = 0.25

RIDER_CELL = h3.latlng_to_cell(RIDER_LAT, RIDER_LNG, H3_RES)
RING_0 = frozenset({RIDER_CELL})
RING_1_ALL = frozenset(h3.grid_disk(RIDER_CELL, 1))
RING_2_ALL = frozenset(h3.grid_disk(RIDER_CELL, 2))
RING_2_ONLY = tuple(sorted(RING_2_ALL - RING_1_ALL))

STATIC_INDEX = Path(__file__).resolve().parent / "static" / "index.html"


def dist_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle Haversine distance in kilometers."""
    r1, g1, r2, g2 = map(math.radians, (lat1, lng1, lat2, lng2))
    dlat = r2 - r1
    dlng = g2 - g1
    a = math.sin(dlat / 2) ** 2 + math.cos(r1) * math.cos(r2) * math.sin(dlng / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


@dataclass
class SimCab:
    cab_id: str
    lat: float
    lng: float
    dlat: float
    dlng: float
    cell: str = ""
    trail: deque[tuple[float, float]] = field(default_factory=lambda: deque(maxlen=10))


def _build_hex_mesh() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for cell in sorted(RING_2_ALL):
        ring_k = 0 if cell in RING_0 else (1 if cell in RING_1_ALL else 2)
        boundary = [[round(lat, 6), round(lng, 6)] for lat, lng in h3.cell_to_boundary(cell)]
        center_lat, center_lng = h3.cell_to_latlng(cell)
        cells.append(
            {
                "cell": cell,
                "ring_k": ring_k,
                "center": [round(center_lat, 6), round(center_lng, 6)],
                "boundary": boundary,
            }
        )
    return cells


async def connect_redis() -> tuple[aioredis.Redis, str]:
    """Try localhost:6379 first; fall back cleanly to in-memory fakeredis."""
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


class DispatchEngine:
    def __init__(self) -> None:
        self.redis: aioredis.Redis | None = None
        self.redis_mode = "initializing"
        self.hex_mesh = _build_hex_mesh()
        self.cabs: dict[str, SimCab] = {}
        self.lock_holders: dict[str, str] = {}
        self.lock_expires_at: dict[str, float] = {}
        self.events: deque[dict[str, Any]] = deque(maxlen=16)
        self.active_ring_k: int = 0
        self.rush_hour_until: float = 0.0
        self._rider_seq: int = 0
        self._task: asyncio.Task[None] | None = None
        self._clients: set[WebSocket] = set()
        self._spawn_fleet()

    def _spawn_fleet(self) -> None:
        rng = random.Random(2026)
        self.cabs.clear()
        for i in range(18):
            cab_id = f"CAB-{i + 1:02d}"
            # Keep 4 cabs near center / ring-1 and 14 spread across ring-2 & wider Bengaluru
            spread = 0.0022 if i < 4 else 0.0072
            lat = RIDER_LAT + rng.uniform(-spread, spread)
            lng = RIDER_LNG + rng.uniform(-spread, spread)
            angle = rng.uniform(0, math.tau)
            speed = rng.uniform(0.00014, 0.00024)
            cab = SimCab(
                cab_id=cab_id,
                lat=lat,
                lng=lng,
                dlat=math.sin(angle) * speed,
                dlng=math.cos(angle) * speed,
                cell=h3.latlng_to_cell(lat, lng, H3_RES),
            )
            cab.trail.append((lat, lng))
            self.cabs[cab_id] = cab

    async def start(self) -> None:
        self.redis, self.redis_mode = await connect_redis()
        await self.redis.delete(GEO_KEY)
        await self._sync_geoadd()
        self._log_event("info", f"Live WebSocket stream online ({self.redis_mode})")
        self._task = asyncio.create_task(self._sim_loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self.redis is not None:
            await self.redis.aclose()
            self.redis = None

    def _log_event(self, kind: str, message: str) -> None:
        self.events.appendleft(
            {
                "ts": time.strftime("%H:%M:%S"),
                "kind": kind,
                "message": message,
            }
        )

    async def _sync_geoadd(self) -> None:
        assert self.redis is not None
        for cab in self.cabs.values():
            await self.redis.geoadd(GEO_KEY, (cab.lng, cab.lat, cab.cab_id))

    async def _refresh_locks(self) -> None:
        assert self.redis is not None
        now = time.monotonic()
        for cab_id in list(self.cabs):
            holder = await self.redis.get(f"lock:{cab_id}")
            ttl = await self.redis.ttl(f"lock:{cab_id}")
            if holder is not None and ttl > 0:
                self.lock_holders[cab_id] = holder
                if cab_id not in self.lock_expires_at or self.lock_expires_at[cab_id] <= now:
                    self.lock_expires_at[cab_id] = now + float(ttl)
            else:
                self.lock_holders.pop(cab_id, None)
                self.lock_expires_at.pop(cab_id, None)

    def _step_fleet(self) -> None:
        now = time.monotonic()
        rush_active = now < self.rush_hour_until
        min_lat, max_lat = RIDER_LAT - 0.0082, RIDER_LAT + 0.0082
        min_lng, max_lng = RIDER_LNG - 0.0086, RIDER_LNG + 0.0086

        for cab in self.cabs.values():
            if cab.cab_id in self.lock_holders:
                continue  # Locked cabs pause briefly during pickup confirmation
            next_lat = cab.lat + cab.dlat
            next_lng = cab.lng + cab.dlng
            if next_lat < min_lat or next_lat > max_lat:
                cab.dlat *= -1
                next_lat = max(min_lat, min(max_lat, next_lat))
            if next_lng < min_lng or next_lng > max_lng:
                cab.dlng *= -1
                next_lng = max(min_lng, min(max_lng, next_lng))

            next_cell = h3.latlng_to_cell(next_lat, next_lng, H3_RES)
            if rush_active and next_cell in RING_1_ALL:
                # Keep Ring 0 & Ring 1 clear during active rush-hour demo
                cab.dlat *= -1
                cab.dlng *= -1
                continue

            cab.lat, cab.lng, cab.cell = next_lat, next_lng, next_cell
            cab.trail.append((cab.lat, cab.lng))

    async def _sim_loop(self) -> None:
        while True:
            self._step_fleet()
            await self._sync_geoadd()
            await self._refresh_locks()
            await self.broadcast_state()
            await asyncio.sleep(TICK_SEC)

    def build_snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        rush_active = now < self.rush_hour_until
        cabs_out: list[dict[str, Any]] = []
        active_locks: list[dict[str, Any]] = []

        for cab in self.cabs.values():
            holder = self.lock_holders.get(cab.cab_id)
            rem_ttl = 0.0
            if holder is not None:
                rem_ttl = max(0.0, self.lock_expires_at.get(cab.cab_id, now) - now)
                active_locks.append(
                    {
                        "cab_id": cab.cab_id,
                        "rider_id": holder,
                        "ttl_remaining": round(rem_ttl, 1),
                    }
                )
            cabs_out.append(
                {
                    "cab_id": cab.cab_id,
                    "lat": round(cab.lat, 6),
                    "lng": round(cab.lng, 6),
                    "cell": cab.cell,
                    "locked_by": holder,
                    "ttl_remaining": round(rem_ttl, 1),
                    "trail": [[round(p[0], 6), round(p[1], 6)] for p in cab.trail],
                }
            )

        return {
            "type": "tick",
            "redis_mode": self.redis_mode,
            "rush_hour": rush_active,
            "active_ring_k": self.active_ring_k,
            "rider": {
                "lat": RIDER_LAT,
                "lng": RIDER_LNG,
                "cell": RIDER_CELL,
                "res": H3_RES,
            },
            "hex_cells": self.hex_mesh,
            "cabs": cabs_out,
            "active_locks": active_locks,
            "events": list(self.events),
        }

    async def broadcast_state(self) -> None:
        if not self._clients:
            return
        payload = self.build_snapshot()
        stale: list[WebSocket] = []
        for ws in list(self._clients):
            try:
                await ws.send_json(payload)
            except Exception:
                stale.append(ws)
        for ws in stale:
            self._clients.discard(ws)

    async def _try_lock(self, cab_id: str, rider_id: str) -> bool:
        assert self.redis is not None
        ok = await self.redis.set(f"lock:{cab_id}", rider_id, nx=True, ex=LOCK_TTL_SEC)
        if ok:
            self.lock_holders[cab_id] = rider_id
            self.lock_expires_at[cab_id] = time.monotonic() + float(LOCK_TTL_SEC)
            return True
        return False

    async def find_nearest_candidates(self) -> tuple[int, list[tuple[SimCab, float]]]:
        """Expand outward with h3.grid_disk(k=0..3) and rank with Redis GEOSEARCH."""
        assert self.redis is not None
        geo_rows = await self.redis.geosearch(
            GEO_KEY,
            longitude=RIDER_LNG,
            latitude=RIDER_LAT,
            radius=5.0,
            unit="km",
            withdist=True,
            sort="ASC",
        )
        geo_dist_map = {cab_id: float(d_km) for cab_id, d_km in geo_rows}

        for k in range(0, 4):
            ring_cells = set(h3.grid_disk(RIDER_CELL, k))
            candidates: list[tuple[SimCab, float]] = []
            for cab in self.cabs.values():
                if cab.cell in ring_cells and cab.cab_id not in self.lock_holders:
                    d_km = geo_dist_map.get(cab.cab_id, dist_km(RIDER_LAT, RIDER_LNG, cab.lat, cab.lng))
                    candidates.append((cab, d_km))
            if candidates:
                candidates.sort(key=lambda item: item[1])
                return k, candidates

        return 3, []

    async def request_ride(self, rider_id: str | None = None) -> dict[str, Any]:
        if rider_id is None:
            self._rider_seq += 1
            rider_id = f"Rider-{self._rider_seq}"

        ring_k, candidates = await self.find_nearest_candidates()
        self.active_ring_k = ring_k

        for cab, d_km in candidates:
            if await self._try_lock(cab.cab_id, rider_id):
                self._log_event(
                    "match",
                    f"{rider_id} locked {cab.cab_id} (ring k={ring_k}, {d_km:.2f} km, TTL={LOCK_TTL_SEC}s)",
                )
                await self.broadcast_state()
                return {
                    "status": "matched",
                    "rider_id": rider_id,
                    "cab_id": cab.cab_id,
                    "ring_k": ring_k,
                    "distance_km": round(d_km, 3),
                    "ttl": LOCK_TTL_SEC,
                }

        self._log_event("alert", f"{rider_id}: no unlocked cabs available in k=0..3 rings")
        await self.broadcast_state()
        return {"status": "unavailable", "rider_id": rider_id, "ring_k": ring_k}

    async def race_demo(self) -> dict[str, Any]:
        """Launch Rider-A and Rider-B concurrently for the exact same cab."""
        ring_k, candidates = await self.find_nearest_candidates()
        if not candidates:
            return {"status": "unavailable", "results": []}

        target_cab, d_km = candidates[0]
        self.active_ring_k = ring_k

        async def _attempt(rider_id: str) -> dict[str, Any]:
            won = await self._try_lock(target_cab.cab_id, rider_id)
            return {
                "rider_id": rider_id,
                "cab_id": target_cab.cab_id,
                "status": "matched" if won else "taken",
            }

        results = list(await asyncio.gather(_attempt("Rider-A"), _attempt("Rider-B")))
        winner = next((r["rider_id"] for r in results if r["status"] == "matched"), "none")
        self._log_event(
            "race",
            f"RACE on {target_cab.cab_id} ({d_km:.2f} km): {winner} won nx=True lock; dupe rejected",
        )
        await self.broadcast_state()
        return {
            "status": "ok",
            "cab_id": target_cab.cab_id,
            "ring_k": ring_k,
            "results": results,
        }

    async def simulate_rush_hour(self) -> dict[str, Any]:
        """Empty k=0 and k=1 hexes so the next ride request expands via grid_disk(k=2)."""
        self.rush_hour_until = time.monotonic() + 12.0
        self.active_ring_k = 0
        moved = 0
        for idx, cab in enumerate(self.cabs.values()):
            if cab.cab_id in self.lock_holders:
                continue
            if cab.cell in RING_1_ALL:
                outer_cell = RING_2_ONLY[idx % len(RING_2_ONLY)]
                olat, olng = h3.cell_to_latlng(outer_cell)
                cab.lat, cab.lng, cab.cell = olat, olng, outer_cell
                cab.trail.clear()
                cab.trail.append((olat, olng))
                moved += 1
        await self._sync_geoadd()
        self._log_event(
            "alert",
            f"RUSH HOUR: emptied k=0 & k=1 hexes ({moved} cabs pushed to Ring 2) — click Request Ride!",
        )
        await self.broadcast_state()
        return {"status": "rush_hour_active", "moved_cabs": moved}

    async def reset_fleet(self) -> dict[str, Any]:
        assert self.redis is not None
        for cab_id in list(self.cabs):
            await self.redis.delete(f"lock:{cab_id}")
        self.lock_holders.clear()
        self.lock_expires_at.clear()
        self.rush_hour_until = 0.0
        self.active_ring_k = 0
        self._spawn_fleet()
        await self._sync_geoadd()
        self._log_event("info", "Fleet & 15s locks reset around Bengaluru (12.97, 77.59)")
        await self.broadcast_state()
        return {"status": "reset"}


engine = DispatchEngine()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await engine.start()
    try:
        yield
    finally:
        await engine.stop()


app = FastAPI(title="Main To Scale — Real-Time Ride Matcher", lifespan=lifespan)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_INDEX)


@app.post("/api/request-ride")
async def api_request_ride() -> dict[str, Any]:
    return await engine.request_ride()


@app.post("/api/race-demo")
async def api_race_demo() -> dict[str, Any]:
    return await engine.race_demo()


@app.post("/api/rush-hour")
async def api_rush_hour() -> dict[str, Any]:
    return await engine.simulate_rush_hour()


@app.post("/api/reset")
async def api_reset() -> dict[str, Any]:
    return await engine.reset_fleet()


@app.websocket("/ws")
async def websocket_stream(ws: WebSocket) -> None:
    await ws.accept()
    engine._clients.add(ws)
    try:
        await ws.send_json(engine.build_snapshot())
        while True:
            # Keep socket alive and allow optional client ping frames without busy-polling
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        engine._clients.discard(ws)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
