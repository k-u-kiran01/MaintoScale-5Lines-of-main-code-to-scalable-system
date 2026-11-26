"""Beat 2-5 (+ Beat 1 failure comparison): Live HLS + ABR + Edge CDN + Prefetch Server.

Combines every beat from the Main To Scale Video Streaming episode:
- Beat 1: Raw 4 GB main() choke simulator (`POST /api/raw-play`)
- Beat 2: 4-second HLS chunk playlists (`GET /hls/master.m3u8`, `GET /hls/{tier}.m3u8`)
- Beat 3: Adaptive Bitrate tier switching (`1080p` / `720p` / `480p`) driven by live signal
- Beat 4: Regional Edge CDN POP cache (`redis` / `fakeredis`) emitting `Cache-Control: public, max-age=86400`
- Beat 5: Client +2 prefetch playhead buffer surviving dead zones cleanly in `static/index.html`

Run:
    python 04_live_stream_server.py
    # or: uvicorn 04_live_stream_server:app --reload
Then open http://127.0.0.1:8000 in your browser.
"""

from __future__ import annotations

import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import fakeredis.aioredis as fake_aioredis
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, PlainTextResponse, Response
from pydantic import BaseModel
from redis.exceptions import RedisError

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

STATIC_INDEX = Path(__file__).resolve().parent / "static" / "index.html"

TIERS: dict[str, dict[str, Any]] = {
    "1080p": {"bandwidth": 5_000_000, "mbps": 5.0, "resolution": "1920x1080", "chunk_kb": 2500},
    "720p": {"bandwidth": 2_500_000, "mbps": 2.5, "resolution": "1280x720", "chunk_kb": 1250},
    "480p": {"bandwidth": 1_000_000, "mbps": 1.0, "resolution": "854x480", "chunk_kb": 500},
}

POPS = ("us", "eu", "asia")
POP_BASE_LATENCY_MS = {"us": 12, "eu": 14, "asia": 16}
ORIGIN_BASE_LATENCY_MS = {"us": 840, "eu": 910, "asia": 980}
NUM_SEGMENTS = 15
CACHE_MAX_AGE = 86400


class RawPlayRequest(BaseModel):
    viewers: int = 10_000


class StormRequest(BaseModel):
    requests: int = 300
    cdn: bool = True


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


class StreamingHub:
    def __init__(self) -> None:
        self.redis: aioredis.Redis | None = None
        self.redis_mode: str = "initializing"
        self.clients: set[WebSocket] = set()
        self.reset_metrics()

    def reset_metrics(self) -> None:
        self.origin_requests = 0
        self.edge_hits = 0
        self.edge_misses = 0
        self.origin_cpu_pct = 4
        self.pop_stats: dict[str, dict[str, Any]] = {
            pop: {"hits": 0, "misses": 0, "last_latency_ms": POP_BASE_LATENCY_MS[pop]}
            for pop in POPS
        }
        self.recent_logs: list[dict[str, Any]] = []

    async def start(self) -> None:
        self.redis, self.redis_mode = await connect_redis()
        await self.purge_cache(broadcast=False)

    async def stop(self) -> None:
        if self.redis is not None:
            await self.redis.aclose()
            self.redis = None

    async def purge_cache(self, broadcast: bool = True) -> int:
        assert self.redis is not None
        deleted = 0
        keys = await self.redis.keys("cdn:*")
        if keys:
            deleted = await self.redis.delete(*keys)
        self.reset_metrics()
        if broadcast:
            await self.broadcast_state()
        return deleted

    async def cache_entry_count(self) -> int:
        assert self.redis is not None
        keys = await self.redis.keys("cdn:*")
        return len(keys)

    async def snapshot(self) -> dict[str, Any]:
        total = self.edge_hits + self.edge_misses
        hit_rate = round((self.edge_hits / total) * 100.0, 1) if total else 0.0
        cached_keys = await self.cache_entry_count()
        return {
            "type": "state",
            "redis_mode": self.redis_mode,
            "origin_requests": self.origin_requests,
            "origin_cpu_pct": self.origin_cpu_pct,
            "edge_hits": self.edge_hits,
            "edge_misses": self.edge_misses,
            "hit_rate_pct": hit_rate,
            "cached_chunks": cached_keys,
            "pops": self.pop_stats,
            "recent_logs": self.recent_logs[:8],
        }

    async def broadcast_state(self) -> None:
        if not self.clients:
            return
        payload = await self.snapshot()
        dead: list[WebSocket] = []
        for ws in list(self.clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)

    async def fetch_segment(
        self, tier: str, segment: str, region: str, cdn_enabled: bool, record_log: bool = True
    ) -> tuple[bytes, dict[str, str], int]:
        assert self.redis is not None
        pop = region.lower() if region.lower() in POPS else "us"
        key = f"cdn:{pop}:{tier}:{segment}"

        cache_status = "BYPASS"
        if cdn_enabled:
            cached = await self.redis.get(key)
            if cached is not None:
                cache_status = "HIT"
                self.edge_hits += 1
                self.pop_stats[pop]["hits"] += 1
                latency_ms = POP_BASE_LATENCY_MS[pop]
                self.pop_stats[pop]["last_latency_ms"] = latency_ms
                self.origin_cpu_pct = max(4, self.origin_cpu_pct - 2)
            else:
                cache_status = "MISS"
                self.edge_misses += 1
                self.origin_requests += 1
                self.pop_stats[pop]["misses"] += 1
                latency_ms = ORIGIN_BASE_LATENCY_MS[pop]
                self.pop_stats[pop]["last_latency_ms"] = latency_ms
                self.origin_cpu_pct = min(98, max(12, self.origin_cpu_pct + 4))
                await self.redis.set(key, f"{tier}/{segment}", ex=CACHE_MAX_AGE)
        else:
            self.edge_misses += 1
            self.origin_requests += 1
            self.pop_stats[pop]["misses"] += 1
            latency_ms = ORIGIN_BASE_LATENCY_MS[pop]
            self.pop_stats[pop]["last_latency_ms"] = latency_ms
            self.origin_cpu_pct = min(99, max(28, self.origin_cpu_pct + 9))

        headers = {
            "Cache-Control": f"public, max-age={CACHE_MAX_AGE}",
            "CDN-Cache-Control": f"max-age={CACHE_MAX_AGE}",
            "X-Edge-Pop": pop.upper(),
            "X-Cache": cache_status,
            "X-Latency-Ms": str(latency_ms),
            "X-Tier": tier,
        }

        if record_log:
            self.recent_logs.insert(
                0,
                {
                    "ts": round(time.time(), 2),
                    "path": f"/hls/{tier}/{segment}",
                    "pop": pop.upper(),
                    "cache": cache_status,
                    "latency_ms": latency_ms,
                    "cache_control": headers["Cache-Control"],
                },
            )
            self.recent_logs = self.recent_logs[:12]

        payload = (
            f"HLS_CHUNK tier={tier} seg={segment} pop={pop.upper()} cache={cache_status}\n"
        ).encode("utf-8")
        return payload, headers, latency_ms


hub = StreamingHub()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await hub.start()
    try:
        yield
    finally:
        await hub.stop()


app = FastAPI(title="Main To Scale — Video Streaming Server", lifespan=lifespan)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_INDEX)


@app.get("/hls/master.m3u8")
async def hls_master() -> PlainTextResponse:
    lines = ["#EXTM3U", "#EXT-X-VERSION:3"]
    for tier, meta in TIERS.items():
        lines.append(
            f"#EXT-X-STREAM-INF:BANDWIDTH={meta['bandwidth']},RESOLUTION={meta['resolution']}"
        )
        lines.append(f"/hls/{tier}.m3u8")
    return PlainTextResponse(
        "\n".join(lines) + "\n",
        media_type="application/vnd.apple.mpegurl",
        headers={"Cache-Control": "public, max-age=10", "CDN-Cache-Control": "max-age=10"},
    )


@app.get("/hls/{tier}.m3u8")
async def hls_tier_playlist(tier: str) -> PlainTextResponse:
    if tier not in TIERS:
        raise HTTPException(status_code=404, detail=f"Unknown ABR tier: {tier}")
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        "#EXT-X-TARGETDURATION:4",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXT-X-PLAYLIST-TYPE:VOD",
    ]
    for idx in range(1, NUM_SEGMENTS + 1):
        lines.append("#EXTINF:4.000,")
        lines.append(f"/hls/{tier}/seg_{idx:03d}.ts")
    lines.append("#EXT-X-ENDLIST")
    return PlainTextResponse(
        "\n".join(lines) + "\n",
        media_type="application/vnd.apple.mpegurl",
        headers={"Cache-Control": "public, max-age=10", "CDN-Cache-Control": "max-age=10"},
    )


@app.get("/hls/{tier}/{segment}")
async def hls_segment(
    tier: str,
    segment: str,
    region: str = Query(default="us"),
    cdn: int = Query(default=1),
) -> Response:
    if tier not in TIERS:
        raise HTTPException(status_code=404, detail=f"Unknown ABR tier: {tier}")
    if not segment.endswith(".ts"):
        raise HTTPException(status_code=404, detail=f"Invalid segment: {segment}")
    payload, headers, _ = await hub.fetch_segment(
        tier=tier,
        segment=segment,
        region=region,
        cdn_enabled=bool(cdn),
        record_log=True,
    )
    await hub.broadcast_state()
    return Response(content=payload, media_type="video/mp2t", headers=headers)


@app.post("/api/raw-play")
async def simulate_raw_play(req: RawPlayRequest) -> dict[str, Any]:
    viewers = max(1, min(100_000, req.viewers))
    ram_needed_gb = viewers * 4.0
    per_viewer_mbps = min(100.0, 10_000.0 / viewers)
    wait_seconds = round(32_000.0 / per_viewer_mbps, 1)
    crashed = viewers > 8
    return {
        "viewers": viewers,
        "file": "movie.mp4 (4 GB raw)",
        "ram_needed_gb": ram_needed_gb,
        "per_viewer_mbps": round(per_viewer_mbps, 2),
        "estimated_wait_sec": wait_seconds,
        "status": 502 if crashed else 200,
        "verdict": "502 CRASH — OOM & PIPE CHOKED" if crashed else "200 OK (1-viewer Wi-Fi only)",
    }


@app.post("/api/storm")
async def simulate_global_storm(req: StormRequest) -> dict[str, Any]:
    count = max(3, min(1200, req.requests))
    hot_segs = ("seg_001.ts", "seg_002.ts", "seg_003.ts")
    for idx in range(count):
        pop = POPS[idx % len(POPS)]
        seg = hot_segs[(idx // len(POPS)) % len(hot_segs)]
        await hub.fetch_segment(
            tier="720p",
            segment=seg,
            region=pop,
            cdn_enabled=req.cdn,
            record_log=(idx >= count - 4),
        )
    state = await hub.snapshot()
    await hub.broadcast_state()
    return state


@app.post("/api/cdn/purge")
async def purge_cdn() -> dict[str, Any]:
    deleted = await hub.purge_cache(broadcast=True)
    return {"purged_keys": deleted, "state": await hub.snapshot()}


@app.get("/api/state")
async def get_state() -> dict[str, Any]:
    return await hub.snapshot()


@app.websocket("/ws/telemetry")
async def telemetry_ws(ws: WebSocket) -> None:
    await ws.accept()
    hub.clients.add(ws)
    try:
        await ws.send_json(await hub.snapshot())
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        hub.clients.discard(ws)
    except Exception:
        hub.clients.discard(ws)


if __name__ == "__main__":
    print("Starting Main To Scale — Video Streaming Server on http://127.0.0.1:8000")
    uvicorn.run(app, host="127.0.0.1", port=8000)
