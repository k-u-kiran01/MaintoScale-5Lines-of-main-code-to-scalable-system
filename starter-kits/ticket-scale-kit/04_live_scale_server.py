"""Beat 5 (+ Full Part 2 Architecture Shield): Live FastAPI + WebSocket Simulator.

Unites every scaling layer from the Main To Scale Part 2 episode:
- Beat 1: 50 Autoscaled Pods (`pool_size=20` -> 1,000 connections -> `FATAL` without pooler)
- Beat 2: PgBouncer (`pool_mode = transaction`, `default_pool_size = 40`)
- Beat 3: Read Replicas (90% seat browsing -> Replicas 1 & 2, 10% writes -> Primary)
- Beat 4: Event-ID Sharding (`event_id % 8` DB shards + `{event_id}:seats` Redis Cluster)
- Beat 5: CDN Edge Waiting Room (`verifyQueueToken(req)` HMAC-SHA256 + 5,000 buyers/s drip)

Run:
    python 04_live_scale_server.py
    # or: uvicorn 04_live_scale_server:app --reload
Then open http://127.0.0.1:8000 in your browser.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

import fakeredis.aioredis as fake_aioredis
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from redis.exceptions import RedisError

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

STATIC_INDEX = Path(__file__).resolve().parent / "static" / "index.html"
QUEUE_SECRET = os.environ.get("QUEUE_SECRET", "main-to-scale-edge-secret").encode("utf-8")
PG_MAX_CONNECTIONS = 100
PGBOUNCER_POOL_SIZE = 40
DRIP_RATE_PER_SEC = 5_000


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


def _b64u_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64u_decode(token_part: str) -> bytes:
    pad = "=" * ((4 - len(token_part) % 4) % 4)
    return base64.urlsafe_b64decode(token_part + pad)


def issue_queue_token(client_id: str, ttl_sec: int = 120) -> tuple[str, dict[str, Any]]:
    """Issue a real HMAC-SHA256 signed queue token compatible with `edge_gate.ts`."""
    payload = {
        "sub": client_id,
        "exp": int(time.time()) + ttl_sec,
        "rate": DRIP_RATE_PER_SEC,
        "jti": uuid.uuid4().hex[:8],
    }
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    payload_b64 = _b64u_encode(payload_bytes)
    sig = hmac.new(QUEUE_SECRET, payload_b64.encode("ascii"), hashlib.sha256).digest()
    return f"{payload_b64}.{_b64u_encode(sig)}", payload


def verify_queue_token(token: str | None) -> tuple[bool, dict[str, Any] | None]:
    """Verify HMAC-SHA256 signature and expiration (mirrors `verifyQueueToken` in `edge_gate.ts`)."""
    if not token or "." not in token:
        return False, None
    parts = token.split(".", 1)
    if len(parts) != 2:
        return False, None
    payload_b64, sig_b64 = parts
    try:
        expected_sig = hmac.new(QUEUE_SECRET, payload_b64.encode("ascii"), hashlib.sha256).digest()
        actual_sig = _b64u_decode(sig_b64)
        if not hmac.compare_digest(expected_sig, actual_sig):
            return False, None
        payload = json.loads(_b64u_decode(payload_b64).decode("utf-8"))
        if int(payload.get("exp", 0)) <= int(time.time()):
            return False, None
        return True, payload
    except Exception:
        return False, None


class ConfigUpdate(BaseModel):
    pods: int | None = Field(default=None, ge=5, le=50)
    pgbouncer: bool | None = None
    read_replicas: bool | None = None
    sharding: bool | None = None
    edge_waiting_room: bool | None = None
    fan_storm: bool | None = None


class BookRequest(BaseModel):
    event_id: int = Field(default=101, ge=1, le=999999)
    uid: str = Field(default="buyer_42", min_length=1, max_length=64)


class ScaleControlPlane:
    def __init__(self) -> None:
        self.redis: aioredis.Redis | None = None
        self.redis_mode: str = "initializing"
        self.clients: set[WebSocket] = set()
        self.events: list[dict[str, Any]] = []
        self.config: dict[str, Any] = {
            "pods": 50,
            "pool_size": 20,
            "pgbouncer": False,
            "read_replicas": False,
            "sharding": False,
            "edge_waiting_room": False,
            "fan_storm": True,
        }
        self.counters: dict[str, int] = {
            "http_200": 0,
            "http_202": 0,
            "http_503": 0,
            "tokens_issued": 0,
        }
        self._bg_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self.redis, self.redis_mode = await connect_redis()
        # Seed initial seat inventory in Redis Cluster hash-tagged keys (`{event_id}:seats`)
        for eid, seats in ((101, 50_000), (102, 4_500), (103, 1_800)):
            await self.redis.set("{%d}:seats" % eid, str(seats))
        self.log_event("BOOT", f"Scale Control Plane online ({self.redis_mode})")
        self._bg_task = asyncio.create_task(self._telemetry_loop())

    async def stop(self) -> None:
        if self._bg_task is not None:
            self._bg_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._bg_task
            self._bg_task = None
        if self.redis is not None:
            await self.redis.aclose()
            self.redis = None

    def log_event(self, kind: str, msg: str) -> None:
        self.events.insert(0, {"ts": round(time.time(), 2), "kind": kind, "msg": msg})
        if len(self.events) > 25:
            self.events.pop()

    def apply_preset(self, preset_id: str) -> None:
        presets = {
            "ddos": {
                "pods": 50,
                "pgbouncer": False,
                "read_replicas": False,
                "sharding": False,
                "edge_waiting_room": False,
                "fan_storm": False,
            },
            "pgbouncer": {
                "pods": 50,
                "pgbouncer": True,
                "read_replicas": False,
                "sharding": False,
                "edge_waiting_room": False,
                "fan_storm": False,
            },
            "replicas": {
                "pods": 50,
                "pgbouncer": True,
                "read_replicas": True,
                "sharding": False,
                "edge_waiting_room": False,
                "fan_storm": False,
            },
            "sharding": {
                "pods": 50,
                "pgbouncer": True,
                "read_replicas": True,
                "sharding": True,
                "edge_waiting_room": False,
                "fan_storm": True,
            },
            "full_shield": {
                "pods": 50,
                "pgbouncer": True,
                "read_replicas": True,
                "sharding": True,
                "edge_waiting_room": True,
                "fan_storm": True,
            },
        }
        if preset_id not in presets:
            raise HTTPException(status_code=404, detail=f"Unknown preset '{preset_id}'")
        self.config.update(presets[preset_id])
        self.log_event("PRESET", f"Switched topology to preset `{preset_id}`")

    async def compute_snapshot(self) -> dict[str, Any]:
        c = self.config
        worker_conns = int(c["pods"]) * int(c["pool_size"])
        fan_storm = bool(c["fan_storm"])
        pgb = bool(c["pgbouncer"])
        rep = bool(c["read_replicas"])
        sharded = bool(c["sharding"])
        edge = bool(c["edge_waiting_room"])

        # Connections & FATAL rejection computation
        if pgb:
            pg_conns = min(PGBOUNCER_POOL_SIZE, max(4, worker_conns // 25))
            fatal_per_sec = 0
            pg_status = "HEALTHY"
        else:
            pg_conns = min(PG_MAX_CONNECTIONS, worker_conns)
            fatal_per_sec = max(0, worker_conns - PG_MAX_CONNECTIONS)
            pg_status = "FATAL: too many clients already" if fatal_per_sec > 0 else "HEALTHY"

        # Edge Waiting Room absorbs 2M fans at CDN and drips 5,000/s
        incoming_fans = 2_000_000 if fan_storm else 120_000
        if fan_storm and not edge:
            edge_queue = 0
            origin_req_sec = 2_000_000
            origin_bw_pct = 100
            drip_rate = origin_req_sec
        elif edge:
            edge_queue = 1_920_000 if fan_storm else 45_000
            drip_rate = DRIP_RATE_PER_SEC if fan_storm else 2_400
            origin_req_sec = drip_rate
            origin_bw_pct = 18 if fan_storm else 9
        else:
            edge_queue = 0
            origin_req_sec = 42_000
            origin_bw_pct = 38
            drip_rate = origin_req_sec

        # Primary vs Replicas & Checkout latency
        if fatal_per_sec > 0:
            checkout_p95_ms = 4800
            primary_qps = 0
            replica_qps = 0
        elif not rep:
            checkout_p95_ms = 4200 if origin_req_sec > 10_000 else 1850
            primary_qps = origin_req_sec
            replica_qps = 0
        elif not sharded:
            checkout_p95_ms = 340 if origin_req_sec > 10_000 else 65
            primary_qps = int(origin_req_sec * 0.10)
            replica_qps = int(origin_req_sec * 0.90)
        else:
            checkout_p95_ms = 19 if edge else 280
            primary_qps = int(origin_req_sec * 0.10)
            replica_qps = int(origin_req_sec * 0.90)

        # Redis + Shard CPU isolation
        if sharded:
            if edge:
                shard_cpu = {"event_a": 24, "event_b": 16, "event_c": 14}
            else:
                shard_cpu = {"event_a": 92, "event_b": 22, "event_c": 19}
            redis_cpu = max(shard_cpu.values())
        else:
            redis_cpu = 100 if (fatal_per_sec > 0 or fan_storm or origin_req_sec > 20_000) else 64
            shard_cpu = {"event_a": redis_cpu, "event_b": redis_cpu, "event_c": redis_cpu}

        # Overall status classification
        if fatal_per_sec > 0 or (fan_storm and not edge):
            overall_status = "MELTDOWN"
        elif not pgb or not rep or not sharded or not edge:
            overall_status = "DEGRADED"
        else:
            overall_status = "SHIELD ACTIVE · 200 OK"

        seats_map: dict[str, int] = {}
        if self.redis is not None:
            for eid in (101, 102, 103):
                val = await self.redis.get("{%d}:seats" % eid)
                seats_map[str(eid)] = int(val) if val is not None else 0

        return {
            "type": "state",
            "redis_mode": self.redis_mode,
            "overall_status": overall_status,
            "config": dict(self.config),
            "metrics": {
                "worker_connections": worker_conns,
                "pg_active_connections": pg_conns,
                "pg_max_connections": PG_MAX_CONNECTIONS,
                "pgbouncer_pool_size": PGBOUNCER_POOL_SIZE,
                "fatal_errors_per_sec": fatal_per_sec,
                "pg_status": pg_status,
                "redis_cpu_pct": redis_cpu,
                "shard_cpu": shard_cpu,
                "primary_qps": primary_qps,
                "replica_qps": replica_qps,
                "checkout_p95_ms": checkout_p95_ms,
                "incoming_fans": incoming_fans,
                "edge_queue_depth": edge_queue,
                "drip_rate_per_sec": drip_rate,
                "origin_req_sec": origin_req_sec,
                "origin_bw_pct": origin_bw_pct,
                "seats": seats_map,
            },
            "counters": dict(self.counters),
            "events": list(self.events[:12]),
        }

    async def _telemetry_loop(self) -> None:
        while True:
            await asyncio.sleep(0.45)
            if self.clients:
                snap = await self.compute_snapshot()
                await self.broadcast(snap)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        dead: list[WebSocket] = []
        for ws in list(self.clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


control = ScaleControlPlane()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await control.start()
    try:
        yield
    finally:
        await control.stop()


app = FastAPI(title="Main To Scale — Part 2 Ticket Drop Scaling", lifespan=lifespan)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_INDEX)


@app.get("/api/state")
async def get_state() -> dict[str, Any]:
    return await control.compute_snapshot()


@app.post("/api/config")
async def update_config(body: ConfigUpdate) -> dict[str, Any]:
    updates = body.model_dump(exclude_none=True)
    control.config.update(updates)
    control.log_event("CONFIG", f"Updated controls: {updates}")
    snap = await control.compute_snapshot()
    await control.broadcast(snap)
    return snap


@app.post("/api/preset/{preset_id}")
async def set_preset(preset_id: str) -> dict[str, Any]:
    control.apply_preset(preset_id)
    snap = await control.compute_snapshot()
    await control.broadcast(snap)
    return snap


@app.post("/api/token")
async def mint_token(uid: str = "buyer_42") -> dict[str, Any]:
    token, payload = issue_queue_token(uid)
    control.counters["tokens_issued"] += 1
    control.log_event("EDGE", f"Issued signed HMAC queue token for {uid} (drip {DRIP_RATE_PER_SEC}/s)")
    snap = await control.compute_snapshot()
    await control.broadcast(snap)
    return {"token": token, "payload": payload, "drip_rate_per_sec": DRIP_RATE_PER_SEC}


@app.post("/api/book")
async def book_seat(
    body: BookRequest,
    x_queue_token: str | None = Header(default=None, alias="X-Queue-Token"),
) -> JSONResponse:
    snap = await control.compute_snapshot()
    m = snap["metrics"]
    c = control.config

    # 1. Edge Waiting Room Gate (mirrors `edge_gate.ts`)
    if c["edge_waiting_room"]:
        valid, _claims = verify_queue_token(x_queue_token)
        if not valid:
            control.counters["http_202"] += 1
            control.log_event("202 WAIT", f"No valid queue token for {body.uid} -> held in CDN Waiting Room")
            await control.broadcast(await control.compute_snapshot())
            return JSONResponse(
                status_code=202,
                content={
                    "status": "waiting_room",
                    "http_status": 202,
                    "queue_position": max(1, m["edge_queue_depth"]),
                    "drip_rate_per_sec": DRIP_RATE_PER_SEC,
                    "message": "Missing/invalid X-Queue-Token -> held at CDN Edge Waiting Room.",
                },
            )

    # 2. Backend Crash Check (Postgres FATAL or unshielded 2M-fan storm melting origin)
    if m["fatal_errors_per_sec"] > 0 or (c["fan_storm"] and not c["edge_waiting_room"]):
        reason = (
            "FATAL: too many clients already (50 pods x 20 = 1,000 conns > max_connections=100)"
            if m["fatal_errors_per_sec"] > 0
            else "503 Origin Bandwidth Exhausted: 2M fan refreshes without Edge Waiting Room"
        )
        control.counters["http_503"] += 1
        control.log_event("503 FAIL", f"Booking rejected for event {body.event_id}: {reason}")
        await control.broadcast(await control.compute_snapshot())
        return JSONResponse(
            status_code=503,
            content={"status": "overloaded", "http_status": 503, "error": reason},
        )

    # 3. Sharded Redis + Postgres checkout
    assert control.redis is not None
    shard_id = (body.event_id % 8) if c["sharding"] else 0
    rkey = "{%d}:seats" % body.event_id
    if await control.redis.get(rkey) is None:
        await control.redis.set(rkey, "5000")
    remaining = int(await control.redis.decr(rkey))

    control.counters["http_200"] += 1
    control.log_event("200 BOOKED", f"uid={body.uid} booked event={body.event_id} on shard=#{shard_id} ({rkey}={remaining})")
    await control.broadcast(await control.compute_snapshot())
    return JSONResponse(
        status_code=200,
        content={
            "status": "booked",
            "http_status": 200,
            "uid": body.uid,
            "event_id": body.event_id,
            "shard": shard_id,
            "rkey": rkey,
            "remaining_seats": remaining,
            "checkout_p95_ms": m["checkout_p95_ms"],
        },
    )


@app.websocket("/ws/metrics")
async def ws_metrics(ws: WebSocket) -> None:
    await ws.accept()
    control.clients.add(ws)
    try:
        await ws.send_json(await control.compute_snapshot())
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        control.clients.discard(ws)


if __name__ == "__main__":
    uvicorn.run("04_live_scale_server:app", host="127.0.0.1", port=8000, reload=False)
