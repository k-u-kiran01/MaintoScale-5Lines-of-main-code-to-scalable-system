"""Beat 2 (+ full pipeline): FastAPI WebSocket real-time chat relay.

Combines all 5 beats from the Main To Scale WhatsApp episode:
- Beat 2: Persistent WebSockets (`await ws.accept()`, zero HTTP polling)
- Beat 3: Redis Offline Queue (`redis.rpush(f"inbox:{uid}", msg)`) with `fakeredis` fallback
- Beat 4: Delivery & Read receipts (`ack = {"id": msg_id, "status": "delivered"}`)
- Beat 5: Zero-knowledge server relay (hacked-server inspector sees plaintext vs E2EE ciphertext)

Run:
    python 04_live_chat_server.py
    # or: uvicorn 04_live_chat_server:app --reload
Then open http://127.0.0.1:8000 in your browser.
"""

from __future__ import annotations

import json
import sys
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import fakeredis.aioredis as fake_aioredis
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from redis.exceptions import RedisError

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

STATIC_INDEX = Path(__file__).resolve().parent / "static" / "index.html"


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


class ChatRelayHub:
    def __init__(self) -> None:
        self.redis: aioredis.Redis | None = None
        self.redis_mode: str = "initializing"
        self.online: dict[str, WebSocket] = {}
        self.pubkeys: dict[str, dict[str, Any]] = {}
        self.inspectors: set[WebSocket] = set()
        self.stats: dict[str, int] = {
            "messages_relayed": 0,
            "messages_queued": 0,
            "messages_replayed": 0,
            "acks_relayed": 0,
        }

    async def start(self) -> None:
        self.redis, self.redis_mode = await connect_redis()
        await self.redis.delete("inbox:alice", "inbox:bob")

    async def stop(self) -> None:
        if self.redis is not None:
            await self.redis.aclose()
            self.redis = None

    async def _queue_snapshot(self) -> dict[str, list[dict[str, Any]]]:
        assert self.redis is not None
        out: dict[str, list[dict[str, Any]]] = {}
        for uid in ("alice", "bob"):
            raw_items = await self.redis.lrange(f"inbox:{uid}", 0, -1)
            out[uid] = [json.loads(item) for item in raw_items]
        return out

    async def broadcast_inspector(self, event: dict[str, Any]) -> None:
        if not self.inspectors:
            return
        dead: list[WebSocket] = []
        for ws in list(self.inspectors):
            try:
                await ws.send_json(event)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.inspectors.discard(ws)

    async def push_state_to_all(self) -> None:
        queues = await self._queue_snapshot()
        state_event = {
            "type": "state",
            "redis_mode": self.redis_mode,
            "online": sorted(self.online.keys()),
            "queued": {uid: len(items) for uid, items in queues.items()},
            "queue_items": queues,
            "pubkeys": self.pubkeys,
            "stats": dict(self.stats),
        }
        await self.broadcast_inspector(state_event)
        for ws in list(self.online.values()):
            try:
                await ws.send_json(
                    {
                        "type": "presence",
                        "online": sorted(self.online.keys()),
                        "pubkeys": self.pubkeys,
                    }
                )
            except Exception:
                pass

    async def deliver_or_queue(self, target_uid: str, frame: dict[str, Any]) -> bool:
        """Send immediately over persistent WebSocket if online; else RPUSH to Redis."""
        assert self.redis is not None
        peer_ws = self.online.get(target_uid)
        if peer_ws is not None:
            try:
                await peer_ws.send_json(frame)
                return True
            except Exception:
                self.online.pop(target_uid, None)

        await self.redis.rpush(f"inbox:{target_uid}", json.dumps(frame))
        if frame.get("type") == "chat":
            self.stats["messages_queued"] += 1
        return False

    async def flush_offline_inbox(self, uid: str, ws: WebSocket) -> None:
        assert self.redis is not None
        key = f"inbox:{uid}"
        raw_items = await self.redis.lrange(key, 0, -1)
        if not raw_items:
            return
        await self.redis.delete(key)

        for raw in raw_items:
            frame = json.loads(raw)
            if frame.get("type") == "chat":
                frame["replayed"] = True
                self.stats["messages_replayed"] += 1
            await ws.send_json(frame)

            # Emit delivery ACK back to original sender once replayed to recipient
            if frame.get("type") == "chat" and frame.get("from"):
                ack = {
                    "type": "ack",
                    "id": frame["id"],
                    "from": uid,
                    "to": frame["from"],
                    "status": "delivered",
                    "ts": time.time(),
                }
                self.stats["acks_relayed"] += 1
                await self.deliver_or_queue(frame["from"], ack)
                await self.broadcast_inspector({"type": "ack_log", "ack": ack, "replayed": True})


hub = ChatRelayHub()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await hub.start()
    try:
        yield
    finally:
        await hub.stop()


app = FastAPI(title="Main To Scale — WhatsApp Chat Relay", lifespan=lifespan)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_INDEX)


@app.get("/api/state")
async def api_state() -> dict[str, Any]:
    queues = await hub._queue_snapshot()
    return {
        "redis_mode": hub.redis_mode,
        "online": sorted(hub.online.keys()),
        "queued": {uid: len(items) for uid, items in queues.items()},
        "polling_rps": 0,
        "stats": dict(hub.stats),
    }


@app.websocket("/ws/inspector")
async def ws_inspector(ws: WebSocket) -> None:
    await ws.accept()
    hub.inspectors.add(ws)
    try:
        queues = await hub._queue_snapshot()
        await ws.send_json(
            {
                "type": "state",
                "redis_mode": hub.redis_mode,
                "online": sorted(hub.online.keys()),
                "queued": {uid: len(items) for uid, items in queues.items()},
                "queue_items": queues,
                "pubkeys": hub.pubkeys,
                "stats": dict(hub.stats),
            }
        )
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        hub.inspectors.discard(ws)


@app.websocket("/ws/{uid}")
async def ws_chat(ws: WebSocket, uid: str) -> None:
    await ws.accept()
    hub.online[uid] = ws
    await ws.send_json(
        {
            "type": "hello",
            "uid": uid,
            "redis_mode": hub.redis_mode,
            "online": sorted(hub.online.keys()),
            "pubkeys": hub.pubkeys,
        }
    )

    try:
        await hub.flush_offline_inbox(uid, ws)
        await hub.push_state_to_all()

        while True:
            data = await ws.receive_json()
            msg_type = data.get("type")

            if msg_type == "key_exchange":
                hub.pubkeys[uid] = data.get("pubkey")
                await hub.push_state_to_all()
                continue

            if msg_type == "chat":
                target = str(data.get("to", "")).strip()
                msg_id = str(data.get("id") or uuid.uuid4().hex[:10])
                frame = {
                    "type": "chat",
                    "id": msg_id,
                    "from": uid,
                    "to": target,
                    "encrypted": bool(data.get("encrypted", False)),
                    "payload": data.get("payload", ""),
                    "iv": data.get("iv", ""),
                    "algo": data.get("algo", "plaintext"),
                    "ciphertext_preview": data.get("ciphertext_preview", ""),
                    "ts": data.get("ts") or time.time(),
                }
                hub.stats["messages_relayed"] += 1

                # Immediate 1-tick ('sent') receipt back to sender
                sent_ack = {
                    "type": "ack",
                    "id": msg_id,
                    "from": "server",
                    "to": uid,
                    "status": "sent",
                    "ts": time.time(),
                }
                await ws.send_json(sent_ack)

                delivered_live = await hub.deliver_or_queue(target, frame)

                await hub.broadcast_inspector(
                    {
                        "type": "wire",
                        "id": msg_id,
                        "from": uid,
                        "to": target,
                        "encrypted": frame["encrypted"],
                        "algo": frame["algo"],
                        "wire_preview": frame["ciphertext_preview"] or str(frame["payload"]),
                        "queued": not delivered_live,
                        "ts": frame["ts"],
                    }
                )
                await hub.push_state_to_all()
                continue

            if msg_type == "ack":
                target = str(data.get("to", "")).strip()
                status = str(data.get("status", "delivered"))
                ack = {
                    "type": "ack",
                    "id": str(data.get("id", "")),
                    "from": uid,
                    "to": target,
                    "status": status,
                    "ts": time.time(),
                }
                hub.stats["acks_relayed"] += 1
                await hub.deliver_or_queue(target, ack)
                await hub.broadcast_inspector({"type": "ack_log", "ack": ack, "replayed": False})
                await hub.push_state_to_all()
    except WebSocketDisconnect:
        pass
    finally:
        if hub.online.get(uid) is ws:
            hub.online.pop(uid, None)
        await hub.push_state_to_all()


if __name__ == "__main__":
    uvicorn.run("04_live_chat_server:app", host="127.0.0.1", port=8000, reload=False)
