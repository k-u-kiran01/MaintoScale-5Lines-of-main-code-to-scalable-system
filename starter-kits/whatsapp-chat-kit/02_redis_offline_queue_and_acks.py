"""Beats 3 & 4: Redis offline queue (`redis.rpush`) + delivery & read receipts.

Automatically falls back to in-memory `fakeredis` if Redis is not running on localhost:6379.

Run:
    python 02_redis_offline_queue_and_acks.py
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid

import fakeredis.aioredis as fake_aioredis
import redis.asyncio as aioredis
from redis.exceptions import RedisError

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


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


def tick_badge(status: str) -> str:
    if status == "sent":
        return "[1 tick  - sent]"
    if status == "delivered":
        return "[2 ticks - delivered]"
    if status == "read":
        return "[2 BLUE  - read]"
    return f"[{status}]"


class ChatRelayDemo:
    """Simulates Beats 3 & 4: persistent online delivery, Redis RPUSH offline queue, and ACKs."""

    def __init__(self, r: aioredis.Redis) -> None:
        self.redis = r
        self.online: dict[str, list[dict]] = {}
        self.receipts: dict[str, str] = {}

    async def connect_user(self, uid: str) -> list[dict]:
        """Bring user online and replay every queued message in FIFO order."""
        self.online.setdefault(uid, [])
        key = f"inbox:{uid}"
        queued_raw = await self.redis.lrange(key, 0, -1)
        replayed: list[dict] = []
        if queued_raw:
            await self.redis.delete(key)
            for raw in queued_raw:
                msg = json.loads(raw)
                self.online[uid].append(msg)
                replayed.append(msg)
                await self.send_ack(msg["id"], "delivered")
        return replayed

    def set_airplane_mode(self, uid: str) -> None:
        self.online.pop(uid, None)

    async def send_ack(self, msg_id: str, status: str) -> dict:
        """Beat 4: Return delivery / read receipts to update ticks."""
        ack = {"id": msg_id, "status": status}
        self.receipts[msg_id] = status
        print(f"    -> ACK {ack}  {tick_badge(status)}")
        return ack

    async def send_message(self, sender: str, uid: str, text: str) -> str:
        """Beat 3: Push instantly if online; otherwise RPUSH into Redis offline inbox."""
        msg_id = uuid.uuid4().hex[:8]
        payload = {"id": msg_id, "from": sender, "to": uid, "text": text}

        # Server accepted message -> 1 tick ('sent')
        await self.send_ack(msg_id, "sent")

        if uid in self.online:
            self.online[uid].append(payload)
            await self.send_ack(msg_id, "delivered")
        else:
            await self.redis.rpush(f"inbox:{uid}", json.dumps(payload))
            q_len = await self.redis.llen(f"inbox:{uid}")
            print(f"    [Redis Gate CLOSED] {uid} in Airplane Mode -> redis.rpush('inbox:{uid}') (queued={q_len})")

        return msg_id


async def main() -> None:
    print("=== Beats 3 & 4: Redis Offline Queue + Delivery/Read Receipts ===\n")
    r, backend = await connect_redis()
    print(f"Connected backend: {backend}\n")

    try:
        await r.delete("inbox:alice", "inbox:bob")
        relay = ChatRelayDemo(r)
        await relay.connect_user("alice")
        await relay.connect_user("bob")

        # 1. Both users online -> instant delivery + read receipt
        print("[1] Bob is ONLINE. Alice sends 'Hey Bob, coffee?':")
        m1 = await relay.send_message("alice", "bob", "Hey Bob, coffee?")
        print("  Bob opens the chat:")
        await relay.send_ack(m1, "read")

        # 2. Bob switches on Airplane Mode -> messages queue in Redis
        print("\n[2] Bob switches on AIRPLANE MODE. Alice sends 4 messages:")
        relay.set_airplane_mode("bob")
        queued_ids = []
        for text in ("Meet at 9", "Bring your laptop", "Table near window", "See you soon!"):
            print(f"  Alice sends: '{text}'")
            queued_ids.append(await relay.send_message("alice", "bob", text))

        depth = await r.llen("inbox:bob")
        print(f"\n  Redis key inbox:bob currently holds {depth} waiting messages (0 lost!).")

        # 3. Bob reconnects -> queue gate lifts, FIFO replay + delivered/read ACKs
        print("\n[3] Bob turns OFF Airplane Mode and reconnects -> Gate OPENS:")
        replayed = await relay.connect_user("bob")
        print(f"  Replayed {len(replayed)}/4 messages in exact order -> remaining in Redis: {await r.llen('inbox:bob')}")
        for msg in replayed:
            print(f"    Bob received [{msg['id']}]: {msg['text']}")

        print("\n  Bob reads all 4 replayed messages:")
        for mid in queued_ids:
            await relay.send_ack(mid, "read")
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
