"""Beat 1: The 5-line naive chat list + 1-second polling meltdown benchmark.

Run:
    python 01_naive_polling.py
"""

from __future__ import annotations

import json
import sys
import time

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# --- The exact 5-line main() loop from Beat 1 ----------------------------------------------
inbox: list[tuple[str, str]] = []


def main(user: str, text: str | None = None) -> list[tuple[str, str]]:
    if text:
        inbox.append((user, text))
    return inbox[-20:]


def benchmark_polling() -> None:
    print("=== Beat 1: 5-Line main() + 1-Second Polling Meltdown ===\n")

    # Warm up with two friends chatting
    inbox.clear()
    main("alice", "Hey! Free at 9?")
    main("bob", "Yep, see you then!")
    print(f"2 friends demo -> main('alice') returns last {len(main('alice'))} messages: {main('alice')}\n")

    # Seed 20 recent chat messages in memory
    for i in range(20):
        main(f"user_{i % 2}", f"msg #{i + 1}")

    sample_polls = 10_000
    t0 = time.perf_counter()
    bytes_per_poll = 0
    for _ in range(sample_polls):
        payload = json.dumps(main("poller"))
        bytes_per_poll = len(payload.encode("utf-8")) + 220  # + ~220 B HTTP/1.1 headers
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    ms_per_poll = elapsed_ms / sample_polls

    # Suppose each active chat averages 1 real message every 60s -> 59/60 polls return nothing new
    wasted_pct = (59.0 / 60.0) * 100.0

    for chats in (2, 10_000, 1_000_000):
        req_per_sec = chats  # 1 poll/sec per active chat
        cpu_ms_per_sec = req_per_sec * ms_per_poll
        cores_saturated = cpu_ms_per_sec / 1000.0
        bandwidth_mb_s = (req_per_sec * bytes_per_poll) / (1024 * 1024)
        phone_battery_note = "95% -> 94%/hr" if chats <= 2 else "95% -> 12% (radio never sleeps)"

        print(
            f"Chats = {chats:>10,} | {req_per_sec:>10,} req/s | "
            f"wasted empty polls={wasted_pct:5.2f}% | "
            f"wire={bandwidth_mb_s:8.2f} MB/s | "
            f"est. Python CPU={cores_saturated:6.1f} cores | battery: {phone_battery_note}"
        )

    print(
        "\nTakeaway: polling every second works for two friends, "
        "but at 1,000,000 chats 98.3% of traffic is empty busywork that drains batteries and melts servers."
    )


if __name__ == "__main__":
    benchmark_polling()
