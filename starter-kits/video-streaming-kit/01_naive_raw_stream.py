"""Beat 1: The 4-line naive raw-MP4 server + buffer/crash benchmark.

Run:
    python 01_naive_raw_stream.py
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
from dataclasses import dataclass

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


@dataclass
class Response:
    body: bytes
    media_type: str


# --- Exact 4-line main() from Beat 1 ---------------------------------------------------------
def main(path="movie.mp4"):
    with open(path, "rb") as f:
        data = f.read()
    return Response(data, "video/mp4")


def demonstrate_four_line_main() -> None:
    """Execute the exact 4-line main() safely on a temporary sample file, then clean up."""
    sample_bytes = b"\x00\x00\x00\x18ftypmp42" + b"VIDEO_FRAME" * 4096
    fd, tmp_path = tempfile.mkstemp(prefix="movie_", suffix=".mp4")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(sample_bytes)
        t0 = time.perf_counter()
        resp = main(tmp_path)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        print(
            f"Demo main() read {len(resp.body):,} bytes ({resp.media_type}) "
            f"into RAM in {elapsed_ms:.2f} ms before sending byte 1.\n"
        )
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def benchmark_raw_mp4_meltdown() -> None:
    print("=== Beat 1: 4-Line main() Streaming Raw 4 GB movie.mp4 ===\n")
    demonstrate_four_line_main()

    movie_gb = 4.0
    movie_bits = movie_gb * 8.0 * 1000.0  # 32,000 megabits (decimal GB)
    origin_nic_mbps = 10_000.0  # 10 Gbps single origin server uplink
    origin_ram_gb = 32.0
    fast_wifi_mbps = 100.0

    print(
        f"Origin server specs: 1 box | {origin_ram_gb:.0f} GB RAM | "
        f"{origin_nic_mbps / 1000:.0f} Gbps NIC | File: {movie_gb:.0f} GB movie.mp4\n"
    )

    for viewers in (1, 100, 1_000, 10_000):
        ram_needed_gb = viewers * movie_gb
        per_viewer_mbps = min(fast_wifi_mbps, origin_nic_mbps / viewers)
        full_download_sec = movie_bits / per_viewer_mbps
        # Progressive MP4 moov/buffer stall estimate before steady playback
        if viewers == 1:
            status = "200 OK (Fast Wi-Fi)"
            wait_str = "0.6s start"
        elif ram_needed_gb > origin_ram_gb:
            status = "502 CRASH (OOM + Pipe Choked)"
            mins = int(full_download_sec // 60)
            wait_str = f"~{mins:,}m buffer -> OOM"
        else:
            status = "BUFFERING"
            wait_str = f"{full_download_sec:.1f}s"

        print(
            f"Viewers = {viewers:>6,} | RAM needed = {ram_needed_gb:>8,.0f} GB | "
            f"Share/viewer = {per_viewer_mbps:>6.2f} Mbps | "
            f"Wait = {wait_str:<20} | {status}"
        )

    print(
        "\nTakeaway: f.read() loads the entire 4 GB movie into RAM per request. "
        "It works for 1 viewer on fast Wi-Fi, buffers for minutes under crowd load, "
        "and crashes with 502 OOM when thousands press play."
    )


if __name__ == "__main__":
    benchmark_raw_mp4_meltdown()
