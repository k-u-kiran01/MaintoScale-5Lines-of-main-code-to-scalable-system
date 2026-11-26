"""Beat 2 & Beat 3: HLS 4-second .ts chunking (.m3u8) + Adaptive Bitrate (ABR).

Run:
    python 02_hls_chunking_and_abr.py
"""

from __future__ import annotations

import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Bitrate ladder (Beat 3)
TIERS = {
    "1080p.m3u8": {"label": "1080p", "bandwidth": 5_000_000, "mbps": 5.0, "resolution": "1920x1080"},
    "720p.m3u8": {"label": "720p", "bandwidth": 2_500_000, "mbps": 2.5, "resolution": "1280x720"},
    "480p.m3u8": {"label": "480p", "bandwidth": 1_000_000, "mbps": 1.0, "resolution": "854x480"},
}


# --- Exact Beat 3 ABR tier selector ---------------------------------------------------------
def pick_tier(mbps: float) -> str:
    if mbps >= 5:
        return "1080p.m3u8"
    if mbps >= 2:
        return "720p.m3u8"
    return "480p.m3u8"


def build_master_playlist() -> str:
    """Build a valid RFC 8216 HLS master playlist referencing 1080p, 720p, and 480p."""
    lines = ["#EXTM3U", "#EXT-X-VERSION:3"]
    for playlist, meta in TIERS.items():
        lines.append(
            f"#EXT-X-STREAM-INF:BANDWIDTH={meta['bandwidth']},RESOLUTION={meta['resolution']}"
        )
        lines.append(playlist)
    return "\n".join(lines) + "\n"


def build_media_playlist(tier_playlist: str, num_chunks: int = 15, chunk_sec: int = 4) -> str:
    """Build a valid RFC 8216 media playlist indexing 4-second .ts chunks."""
    tier_name = tier_playlist.replace(".m3u8", "")
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{chunk_sec}",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXT-X-PLAYLIST-TYPE:VOD",
    ]
    for idx in range(1, num_chunks + 1):
        lines.append(f"#EXTINF:{chunk_sec:.3f},")
        lines.append(f"{tier_name}/seg_{idx:03d}.ts")
    lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines) + "\n"


def demo_hls_startup_speedup() -> None:
    print("=== Beat 2: Slice 4 GB movie.mp4 -> 4-Second .ts Chunks + .m3u8 ===\n")
    master = build_master_playlist()
    media_1080 = build_media_playlist("1080p.m3u8", num_chunks=6)

    print("Generated master.m3u8:")
    for line in master.strip().splitlines():
        print(f"  {line}")

    print("\nGenerated 1080p.m3u8 (first 6 chunks):")
    for line in media_1080.strip().splitlines()[:11]:
        print(f"  {line}")
    print("  ...")

    # Startup math: 1080p @ 5 Mbps * 4s = 20 megabits = 2.5 MB first chunk vs 4,000 MB monolith
    chunk_mb = (5.0 * 4.0) / 8.0
    wifi_mbps = 50.0
    startup_sec = (chunk_mb * 8.0) / wifi_mbps
    monolith_sec = (4_000.0 * 8.0) / wifi_mbps
    print(
        f"\nStartup comparison on 50 Mbps Wi-Fi:\n"
        f"  Raw 4 GB movie.mp4 : {monolith_sec:6.1f}s to fetch whole file\n"
        f"  First 4s .ts chunk : {startup_sec:6.2f}s ({chunk_mb:.1f} MB) -> starts in under a second!\n"
    )


def demo_train_tunnel_abr() -> None:
    print("=== Beat 3: Train Tunnel 5G -> 3G Dip (Fixed 1080p vs ABR Tiers) ===\n")

    # 15 four-second chunks = 60-second commute entering & exiting a train tunnel
    commute_mbps = [
        18.0, 16.5, 12.0, 6.5,        # Open air 5G
        3.6, 3.1, 2.6,                # Entering tunnel (4G/mid dip)
        1.4, 1.1, 1.3, 1.7,           # Deep tunnel (3G)
        2.8, 4.2, 8.5, 15.0,          # Exiting tunnel back to 5G
    ]

    fixed_stalls = 0
    abr_stalls = 0

    for i, mbps in enumerate(commute_mbps, start=1):
        chosen = pick_tier(mbps)
        abr_need = TIERS[chosen]["mbps"]
        abr_dl_sec = (abr_need * 4.0) / mbps
        fixed_dl_sec = (5.0 * 4.0) / mbps

        fixed_ok = fixed_dl_sec <= 4.0
        abr_ok = abr_dl_sec <= 4.0
        if not fixed_ok:
            fixed_stalls += 1
        if not abr_ok:
            abr_stalls += 1

        zone = "5G" if mbps >= 5 else ("TUNNEL 4G" if mbps >= 2 else "TUNNEL 3G")
        print(
            f"seg_{i:03d}.ts | {zone:<9} ({mbps:>4.1f} Mbps) | "
            f"Fixed 1080p: {fixed_dl_sec:4.2f}s {'OK   ' if fixed_ok else 'STALL'} | "
            f"ABR -> {TIERS[chosen]['label']:<5} ({abr_dl_sec:4.2f}s {'OK' if abr_ok else 'STALL'})"
        )

    print(
        f"\nResult over 60s train tunnel commute: "
        f"Fixed 1080p stalled {fixed_stalls} times | ABR stalled {abr_stalls} times.\n"
    )


if __name__ == "__main__":
    demo_hls_startup_speedup()
    demo_train_tunnel_abr()
