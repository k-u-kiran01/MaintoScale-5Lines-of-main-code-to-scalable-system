#!/usr/bin/env bash
set -euo pipefail

# Beat 2: Slice one giant movie.mp4 into 4-second .ts chunks indexed by index.m3u8.
# Optional: if movie.mp4 does not exist, synthesize a 60s 1080p test clip first:
#   ffmpeg -f lavfi -i testsrc2=size=1920x1080:rate=30 -t 60 -c:v libx264 -pix_fmt yuv420p movie.mp4

INPUT="${1:-movie.mp4}"
OUT_DIR="${2:-hls_out}"
mkdir -p "$OUT_DIR"

# --- Exact Beat 2 command (fast stream copy, 4-second HLS segments) ---
ffmpeg -i "$INPUT" -c copy \
  -f hls -hls_time 4 \
  -hls_segment_filename "${OUT_DIR}/seg_%03d.ts" \
  "${OUT_DIR}/index.m3u8"

# --- Beat 3 extension: 3-tier Adaptive Bitrate (ABR) ladder + master.m3u8 ---
for tier in "1080p:1920x1080:5000k" "720p:1280x720:2500k" "480p:854x480:1000k"; do
  IFS=":" read -r name scale bitrate <<< "$tier"
  mkdir -p "${OUT_DIR}/${name}"
  ffmpeg -y -i "$INPUT" -vf "scale=${scale}" -c:v libx264 -b:v "$bitrate" \
    -g 120 -keyint_min 120 -sc_threshold 0 \
    -f hls -hls_time 4 -hls_playlist_type vod \
    -hls_segment_filename "${OUT_DIR}/${name}/seg_%03d.ts" \
    "${OUT_DIR}/${name}.m3u8"
done

cat > "${OUT_DIR}/master.m3u8" <<'EOF'
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080
1080p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720
720p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=854x480
480p.m3u8
EOF

echo "Generated HLS chunks and master.m3u8 inside ${OUT_DIR}/"
