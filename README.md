# Main To Scale — 5 Lines of Main Code to Scalable System

> **Every massive system started as 5 lines of code.**

Companion developer starter kits for the **Main To Scale** YouTube Shorts & Instagram Reels series.

- **Starter Kits Hub (GitHub Pages):** https://k-u-kiran01.github.io/MaintoScale-5Lines-of-main-code-to-scalable-system/ (`./starter-kits/index.html`)
- **Live Starter Kit (`RIDE`):** [Real-Time Ride Matcher (`starter-kits/ride-matcher-kit/`)](./starter-kits/ride-matcher-kit/) · [Download `.zip`](./starter-kits/ride-matcher-starter-kit.zip)
- **Live Starter Kit (`SCALE`):** [2M-Fan Ticket Drop Scaling (Part 2) (`starter-kits/ticket-scale-kit/`)](./starter-kits/ticket-scale-kit/) · [Download `.zip`](./starter-kits/ticket-scale-starter-kit.zip)
- **Live Starter Kit (`CHAT`):** [WhatsApp-Style Real-Time Chat (`starter-kits/whatsapp-chat-kit/`)](./starter-kits/whatsapp-chat-kit/) · [Download `.zip`](./starter-kits/whatsapp-chat-starter-kit.zip)

---

## Starter Kits Lineup

| Comment Keyword | Episode | Core Stack | Status |
| --- | --- | --- | --- |
| `RIDE` | Real-Time Ride Matcher (Uber & Zomato) | Python · Uber H3 v4 · Redis Geo & 15s Locks · FastAPI WebSockets | **Live** ([Open Kit](./starter-kits/ride-matcher-kit/)) |
| `TICKET` | Concert Sellout Ticket Backend | Python · Load Balancer · Redis Cache · Message Queue · Row Locks · Autoscaling | Coming Soon |
| `SCALE` | 2M-Fan Ticket Drop Scaling (Part 2) | PgBouncer · Postgres Read Replicas · Event Sharding · Edge Waiting Room | **Live** ([Open Kit](./starter-kits/ticket-scale-kit/)) |
| `GIT` | Build Mini-Git From Scratch | Python · SHA-256 Blobs · Commit DAG · Branch Pointers · 3-Way Merge Base | Coming Soon |
| `CHAT` | WhatsApp-Style Real-Time Chat | Python · Persistent WebSockets · Redis Offline Queue · Receipts · E2E Encryption | **Live** ([Open Kit](./starter-kits/whatsapp-chat-kit/)) |
| `STREAM` | Netflix & YouTube Video Streaming | Python · HLS 4s Chunking · Adaptive Bitrate (ABR) · Edge CDN · Prefetch Buffer | Coming Soon |
| `AUDIO` | Shazam Audio Fingerprinting | Python · FFT Spectrogram · Peak Constellation · Anchor-Target Hashes · Inverted Index | Coming Soon |
| `LIMIT` | Production API Rate Limiter | Python · Redis Counters · Token Bucket · Atomic Lua Scripts · Retry-After | Coming Soon |

---

## Repository Layout

- `starter-kits/index.html` & `starter-kits/kits.json` — Zero-build-step GitHub Pages starter kit hub (edit `REPO_BASE_URL` on line 2 of `kits.json` to point all kit links at your fork or repo).
- `starter-kits/ride-matcher-kit/` — Runnable 4-step Bengaluru (`12.97, 77.59`) ride-matching codebase (`01_naive_loop.py` → `02_h3_hex_and_kring.py` → `03_redis_geo_dispatch.py` → `04_live_ws_server.py` + `docker-compose.yml` + automatic `fakeredis` fallback).
- `starter-kits/ride-matcher-starter-kit.zip` — Downloadable archive of the `ride-matcher-kit` codebase.
- `starter-kits/ticket-scale-kit/` — Runnable 5-beat 2M-fan ticket scaling codebase (`01_autoscaler_ddos_and_pgbouncer.py` → `02_read_replicas.py` → `03_event_sharding.py` → `04_live_scale_server.py` + `pgbouncer.ini` + `edge_gate.ts` + `docker-compose.yml` + automatic `fakeredis` / temp SQLite fallback + browser Edge Queue/Sharding dashboard).
- `starter-kits/ticket-scale-starter-kit.zip` — Downloadable archive of the `ticket-scale-kit` codebase.
- `starter-kits/whatsapp-chat-kit/` — Runnable 4-step WhatsApp-style real-time chat codebase (`01_naive_polling.py` → `02_redis_offline_queue_and_acks.py` → `03_e2e_encryption.py` → `04_live_chat_server.py` + `docker-compose.yml` + automatic `fakeredis` fallback + browser E2EE/queue/sniffer dashboard).
- `starter-kits/whatsapp-chat-starter-kit.zip` — Downloadable archive of the `whatsapp-chat-kit` codebase.
- `index.html` & `.nojekyll` — Clean root redirect to `./starter-kits/` for GitHub Pages deployment from `main / (root)`.
