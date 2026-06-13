# Architecture Blueprint
## Distributed Matching Engine Benchmarking Platform
**IICPC Summer Hackathon 2026**

---

## Overview

This platform evaluates contestant-submitted trading engines by containerising them, stress-testing with a 5-wave distributed bot fleet, and streaming per-wave live scores to a real-time leaderboard — updated every wave, not just at the end.

---

## System Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                            CONTESTANT                                │
│              uploads Linux x86-64 binary via browser UI              │
│              logs in with JWT (register / login)                     │
└────────────────────────────┬─────────────────────────────────────────┘
                             │  POST /upload  (Bearer JWT)
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    LEADERBOARD BACKEND  :8081                        │
│   /auth/register  /auth/login  — bcrypt + JWT                        │
│   /upload         — proxies binary to Sandbox, stores username meta  │
│   /ws             — WebSocket, pushes leaderboard JSON every 3s      │
│   /leaderboard    — HTTP snapshot of current scores                  │
│   /progress/{id}  — per-wave live chips for running contestants      │
│   /history/{id}   — last 20 scored runs for a contestant             │
└──────────┬───────────────────────────────────────────────────────────┘
           │  HTTP proxy
           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      SANDBOX ENGINE  :8080                           │
│   Dynamic port pool  8082–8181  (100 parallel slots, Redis set)      │
│   Docker run with  --cpus=1  --memory=256m  --pids-limit=128         │
│   Polls GET /orderbook until container is healthy (≤30s timeout)     │
│   Validates stdout for phantom fills via ORDER:/FILL: prefix         │
│   Publishes  {status:"running", contestant_id, target_url}           │
│              to Redis pub/sub channel  sandbox_logs                  │
│   Writes placeholder  {status:"running", overall_score:0}            │
│              to  leaderboard:scores  +  leaderboard:ranking(-1)      │
└──────────┬───────────────────────────────────────────────────────────┘
           │  Redis pub/sub  (sandbox_logs)
           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR                                  │
│   Subscribes to  sandbox_logs  channel                               │
│   On each  status=running  event:                                    │
│     1. Creates fresh per-contestant Redpanda topic                   │
│        (deletes previous run's topic first — idempotent)             │
│     2. Spawns  ./botfleet_bin  (pre-built binary, no compile delay)  │
│     3. After bot fleet exits, spawns  ./telemetry_bin                │
│   Container Reaper: background goroutine, scans every 60s,           │
│     force-stops any container alive > 35 minutes                     │
└──────────┬───────────────────────────────────────────────────────────┘
           │
     ┌─────┴──────────┐
     │                │
     ▼                ▼
┌─────────┐    ┌──────────────────────────────────────────────────────┐
│  REDIS  │    │                  BOT FLEET  (./botfleet_bin)         │
│         │◄───│  5-wave stress test, 100 bots, Go goroutines         │
│ leaderb │    │                                                      │
│ :scores │    │  Wave 1  Limit Orders    — order book insertion      │
│ leaderb │    │  Wave 2  Market Orders   — matching engine core      │
│ :ranking│    │  Wave 3  Cancel Orders   — O(1) cancel index         │
│ leaderb │    │  Wave 4  Mixed Sustained — all types, full load      │
│ :progre │    │  Wave 5  Chaos Testing   — zero price/qty, extremes  │
│ :history│    │                                                      │
│         │    │  Each wave ramps: 10% → 50% → 100% of max bots       │
│         │    │                                                      │
│         │    │  After EACH wave completes:                          │
│         │    │  ┌──────────────────────────────────────────────┐    │
│         │    │  │  pushWaveProgress()                          │    │
│         │    │  │  • Appends wave stats to leaderboard:progress│    │
│         │    │  │  • Computes running score from waves so far  │    │
│         │    │  │  • Writes live blob to leaderboard:scores    │    │
│         │    │  │  • Updates leaderboard:ranking ZSet          │    │
│         │    │  │  → Leaderboard shows real score mid-test     │    │
│         │    │  └──────────────────────────────────────────────┘    │
│         │    │                                                      │
└────┬────┘    │  All raw results pushed to per-contestant            │
     │         │  Redpanda topic  bot-results-{id}                    │
     │         └──────────────────────────────────────────────────────┘
     │                        │
     │                        ▼
     │         ┌──────────────────────────────────────────────────────┐
     │         │               REDPANDA  :19092                       │
     │         │  Per-contestant topic:  bot-results-{contestant_id}  │
     │         │  Topic deleted + recreated on each new run           │
     │         │  Decouples bot fleet from telemetry (fire-and-forget)│
     │         └──────────────────────────────────────────────────────┘
     │                        │
     │                        ▼
     │         ┌──────────────────────────────────────────────────────┐
     │         │           TELEMETRY INGESTER  (./telemetry_bin)      │
     │         │  Consumes all results from per-contestant topic      │
     │         │  HDR Histogram (hdrhistogram-go) — O(1) recording    │
     │         │  Computes per-wave: p50/p90/p99/p999/TPS/SR/correct  │
     │         │  Breaking point detection across 3 ramp steps        │
     │         │  Scoring formula applied precisely with p999         │
     │         │  Writes final score to leaderboard:scores + :ranking │
     │         │  Appends to leaderboard:history:{id}  (last 20 runs) │
     │         └──────────────────────────────────────────────────────┘
     │
     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      LEADERBOARD FRONTEND  :3000                     │
│  WebSocket pulls fresh leaderboard JSON every 3 seconds              │
│  Running card: shows live score updating after each wave             │
│  Finished card: shows final score, per-wave bar chart, breakdown     │
│  Stats bar: best score / best p99 / avg — excludes running entries   │
│  Score History chart: per-contestant score over time                 │
│  My Runs tab: last 20 runs for logged-in contestant                  │
│  How-to-Submit guide: C++ / Rust / Go compile commands               │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Submission Flow (step by step)

```
Contestant                Browser              Leaderboard Backend       Sandbox
    │                        │                        │                     │
    │──── register/login ────►│                       │                     │
    │◄─── JWT token ─────────│                        │                     │
    │                        │                        │                     │
    │──── upload binary ─────►│                       │                     │
    │                        │──── POST /upload ──────►│                     │
    │                        │     (Bearer JWT)        │──── proxy ─────────►│
    │                        │                        │     POST /upload     │
    │                        │                        │                     │── acquirePort()
    │                        │                        │                     │── docker run
    │                        │                        │                     │── waitForContainer()
    │                        │                        │                     │── validateLogs()
    │                        │                        │                     │── publish sandbox_logs
    │                        │                        │                     │── write placeholder
    │                        │                        │                     │   leaderboard:scores
    │                        │◄──── {status:running} ─│◄────────────────────│
    │◄── card appears with ──│                        │
    │    "Test in progress"  │
    │                        │
    │    (Orchestrator picks up sandbox_logs event)
    │    (Creates Redpanda topic, launches botfleet_bin)
    │
    │   After each wave:     │
    │◄── score updates live ─│◄── WebSocket ──────────│◄── Redis leaderboard:scores updated
    │    W1: 4270            │                        │    by pushWaveProgress()
    │    W2: 12817           │                        │
    │    W3: 15274           │                        │
    │    ...                 │                        │
    │                        │                        │
    │   (Orchestrator launches telemetry_bin after bot fleet exits)
    │   (Telemetry reads Redpanda, computes precise final score)
    │◄── final score ────────│◄── WebSocket ──────────│◄── Redis leaderboard:scores overwritten
    │    with p999, HDR hist │                        │    by telemetry, history appended
```

---

## Components

### 1. Leaderboard Backend (`:8081`)

New in this version — the backend now owns authentication and proxies uploads.

**Auth — JWT + bcrypt**
- `POST /auth/register` — hashes password with bcrypt, stores in `auth:user:{username}` Redis hash, returns signed JWT
- `POST /auth/login` — verifies bcrypt hash, returns JWT
- JWT secret configurable via `JWT_SECRET` env var
- All `/upload` requests require `Authorization: Bearer <token>`

**Upload proxy**
- Strips JWT, injects `contestant_id` (defaults to username), stores `username → contestant_id` mapping in Redis
- Proxies multipart binary to Sandbox at `SANDBOX_URL` (default `localhost:8080`)

**WebSocket leaderboard (`/ws`)**
- Pushes full leaderboard JSON every 3 seconds to all connected clients
- Reads `leaderboard:ranking` ZSet for ordering
- Reads `overall_score` and `status` from the JSON blob in `leaderboard:scores` — **not** from the ZSet score (which is a `-1` sentinel while running)
- Surfaces `status: "running"` to frontend so it renders the live card correctly

**Progress endpoint (`/progress/{id}`)**
- Reads `leaderboard:progress:{id}` from Redis
- Returns array of completed waves with p50/p99/SR/correctness/score per wave
- Called by frontend every WebSocket tick for all running contestants

**History endpoint (`/history/{id}`)**
- Reads `leaderboard:history:{id}` list (capped at 20 entries)
- Returns last 20 scored runs with timestamps and per-wave breakdown

---

### 2. Sandbox Engine (`:8080`)

**Dynamic port pool**
- Ports `8082–8181` (100 slots) stored as a Redis set (`sandbox:port_pool`)
- `acquirePort()` — atomic `SPOP` from the set; fails fast if all slots in use
- `releasePort()` — `SADD` back on container stop, failure, or reaper cleanup
- Port pool seeded once on startup, idempotent on restart

**Container lifecycle**
```
acquirePort()
  └── docker stop/rm contestant_{id}  (remove stale container if any)
  └── releasePort(old port)           (return previous port to pool)
  └── docker run -d
        --name contestant_{id}
        --memory=256m
        --cpus=1.0
        --security-opt=no-new-privileges
        --pids-limit=128
        -p {port}:8080
        -v {binary}:/app/contestant_bot:ro
        ubuntu:22.04 /app/contestant_bot
  └── waitForContainer()              (polls GET /orderbook, 500ms interval, 30s timeout)
  └── validateLogs()                  (phantom fill detection)
  └── publish sandbox_logs
  └── write placeholder to leaderboard:scores
```

**Placeholder written immediately on container start:**
```json
{
  "contestant_id": "stevie_x",
  "username": "stevie_x",
  "overall_score": 0.0,
  "overall_p99": 0.0,
  "status": "running",
  "waves": []
}
```
ZSet score set to `-1` so running contestants sort below all finished ones.

**Log validation (`validator.go`)**
Scans container stdout for `ORDER:` and `FILL:` prefixed lines. If `fills > orders`, the submission is flagged for phantom fills (accepting orders without processing them).

**Container security**
- `--no-new-privileges` — prevents privilege escalation
- `--pids-limit=128` — prevents fork bombs
- `--memory=256m` — hard memory cap
- `--cpus=1.0` — single core, fair comparison across contestants

---

### 3. Orchestrator

Bridges Sandbox events to bot fleet and telemetry. No HTTP server — pure event-driven.

**Event loop**
```
Subscribe to Redis channel: sandbox_logs
  │
  └── On message where status = "running":
        1. Read test_mode:{id} from Redis (blitz/standard/marathon, default=blitz)
        2. Sleep 2s  (container stabilisation grace period)
        3. ensureTopic(id)
              delete bot-results-{id}   (clears previous run's data)
              create bot-results-{id}   (fresh topic for this run)
        4. go runBotFleet(id, mode)
              exec ./botfleet_bin --mode {mode} --bots 100 --contestant {id} --topic bot-results-{id}
              (stdout/stderr piped to orchestrator terminal)
        5. After bot fleet exits → runTelemetry(id, expected, topic)
              exec ./telemetry_bin --contestant {id} --topic bot-results-{id} --timeout 300
```

**Pre-built binaries** — orchestrator calls `./botfleet_bin` and `./telemetry_bin`, not `go run`. This eliminates Go compilation delay (~3s) from the critical path on every submission.

Build once:
```bash
cd bot-fleet  && go build -o ../orchestrator/botfleet_bin  .
cd telemetry  && go build -o ../orchestrator/telemetry_bin .
```

**Container Reaper**
Background goroutine, ticks every 60 seconds. Scans all `sandbox:*:port` Redis keys. Any container whose key TTL has dropped below `(30min - 35min threshold)` is force-stopped and its port returned to the pool. Prevents resource leaks from abandoned submissions.

---

### 4. Bot Fleet — 5-Wave Stress Test

**The 5 waves**

| Wave | Order Type | What it stresses |
|------|-----------|-----------------|
| 1 | Limit Orders | Price level data structure — insertion at O(log N) or better |
| 2 | Market Orders | Matching engine core — execution loop throughput |
| 3 | Cancel Orders | Order lookup index — must be O(1) hash map, not O(N) scan |
| 4 | Mixed Sustained | Real market simulation — all order types under full concurrent load |
| 5 | Chaos Testing | Resilience — zero price, zero qty, extreme price, huge qty, invalid combos |

**Ramp pattern within each wave**
```
Step 1:  10% of max bots  →  baseline latency (light load)
Step 2:  50% of max bots  →  mid-load degradation check
Step 3: 100% of max bots  →  peak load, breaking point detection
```

**Live score push after every wave (`pushWaveProgress`)**

This is the key addition for live leaderboard scores. After each wave:

1. Reads existing `leaderboard:progress:{id}` array from Redis
2. Appends the completed wave's stats
3. Computes a per-wave score using the same formula as telemetry:
   ```
   wave_score = (1000 / (p99 + 1))² × (success_rate / 100) × (correctness / 100)
   ```
   (p999 is proxied as p99 here; telemetry will replace with the precise HDR value)
4. Sums all completed wave scores into `running_total`
5. Writes full live blob to `leaderboard:scores` and updates `leaderboard:ranking` ZSet
6. Frontend picks this up within 3 seconds via WebSocket → score updates live

**Per-bot flow**
```
generate order (type = wave's order type, random side/price/qty)
record send_timestamp (nanosecond via time.Now())
HTTP POST /order
record receive_timestamp
latency = receive_timestamp - send_timestamp
if limit order: GET /orderbook → validate best bid/ask within range
push Result{bot_id, order_type, latency_ns, success, correct, wave} to Redpanda
```

**Additional checks run before the 5 waves:**
- `GET /orderbook/depth` — checks if the engine supports depth endpoint (optional, skipped if absent)
- Price-time priority test — places two orders at same price, verifies earlier arrival fills first (requires engine to return `filled_order_id` in response)

---

### 5. Redpanda (`:19092` external / `:9092` internal)

Per-contestant isolated topics — `bot-results-{contestant_id}`.

The orchestrator deletes and recreates the topic before each run so stale data from a previous submission never contaminates scoring. Bot fleet producers write to it; telemetry consumes from it. The two never communicate directly.

**Why Redpanda over Kafka:**
- No ZooKeeper — single binary, simpler ops
- No JVM — written in C++, lower memory and tail latency
- Drop-in Kafka API — franz-go client works without changes

---

### 6. Telemetry Ingester

Consumes all `bot-results-{id}` messages and produces the final authoritative score.

**HDR Histogram**
Uses `hdrhistogram-go` — O(1) per-recording, ~40KB memory regardless of result count. Produces accurate p50/p90/p99/p999 from millions of samples.

**Scoring formula (per wave)**
```
wave_score = (1000 / (p99_ms + 1)) × (1000 / (p999_ms + 1)) × success_rate × correctness
final_score = sum of all wave scores
```

This rewards:
- Low p99 — 1ms scores ~1000, 100ms scores ~10
- Low p999 — tail latency matters; a single outlier hurts
- High success rate — dropping orders under load is penalised
- High correctness — accepting but not processing is penalised

**Breaking point detection**
For each wave, telemetry scans the three ramp steps (10% / 50% / 100%) and classifies:
```
✓ Stable across all load levels (p99 < 50ms throughout)
⚠ Stable up to N bots, degradation starts at M bots
✗ Stable up to N bots, breaking point at M bots (p99 spike / errors)
```

**Redis writes after scoring**
```
HSet leaderboard:scores   {contestant_id}  {full JSON with per-wave breakdown}
ZAdd leaderboard:ranking  score={final_score}  member={contestant_id}
LPush leaderboard:history:{id}  {timestamp, overall_score, waves}
LTrim leaderboard:history:{id}  0 19   (keep last 20 runs)
```

---

### 7. Leaderboard Frontend (`:3000`)

Served via `python3 -m http.server 3000` from `leaderboard/frontend/`.

**Live score during a test**
- Running contestant card shows actual running score (sum of completed waves so far) — not `—`
- Score label reads "Test in progress... (live score)"
- Wave chips update after each wave: grey `pending` → green `done` with p99 and SR values
- Refreshes on every WebSocket tick (every 3 seconds)

**Finished contestant card**
- Final score, success rate, orders tested
- "Stable / degrades / breaks under load" badge
- Per-wave bar chart (P50 vs P99) via Chart.js
- Full wave breakdown table (expandable)
- P99 distribution histogram

**Stats bar**
- Only counts finished contestants — running contestants (score = 0) are excluded from best score and average to avoid distorting the display

**My Runs tab**
- Shows last 20 scored runs for the logged-in contestant's ID
- Loaded from `GET /history/{id}`

**Score History chart**
- Tracks per-contestant score over the session
- Only plots finished contestants

**How-to-Submit guide (collapsible)**
- API contract contestants must implement
- Compile commands for C++, Rust, and Go
- Common mistakes (forgetting `-static`, wrong port, no concurrency, crashing on chaos orders)

**Auth flow**
- Register / Login tabs → stores JWT in `localStorage`
- Upload button requires valid JWT; backend validates on every upload
- Contestant ID defaults to username if left blank

---

## Data Stores

| Store | Keys / Structure | Purpose |
|---|---|---|
| Redis | `leaderboard:ranking` (ZSet) | Ordered by score; `-1` sentinel for running |
| Redis | `leaderboard:scores` (Hash) | Full JSON blob per contestant |
| Redis | `leaderboard:progress:{id}` (String) | Per-wave live progress array |
| Redis | `leaderboard:history:{id}` (List) | Last 20 scored runs, capped by LTrim |
| Redis | `sandbox:port_pool` (Set) | Available ports 8082–8181 |
| Redis | `sandbox:{id}:port` (String, TTL 30m) | Active port for a running container |
| Redis | `sandbox:{id}:status` (String, TTL 30m) | "running" while container is live |
| Redis | `test_mode:{id}` (String, TTL 24h) | blitz / standard / marathon |
| Redis | `contestant:{id}:meta` (Hash) | username mapping |
| Redis | `auth:user:{username}` (Hash) | bcrypt hash + created_at |
| Redpanda | `bot-results-{id}` (topic) | Raw per-bot results; recreated each run |
| TimescaleDB | (available, not yet wired) | Historical metrics archive |

---

## Infrastructure

Spun up with a single command:
```bash
cd infra && docker compose up -d
```

| Service | Image | Ports | Health check |
|---|---|---|---|
| redpanda | redpandadata/redpanda:latest | 9092 (internal), 19092 (external), 9644 | `rpk cluster info` |
| redis | redis:alpine | 6379 | `redis-cli ping` |
| timescaledb | timescale/timescaledb:latest-pg15 | 5432 | `pg_isready` |

---

## Running the Platform

Start services in this order:

```
Tab 1 — infra
cd infra && docker compose up -d

Tab 2 — sandbox (manages contestant containers)
cd sandbox && go run main.go validator.go

Tab 3 — leaderboard backend (API + WebSocket)
cd leaderboard/backend && go run main.go

Tab 4 — orchestrator (runs bot fleet + telemetry after each upload)
cd orchestrator && go run main.go

Tab 5 — frontend
cd leaderboard/frontend && python3 -m http.server 3000
→ open http://localhost:3000
```

**One-time binary build** (required before first run or after any bot-fleet / telemetry code change):
```bash
cd bot-fleet && go build -o ../orchestrator/botfleet_bin .
cd telemetry  && go build -o ../orchestrator/telemetry_bin .
```

---

## Scoring Formula Reference

```
Per-wave score:
  wave_score = (1000 / (p99_ms + 1)) × (1000 / (p999_ms + 1)) × success_rate × correctness

  where success_rate and correctness are fractions (0.0–1.0)

Final score:
  final_score = Σ wave_scores  (sum of all 5 waves)

Live score during test (bot-fleet approximation):
  live_wave_score = (1000 / (p99_ms + 1))² × success_rate × correctness
  live_score = Σ live_wave_scores  (updated after each wave)
  Note: p999 not available mid-test; telemetry overwrites with precise value at end
```

| p99 | score factor |
|-----|-------------|
| 1ms | ~500 |
| 5ms | ~28,000 |
| 10ms | ~8,300 |
| 50ms | ~384 |
| 100ms | ~99 |
| 500ms | ~4 |

---

## Known Limitations and Future Work

### Coordinated Omission
Latency is measured at the HTTP client level (application latency), not wire-to-wire. TCP setup and HTTP overhead are included. True exchange latency measurement requires kernel bypass (DPDK, io_uring) from NIC to NIC.

### Correctness Validation Round Trip
The GET /orderbook correctness check after each limit order adds an extra HTTP round trip inside the latency measurement window. A cleaner design would run a separate validation pass after the load test, keeping latency measurement tight.

### Statistical Sample Size
P99 is statistically meaningful at ≥1000 samples per wave. Blitz mode (100 bots) produces ~160 samples per wave — telemetry warns when below threshold. Run with `--bots 1000` (standard/marathon modes) for reliable percentiles.

### Parallel Testing
The platform currently tests one contestant sequentially per orchestrator. True parallel testing requires Kubernetes pod-per-contestant isolation and Redpanda consumer groups. Manifests are in `infra/k8s/`.

### TimescaleDB
Wired into the infra compose file but not yet connected to telemetry. Intended for historical metrics archive and time-series latency queries across many runs.

### Cross-Architecture Compilation
Contestant binaries must be Linux x86-64. The sandbox runs Ubuntu 22.04 containers. Mac ARM binaries (Apple Silicon) will not run. A future improvement would auto-detect and cross-compile server-side.

### Price-Time Priority Verification
Current correctness check validates orderbook consistency after each order. Strict price-time priority verification requires engines to return `filled_order_id` in the `/order` response — currently optional and logged as a warning if absent.

---

## Team

| Member | Component |
|---|---|
| Upanshu Smit (stevie-x) | Bot Fleet · Telemetry Ingester · Live Score Pipeline |
| Abhisoumya Kapoor (The-Asterix) | Submission & Sandboxing Engine · Port Pool · Container Reaper |
| Shubhayu Brahmachari (Snoob965) | Leaderboard Frontend · Backend API · Auth · IaC |
