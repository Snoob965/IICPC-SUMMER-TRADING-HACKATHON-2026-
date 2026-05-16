# Architecture Blueprint
## Distributed Matching Engine Benchmarking Platform
**IICPC Summer Hackathon 2026**

---

## Overview

This platform evaluates contestant-submitted trading infrastructure by containerizing their matching engines, stress-testing them with a distributed bot fleet, and scoring them on latency, throughput, and correctness — streamed to a live leaderboard.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CONTESTANT                               │
│                    uploads binary/source                        │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                  SANDBOX ENGINE                                 │
│         Docker container with CPU pinning + memory limits       │
│         Exposes: POST /order  DELETE /order/:id  GET /orderbook │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                   BOT FLEET (Go)                                │
│         1000 concurrent goroutines                              │
│         60% limit orders · 30% market orders · 10% cancels     │
│         Measures per-request latency (send → response)         │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                     REDPANDA                                    │
│              Topic: bot-results                                 │
│         Decouples bot fleet from telemetry ingester             │
│         Handles millions of events/sec, no JVM overhead         │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│               TELEMETRY INGESTER (Go)                           │
│         Consumes from Redpanda topic                            │
│         Computes: p50 / p90 / p99 latency                      │
│         Computes: TPS, success rate, final score               │
│         Score = (1000 / (p99 + 1)) × (success_rate / 100)     │
└──────────────┬──────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│            REDIS                 │
│  leaderboard:ranking  (sorted set) ← ranked by score           │
│  leaderboard:scores   (hash)     ← full JSON per contestant    │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│       LEADERBOARD FRONTEND       │
│   WebSocket stream of live scores│
│   p50 / p99 / TPS / score table  │
└──────────────────────────────────┘
```

---

## Components

### 1. Submission & Sandboxing Engine

Contestants upload their matching engine source code or binary. The platform:

- Compiles the submission inside a pre-built Docker base image
- Runs the binary in a strictly isolated container with `--cpus=1` and `--memory=512m`
- Uses a **warm container pool** — containers are pre-initialized and waiting, so startup latency is ~50ms instead of ~3 seconds from cold start
- Exposes the contestant's engine on an internal port via a predefined REST API contract

**API Contract every contestant must implement:**
```
POST   /order          — place limit or market order
DELETE /order/:id      — cancel order
GET    /orderbook      — current best bid/ask
```

---

### 2. Distributed Bot Fleet

Built in **Go** using goroutines. A single Go service spawns 1000 concurrent goroutines, each simulating an independent market participant.

**Why Go over C++ or Python:**
- Go goroutines are not OS threads — the Go scheduler multiplexes them onto a small thread pool, allowing 100,000+ concurrent goroutines on a single machine without kernel-level context switching overhead
- Python threads are limited by the GIL. C++ concurrent WebSocket clients require significant boilerplate (Boost.Beast + asio)

**Order mix per run (realistic market simulation):**
- 60% limit orders
- 30% market orders
- 10% cancel orders

**Per-bot flow:**
1. Generate random order (type, side, price, quantity)
2. Record `send_timestamp`
3. HTTP POST to contestant's `/order` endpoint
4. Record `receive_timestamp`
5. Push `Result{bot_id, order_type, latency_ns, success}` to Redpanda

**Measured results on dummy target:**
- 1000 bots: 1000/1000 success, avg latency 56ms
- Redpanda push: 1000 messages in <100ms

---

### 3. Redpanda Message Queue

**Why Redpanda over Kafka:**
- No ZooKeeper dependency — Kafka requires a separate ZooKeeper cluster just to manage itself
- No JVM — Redpanda is written in C++, significantly lower memory footprint and tail latency
- Drop-in Kafka API compatibility — any Kafka client works without code changes
- Single binary deployment

**Topic:** `bot-results`  
**Message format:**
```json
{
  "bot_id": 127,
  "order_type": "limit",
  "latency_ns": 24673625,
  "success": true
}
```

Redpanda decouples the bot fleet from the telemetry ingester. The bots never wait for scoring to complete — they fire and forget into the topic. This keeps the load generator on the hot path and the analytics completely async.

---

### 4. Telemetry Ingester

Consumes all messages from the `bot-results` Redpanda topic and computes:

**Latency percentiles** (industry standard measurement):
- **P50** — median latency, typical experience
- **P90** — 90th percentile
- **P99** — worst-case latency that actually matters (averages are misleading under load)

**Why percentiles over averages:**  
If 990 orders take 1ms and 10 orders take 5000ms, the average is ~51ms — completely misleading. P99 correctly surfaces the 5000ms worst case.

**Scoring formula:**
```
score = (1000 / (p99_ms + 1)) × (success_rate / 100)
```

This rewards:
- Low p99 latency — a 1ms p99 scores ~10x higher than a 100ms p99
- High success rate — engines that drop orders under load are penalized

**Output — two Redis writes:**
- `leaderboard:ranking` — Redis sorted set, automatically ordered by score. `ZREVRANGE leaderboard:ranking 0 -1 WITHSCORES` returns full ranking instantly
- `leaderboard:scores` — Redis hash, full JSON breakdown per contestant

---

### 5. Real-Time Leaderboard

Reads from Redis sorted set and streams updates to a WebSocket-connected frontend. The leaderboard never touches Redpanda or TimescaleDB directly — it only reads pre-computed scores from Redis, which handles 100,000+ reads/second at microsecond latency.

**Displayed metrics per contestant:**
- P50 / P90 / P99 latency
- Success rate
- TPS (transactions per second)
- Composite score
- Live rank

---

## Data Stores

| Store | Purpose | Why |
|---|---|---|
| Redpanda | Raw metrics pipeline | High throughput, decoupled, async |
| Redis | Leaderboard scores | Sub-millisecond reads, sorted sets built-in |
| TimescaleDB | Historical metrics archive | Time-series queries, Postgres-compatible |

---

## Infrastructure as Code

All services defined in `infra/docker-compose.yml` for local development. Production deployment via Kubernetes manifests in `infra/k8s/`.

**Services:**
- `redpanda` — message queue on port 9092
- `redis` — leaderboard store on port 6379
- `timescaledb` — metrics archive on port 5432

Spin up entire platform locally:
```bash
cd infra
docker compose up -d
```

---

## Key Architectural Decisions

| Decision | Alternative | Reason |
|---|---|---|
| Go for bot fleet | C++ / Python | Goroutines handle 1000+ concurrent bots trivially |
| Redpanda over Kafka | Kafka | No ZooKeeper, no JVM, lower tail latency |
| Redis sorted set for leaderboard | DB query | O(log N) insert, O(1) range read, never blocks |
| Warm container pool | Cold start | 50ms vs 3000ms container startup |
| Percentile metrics | Averages | Averages hide worst-case behavior under load |

---

## Team

| Member | Component |
|---|---|
| Upanshu (stevie-x) | Bot Fleet + Telemetry Ingester |
| Teammate 2 | Submission & Sandboxing Engine |
| Teammate 3 | Leaderboard Frontend + IaC |