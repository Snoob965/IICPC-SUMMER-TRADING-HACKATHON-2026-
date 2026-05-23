# Architecture Blueprint
## Distributed Matching Engine Benchmarking Platform
**IICPC Summer Hackathon 2026**

---

## Overview

This platform evaluates contestant-submitted trading infrastructure by containerizing their matching engines, stress-testing them with a 4-wave distributed bot fleet, and scoring them on latency, throughput, and correctness — streamed to a live leaderboard.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CONTESTANT                               │
│                   uploads binary/source                         │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                        SANDBOX ENGINE                           │
│         Docker container with CPU pinning + memory limits       │
│               Exposes: POST /order  GET /orderbook              │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                        BOT FLEET (Go)                           │
│             4-wave stress test with ramping load                │
│        Wave 1: Limit Orders  (10% → 50% → 100% of max bots)     │
│        Wave 2: Market Orders (10% → 50% → 100% of max bots)     │
│        Wave 3: Cancel Orders (10% → 50% → 100% of max bots)     │
│        Wave 4: Mixed Sustained (all order types, full load)     │
│        Per-bot: measures latency + correctness validation       │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                           REDPANDA                              │
│                      Topic: bot-results                         │
│            Decouples bot fleet from telemetry ingester          │
│          Handles millions of events/sec, no JVM overhead        │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                     TELEMETRY INGESTER (Go)                     │
│                  Consumes from Redpanda topic                   │
│      Computes per wave: p50 / p90 / p99 / TPS / correctness     │
│               Latency histogram across all waves                │
│                Breaking point detection per wave                │
│        Score = (1000/(p99+1)) × success_rate × correctness      │
└──────────────┬──────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│              REDIS               │
│ leaderboard:ranking (sorted set) │
│   leaderboard:scores  (hash)     │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│       LEADERBOARD FRONTEND       │
│  WebSocket stream of live scores │
│    Per-wave breakdown + charts   │
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
POST   /order      — place order (type: "limit", "market", or "cancel")
GET    /orderbook  — current best bid/ask
```

**Order payload:**
```json
{
  "bot_id": 1,
  "type": "limit",
  "side": "buy",
  "price": 1820.50,
  "quantity": 3
}
```

---

### 2. Distributed Bot Fleet — 4-Wave Stress Test

Built in **Go** using goroutines. A single Go service runs a structured 4-wave stress test, each wave isolating a specific order type with ramping concurrency.

**Why Go over C++ or Python:**
- Go goroutines are not OS threads — the Go scheduler multiplexes them onto a small thread pool, allowing 100,000+ concurrent goroutines on a single machine without kernel-level context switching overhead
- Python threads are limited by the GIL
- C++ concurrent HTTP clients require significant boilerplate (Boost.Beast + asio)

**The 4-Wave Design:**

| Wave | Order Type | Purpose |
|------|-----------|---------|
| 1 | Limit Orders | Stress order book insertion — tests price level data structure |
| 2 | Market Orders | Stress matching engine core — tests execution loop speed |
| 3 | Cancel Orders | Stress order lookup — tests whether O(1) cancel index exists |
| 4 | Mixed Sustained | Real market simulation — all types under full load |

**Ramping load within each wave:**
```
Step 1: 10% of max bots  → baseline latency
Step 2: 50% of max bots  → mid-load behavior
Step 3: 100% of max bots → peak load, breaking point detection
```

This design finds the exact bot count where each order type starts degrading — not just a single snapshot.

**CLI flags:**
```bash
go run main.go --bots 1000 --duration 60s --target http://localhost:8080 --contestant contestant_001
```

**Per-bot flow:**
1. Generate order of the wave's type (random side, price, quantity)
2. Record `send_timestamp` (nanosecond precision via `time.Now()`)
3. HTTP POST to contestant's `/order` endpoint
4. Record `receive_timestamp`
5. For limit orders: GET `/orderbook` and validate best bid/ask is consistent
6. Push `Result{bot_id, order_type, latency_ns, success, correct, wave}` to Redpanda

**Correctness validation:**
After each limit order, the bot queries `/orderbook` and verifies the best bid/ask is within a valid range of the submitted price. This catches engines that accept orders but don't actually process them.

**Measured results on dummy target (100 bots, 40s):**

| Wave | P50 | P99 | Success | Correctness |
|------|-----|-----|---------|-------------|
| Limit Orders | 12.6ms | 29.5ms | 100% | 100% |
| Market Orders | 9.7ms | 22.8ms | 100% | 100% |
| Cancel Orders | 9.3ms | 15.8ms | 100% | 100% |
| Mixed | 9.0ms | 18.0ms | 100% | 99.4% |

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
  "success": true,
  "correct": true,
  "wave": 1
}
```

Redpanda decouples the bot fleet from the telemetry ingester. The bots never wait for scoring to complete — they fire and forget into the topic. This keeps the load generator on the hot path and analytics completely async.

---

### 4. Telemetry Ingester

Consumes all messages from the `bot-results` Redpanda topic and produces a full scoring report.

**Per-wave scoring:**

Each wave is scored independently, giving contestants specific feedback on which order type their engine handles poorly:

```
Wave 1 Limit Orders    ✓  p50=12ms  p99=29ms  success=100%  correct=100%  score=32.8
Wave 2 Market Orders   ✓  p50=9ms   p99=22ms  success=100%  correct=100%  score=42.0
Wave 3 Cancel Orders   ✓  p50=9ms   p99=15ms  success=100%  correct=100%  score=59.5
Wave 4 Mixed           ✓  p50=9ms   p99=18ms  success=100%  correct=99%   score=52.4
```

**Scoring formula per wave:**
```
wave_score = (1000 / (p99_ms + 1)) × (success_rate / 100) × (correctness / 100)
final_score = average of all wave scores
```

This rewards:
- Low p99 latency — 1ms p99 scores ~1000, 100ms p99 scores ~10
- High success rate — engines that drop orders under load are penalized
- High correctness — engines that accept but don't process orders are penalized

**Breaking point detection:**

For each wave, the telemetry ingester scans the three ramp steps and classifies the engine:
```
✓ Stable across all load levels (p99 < 50ms throughout)
⚠ Stable up to N bots, degradation starts at M bots
✗ Stable up to N bots, breaking point at M bots (p99 spike / errors)
```

**Latency histogram:**
```
0-10ms       │█████████████████████ 278 (43.4%)
10-50ms      │████████████████████████████ 362 (56.6%)
50-100ms     │ 0 (0.0%)
100-250ms    │ 0 (0.0%)
```

**Why percentiles over averages:**
If 990 orders take 1ms and 10 orders take 5000ms, the average is ~51ms — completely misleading. P99 correctly surfaces the 5000ms worst case.

**Output — two Redis writes per test:**
- `leaderboard:ranking` — Redis sorted set, auto-ordered by score
- `leaderboard:scores` — Redis hash, full JSON breakdown per contestant including per-wave scores

---

### 5. Real-Time Leaderboard

Reads from Redis sorted set and streams updates to a WebSocket-connected frontend. The leaderboard never touches Redpanda or TimescaleDB directly — it only reads pre-computed scores from Redis, which handles 100,000+ reads/second at microsecond latency.

**Displayed metrics per contestant:**
- Per-wave p50 / p90 / p99 latency
- Per-wave success rate and correctness
- Breaking point per wave
- TPS (transactions per second)
- Composite final score and live rank

---

## Data Stores

| Store | Purpose | Why |
|---|---|---|
| Redpanda | Raw metrics pipeline | High throughput, decoupled, async |
| Redis | Leaderboard scores | Sub-millisecond reads, sorted sets built-in |
| TimescaleDB | Historical metrics archive | Time-series queries, Postgres-compatible |

---

## Infrastructure as Code

All services defined in `infra/docker-compose.yml` for local development.
Production deployment via Kubernetes manifests in `infra/k8s/`.

**Services:**
- `redpanda` — message queue on port 9092
- `redis` — leaderboard store on port 6379
- `timescaledb` — metrics archive on port 5432

Spin up entire platform locally:
```bash
docker compose -f infra/docker-compose.yml up -d
```

---

## Key Architectural Decisions

| Decision | Alternative | Reason |
|---|---|---|
| Go for bot fleet | C++ / Python | Goroutines handle 1000+ concurrent bots trivially |
| Redpanda over Kafka | Kafka | No ZooKeeper, no JVM, lower tail latency |
| 4-wave isolated test | Single mixed test | Isolates which order type causes degradation |
| Ramping load | Fixed concurrency | Finds breaking point, not just peak snapshot |
| Redis sorted set for leaderboard | DB query | O(log N) insert, O(1) range read, never blocks |
| Warm container pool | Cold start | 50ms vs 3000ms container startup |
| Percentile metrics | Averages | Averages hide worst-case behavior under load |
| Per-wave correctness | End-to-end only | Pinpoints which operation type has bugs |

---

## Known Limitations and Future Work

### Measurement Accuracy — Coordinated Omission

The current latency measurement timestamps at the HTTP client level
(application-level latency), not at the network interface level
(wire-to-wire latency). This means TCP connection setup and HTTP
overhead are included in every measurement. Real exchange latency
measurement uses kernel bypass networking (DPDK, io_uring) to
measure from the moment a packet hits the NIC.

Additionally, the correctness validation step (GET /orderbook after
each limit order) adds an extra HTTP round trip that slightly inflates
p99 measurements. A production implementation would separate
correctness validation from latency measurement via a dedicated
validation pass after the load test completes.

The current percentile computation stores all latency values in memory
and sorts them — O(n log n). For high-volume tests (1M+ results) this
becomes expensive. HDR Histogram (High Dynamic Range) would reduce
this to O(1) per recording with ~40KB memory regardless of result
count, and eliminates the coordinated omission problem by recording
every latency value with full precision.

### Statistical Sample Size

P99 requires a minimum of 1000 samples to be statistically meaningful.
At 100 bots per wave step, each step produces ~100-160 samples —
below the threshold for reliable p99. Running with --bots 1000
produces sufficient sample sizes for valid percentile computation.

### Horizontal Scaling

The current architecture tests one contestant at a time. True 1000-
contestant parallel testing requires Kubernetes horizontal pod
autoscaling — each contestant runs in an isolated pod, the bot fleet
spawns per-pod goroutine pools, and the telemetry ingester uses
Redpanda consumer groups to process per-contestant result streams
independently.

### Price-Time Priority Correctness

The current correctness check validates that the orderbook best bid/ask
is within a valid range after each order. A stricter correctness check
would verify price-time priority — that orders at the same price level
are filled in arrival order. This requires persistent bot identities
and fill notification parsing, similar to FIX protocol ExecutionReport
messages.

---

## Team

| Member | Component |
|---|---|
| Upanshu Smit (stevie-x) | Bot Fleet + Telemetry Ingester |
| Abhisoumya Kapoor (The-Asterix) | Submission & Sandboxing Engine |
| Shubhayu Brahmachari (Snoob965) | Leaderboard Frontend + IaC |
