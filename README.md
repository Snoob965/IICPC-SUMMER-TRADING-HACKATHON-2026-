# Distributed Matching Engine Benchmarking Platform

A platform that evaluates contestant-submitted trading engines by containerizing them, stress-testing with a distributed bot fleet, and scoring on latency, throughput, and correctness — streamed to a live leaderboard.

Built for **IICPC Summer Hackathon 2026**.

---

## What It Does

Contestants submit their matching engine or order book implementation. The platform:

1. Containerizes and isolates the submission in Docker with strict CPU and memory limits
2. Spawns 1000 concurrent bots that hammer the engine with realistic order traffic
3. Measures p50/p90/p99 latency, TPS, success rate, and correctness
4. Scores the submission and streams results to a live leaderboard

---

## Benchmark Numbers

Measured on a local dummy target server:

| Bots | Success Rate | Avg Latency | P99 Latency | Correctness |
|------|-------------|-------------|-------------|-------------|
| 100  | 100%        | 17ms        | 43ms        | 99%         |
| 500  | 100%        | 88ms        | ~120ms      | 99%         |
| 1000 | 100%        | 56ms        | 107ms       | 99%         |

---

## Architecture

```
Bot Fleet (Go, 1000 goroutines)
        ↓
    Redpanda
        ↓
Telemetry Ingester → Redis leaderboard:ranking
                   → Redis leaderboard:scores
                           ↓
                  Leaderboard Frontend
```

See [ARCHITECTURE.md](./ARCHITECTURE.md) for full system design.

---

## Project Structure

```
├── bot-fleet/       # Go service — 1000 concurrent bots, CLI configurable
├── telemetry/       # Go service — consumes Redpanda, computes p50/p99/score
├── sandbox/         # Docker sandboxing engine for contestant submissions
├── leaderboard/     # Frontend — live WebSocket leaderboard
├── dummy-target/    # Local test server simulating a contestant engine
├── infra/           # docker-compose.yml (Redpanda, Redis, TimescaleDB)
└── ARCHITECTURE.md  # Full system design document
```

---

## Running Locally

**1. Start infrastructure:**
```bash
cd infra
docker compose up -d
```

**2. Start a target engine (or use dummy):**
```bash
go run dummy-target/main.go
```

**3. Run the bot fleet:**
```bash
cd bot-fleet
go run main.go --bots 1000 --target http://localhost:8080 --contestant contestant_001
```

**4. Run telemetry scoring:**
```bash
cd telemetry
go run main.go
```

---

## Contestant API Contract

Every submitted engine must expose:

```
POST   /order          — place limit or market order
DELETE /order/:id      — cancel order
GET    /orderbook      — current best bid/ask
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

## Scoring Formula

```
score = (1000 / (p99_ms + 1)) × (success_rate / 100) × (correctness / 100)
```

A contestant with 1ms p99, 100% success, and 100% correctness scores ~1000. A contestant with 500ms p99 scores ~2. Correctness and reliability matter as much as raw speed.

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Bot Fleet | Go + goroutines | 100k+ concurrent bots on one machine |
| Message Queue | Redpanda | No ZooKeeper, no JVM, Kafka-compatible |
| Leaderboard Store | Redis sorted set | O(log N) insert, microsecond reads |
| Metrics Archive | TimescaleDB | Time-series optimized Postgres |
| Sandboxing | Docker | CPU pinning, memory limits, isolation |

---

## Team

| Member | Role |
|---|---|
| Upanshu Smit (stevie-x) | Bot Fleet + Telemetry Ingester |
| Abhisoumya Kapoor (The-Asterix) | Submission & Sandboxing Engine |
| Shubhayu Brahmachari (Snoob965) | Leaderboard Frontend + IaC |