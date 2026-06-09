package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type SandboxEvent struct {
	Status       string `json:"status"`
	ContestantID string `json:"contestant_id"`
	TargetURL    string `json:"target_url"`
}

// topicName returns a per-contestant Redpanda topic name.
// This prevents concurrent submissions from flushing each other's data.
func topicName(contestantID string) string {
	return fmt.Sprintf("bot-results-%s", contestantID)
}

// ensureTopic creates a fresh topic for this contestant's run.
// It deletes any leftover topic from a previous run first (idempotent).
func ensureTopic(contestantID string) {
	topic := topicName(contestantID)
	exec.Command("docker", "exec", "infra-redpanda-1", "rpk", "topic", "delete", topic).Run()
	out, err := exec.Command("docker", "exec", "infra-redpanda-1", "rpk", "topic", "create", topic).CombinedOutput()
	if err != nil {
		fmt.Printf("[Orchestrator] Warning: topic create error for %s: %s\n", topic, string(out))
	} else {
		fmt.Printf("[Orchestrator] Redpanda topic ready: %s\n", topic)
	}
}

func runBotFleet(contestantID string, mode string) {
	fmt.Printf("[Orchestrator] Starting bot fleet for %s mode=%s\n", contestantID, mode)

	topic := topicName(contestantID)

	cmd := exec.Command("go", "run", "main.go",
		"--mode", mode,
		"--bots", "100",
		"--contestant", contestantID,
		"--topic", topic,
	)
	cmd.Dir = "../bot-fleet"
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("[Orchestrator] Bot fleet error: %v\n%s\n", err, string(output))
		return
	}
	fmt.Printf("[Orchestrator] Bot fleet complete for %s\n", contestantID)

	// Expected result counts per mode — must match what bot-fleet actually sends
	expected := 690
	if mode == "standard" {
		expected = 1600
	} else if mode == "marathon" {
		expected = 3200
	}

	runTelemetry(contestantID, expected, topic)
}

func runTelemetry(contestantID string, expected int, topic string) {
	fmt.Printf("[Orchestrator] Running telemetry for %s (topic: %s)\n", contestantID, topic)

	cmd := exec.Command("go", "run", "main.go",
		"--expected", fmt.Sprintf("%d", expected),
		"--contestant", contestantID,
		"--maxbots", "100",
		"--topic", topic,
		"--timeout", "300",
	)
	cmd.Dir = "../telemetry"
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("[Orchestrator] Telemetry error: %v\n%s\n", err, string(output))
		return
	}
	fmt.Printf("[Orchestrator] Scoring complete for %s\n", contestantID)
	fmt.Println(string(output))
}

// reaperTTL is how long a contestant container is allowed to run before the
// reaper forcibly stops it. Covers crashes, hangs, and abandoned submissions.
const reaperTTL = 35 * time.Minute

// startReaper runs as a background goroutine. Every minute it scans all
// sandbox:{id}:port keys in Redis — if a container's TTL has dropped below
// the reaper threshold (meaning it's been running longer than expected), it
// force-stops the container and releases the port back to the pool.
func startReaper(rdb *redis.Client) {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			keys, err := rdb.Keys(ctx, "sandbox:*:port").Result()
			if err != nil {
				continue
			}
			for _, key := range keys {
				// key format: sandbox:{contestantID}:port
				parts := strings.SplitN(key, ":", 3)
				if len(parts) != 3 {
					continue
				}
				contestantID := parts[1]

				ttl, err := rdb.TTL(ctx, key).Result()
				if err != nil {
					continue
				}

				// Original TTL was 30 min. If remaining TTL < (30min - reaperTTL),
				// the container has been alive longer than reaperTTL — reap it.
				if ttl > 0 && ttl < (30*time.Minute-reaperTTL) {
					fmt.Printf("[Reaper] Container for %s has been running >%v — force stopping\n",
						contestantID, reaperTTL)

					// Release port back to pool
					if port, err := rdb.Get(ctx, key).Result(); err == nil {
						rdb.SAdd(ctx, "sandbox:port_pool", port)
					}

					exec.Command("docker", "stop", fmt.Sprintf("contestant_%s", contestantID)).Run()
					exec.Command("docker", "rm", fmt.Sprintf("contestant_%s", contestantID)).Run()
					rdb.Del(ctx, fmt.Sprintf("sandbox:%s:port", contestantID))
					rdb.Del(ctx, fmt.Sprintf("sandbox:%s:status", contestantID))

					fmt.Printf("[Reaper] Cleaned up container for %s\n", contestantID)
				}
			}
		}
	}()
	fmt.Println("[Orchestrator] Container reaper started (TTL:", reaperTTL, ")")
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Println("Redis connection failed:", err)
		return
	}

	fmt.Println("[Orchestrator] Started — watching for new submissions...")

	// Start background reaper — cleans up stale/abandoned containers automatically
	startReaper(rdb)

	pubsub := rdb.Subscribe(ctx, "sandbox_logs")
	defer pubsub.Close()

	for msg := range pubsub.Channel() {
		var event SandboxEvent
		if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
			continue
		}

		if event.Status != "running" {
			continue
		}

		fmt.Printf("[Orchestrator] New submission: %s\n", event.ContestantID)

		mode, err := rdb.Get(ctx, fmt.Sprintf("test_mode:%s", event.ContestantID)).Result()
		if err != nil {
			mode = "blitz"
		}

		// Small grace period for the container to fully stabilise
		time.Sleep(2 * time.Second)

		// Create a fresh, isolated topic for this contestant — never touches other contestants' topics
		ensureTopic(event.ContestantID)

		go runBotFleet(event.ContestantID, mode)
	}
}