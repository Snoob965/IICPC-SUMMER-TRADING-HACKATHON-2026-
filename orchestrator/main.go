package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type SandboxEvent struct {
	Status       string `json:"status"`
	ContestantID string `json:"contestant_id"`
	TargetURL    string `json:"target_url"`
}

func flushAndCreateTopic() {
	exec.Command("docker", "exec", "infra-redpanda-1", "rpk", "topic", "delete", "bot-results").Run()
	exec.Command("docker", "exec", "infra-redpanda-1", "rpk", "topic", "create", "bot-results").Run()
	fmt.Println("[Orchestrator] Redpanda topic reset")
}

func runBotFleet(contestantID string, mode string) {
	fmt.Printf("[Orchestrator] Starting bot fleet for %s mode=%s\n", contestantID, mode)

	cmd := exec.Command("go", "run", "main.go",
		"--mode", mode,
		"--bots", "100",
		"--contestant", contestantID,
	)
	cmd.Dir = "../bot-fleet"
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("[Orchestrator] Bot fleet error: %v\n%s\n", err, string(output))
		return
	}
	fmt.Printf("[Orchestrator] Bot fleet complete for %s\n", contestantID)

	expected := 690
	if mode == "standard" {
		expected = 1600
	} else if mode == "marathon" {
		expected = 3200
	}

	runTelemetry(contestantID, expected)
}

func runTelemetry(contestantID string, expected int) {
	fmt.Printf("[Orchestrator] Running telemetry for %s\n", contestantID)

	cmd := exec.Command("go", "run", "main.go",
		"--expected", fmt.Sprintf("%d", expected),
		"--contestant", contestantID,
		"--maxbots", "100",
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

		time.Sleep(2 * time.Second)
		flushAndCreateTopic()
		go runBotFleet(event.ContestantID, mode)
	}
}