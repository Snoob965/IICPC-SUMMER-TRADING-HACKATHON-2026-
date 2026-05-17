package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

var ctx = context.Background()

type Result struct {
	BotID     int           `json:"bot_id"`
	OrderType string        `json:"order_type"`
	Latency   time.Duration `json:"latency_ns"`
	Success   bool          `json:"success"`
}

type Score struct {
	ContestantID string  `json:"contestant_id"`
	P50          float64 `json:"p50_ms"`
	P90          float64 `json:"p90_ms"`
	P99          float64 `json:"p99_ms"`
	SuccessRate  float64 `json:"success_rate"`
	TPS          float64 `json:"tps"`
	Score        float64 `json:"score"`
}

func percentile(latencies []float64, p float64) float64 {
	if len(latencies) == 0 {
		return 0
	}
	index := int(math.Ceil(p/100.0*float64(len(latencies)))) - 1
	return latencies[index]
}

func consumeFromRedpanda(expectedCount int) []Result {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumeTopics("bot-results"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		fmt.Println("Redpanda connection error:", err)
		return nil
	}
	defer client.Close()

	var results []Result
	for len(results) < expectedCount {
		fetches := client.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			var result Result
			json.Unmarshal(r.Value, &result)
			results = append(results, result)
		})
	}

	fmt.Printf("Consumed %d results from Redpanda\n", len(results))
	return results
}

func computeScore(results []Result, contestantID string) (Score, []float64) {
	var latencies []float64
	success, total := 0, 0

	for _, r := range results {
		latencies = append(latencies, float64(r.Latency)/float64(time.Millisecond))
		total++
		if r.Success {
			success++
		}
	}

	sort.Float64s(latencies)

	p50 := percentile(latencies, 50)
	p90 := percentile(latencies, 90)
	p99 := percentile(latencies, 99)
	successRate := float64(success) / float64(total) * 100
	tps := float64(total) / 10.0
	score := (1000.0 / (p99 + 1)) * (successRate / 100.0)

	return Score{
		ContestantID: contestantID,
		P50:          p50,
		P90:          p90,
		P99:          p99,
		SuccessRate:  successRate,
		TPS:          tps,
		Score:        score,
	}, latencies
}

func repeatChar(c string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += c
	}
	return result
}

func printHistogram(latencies []float64) {
	buckets := []struct {
		label string
		max   float64
	}{
		{"0-10ms", 10},
		{"10-50ms", 50},
		{"50-100ms", 100},
		{"100-250ms", 250},
		{"250-500ms", 500},
		{"500ms+", math.MaxFloat64},
	}

	counts := make([]int, len(buckets))
	for _, l := range latencies {
		for i, b := range buckets {
			if l <= b.max {
				counts[i]++
				break
			}
		}
	}

	total := len(latencies)
	fmt.Printf("\n--- Latency Histogram ---\n")
	for i, b := range buckets {
		pct := float64(counts[i]) / float64(total) * 100
		bar := int(pct / 2)
		fmt.Printf("%-12s │%s %d (%.1f%%)\n",
			b.label,
			repeatChar("█", bar),
			counts[i],
			pct,
		)
	}
}

func pushLeaderboard(rdb *redis.Client, score Score) {
	data, _ := json.Marshal(score)
	rdb.HSet(ctx, "leaderboard:scores", score.ContestantID, data)
	rdb.ZAdd(ctx, "leaderboard:ranking", redis.Z{
		Score:  score.Score,
		Member: score.ContestantID,
	})
	fmt.Printf("Score pushed to leaderboard for %s\n", score.ContestantID)
}

func printScore(score Score) {
	fmt.Printf("\n--- Telemetry Report ---\n")
	fmt.Printf("Contestant:   %s\n", score.ContestantID)
	fmt.Printf("P50 Latency:  %.2f ms\n", score.P50)
	fmt.Printf("P90 Latency:  %.2f ms\n", score.P90)
	fmt.Printf("P99 Latency:  %.2f ms\n", score.P99)
	fmt.Printf("Success Rate: %.1f%%\n", score.SuccessRate)
	fmt.Printf("TPS:          %.1f\n", score.TPS)
	fmt.Printf("Final Score:  %.4f\n", score.Score)
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	contestantID := "contestant_001"
	results := consumeFromRedpanda(1000)
	score, latencies := computeScore(results, contestantID)
	printScore(score)
	printHistogram(latencies)
	pushLeaderboard(rdb, score)
}