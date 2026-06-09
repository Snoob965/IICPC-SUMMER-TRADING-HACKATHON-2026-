package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"sort"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

var ctx = context.Background()

type Result struct {
	BotID     int           `json:"bot_id"`
	OrderType string        `json:"order_type"`
	Latency   time.Duration `json:"latency_ns"`
	Success   bool          `json:"success"`
	Correct   bool          `json:"correct"`
	Wave      int           `json:"wave"`
	StartedAt int64         `json:"started_at_ns"` // unix nanoseconds — for real TPS
}

type WaveScore struct {
	WaveNum     int     `json:"wave_num"`
	Label       string  `json:"label"`
	P50         float64 `json:"p50_ms"`
	P90         float64 `json:"p90_ms"`
	P99         float64 `json:"p99_ms"`
	P999        float64 `json:"p999_ms"`
	SuccessRate float64 `json:"success_rate"`
	Correctness float64 `json:"correctness"`
	TPS         float64 `json:"tps"`
	Score       float64 `json:"score"`
}

type FinalScore struct {
	ContestantID string      `json:"contestant_id"`
	Waves        []WaveScore `json:"waves"`
	OverallScore float64     `json:"overall_score"`
	OverallP99   float64     `json:"overall_p99"`
	OverallP999  float64     `json:"overall_p999"`
	OverallSR    float64     `json:"overall_success_rate"`
}

type StepStats struct {
	BotCount int
	P99      float64
	SR       float64
}

func percentile(latencies []float64, p float64) float64 {
	if len(latencies) == 0 {
		return 0
	}
	index := int(math.Ceil(p/100.0*float64(len(latencies)))) - 1
	return latencies[index]
}

// consumeFromRedpanda reads from a per-contestant topic.
// It stops when expectedCount messages arrive OR the deadline passes —
// whichever comes first. This prevents indefinite blocking on crashes or
// short-runs (chaos wave, partial bot fleet, etc.).
func consumeFromRedpanda(topic string, expectedCount int, timeout time.Duration) []Result {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		fmt.Println("Redpanda connection error:", err)
		return nil
	}
	defer client.Close()

	deadline := time.Now().Add(timeout)
	var results []Result

	for len(results) < expectedCount {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			fmt.Printf("⚠  Telemetry timeout reached — collected %d/%d results, scoring what we have\n",
				len(results), expectedCount)
			break
		}

		pollCtx, cancel := context.WithTimeout(ctx, remaining)
		fetches := client.PollFetches(pollCtx)
		cancel()

		fetches.EachRecord(func(r *kgo.Record) {
			var result Result
			if err := json.Unmarshal(r.Value, &result); err == nil {
				results = append(results, result)
			}
		})
	}

	fmt.Printf("Consumed %d results from topic %s\n", len(results), topic)
	return results
}

func checkSampleSize(results []Result, waveNum int) {
	if len(results) < 1000 {
		fmt.Printf("⚠  Wave %d: only %d samples — p99 may not be statistically reliable (need 1000+). Run with --bots 1000 for valid p99.\n", waveNum, len(results))
	}
}

// computeTPS derives TPS from the actual elapsed time of the wave results
// rather than a hardcoded magic number.
func computeTPS(results []Result) float64 {
	if len(results) == 0 {
		return 0
	}
	var minNs, maxNs int64 = math.MaxInt64, 0
	for _, r := range results {
		if r.StartedAt > 0 {
			if r.StartedAt < minNs {
				minNs = r.StartedAt
			}
			end := r.StartedAt + int64(r.Latency)
			if end > maxNs {
				maxNs = end
			}
		}
	}
	if maxNs <= minNs {
		// StartedAt not populated by bot fleet yet — fall back to wave order count / 15s
		return float64(len(results)) / 15.0
	}
	elapsedSecs := float64(maxNs-minNs) / 1e9
	if elapsedSecs < 0.001 {
		return 0
	}
	return float64(len(results)) / elapsedSecs
}

func scoreWave(results []Result, waveNum int, label string) WaveScore {
	hist := hdrhistogram.New(1, 60000, 3)
	success, correct, total := 0, 0, 0

	for _, r := range results {
		latencyMs := r.Latency.Milliseconds()
		if latencyMs < 1 {
			latencyMs = 1
		}
		hist.RecordValue(latencyMs)
		total++
		if r.Success {
			success++
		}
		if r.Correct {
			correct++
		}
	}

	p50 := float64(hist.ValueAtQuantile(50))
	p90 := float64(hist.ValueAtQuantile(90))
	p99 := float64(hist.ValueAtQuantile(99))
	p999 := float64(hist.ValueAtQuantile(99.9))
	successRate := float64(success) / float64(total) * 100
	correctness := float64(correct) / float64(total) * 100
	tps := computeTPS(results)
	p99Factor := 1000.0 / (float64(p99) + 1.0)
	p999Factor := 1000.0 / (float64(p999) + 1.0)
	score := p99Factor * p999Factor * successRate * correctness

	return WaveScore{
		WaveNum:     waveNum,
		Label:       label,
		P50:         p50,
		P90:         p90,
		P99:         p99,
		P999:        p999,
		SuccessRate: successRate,
		Correctness: correctness,
		TPS:         tps,
		Score:       score,
	}
}

func getStepStats(results []Result, maxBots int) []StepStats {
	third := len(results) / 3
	if third == 0 {
		return nil
	}

	steps := []struct {
		botCount int
		results  []Result
	}{
		{maxBots / 10, results[:third]},
		{maxBots / 2, results[third : third*2]},
		{maxBots, results[third*2:]},
	}

	var stats []StepStats
	for _, step := range steps {
		var latencies []float64
		success, total := 0, 0
		for _, r := range step.results {
			latencies = append(latencies, float64(r.Latency)/float64(time.Millisecond))
			total++
			if r.Success {
				success++
			}
		}
		sort.Float64s(latencies)
		p99 := percentile(latencies, 99)
		sr := float64(success) / float64(total) * 100
		stats = append(stats, StepStats{BotCount: step.botCount, P99: p99, SR: sr})
	}
	return stats
}

func detectBreakingPoint(waveMap map[int][]Result, maxBots int) {
	waveLabels := map[int]string{
		1: "Limit Orders",
		2: "Market Orders",
		3: "Cancel Orders",
		4: "Mixed",
		5: "Chaos Testing",
	}

	fmt.Printf("\n--- Breaking Point Analysis ---\n")

	for waveNum := 1; waveNum <= 5; waveNum++ {
		steps := getStepStats(waveMap[waveNum], maxBots)
		if steps == nil {
			continue
		}

		stableUpto := 0
		degradationPoint := 0
		breakingPoint := 0

		for _, s := range steps {
			if s.P99 < 50 && s.SR >= 99 {
				stableUpto = s.BotCount
			} else if s.P99 >= 50 && s.P99 < 200 && s.SR >= 95 && degradationPoint == 0 {
				degradationPoint = s.BotCount
			} else if (s.P99 >= 200 || s.SR < 95) && breakingPoint == 0 {
				breakingPoint = s.BotCount
			}
		}

		fmt.Printf("Wave %d %-15s ", waveNum, waveLabels[waveNum])

		if breakingPoint > 0 {
			fmt.Printf("✗ Stable up to %d bots | breaking point at %d bots\n", stableUpto, breakingPoint)
		} else if degradationPoint > 0 {
			fmt.Printf("⚠ Stable up to %d bots | degradation starts at %d bots\n", stableUpto, degradationPoint)
		} else {
			fmt.Printf("✓ Stable across all load levels (p99 < 50ms throughout)\n")
		}
	}
}

func printHDRHistogram(results []Result) {
	hist := hdrhistogram.New(1, 60000, 3)
	for _, r := range results {
		latencyMs := r.Latency.Milliseconds()
		if latencyMs < 1 {
			latencyMs = 1
		}
		hist.RecordValue(latencyMs)
	}

	fmt.Printf("\n--- Latency Histogram (HDR) ---\n")
	fmt.Printf("Min: %dms  Max: %dms  Mean: %.1fms\n",
		hist.Min(), hist.Max(), hist.Mean())
	fmt.Printf("P50: %dms  P90: %dms  P99: %dms  P99.9: %dms\n\n",
		hist.ValueAtQuantile(50),
		hist.ValueAtQuantile(90),
		hist.ValueAtQuantile(99),
		hist.ValueAtQuantile(99.9),
	)

	buckets := []struct {
		label string
		min   int64
		max   int64
	}{
		{"0-10ms", 0, 10},
		{"10-50ms", 11, 50},
		{"50-100ms", 51, 100},
		{"100-250ms", 101, 250},
		{"250-500ms", 251, 500},
		{"500ms+", 501, 60000},
	}

	total := len(results)
	for _, b := range buckets {
		count := 0
		for _, r := range results {
			ms := r.Latency.Milliseconds()
			if ms >= b.min && ms <= b.max {
				count++
			}
		}
		pct := float64(count) / float64(total) * 100
		bar := int(pct / 2)
		fmt.Printf("%-12s │%s %d (%.1f%%)\n",
			b.label,
			repeatChar("█", bar),
			count,
			pct,
		)
	}
}

func repeatChar(c string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += c
	}
	return result
}

func pushLeaderboard(rdb *redis.Client, final FinalScore) {
	data, _ := json.Marshal(final)
	rdb.HSet(ctx, "leaderboard:scores", final.ContestantID, data)
	rdb.ZAdd(ctx, "leaderboard:ranking", redis.Z{
		Score:  final.OverallScore,
		Member: final.ContestantID,
	})
	fmt.Printf("\nScore pushed to leaderboard for %s\n", final.ContestantID)
}

func printFinalScore(final FinalScore) {
	waveLabels := map[int]string{
		1: "Limit Orders",
		2: "Market Orders",
		3: "Cancel Orders",
		4: "Mixed Sustained",
		5: "Chaos Testing",
	}

	fmt.Printf("\n╔══════════════════════════════════════════════╗\n")
	fmt.Printf("║           TELEMETRY SCORING REPORT          ║\n")
	fmt.Printf("╚══════════════════════════════════════════════╝\n")
	fmt.Printf("Contestant: %s\n", final.ContestantID)

	for _, w := range final.Waves {
		label := waveLabels[w.WaveNum]
		status := "✓"
		if w.SuccessRate < 95 {
			status = "⚠"
		}
		if w.SuccessRate < 80 {
			status = "✗"
		}
		fmt.Printf("\nWave %d %-20s %s\n", w.WaveNum, label, status)
		fmt.Printf("  P50: %.0fms  P90: %.0fms  P99: %.0fms  P99.9: %.0fms\n", w.P50, w.P90, w.P99, w.P999)
		fmt.Printf("  Success: %.1f%%  Correct: %.1f%%  TPS: %.1f\n", w.SuccessRate, w.Correctness, w.TPS)
		fmt.Printf("  Wave Score: %.4f\n", w.Score)
	}

	fmt.Printf("\n──────────────────────────────────────────────\n")
	fmt.Printf("Overall P99:     %.0fms\n", final.OverallP99)
	fmt.Printf("Overall P99.9:   %.0fms\n", final.OverallP999)
	fmt.Printf("Overall Success: %.1f%%\n", final.OverallSR)
	fmt.Printf("FINAL SCORE:     %.4f\n", final.OverallScore)
	fmt.Printf("──────────────────────────────────────────────\n")
}

func main() {
	expectedResults := flag.Int("expected", 640, "Expected number of results from Redpanda")
	contestantID := flag.String("contestant", "contestant_001", "Contestant ID to score")
	maxBots := flag.Int("maxbots", 100, "Max bots used in the stress test")
	topic := flag.String("topic", "bot-results", "Redpanda topic to consume from")
	timeoutSecs := flag.Int("timeout", 300, "Max seconds to wait for results before scoring partial data")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	allResults := consumeFromRedpanda(*topic, *expectedResults, time.Duration(*timeoutSecs)*time.Second)

	waveMap := map[int][]Result{}
	for _, r := range allResults {
		waveMap[r.Wave] = append(waveMap[r.Wave], r)
	}

	waveLabels := map[int]string{
		1: "Limit Orders",
		2: "Market Orders",
		3: "Cancel Orders",
		4: "Mixed Sustained",
		5: "Chaos Testing",
	}

	var waves []WaveScore
	var allLatencies []float64
	totalSuccess, totalCount := 0, 0

	for i := 1; i <= 5; i++ {
		checkSampleSize(waveMap[i], i)
		ws := scoreWave(waveMap[i], i, waveLabels[i])
		waves = append(waves, ws)
		for _, r := range waveMap[i] {
			allLatencies = append(allLatencies, float64(r.Latency)/float64(time.Millisecond))
			totalCount++
			if r.Success {
				totalSuccess++
			}
		}
	}

	sort.Float64s(allLatencies)
	overallP99 := percentile(allLatencies, 99)
	overallP999 := percentile(allLatencies, 99.9)
	overallSR := float64(totalSuccess) / float64(totalCount) * 100

	overallScore := 0.0
	for _, w := range waves {
		overallScore += w.Score
	}
	overallScore /= float64(len(waves))

	final := FinalScore{
		ContestantID: *contestantID,
		Waves:        waves,
		OverallScore: overallScore,
		OverallP99:   overallP99,
		OverallP999:  overallP999,
		OverallSR:    overallSR,
	}

	printFinalScore(final)
	printHDRHistogram(allResults)
	detectBreakingPoint(waveMap, *maxBots)
	pushLeaderboard(rdb, final)
}