package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

var ctx = context.Background()

var TestModes = map[string]time.Duration{
	"blitz":    40 * time.Second,
	"standard": 90 * time.Second,
	"marathon": 180 * time.Second,
}

type Order struct {
	BotID    int     `json:"bot_id"`
	Type     string  `json:"type"`
	Side     string  `json:"side"`
	Price    float64 `json:"price"`
	Quantity int     `json:"quantity"`
}

type Result struct {
	BotID        int           `json:"bot_id"`
	OrderType    string        `json:"order_type"`
	Latency      time.Duration `json:"latency_ns"`
	StartedAt    int64         `json:"started_at_ns"`
	Success      bool          `json:"success"`
	ContestantID string        `json:"contestant_id"`
	Correct      bool          `json:"correct"`
	Wave         int           `json:"wave"`
	Instrument   string        `json:"instrument"`
}

type WaveResult struct {
	WaveNum   int
	Label     string
	OrderType string
	BotCount  int
	Results   []Result
	P50       float64
	P99       float64
	SR        float64
	CR        float64
}

func makeOrder(botID int, orderType string, instrument string) Order {
	sides := []string{"buy", "sell"}
	side := sides[rand.Intn(2)]
	basePrice := 1820.0
	if instrument == "BTC" {
		basePrice = 95000.0
	}
	price := basePrice + (rand.Float64()*20 - 10)

	switch orderType {
	case "limit":
		return Order{BotID: botID, Type: "limit", Side: side, Price: price, Quantity: rand.Intn(5) + 1}
	case "market":
		return Order{BotID: botID, Type: "market", Side: side, Quantity: rand.Intn(5) + 1}
	case "cancel":
		return Order{BotID: botID, Type: "cancel", Side: side, Quantity: 1}
	case "chaos":
		chaosTypes := []Order{
			{BotID: botID, Type: "limit", Side: "buy", Price: 0, Quantity: 1},
			{BotID: botID, Type: "limit", Side: "sell", Price: 999999999, Quantity: 1},
			{BotID: botID, Type: "limit", Side: "buy", Price: price, Quantity: 0},
			{BotID: botID, Type: "cancel", Side: "buy", Quantity: 1},
			{BotID: botID, Type: "market", Side: side, Quantity: 10000},
		}
		return chaosTypes[rand.Intn(len(chaosTypes))]
	default:
		r := rand.Intn(10)
		if r < 6 {
			return Order{BotID: botID, Type: "limit", Side: side, Price: price, Quantity: rand.Intn(5) + 1}
		} else if r < 9 {
			return Order{BotID: botID, Type: "market", Side: side, Quantity: rand.Intn(5) + 1}
		}
		return Order{BotID: botID, Type: "cancel", Side: side, Quantity: 1}
	}
}

func validateCorrectness(targetURL string, order Order) bool {
	if order.Type != "limit" {
		return true
	}
	resp, err := http.Get(targetURL + "/orderbook")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	var book map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&book)

	bestBid, hasBid := book["best_bid"].(float64)
	bestAsk, hasAsk := book["best_ask"].(float64)

	if order.Side == "buy" && hasBid {
		return bestBid <= order.Price+10
	}
	if order.Side == "sell" && hasAsk {
		return bestAsk >= order.Price-10
	}
	return true
}

func validateDepth(targetURL string) bool {
	resp, err := http.Get(targetURL + "/orderbook/depth")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	var depth map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&depth)

	bids, hasBids := depth["bids"]
	asks, hasAsks := depth["asks"]
	if !hasBids || !hasAsks {
		return false
	}

	bidsSlice, _ := bids.([]interface{})
	asksSlice, _ := asks.([]interface{})
	return len(bidsSlice) > 0 && len(asksSlice) > 0
}

func runPriceTimePriorityTest(targetURL string) bool {
	fmt.Printf("\n[Price-Time Priority Test]\n")
	fmt.Println("  Sending Bot A limit buy at 1820.00...")

	orderA := Order{BotID: 9001, Type: "limit", Side: "buy", Price: 1820.00, Quantity: 5}
	payloadA, _ := json.Marshal(orderA)
	respA, err := http.Post(targetURL+"/order", "application/json", bytes.NewBuffer(payloadA))
	if err != nil {
		fmt.Println("  ✗ Bot A order failed:", err)
		return false
	}
	defer respA.Body.Close()

	var resultA map[string]interface{}
	json.NewDecoder(respA.Body).Decode(&resultA)
	orderIDA, _ := resultA["order_id"].(string)
	fmt.Printf("  Bot A order accepted: %s\n", orderIDA)

	time.Sleep(10 * time.Millisecond)

	fmt.Println("  Sending Bot B limit buy at 1820.00 (same price, later time)...")
	orderB := Order{BotID: 9002, Type: "limit", Side: "buy", Price: 1820.00, Quantity: 5}
	payloadB, _ := json.Marshal(orderB)
	respB, err := http.Post(targetURL+"/order", "application/json", bytes.NewBuffer(payloadB))
	if err != nil {
		fmt.Println("  ✗ Bot B order failed:", err)
		return false
	}
	defer respB.Body.Close()

	var resultB map[string]interface{}
	json.NewDecoder(respB.Body).Decode(&resultB)
	orderIDB, _ := resultB["order_id"].(string)
	fmt.Printf("  Bot B order accepted: %s\n", orderIDB)

	fmt.Println("  Sending market sell qty 5 — should fill Bot A first...")
	orderSell := Order{BotID: 9003, Type: "market", Side: "sell", Quantity: 5}
	payloadSell, _ := json.Marshal(orderSell)
	respSell, err := http.Post(targetURL+"/order", "application/json", bytes.NewBuffer(payloadSell))
	if err != nil {
		fmt.Println("  ✗ Market sell failed:", err)
		return false
	}
	defer respSell.Body.Close()

	var resultSell map[string]interface{}
	json.NewDecoder(respSell.Body).Decode(&resultSell)

	filledID, hasFill := resultSell["filled_order_id"].(string)
	if !hasFill {
		fmt.Println("  ⚠ Engine does not report filled_order_id — cannot verify price-time priority")
		fmt.Println("  ⚠ Add 'filled_order_id' to your /order response to enable this check")
		return false
	}

	if filledID == orderIDA {
		fmt.Println("  ✓ Price-time priority CORRECT — Bot A filled first")
		return true
	}

	fmt.Printf("  ✗ Price-time priority VIOLATED — filled %s instead of %s\n", filledID, orderIDA)
	return false
}

func runBot(botID int, targetURL string, contestantID string, orderType string, waveNum int, instrument string, wg *sync.WaitGroup, results chan<- Result) {
	defer wg.Done()

	order := makeOrder(botID, orderType, instrument)
	payload, _ := json.Marshal(order)

	start := time.Now()
	resp, err := http.Post(targetURL+"/order", "application/json", bytes.NewBuffer(payload))
	latency := time.Since(start)

	if err != nil || resp.StatusCode != 200 {
		results <- Result{BotID: botID, OrderType: order.Type, Latency: latency, StartedAt: start.UnixNano(), Success: false, ContestantID: contestantID, Correct: false, Wave: waveNum, Instrument: instrument}
		return
	}
	defer resp.Body.Close()

	correct := validateCorrectness(targetURL, order)
	results <- Result{BotID: botID, OrderType: order.Type, Latency: latency, StartedAt: start.UnixNano(), Success: true, ContestantID: contestantID, Correct: correct, Wave: waveNum, Instrument: instrument}
}

func spawnBots(targetURL string, contestantID string, numBots int, orderType string, waveNum int, instrument string) []Result {
	resultsChan := make(chan Result, numBots*10)
	var wg sync.WaitGroup
	var mu sync.Mutex
	count := 0

	requestsPerSecond := numBots / 5
	if requestsPerSecond < 1 {
		requestsPerSecond = 1
	}
	interval := time.Second / time.Duration(requestsPerSecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for i := 0; i < numBots; i++ {
		<-ticker.C
		wg.Add(1)
		mu.Lock()
		id := count
		count++
		mu.Unlock()
		go runBot(id, targetURL, contestantID, orderType, waveNum, instrument, &wg, resultsChan)
	}

	wg.Wait()
	close(resultsChan)

	var results []Result
	for r := range resultsChan {
		results = append(results, r)
	}
	return results
}

func computeWaveStats(results []Result) (p50, p99, successRate, correctness float64) {
	var latencies []float64
	success, correct, total := 0, 0, 0

	for _, r := range results {
		latencies = append(latencies, float64(r.Latency)/float64(time.Millisecond))
		total++
		if r.Success {
			success++
		}
		if r.Correct {
			correct++
		}
	}

	if len(latencies) == 0 {
		return
	}

	sort.Float64s(latencies)
	idx50 := int(float64(len(latencies))*0.50) - 1
	idx99 := int(float64(len(latencies))*0.99) - 1
	if idx50 < 0 {
		idx50 = 0
	}
	if idx99 < 0 {
		idx99 = 0
	}

	p50 = latencies[idx50]
	p99 = latencies[idx99]
	successRate = float64(success) / float64(total) * 100
	correctness = float64(correct) / float64(total) * 100
	return
}

func runWave(waveNum int, label string, orderType string, targetURL string, contestantID string, maxBots int, duration time.Duration, instrument string) WaveResult {
	fmt.Printf("\n[Wave %d - %s - %s] duration: %v\n", waveNum, label, instrument, duration)

	steps := []int{maxBots / 10, maxBots / 2, maxBots}
	if steps[0] == 0 {
		steps[0] = 1
	}
	stepDuration := duration / time.Duration(len(steps))

	var allResults []Result

	for _, bots := range steps {
		fmt.Printf("  %d bots firing...\n", bots)
		results := spawnBots(targetURL, contestantID, bots, orderType, waveNum, instrument)
		allResults = append(allResults, results...)
		p50, p99, sr, cr := computeWaveStats(results)
		fmt.Printf("  → p50=%.1fms p99=%.1fms success=%.1f%% correct=%.1f%%\n", p50, p99, sr, cr)
		time.Sleep(stepDuration)
	}

	p50, p99, sr, cr := computeWaveStats(allResults)
	return WaveResult{WaveNum: waveNum, Label: label, OrderType: orderType, BotCount: maxBots, Results: allResults, P50: p50, P99: p99, SR: sr, CR: cr}
}

func runChaosWave(waveNum int, targetURL string, contestantID string, maxBots int, duration time.Duration) WaveResult {
	fmt.Printf("\n[Wave %d - Chaos Testing] duration: %v\n", waveNum, duration)
	fmt.Println("  Firing edge case orders — zero price, extreme price, zero qty, huge qty...")

	results := spawnBots(targetURL, contestantID, maxBots, "chaos", waveNum, "ETH")
	p50, p99, sr, cr := computeWaveStats(results)

	fmt.Printf("  → p50=%.1fms p99=%.1fms success=%.1f%% correct=%.1f%%\n", p50, p99, sr, cr)
	fmt.Printf("  Engine resilience: if success=100%% engine handled all edge cases gracefully\n")

	return WaveResult{WaveNum: waveNum, Label: "Chaos", OrderType: "chaos", BotCount: maxBots, Results: results, P50: p50, P99: p99, SR: sr, CR: cr}
}

func pushToRedpanda(results []Result, topic string) {
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:19092"))
	if err != nil {
		fmt.Println("Redpanda connection error:", err)
		return
	}
	defer client.Close()

	var records []*kgo.Record
	for _, r := range results {
		data, _ := json.Marshal(r)
		records = append(records, &kgo.Record{Topic: topic, Value: data})
	}

	err = client.ProduceSync(ctx, records...).FirstErr()
	if err != nil {
		fmt.Println("Redpanda produce error:", err)
		return
	}
	fmt.Printf("\nPushed %d total results to Redpanda topic: %s\n", len(results), topic)
}

// WaveProgress is written to Redis after each wave so the leaderboard can show
// live chip updates and a running score without waiting for telemetry to finish.
type WaveProgress struct {
	WaveNum     int     `json:"wave_num"`
	Label       string  `json:"label"`
	P50         float64 `json:"p50_ms"`
	P99         float64 `json:"p99_ms"`
	SuccessRate float64 `json:"success_rate"`
	Correctness float64 `json:"correctness"`
	Score       float64 `json:"score"`  // per-wave score using same formula as telemetry
	Done        bool    `json:"done"`   // true = all waves complete
}

// waveScore computes a per-wave score using the identical formula to telemetry:
//   score = (1000/(p99+1)) * (1000/(p999+1)) * successRate * correctness
// Bot-fleet doesn't have p999 so we use p99 for both terms — telemetry will
// replace this with the precise value once it processes Redpanda results.
func waveScore(p99, sr, cr float64) float64 {
	p99Factor := 1000.0 / (p99 + 1.0)
	// Use p99 as a proxy for p999 since we don't have it here
	score := p99Factor * p99Factor * (sr / 100.0) * (cr / 100.0)
	return score
}

// pushWaveProgress writes the completed wave's stats + running score to Redis.
// This is called immediately after each wave finishes so the leaderboard shows
// a live score that updates wave-by-wave, not just at the very end.
func pushWaveProgress(rdb *redis.Client, contestantID string, wave WaveResult, totalWaves int, done bool) {
	progressKey := fmt.Sprintf("leaderboard:progress:%s", contestantID)

	// Read existing progress array and append new wave
	existing, err := rdb.Get(ctx, progressKey).Result()
	var progress []WaveProgress
	if err == nil {
		json.Unmarshal([]byte(existing), &progress)
	}

	thisWaveScore := waveScore(wave.P99, wave.SR, wave.CR)

	progress = append(progress, WaveProgress{
		WaveNum:     wave.WaveNum,
		Label:       wave.Label,
		P50:         wave.P50,
		P99:         wave.P99,
		SuccessRate: wave.SR,
		Correctness: wave.CR,
		Score:       thisWaveScore,
		Done:        done,
	})

	data, _ := json.Marshal(progress)
	rdb.Set(ctx, progressKey, data, 2*time.Hour)

	// ── LIVE SCORE UPDATE ────────────────────────────────────────────────────
	// Compute a running total from all waves completed so far and push it to
	// leaderboard:scores immediately. The leaderboard WebSocket picks this up
	// within 3 seconds and shows a real score during the test.
	// Telemetry will overwrite with the precise final score once it finishes.

	var runningScore float64
	var overallP99 float64
	var overallSR float64

	// Build wave entries for the leaderboard scores blob
	type LiveWaveEntry struct {
		WaveNum     int     `json:"wave_num"`
		Label       string  `json:"label"`
		P50         float64 `json:"p50_ms"`
		P99         float64 `json:"p99_ms"`
		SuccessRate float64 `json:"success_rate"`
		Correctness float64 `json:"correctness"`
		Score       float64 `json:"score"`
	}
	var waveEntries []LiveWaveEntry

	for _, w := range progress {
		runningScore += w.Score
		if w.P99 > overallP99 {
			overallP99 = w.P99
		}
		overallSR += w.SuccessRate
		waveEntries = append(waveEntries, LiveWaveEntry{
			WaveNum:     w.WaveNum,
			Label:       w.Label,
			P50:         w.P50,
			P99:         w.P99,
			SuccessRate: w.SuccessRate,
			Correctness: w.Correctness,
			Score:       w.Score,
		})
	}
	if len(progress) > 0 {
		overallSR /= float64(len(progress))
	}

	// Fetch username if set
	username, _ := rdb.HGet(ctx, "contestant:"+contestantID+":meta", "username").Result()

	liveStatus := "running"
	if done {
		liveStatus = "" // telemetry will overwrite with full data shortly
	}

	liveBlob := map[string]interface{}{
		"contestant_id":        contestantID,
		"username":             username,
		"overall_score":        runningScore,
		"overall_p99":          overallP99,
		"overall_p999":         overallP99, // proxy until telemetry computes real value
		"overall_success_rate": overallSR,
		"status":               liveStatus,
		"waves":                waveEntries,
	}
	blobJSON, _ := json.Marshal(liveBlob)
	rdb.HSet(ctx, "leaderboard:scores", contestantID, blobJSON)

	// Also update the ZSet score so ranking reflects the running total
	rdb.ZAdd(ctx, "leaderboard:ranking", redis.Z{
		Score:  runningScore,
		Member: contestantID,
	})

	fmt.Printf("[Progress] Wave %d/%d pushed for %s (running score: %.2f)\n",
		wave.WaveNum, totalWaves, contestantID, runningScore)
}

func printFinalReport(waves []WaveResult, totalDuration time.Duration, mode string) {
	fmt.Printf("\n╔══════════════════════════════════════════════╗\n")
	fmt.Printf("║           STRESS TEST FINAL REPORT          ║\n")
	fmt.Printf("║  Mode: %-38s║\n", mode)
	fmt.Printf("╚══════════════════════════════════════════════╝\n")

	for _, w := range waves {
		status := "✓"
		if w.SR < 95 {
			status = "⚠"
		}
		if w.SR < 80 {
			status = "✗"
		}
		fmt.Printf("\nWave %d %-20s %s  p50=%.1fms  p99=%.1fms  success=%.1f%%  correct=%.1f%%\n",
			w.WaveNum, w.Label, status, w.P50, w.P99, w.SR, w.CR)
	}

	fmt.Printf("\n──────────────────────────────────────────────\n")
	fmt.Printf("Total Duration: %v\n", totalDuration)
	fmt.Printf("──────────────────────────────────────────────\n")
}

func getTargetURL(rdb *redis.Client, contestantID string, fallback string) string {
	port, err := rdb.Get(ctx, fmt.Sprintf("sandbox:%s:port", contestantID)).Result()
	if err != nil {
		fmt.Printf("No sandbox port found for %s, using fallback: %s\n", contestantID, fallback)
		return fallback
	}
	url := fmt.Sprintf("http://localhost:%s", port)
	fmt.Printf("Auto-discovered target: %s\n", url)
	return url
}

func main() {
	var numBots int
	var targetURL string
	var contestantID string
	var topic string
	var durationStr string
	var mode string
	var multiInstrument bool

	var rootCmd = &cobra.Command{
		Use:   "bot-fleet",
		Short: "Distributed bot fleet for stress testing trading engines",
		Run: func(cmd *cobra.Command, args []string) {
			var totalDuration time.Duration
			if d, ok := TestModes[mode]; ok {
				totalDuration = d
				fmt.Printf("Mode: %s (%v)\n", mode, totalDuration)
			} else {
				var err error
				totalDuration, err = time.ParseDuration(durationStr)
				if err != nil {
					fmt.Println("Invalid duration. Use format like 60s, 90s, 120s or mode: blitz, standard, marathon")
					os.Exit(1)
				}
			}

			rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
			resolvedURL := getTargetURL(rdb, contestantID, targetURL)

			// depth check
			fmt.Print("\nChecking orderbook depth endpoint... ")
			if validateDepth(resolvedURL) {
				fmt.Println("✓ /orderbook/depth supported")
			} else {
				fmt.Println("⚠ /orderbook/depth not found — depth scoring skipped")
			}

			// price-time priority test
			fmt.Print("\nRunning price-time priority test... ")
			ptpResult := runPriceTimePriorityTest(resolvedURL)
			if ptpResult {
				fmt.Println("✓ Price-time priority verified")
			} else {
				fmt.Println("⚠ Price-time priority could not be verified")
			}

			waveDuration := totalDuration / 6

			fmt.Printf("\n=== STRESS TEST: %v | %d bots | %s | mode=%s ===\n", totalDuration, numBots, contestantID, mode)

			start := time.Now()

			totalWaves := 5
			if multiInstrument {
				totalWaves = 6
			}

			w1 := runWave(1, "Limit Orders", "limit", resolvedURL, contestantID, numBots, waveDuration, "ETH")
			pushWaveProgress(rdb, contestantID, w1, totalWaves, false)

			w2 := runWave(2, "Market Orders", "market", resolvedURL, contestantID, numBots, waveDuration, "ETH")
			pushWaveProgress(rdb, contestantID, w2, totalWaves, false)

			w3 := runWave(3, "Cancel Orders", "cancel", resolvedURL, contestantID, numBots, waveDuration, "ETH")
			pushWaveProgress(rdb, contestantID, w3, totalWaves, false)

			w4 := runWave(4, "Mixed Sustained", "mixed", resolvedURL, contestantID, numBots, waveDuration, "ETH")
			pushWaveProgress(rdb, contestantID, w4, totalWaves, false)

			w5 := runChaosWave(5, resolvedURL, contestantID, numBots/2, waveDuration)

			if multiInstrument {
				pushWaveProgress(rdb, contestantID, w5, totalWaves, false)
				w6 := runWave(6, "BTC Multi-Instrument", "mixed", resolvedURL, contestantID, numBots/2, waveDuration, "BTC")
				pushWaveProgress(rdb, contestantID, w6, totalWaves, true)
				elapsed := time.Since(start)
				printFinalReport([]WaveResult{w1, w2, w3, w4, w5, w6}, elapsed, mode)
				pushToRedpanda(append(w1.Results, append(w2.Results, append(w3.Results, append(w4.Results, append(w5.Results, w6.Results...)...)...)...)...), topic)
			} else {
				pushWaveProgress(rdb, contestantID, w5, totalWaves, true)
				elapsed := time.Since(start)
				printFinalReport([]WaveResult{w1, w2, w3, w4, w5}, elapsed, mode)
				pushToRedpanda(append(w1.Results, append(w2.Results, append(w3.Results, append(w4.Results, w5.Results...)...)...)...), topic)
			}
		},
	}

	rootCmd.Flags().IntVarP(&numBots, "bots", "b", 1000, "Number of concurrent bots")
	rootCmd.Flags().StringVarP(&targetURL, "target", "t", "http://localhost:8080", "Target URL fallback")
	rootCmd.Flags().StringVarP(&contestantID, "contestant", "c", "contestant_001", "Contestant ID")
	rootCmd.Flags().StringVarP(&topic, "topic", "p", "bot-results", "Redpanda topic")
	rootCmd.Flags().StringVarP(&durationStr, "duration", "d", "60s", "Total test duration")
	rootCmd.Flags().StringVarP(&mode, "mode", "m", "standard", "Test mode: blitz, standard, marathon")
	rootCmd.Flags().BoolVarP(&multiInstrument, "multi", "i", false, "Enable multi-instrument BTC+ETH testing")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}