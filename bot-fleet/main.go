package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

var ctx = context.Background()

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
	Success      bool          `json:"success"`
	ContestantID string        `json:"contestant_id"`
	Correct      bool          `json:"correct"`
	Wave         int           `json:"wave"`
}

type WaveResult struct {
	WaveNum     int
	Label       string
	OrderType   string
	BotCount    int
	Results     []Result
	P50         float64
	P99         float64
	SuccessRate float64
	Correctness float64
}

func makeOrder(botID int, orderType string) Order {
	sides := []string{"buy", "sell"}
	side := sides[rand.Intn(2)]
	basePrice := 1820.0
	price := basePrice + (rand.Float64()*20 - 10)

	switch orderType {
	case "limit":
		return Order{BotID: botID, Type: "limit", Side: side, Price: price, Quantity: rand.Intn(5) + 1}
	case "market":
		return Order{BotID: botID, Type: "market", Side: side, Quantity: rand.Intn(5) + 1}
	case "cancel":
		return Order{BotID: botID, Type: "cancel", Side: side, Quantity: 1}
	default:
		// mixed
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

func runBot(botID int, targetURL string, contestantID string, orderType string, waveNum int, wg *sync.WaitGroup, results chan<- Result) {
	defer wg.Done()

	order := makeOrder(botID, orderType)
	payload, _ := json.Marshal(order)

	start := time.Now()
	resp, err := http.Post(targetURL+"/order", "application/json", bytes.NewBuffer(payload))
	latency := time.Since(start)

	if err != nil || resp.StatusCode != 200 {
		results <- Result{BotID: botID, OrderType: order.Type, Latency: latency, Success: false, ContestantID: contestantID, Correct: false, Wave: waveNum}
		return
	}
	defer resp.Body.Close()

	correct := validateCorrectness(targetURL, order)
	results <- Result{BotID: botID, OrderType: order.Type, Latency: latency, Success: true, ContestantID: contestantID, Correct: correct, Wave: waveNum}
}

func spawnBots(targetURL string, contestantID string, numBots int, orderType string, waveNum int) []Result {
	resultsChan := make(chan Result, numBots)
	var wg sync.WaitGroup

	for i := 0; i < numBots; i++ {
		wg.Add(1)
		go runBot(i, targetURL, contestantID, orderType, waveNum, &wg, resultsChan)
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

	sort.Float64s(latencies)

	idx50 := int(math.Ceil(50.0/100.0*float64(len(latencies)))) - 1
	idx99 := int(math.Ceil(99.0/100.0*float64(len(latencies)))) - 1

	if len(latencies) > 0 {
		p50 = latencies[idx50]
		p99 = latencies[idx99]
	}

	successRate = float64(success) / float64(total) * 100
	correctness = float64(correct) / float64(total) * 100
	return
}

func runWave(waveNum int, label string, orderType string, targetURL string, contestantID string, maxBots int, duration time.Duration) WaveResult {
	fmt.Printf("\n[Wave %d - %s] duration: %v\n", waveNum, label, duration)

	steps := []int{maxBots / 10, maxBots / 2, maxBots}
	stepDuration := duration / time.Duration(len(steps))

	var allResults []Result

	for _, bots := range steps {
		fmt.Printf("  %d bots firing...\n", bots)
		results := spawnBots(targetURL, contestantID, bots, orderType, waveNum)
		allResults = append(allResults, results...)
		p50, p99, sr, cr := computeWaveStats(results)
		fmt.Printf("  → p50=%.1fms p99=%.1fms success=%.1f%% correct=%.1f%%\n", p50, p99, sr, cr)
		time.Sleep(stepDuration)
	}

	p50, p99, sr, cr := computeWaveStats(allResults)

	return WaveResult{
		WaveNum:     waveNum,
		Label:       label,
		OrderType:   orderType,
		BotCount:    maxBots,
		Results:     allResults,
		P50:         p50,
		P99:         p99,
		SuccessRate: sr,
		Correctness: cr,
	}
}

func pushToRedpanda(results []Result, topic string) {
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
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

func printFinalReport(waves []WaveResult, totalDuration time.Duration) float64 {
	fmt.Printf("\n╔══════════════════════════════════════════════╗\n")
	fmt.Printf("║           STRESS TEST FINAL REPORT          ║\n")
	fmt.Printf("╚══════════════════════════════════════════════╝\n")

	var overallScore float64

	for _, w := range waves {
		status := "✓"
		if w.SuccessRate < 95 {
			status = "⚠"
		}
		if w.SuccessRate < 80 {
			status = "✗"
		}
		fmt.Printf("\nWave %d %-20s %s  p50=%.1fms  p99=%.1fms  success=%.1f%%  correct=%.1f%%\n",
			w.WaveNum, w.Label, status, w.P50, w.P99, w.SuccessRate, w.Correctness)

		score := (1000.0 / (w.P99 + 1)) * (w.SuccessRate / 100.0) * (w.Correctness / 100.0)
		overallScore += score
	}

	overallScore = overallScore / float64(len(waves))
	fmt.Printf("\n──────────────────────────────────────────────\n")
	fmt.Printf("Total Duration:  %v\n", totalDuration)
	fmt.Printf("FINAL SCORE:     %.4f\n", overallScore)
	fmt.Printf("──────────────────────────────────────────────\n")

	return overallScore
}

func main() {
	var numBots int
	var targetURL string
	var contestantID string
	var topic string
	var durationStr string

	var rootCmd = &cobra.Command{
		Use:   "bot-fleet",
		Short: "Distributed bot fleet for stress testing trading engines",
		Run: func(cmd *cobra.Command, args []string) {
			totalDuration, err := time.ParseDuration(durationStr)
			if err != nil {
				fmt.Println("Invalid duration. Use format like 60s, 90s, 120s")
				os.Exit(1)
			}

			// split duration: 25% each for waves 1-3, 25% for wave 4
			waveDuration := totalDuration / 4

			fmt.Printf("\n=== STRESS TEST: %v | %d bots | %s ===\n", totalDuration, numBots, contestantID)

			start := time.Now()
			var allResults []Result

			w1 := runWave(1, "Limit Orders", "limit", targetURL, contestantID, numBots, waveDuration)
			w2 := runWave(2, "Market Orders", "market", targetURL, contestantID, numBots, waveDuration)
			w3 := runWave(3, "Cancel Orders", "cancel", targetURL, contestantID, numBots, waveDuration)
			w4 := runWave(4, "Mixed Sustained", "mixed", targetURL, contestantID, numBots, waveDuration)

			allResults = append(allResults, w1.Results...)
			allResults = append(allResults, w2.Results...)
			allResults = append(allResults, w3.Results...)
			allResults = append(allResults, w4.Results...)

			elapsed := time.Since(start)
			printFinalReport([]WaveResult{w1, w2, w3, w4}, elapsed)
			pushToRedpanda(allResults, topic)
		},
	}

	rootCmd.Flags().IntVarP(&numBots, "bots", "b", 1000, "Number of concurrent bots")
	rootCmd.Flags().StringVarP(&targetURL, "target", "t", "http://localhost:8080", "Target URL of contestant engine")
	rootCmd.Flags().StringVarP(&contestantID, "contestant", "c", "contestant_001", "Contestant ID")
	rootCmd.Flags().StringVarP(&topic, "topic", "p", "bot-results", "Redpanda topic")
	rootCmd.Flags().StringVarP(&durationStr, "duration", "d", "60s", "Total test duration (e.g. 40s, 60s, 90s, 120s)")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}