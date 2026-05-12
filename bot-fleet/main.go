package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type Result struct {
	BotID   int           `json:"bot_id"`
	Latency time.Duration `json:"latency_ns"`
	Success bool          `json:"success"`
}

func runBot(botID int, targetURL string, wg *sync.WaitGroup, results chan<- Result) {
	defer wg.Done()

	start := time.Now()
	resp, err := http.Get(targetURL + "/orderbook")
	latency := time.Since(start)

	if err != nil || resp.StatusCode != 200 {
		results <- Result{BotID: botID, Latency: latency, Success: false}
		return
	}
	defer resp.Body.Close()
	results <- Result{BotID: botID, Latency: latency, Success: true}
}

func pushToRedis(rdb *redis.Client, results []Result) {
	for _, r := range results {
		data, _ := json.Marshal(r)
		rdb.RPush(ctx, "bot:results", data)
	}
	fmt.Printf("Pushed %d results to Redis\n", len(results))
}

func main() {
	targetURL := "http://localhost:8080"
	numBots := 100

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	resultsChan := make(chan Result, numBots)
	var wg sync.WaitGroup

	fmt.Printf("Spawning %d bots against %s\n", numBots, targetURL)

	for i := 0; i < numBots; i++ {
		wg.Add(1)
		go runBot(i, targetURL, &wg, resultsChan)
	}

	wg.Wait()
	close(resultsChan)

	var allResults []Result
	var totalLatency time.Duration
	success, failed := 0, 0

	for r := range resultsChan {
		allResults = append(allResults, r)
		totalLatency += r.Latency
		if r.Success {
			success++
		} else {
			failed++
		}
	}

	avg := totalLatency / time.Duration(numBots)
	fmt.Printf("\n--- Results ---\n")
	fmt.Printf("Success: %d  Failed: %d\n", success, failed)
	fmt.Printf("Avg Latency: %v\n", avg)

	pushToRedis(rdb, allResults)
}
