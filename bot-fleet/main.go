package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Result struct {
	BotID   int
	Latency time.Duration
	Success bool
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

func main() {
	targetURL := "http://localhost:8080"
	numBots := 100

	results := make(chan Result, numBots)
	var wg sync.WaitGroup

	fmt.Printf("Spawning %d bots against %s\n", numBots, targetURL)

	for i := 0; i < numBots; i++ {
		wg.Add(1)
		go runBot(i, targetURL, &wg, results)
	}

	wg.Wait()
	close(results)

	// collect metrics
	var totalLatency time.Duration
	success := 0
	failed := 0

	for r := range results {
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
}