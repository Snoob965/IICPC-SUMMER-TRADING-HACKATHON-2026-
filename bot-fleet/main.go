package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type Order struct {
	BotID    int     `json:"bot_id"`
	Type     string  `json:"type"`  // "limit", "market", "cancel"
	Side     string  `json:"side"`  // "buy", "sell"
	Price    float64 `json:"price"` // only for limit orders
	Quantity int     `json:"quantity"`
}

type Result struct {
	BotID     int           `json:"bot_id"`
	OrderType string        `json:"order_type"`
	Latency   time.Duration `json:"latency_ns"`
	Success   bool          `json:"success"`
}

func randomOrder(botID int) Order {
	sides := []string{"buy", "sell"}
	side := sides[rand.Intn(2)]
	basePrice := 1820.0
	price := basePrice + (rand.Float64()*20 - 10) // +/- 10 from base

	r := rand.Intn(10)
	if r < 6 {
		// 60% limit orders
		return Order{BotID: botID, Type: "limit", Side: side, Price: price, Quantity: rand.Intn(5) + 1}
	} else if r < 9 {
		// 30% market orders
		return Order{BotID: botID, Type: "market", Side: side, Quantity: rand.Intn(5) + 1}
	} else {
		// 10% cancels
		return Order{BotID: botID, Type: "cancel", Side: side, Quantity: 1}
	}
}

func runBot(botID int, targetURL string, wg *sync.WaitGroup, results chan<- Result) {
	defer wg.Done()

	order := randomOrder(botID)
	payload, _ := json.Marshal(order)

	start := time.Now()
	resp, err := http.Post(targetURL+"/order", "application/json", bytes.NewBuffer(payload))
	latency := time.Since(start)

	if err != nil || resp.StatusCode != 200 {
		results <- Result{BotID: botID, OrderType: order.Type, Latency: latency, Success: false}
		return
	}
	defer resp.Body.Close()
	results <- Result{BotID: botID, OrderType: order.Type, Latency: latency, Success: true}
}

func pushToRedis(rdb *redis.Client, results []Result) {
	for _, r := range results {
		data, _ := json.Marshal(r)
		rdb.RPush(ctx, "bot:results", data)
	}
	fmt.Printf("Pushed %d results to Redis\n", len(results))
}

func printStats(results []Result) {
	var totalLatency time.Duration
	success, failed := 0, 0
	orderTypes := map[string]int{}

	for _, r := range results {
		totalLatency += r.Latency
		orderTypes[r.OrderType]++
		if r.Success {
			success++
		} else {
			failed++
		}
	}

	avg := totalLatency / time.Duration(len(results))
	fmt.Printf("\n--- Results ---\n")
	fmt.Printf("Success: %d  Failed: %d\n", success, failed)
	fmt.Printf("Avg Latency: %v\n", avg)
	fmt.Printf("Order mix: limit=%d market=%d cancel=%d\n",
		orderTypes["limit"], orderTypes["market"], orderTypes["cancel"])
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
	for r := range resultsChan {
		allResults = append(allResults, r)
	}

	printStats(allResults)
	pushToRedis(rdb, allResults)
}
