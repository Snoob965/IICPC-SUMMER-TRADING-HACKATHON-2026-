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

	"github.com/twmb/franz-go/pkg/kgo"
)

var ctx = context.Background()

type Order struct {
	BotID    int     `json:"bot_id"`
	Type     string  `json:"type"`    // "limit", "market", "cancel"
	Side     string  `json:"side"`    // "buy", "sell"
	Price    float64 `json:"price"`   // only matters for limit orders
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
	price := basePrice + (rand.Float64()*20 - 10)

	r := rand.Intn(10)
	if r < 6 {
		return Order{BotID: botID, Type: "limit", Side: side, Price: price, Quantity: rand.Intn(5) + 1}
	} else if r < 9 {
		return Order{BotID: botID, Type: "market", Side: side, Quantity: rand.Intn(5) + 1}
	} else {
		return Order{BotID: botID, Type: "cancel", Side: side, Quantity: 1}
	}
}

func runBot(botID int, targetURL string, wg *sync.WaitGroup, results chan<- Result) {
	defer wg.Done()      // tells main() "I'm done" when this function exits
 
	order := randomOrder(botID)         // pick a random order type
	payload, _ := json.Marshal(order)   // convert to JSON

	start := time.Now()                 // start the clock
	resp, err := http.Post(targetURL+"/order", "application/json", bytes.NewBuffer(payload)) // send the order
	latency := time.Since(start)        // stop the clock

	if err != nil || resp.StatusCode != 200 {
		results <- Result{BotID: botID, OrderType: order.Type, Latency: latency, Success: false}
		return
	}
	defer resp.Body.Close()
	results <- Result{BotID: botID, OrderType: order.Type, Latency: latency, Success: true}
}

func pushToRedpanda(results []Result) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
	)
	if err != nil {
		fmt.Println("Redpanda connection error:", err)
		return
	}
	defer client.Close()

	var records []*kgo.Record
	for _, r := range results {
		data, _ := json.Marshal(r)
		records = append(records, &kgo.Record{
			Topic: "bot-results",
			Value: data,
		})
	}

	err = client.ProduceSync(ctx, records...).FirstErr()
	if err != nil {
		fmt.Println("Redpanda produce error:", err)
		return
	}
	fmt.Printf("Pushed %d results to Redpanda topic: bot-results\n", len(results))
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
	numBots := 1000

	resultsChan := make(chan Result, numBots)
	var wg sync.WaitGroup

	fmt.Printf("Spawning %d bots against %s\n", numBots, targetURL)

	for i := 0; i < numBots; i++ {
		wg.Add(1)                                   // "expecting one more bot to finish"
		go runBot(i, targetURL, &wg, resultsChan)   // launch bot as goroutine
	}

	wg.Wait()                  // block here until all 100 call wg.Done()
	close(resultsChan)

	var allResults []Result
	for r := range resultsChan {
		allResults = append(allResults, r)
	}

	printStats(allResults)
	pushToRedpanda(allResults)
}
