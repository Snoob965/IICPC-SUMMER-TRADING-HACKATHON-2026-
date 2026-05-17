package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
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

func runBot(botID int, targetURL string, contestantID string, wg *sync.WaitGroup, results chan<- Result) {
	defer wg.Done()

	order := randomOrder(botID)
	payload, _ := json.Marshal(order)

	start := time.Now()
	resp, err := http.Post(targetURL+"/order", "application/json", bytes.NewBuffer(payload))
	latency := time.Since(start)

	if err != nil || resp.StatusCode != 200 {
		results <- Result{BotID: botID, OrderType: order.Type, Latency: latency, Success: false, ContestantID: contestantID}
		return
	}
	defer resp.Body.Close()
	results <- Result{BotID: botID, OrderType: order.Type, Latency: latency, Success: true, ContestantID: contestantID}
}

func pushToRedpanda(results []Result, topic string) {
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
			Topic: topic,
			Value: data,
		})
	}

	err = client.ProduceSync(ctx, records...).FirstErr()
	if err != nil {
		fmt.Println("Redpanda produce error:", err)
		return
	}
	fmt.Printf("Pushed %d results to Redpanda topic: %s\n", len(results), topic)
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
	var numBots int
	var targetURL string
	var contestantID string
	var topic string

	var rootCmd = &cobra.Command{
		Use:   "bot-fleet",
		Short: "Distributed bot fleet for stress testing trading engines",
		Run: func(cmd *cobra.Command, args []string) {
			resultsChan := make(chan Result, numBots)
			var wg sync.WaitGroup

			fmt.Printf("Spawning %d bots against %s (contestant: %s)\n", numBots, targetURL, contestantID)

			for i := 0; i < numBots; i++ {
				wg.Add(1)
				go runBot(i, targetURL, contestantID, &wg, resultsChan)
			}

			wg.Wait()
			close(resultsChan)

			var allResults []Result
			for r := range resultsChan {
				allResults = append(allResults, r)
			}

			printStats(allResults)
			pushToRedpanda(allResults, topic)
		},
	}

	rootCmd.Flags().IntVarP(&numBots, "bots", "b", 1000, "Number of concurrent bots")
	rootCmd.Flags().StringVarP(&targetURL, "target", "t", "http://localhost:8080", "Target URL of contestant engine")
	rootCmd.Flags().StringVarP(&contestantID, "contestant", "c", "contestant_001", "Contestant ID")
	rootCmd.Flags().StringVarP(&topic, "topic", "p", "bot-results", "Redpanda topic to publish results")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}