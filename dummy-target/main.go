package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
)

type PriceLevel struct {
	Price    float64 `json:"price"`
	Quantity int     `json:"quantity"`
	Orders   int     `json:"orders"`
}

type OrderBook struct {
	mu   sync.Mutex
	bids map[float64]int
	asks map[float64]int
}

var book = &OrderBook{
	bids: map[float64]int{
		1820.50: 10, 1820.00: 5, 1819.50: 8,
		1819.00: 3, 1818.50: 6,
	},
	asks: map[float64]int{
		1821.00: 10, 1821.50: 5, 1822.00: 8,
		1822.50: 3, 1823.00: 6,
	},
}

func orderbookHandler(w http.ResponseWriter, r *http.Request) {
	book.mu.Lock()
	defer book.mu.Unlock()

	bestBid, bestAsk := 0.0, 0.0
	for p := range book.bids {
		if p > bestBid {
			bestBid = p
		}
	}
	for p := range book.asks {
		if bestAsk == 0 || p < bestAsk {
			bestAsk = p
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"best_bid": bestBid,
		"best_ask": bestAsk,
		"spread":   bestAsk - bestBid,
	})
}

func depthHandler(w http.ResponseWriter, r *http.Request) {
	book.mu.Lock()
	defer book.mu.Unlock()

	var bids, asks []PriceLevel
	for p, q := range book.bids {
		bids = append(bids, PriceLevel{Price: p, Quantity: q, Orders: q / 2})
	}
	for p, q := range book.asks {
		asks = append(asks, PriceLevel{Price: p, Quantity: q, Orders: q / 2})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"bids": bids,
		"asks": asks,
	})
}

func orderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var order map[string]interface{}
	json.NewDecoder(r.Body).Decode(&order)

	book.mu.Lock()
	orderType, _ := order["type"].(string)
	side, _ := order["side"].(string)
	price, _ := order["price"].(float64)
	qty := 1

	if orderType == "limit" && price > 0 {
		if side == "buy" {
			book.bids[price] += qty
		} else {
			book.asks[price] += qty
		}
	}
	book.mu.Unlock()

	// add some random latency to simulate real engine
	// time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "accepted",
		"order_id": fmt.Sprintf("ord_%v_%d", order["bot_id"], rand.Intn(10000)),
	})
}

func main() {
	http.HandleFunc("/orderbook", orderbookHandler)
	http.HandleFunc("/orderbook/depth", depthHandler)
	http.HandleFunc("/order", orderHandler)
	fmt.Println("Dummy target running on :8080")
	http.ListenAndServe(":8080", nil)
}