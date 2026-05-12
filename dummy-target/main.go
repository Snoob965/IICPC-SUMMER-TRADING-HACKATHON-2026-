package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func orderbookHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"best_bid": 1820.50,
		"best_ask": 1821.00,
		"spread":   0.50,
	})
}

func orderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var order map[string]interface{}
	json.NewDecoder(r.Body).Decode(&order)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "accepted",
		"order_id": fmt.Sprintf("ord_%v", order["bot_id"]),
	})
}

func main() {
	http.HandleFunc("/orderbook", orderbookHandler)
	http.HandleFunc("/order", orderHandler)
	fmt.Println("Dummy target running on :8080")
	http.ListenAndServe(":8080", nil)
}