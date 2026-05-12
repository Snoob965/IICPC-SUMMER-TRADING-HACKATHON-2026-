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

func main() {
	http.HandleFunc("/orderbook", orderbookHandler)
	fmt.Println("Dummy target running on :8080")
	http.ListenAndServe(":8080", nil)
}
