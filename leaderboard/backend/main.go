package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type WaveScore struct {
	WaveNum     int     `json:"wave_num"`
	Label       string  `json:"label"`
	P50         float64 `json:"p50_ms"`
	P90         float64 `json:"p90_ms"`
	P99         float64 `json:"p99_ms"`
	SuccessRate float64 `json:"success_rate"`
	Correctness float64 `json:"correctness"`
	TPS         float64 `json:"tps"`
	Score       float64 `json:"score"`
}

type ContestantEntry struct {
	ContestantID string      `json:"contestant_id"`
	OverallScore float64     `json:"overall_score"`
	OverallP99   float64     `json:"overall_p99"`
	OverallSR    float64     `json:"overall_success_rate"`
	Waves        []WaveScore `json:"waves"`
	Rank         int         `json:"rank"`
}

func getLeaderboardData(rdb *redis.Client) ([]ContestantEntry, error) {
	contestants, err := rdb.ZRevRangeWithScores(ctx, "leaderboard:ranking", 0, -1).Result()
	if err != nil {
		return nil, err
	}

	var leaderboard []ContestantEntry
	for i, z := range contestants {
		contestantID := z.Member.(string)
		dataStr, err := rdb.HGet(ctx, "leaderboard:scores", contestantID).Result()
		if err != nil {
			continue
		}

		var entry ContestantEntry
		json.Unmarshal([]byte(dataStr), &entry)
		entry.ContestantID = contestantID
		entry.OverallScore = z.Score
		entry.Rank = i + 1
		leaderboard = append(leaderboard, entry)
	}
	return leaderboard, nil
}

func handleConnections(w http.ResponseWriter, r *http.Request, rdb *redis.Client) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}
	defer ws.Close()

	fmt.Println("Client connected to leaderboard")

	for {
		data, err := getLeaderboardData(rdb)
		if err != nil {
			log.Println("Redis error:", err)
		} else {
			ws.WriteJSON(data)
		}
		time.Sleep(1 * time.Second)
	}
}

func handleHTTP(w http.ResponseWriter, r *http.Request, rdb *redis.Client) {
	data, err := getLeaderboardData(rdb)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(data)
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// test Redis connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}
	fmt.Println("Redis connected")

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleConnections(w, r, rdb)
	})

	http.HandleFunc("/leaderboard", func(w http.ResponseWriter, r *http.Request) {
		handleHTTP(w, r, rdb)
	})

	fmt.Println("Leaderboard server started on :8081")
	fmt.Println("WebSocket: ws://localhost:8081/ws")
	fmt.Println("HTTP:      http://localhost:8081/leaderboard")
	log.Fatal(http.ListenAndServe(":8081", nil))
}