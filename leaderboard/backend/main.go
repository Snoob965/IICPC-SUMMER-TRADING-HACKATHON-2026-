package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
)

var ctx = context.Background()
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func getLeaderboardData(rdb *redis.Client) ([]map[string]interface{}, error) {
	contestants, err := rdb.ZRevRange(ctx, "leaderboard:ranking", 0, -1).Result()
	if err != nil {
		return nil, err
	}

	var leaderboard []map[string]interface{}
	for _, contestantID := range contestants {
		dataStr, err := rdb.HGet(ctx, "leaderboard:scores", contestantID).Result()
		if err == nil {
			var data map[string]interface{}
			json.Unmarshal([]byte(dataStr), &data)
			data["contestant_id"] = contestantID
			leaderboard = append(leaderboard, data)
		}
	}
	return leaderboard, nil
}

func handleConnections(w http.ResponseWriter, r *http.Request, rdb *redis.Client) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer ws.Close()

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

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", 
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleConnections(w, r, rdb)
	})

	fmt.Println("Leaderboard WebSocket Server started on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
