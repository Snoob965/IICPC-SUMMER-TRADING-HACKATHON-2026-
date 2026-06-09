package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// sandboxURL returns the sandbox base URL.
// Override via SANDBOX_URL env var for Docker or remote deployments.
func sandboxURL() string {
	if u := os.Getenv("SANDBOX_URL"); u != "" {
		return u
	}
	return "http://localhost:8080"
}

var ctx = context.Background()
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var rdb *redis.Client

type WaveScore struct {
	WaveNum     int     `json:"wave_num"`
	Label       string  `json:"label"`
	P50         float64 `json:"p50_ms"`
	P90         float64 `json:"p90_ms"`
	P99         float64 `json:"p99_ms"`
	P999        float64 `json:"p999_ms"`
	SuccessRate float64 `json:"success_rate"`
	Correctness float64 `json:"correctness"`
	TPS         float64 `json:"tps"`
	Score       float64 `json:"score"`
}

type ContestantEntry struct {
	ContestantID string      `json:"contestant_id"`
	OverallScore float64     `json:"overall_score"`
	OverallP99   float64     `json:"overall_p99"`
	OverallP999  float64     `json:"overall_p999"`
	OverallSR    float64     `json:"overall_success_rate"`
	Waves        []WaveScore `json:"waves"`
	Rank         int         `json:"rank"`
}

func getLeaderboardData() ([]ContestantEntry, error) {
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

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}
	defer ws.Close()

	fmt.Println("Client connected to leaderboard")

	for {
		data, err := getLeaderboardData()
		if err != nil {
			log.Println("Redis error:", err)
		} else {
			ws.WriteJSON(data)
		}
		time.Sleep(3 * time.Second)
	}
}

func handleHTTP(w http.ResponseWriter, r *http.Request) {
	data, err := getLeaderboardData()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(data)
}

func uploadProxy(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.ParseMultipartForm(50 << 20)

	testMode := r.FormValue("test_mode")
	contestantID := r.FormValue("contestant_id")

	if testMode != "" && contestantID != "" {
		rdb.Set(ctx, fmt.Sprintf("test_mode:%s", contestantID), testMode, 24*time.Hour)
	}

	file, header, err := r.FormFile("binary")
	if err != nil {
		http.Error(w, "no binary provided", http.StatusBadRequest)
		return
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, _ := writer.CreateFormFile("binary", header.Filename)
	io.Copy(part, file)
	writer.WriteField("contestant_id", contestantID)
	writer.Close()

	resp, err := http.Post(sandboxURL()+"/upload",
		writer.FormDataContentType(), body)
	if err != nil {
		http.Error(w, "sandbox unreachable", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	io.Copy(w, resp.Body)
}

func main() {
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}
	fmt.Println("Redis connected")

	http.HandleFunc("/ws", handleConnections)
	http.HandleFunc("/leaderboard", handleHTTP)
	http.HandleFunc("/upload", uploadProxy)

	fmt.Println("Leaderboard server started on :8081")
	fmt.Println("WebSocket: ws://localhost:8081/ws")
	fmt.Println("HTTP:      http://localhost:8081/leaderboard")
	fmt.Println("Upload:    http://localhost:8081/upload")
	log.Fatal(http.ListenAndServe(":8081", nil))
}