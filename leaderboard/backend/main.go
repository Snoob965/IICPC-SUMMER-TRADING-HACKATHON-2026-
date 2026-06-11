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
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

var ctx = context.Background()
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}
var rdb *redis.Client

// jwtSecret is read from env so it can be rotated without code changes.
// Always override JWT_SECRET in production.
func jwtSecret() []byte {
	if s := os.Getenv("JWT_SECRET"); s != "" {
		return []byte(s)
	}
	return []byte("iicpc-hackathon-secret-change-in-prod")
}

// sandboxURL returns the sandbox base URL.
// Override via SANDBOX_URL env var for Docker or remote deployments.
func sandboxURL() string {
	if u := os.Getenv("SANDBOX_URL"); u != "" {
		return u
	}
	return "http://localhost:8080"
}

// redisAddr returns the Redis address from env.
func redisAddr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	return "localhost:6379"
}

// ─── Data types ──────────────────────────────────────────────────────────────

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
	Username     string      `json:"username,omitempty"`
	OverallScore float64     `json:"overall_score"`
	OverallP99   float64     `json:"overall_p99"`
	OverallP999  float64     `json:"overall_p999"`
	OverallSR    float64     `json:"overall_success_rate"`
	Waves        []WaveScore `json:"waves"`
	Rank         int         `json:"rank"`
}

type HistoryEntry struct {
	Timestamp    int64       `json:"timestamp"`
	OverallScore float64     `json:"overall_score"`
	OverallP99   float64     `json:"overall_p99"`
	OverallSR    float64     `json:"overall_success_rate"`
	Waves        []WaveScore `json:"waves"`
}

// ─── Auth ─────────────────────────────────────────────────────────────────────

type RegisterRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// handleRegister — POST /auth/register
// Stores bcrypt-hashed password in Redis. Idempotent on same username.
func handleRegister(w http.ResponseWriter, r *http.Request) {
	corsHeaders(w)
	if r.Method == "OPTIONS" { w.WriteHeader(http.StatusOK); return }
	if r.Method != "POST" { http.Error(w, "method not allowed", 405); return }

	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Username == "" || req.Password == "" {
		jsonError(w, "username and password required", 400)
		return
	}
	req.Username = strings.ToLower(strings.TrimSpace(req.Username))

	// Check username not already taken
	exists, _ := rdb.Exists(ctx, "auth:user:"+req.Username).Result()
	if exists > 0 {
		jsonError(w, "username already taken", 409)
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		jsonError(w, "internal error", 500)
		return
	}

	rdb.HSet(ctx, "auth:user:"+req.Username,
		"password_hash", string(hash),
		"created_at", time.Now().Unix(),
	)

	token, err := makeJWT(req.Username)
	if err != nil {
		jsonError(w, "token generation failed", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"token":    token,
		"username": req.Username,
	})
}

// handleLogin — POST /auth/login
func handleLogin(w http.ResponseWriter, r *http.Request) {
	corsHeaders(w)
	if r.Method == "OPTIONS" { w.WriteHeader(http.StatusOK); return }
	if r.Method != "POST" { http.Error(w, "method not allowed", 405); return }

	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request", 400)
		return
	}
	req.Username = strings.ToLower(strings.TrimSpace(req.Username))

	hashStr, err := rdb.HGet(ctx, "auth:user:"+req.Username, "password_hash").Result()
	if err != nil {
		jsonError(w, "invalid credentials", 401)
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hashStr), []byte(req.Password)); err != nil {
		jsonError(w, "invalid credentials", 401)
		return
	}

	token, err := makeJWT(req.Username)
	if err != nil {
		jsonError(w, "token generation failed", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"token":    token,
		"username": req.Username,
	})
}

func makeJWT(username string) (string, error) {
	claims := jwt.MapClaims{
		"sub": username,
		"exp": time.Now().Add(24 * time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(jwtSecret())
}

// requireAuth middleware — extracts username from Bearer JWT.
// Returns "" and writes 401 if invalid.
func requireAuth(w http.ResponseWriter, r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "Bearer ") {
		jsonError(w, "missing or invalid Authorization header", 401)
		return ""
	}
	tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return jwtSecret(), nil
	})
	if err != nil || !token.Valid {
		jsonError(w, "invalid or expired token", 401)
		return ""
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		jsonError(w, "invalid token claims", 401)
		return ""
	}
	return claims["sub"].(string)
}

// ─── Leaderboard ─────────────────────────────────────────────────────────────

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

		// Parse the full FinalScore JSON from telemetry.
		// Use a flexible map so we handle both FinalScore and ContestantEntry shapes.
		var raw map[string]json.RawMessage
		if err := json.Unmarshal([]byte(dataStr), &raw); err != nil {
			continue
		}

		entry := ContestantEntry{
			ContestantID: contestantID,
			OverallScore: z.Score,
			Rank:         i + 1,
		}

		// Extract overall_p99, overall_p999, overall_success_rate
		if v, ok := raw["overall_p99"]; ok {
			json.Unmarshal(v, &entry.OverallP99)
		}
		if v, ok := raw["overall_p999"]; ok {
			json.Unmarshal(v, &entry.OverallP999)
		}
		if v, ok := raw["overall_success_rate"]; ok {
			json.Unmarshal(v, &entry.OverallSR)
		}

		// Extract waves array
		if v, ok := raw["waves"]; ok {
			json.Unmarshal(v, &entry.Waves)
		}

		// Attach username if registered
		if uname, err := rdb.HGet(ctx, "contestant:"+contestantID+":meta", "username").Result(); err == nil {
			entry.Username = uname
		}

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
	corsHeaders(w)
	data, err := getLeaderboardData()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// ─── Score history ────────────────────────────────────────────────────────────

// handleHistory — GET /history/{contestantID}
// Returns the last 20 runs for a contestant as a JSON array.
func handleHistory(w http.ResponseWriter, r *http.Request) {
	corsHeaders(w)
	if r.Method == "OPTIONS" { w.WriteHeader(http.StatusOK); return }

	// Extract contestantID from path: /history/{id}
	contestantID := strings.TrimPrefix(r.URL.Path, "/history/")
	if contestantID == "" {
		jsonError(w, "contestant_id required", 400)
		return
	}

	key := fmt.Sprintf("leaderboard:history:%s", contestantID)
	entries, err := rdb.LRange(ctx, key, 0, 19).Result()
	if err != nil || len(entries) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
		return
	}

	var history []HistoryEntry
	for _, e := range entries {
		var h HistoryEntry
		if json.Unmarshal([]byte(e), &h) == nil {
			history = append(history, h)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(history)
}

// ─── Live wave progress ───────────────────────────────────────────────────────

// handleProgress — GET /progress/{contestantID}
// Returns the per-wave progress pushed by bot-fleet during an ongoing test.
func handleProgress(w http.ResponseWriter, r *http.Request) {
	corsHeaders(w)
	if r.Method == "OPTIONS" { w.WriteHeader(http.StatusOK); return }

	contestantID := strings.TrimPrefix(r.URL.Path, "/progress/")
	if contestantID == "" {
		jsonError(w, "contestant_id required", 400)
		return
	}

	key := fmt.Sprintf("leaderboard:progress:%s", contestantID)
	data, err := rdb.Get(ctx, key).Result()
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(data))
}

// ─── Upload proxy ─────────────────────────────────────────────────────────────

// uploadProxy — POST /upload (JWT protected)
func uploadProxy(w http.ResponseWriter, r *http.Request) {
	corsHeaders(w)
	if r.Method == "OPTIONS" { w.WriteHeader(http.StatusOK); return }
	if r.Method != "POST" { http.Error(w, "method not allowed", 405); return }

	// Require valid JWT
	username := requireAuth(w, r)
	if username == "" {
		return // requireAuth already wrote the error
	}

	r.ParseMultipartForm(50 << 20)

	testMode := r.FormValue("test_mode")
	contestantID := r.FormValue("contestant_id")
	if contestantID == "" {
		contestantID = username // default contestant_id to username
	}

	// Store who owns this contestantID
	rdb.HSet(ctx, "contestant:"+contestantID+":meta", "username", username)

	if testMode != "" {
		rdb.Set(ctx, fmt.Sprintf("test_mode:%s", contestantID), testMode, 24*time.Hour)
	}

	file, header, err := r.FormFile("binary")
	if err != nil {
		jsonError(w, "no binary provided", 400)
		return
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, _ := writer.CreateFormFile("binary", header.Filename)
	io.Copy(part, file)
	writer.WriteField("contestant_id", contestantID)
	writer.Close()

	resp, err := http.Post(sandboxURL()+"/upload", writer.FormDataContentType(), body)
	if err != nil {
		jsonError(w, "sandbox unreachable", 503)
		return
	}
	defer resp.Body.Close()

	// Record this run in the contestant's history list (cap at 20 entries)
	// We push a placeholder now; telemetry will overwrite leaderboard:scores
	// but history needs to be appended here so the timestamp is accurate.
	histEntry := HistoryEntry{
		Timestamp: time.Now().Unix(),
	}
	histJSON, _ := json.Marshal(histEntry)
	histKey := fmt.Sprintf("leaderboard:history:%s", contestantID)
	rdb.LPush(ctx, histKey, histJSON)
	rdb.LTrim(ctx, histKey, 0, 19) // keep last 20 only

	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func corsHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr(),
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}
	fmt.Println("Redis connected at", redisAddr())

	http.HandleFunc("/ws",          handleConnections)
	http.HandleFunc("/leaderboard", handleHTTP)
	http.HandleFunc("/upload",      uploadProxy)          // JWT protected
	http.HandleFunc("/auth/register", handleRegister)
	http.HandleFunc("/auth/login",    handleLogin)
	http.HandleFunc("/history/",    handleHistory)        // GET /history/{id}
	http.HandleFunc("/progress/",   handleProgress)       // GET /progress/{id}

	fmt.Println("Leaderboard server started on :8081")
	fmt.Println("  WS:       ws://localhost:8081/ws")
	fmt.Println("  HTTP:     http://localhost:8081/leaderboard")
	fmt.Println("  Upload:   http://localhost:8081/upload      (JWT required)")
	fmt.Println("  Register: http://localhost:8081/auth/register")
	fmt.Println("  Login:    http://localhost:8081/auth/login")
	fmt.Println("  History:  http://localhost:8081/history/{id}")
	fmt.Println("  Progress: http://localhost:8081/progress/{id}")
	log.Fatal(http.ListenAndServe(":8081", nil))
}