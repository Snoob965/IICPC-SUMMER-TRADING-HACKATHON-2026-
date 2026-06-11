package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

var rdb = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379",
	Password: "",
	DB:       0,
})

// PORT POOL — ports 8082–8181 (100 slots)
const portPoolKey = "sandbox:port_pool"
const portMin = 8082
const portMax = 8181

func initPortPool() {
	// Only seed the pool if it's empty
	size, _ := rdb.SCard(ctx, portPoolKey).Result()
	if size > 0 {
		return
	}
	pipe := rdb.Pipeline()
	for p := portMin; p <= portMax; p++ {
		pipe.SAdd(ctx, portPoolKey, fmt.Sprintf("%d", p))
	}
	pipe.Exec(ctx)
	fmt.Printf("Port pool initialised: %d–%d (%d slots)\n", portMin, portMax, portMax-portMin+1)
}

func acquirePort() (string, error) {
	port, err := rdb.SPop(ctx, portPoolKey).Result()
	if err != nil {
		return "", fmt.Errorf("no free ports available — all %d slots in use", portMax-portMin+1)
	}
	return port, nil
}

func releasePort(port string) {
	rdb.SAdd(ctx, portPoolKey, port)
}

type ExecutionResult struct {
	Status       string           `json:"status"`
	Output       string           `json:"output"`
	Validation   ValidationResult `json:"validation"`
	Error        string           `json:"error,omitempty"`
	ContestantID string           `json:"contestant_id"`
	TargetURL    string           `json:"target_url"`
}

func startContestantContainer(binaryPath string, contestantID string) (string, error) {
	absPath, _ := filepath.Abs(binaryPath)

	// Acquire a free port from the pool
	port, err := acquirePort()
	if err != nil {
		return "", err
	}

	// Stop and remove only this contestant's existing container — never another's
	exec.Command("docker", "stop", fmt.Sprintf("contestant_%s", contestantID)).Run()
	exec.Command("docker", "rm", fmt.Sprintf("contestant_%s", contestantID)).Run()

	// Release any port this contestant was previously holding
	if oldPort, err := rdb.Get(ctx, fmt.Sprintf("sandbox:%s:port", contestantID)).Result(); err == nil {
		releasePort(oldPort)
	}

	cmd := exec.Command("docker", "run", "-d",
		"--name", fmt.Sprintf("contestant_%s", contestantID),
		"--memory=256m",
		"--cpus=1.0",
		"--security-opt=no-new-privileges",
		"--pids-limit=128",
		"-p", fmt.Sprintf("%s:8080", port),
		"-v", fmt.Sprintf("%s:/app/contestant_bot:ro", absPath),
		"ubuntu:22.04",
		"/app/contestant_bot")

	output, err := cmd.CombinedOutput()
	if err != nil {
		releasePort(port) // give the port back on failure
		return "", fmt.Errorf("container start failed: %s", string(output))
	}

	// Persist port and status in Redis
	rdb.Set(ctx, fmt.Sprintf("sandbox:%s:port", contestantID), port, 30*time.Minute)
	rdb.Set(ctx, fmt.Sprintf("sandbox:%s:status", contestantID), "running", 30*time.Minute)

	fmt.Printf("Container started for %s on port %s\n", contestantID, port)
	return port, nil
}

// waitForContainer polls GET /orderbook on the container until it responds
// or the timeout is reached.
// The 1s initial delay accounts for Docker Desktop on Mac port-forwarding warmup.
func waitForContainer(port string) error {
	time.Sleep(1 * time.Second)
	url := fmt.Sprintf("http://localhost:%s/orderbook", port)
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("container did not become ready within 30 seconds on port %s", port)
}

func stopContestantContainer(contestantID string) {
	// Release the port back to the pool before stopping
	if port, err := rdb.Get(ctx, fmt.Sprintf("sandbox:%s:port", contestantID)).Result(); err == nil {
		releasePort(port)
	}
	exec.Command("docker", "stop", fmt.Sprintf("contestant_%s", contestantID)).Run()
	exec.Command("docker", "rm", fmt.Sprintf("contestant_%s", contestantID)).Run()
	rdb.Del(ctx, fmt.Sprintf("sandbox:%s:port", contestantID))
	rdb.Del(ctx, fmt.Sprintf("sandbox:%s:status", contestantID))
	fmt.Printf("Container stopped for %s\n", contestantID)
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.ParseMultipartForm(10 << 20)
	file, handler, err := r.FormFile("binary")
	if err != nil {
		http.Error(w, "Error retrieving the file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	contestantID := r.FormValue("contestant_id")
	if contestantID == "" {
		contestantID = "contestant_001"
	}

	os.MkdirAll("./uploads", os.ModePerm)
	dstPath := filepath.Join("./uploads", handler.Filename)
	dst, _ := os.Create(dstPath)
	defer dst.Close()
	io.Copy(dst, file)
	os.Chmod(dstPath, 0755)

	port, err := startContestantContainer(dstPath, contestantID)
	if err != nil {
		result := ExecutionResult{
			Status:       "failed",
			Error:        err.Error(),
			ContestantID: contestantID,
		}
		jsonBytes, _ := json.Marshal(result)
		rdb.Publish(ctx, "sandbox_logs", jsonBytes)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Poll until the container is actually serving — no more fixed sleep
	if err := waitForContainer(port); err != nil {
		stopContestantContainer(contestantID)
		result := ExecutionResult{
			Status:       "failed",
			Error:        err.Error(),
			ContestantID: contestantID,
		}
		jsonBytes, _ := json.Marshal(result)
		rdb.Publish(ctx, "sandbox_logs", jsonBytes)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Capture container stdout via docker logs and run basic exchange logic validation.
	// This checks for phantom fills (more fills than orders) using the ORDER:/FILL: prefix
	// convention. Contestants must log in this format for the check to fire.
	logs, _ := exec.Command("docker", "logs", fmt.Sprintf("contestant_%s", contestantID)).CombinedOutput()
	validation := ValidateExecutionLogs(string(logs))
	if !validation.IsValid {
		fmt.Printf("[Sandbox] Validation failed for %s: %s\n", contestantID, validation.Reason)
	} else {
		fmt.Printf("[Sandbox] Validation passed for %s: %s\n", contestantID, validation.Reason)
	}

	result := ExecutionResult{
		Status:       "running",
		ContestantID: contestantID,
		TargetURL:    fmt.Sprintf("http://localhost:%s", port),
		Output:       "Container started successfully",
		Validation:   validation,
	}

	jsonBytes, _ := json.Marshal(result)
	rdb.Publish(ctx, "sandbox_logs", jsonBytes)

	// Push a "test in progress" placeholder to the leaderboard immediately.
	// The WebSocket will pick this up within 3 seconds and show the contestant
	// on the leaderboard while the test is running. Telemetry overwrites this
	// with real scores when done.
	placeholder := map[string]interface{}{
		"contestant_id":        contestantID,
		"overall_score":        0.0,
		"overall_p99":          0.0,
		"overall_p999":         0.0,
		"overall_success_rate": 0.0,
		"status":               "running",
		"waves":                []interface{}{},
	}
	if uname, err := rdb.HGet(ctx, "contestant:"+contestantID+":meta", "username").Result(); err == nil {
		placeholder["username"] = uname
	}
	placeholderJSON, _ := json.Marshal(placeholder)
	rdb.HSet(ctx, "leaderboard:scores", contestantID, placeholderJSON)
	rdb.ZAdd(ctx, "leaderboard:ranking", redis.Z{
		Score:  -1, // negative score so it sorts below all real scores
		Member: contestantID,
	})

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonBytes)
}

func stopHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	contestantID := r.FormValue("contestant_id")
	if contestantID == "" {
		http.Error(w, "contestant_id required", http.StatusBadRequest)
		return
	}

	stopContestantContainer(contestantID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":        "stopped",
		"contestant_id": contestantID,
	})
}

func main() {
	// Seed the port pool on startup (idempotent — safe to call on restart)
	initPortPool()

	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/stop", stopHandler)

	port := ":8080"
	fmt.Printf("Sandbox Engine starting on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}