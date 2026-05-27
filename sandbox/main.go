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

type ExecutionResult struct {
	Status        string           `json:"status"`
	Output        string           `json:"output"`
	Validation    ValidationResult `json:"validation"`
	Error         string           `json:"error,omitempty"`
	ContestantID  string           `json:"contestant_id"`
	TargetURL     string           `json:"target_url"`
}

func startContestantContainer(binaryPath string, contestantID string) (string, error) {
	absPath, _ := filepath.Abs(binaryPath)
	port := "8082"

	// stop any existing container for this contestant
	exec.Command("docker", "stop", fmt.Sprintf("contestant_%s", contestantID)).Run()
	exec.Command("docker", "rm", fmt.Sprintf("contestant_%s", contestantID)).Run()

	cmd := exec.Command("docker", "run", "-d",
		"--name", fmt.Sprintf("contestant_%s", contestantID),
		"--memory=256m",
		"--cpus=1.0",
		"--security-opt=no-new-privileges",
		"-p", fmt.Sprintf("%s:8080", port),
		"-v", fmt.Sprintf("%s:/app/contestant_bot:ro", absPath),
		"ubuntu:22.04",
		"/app/contestant_bot")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("container start failed: %s", string(output))
	}

	// publish port to Redis so bot fleet can find it
	rdb.Set(ctx, fmt.Sprintf("sandbox:%s:port", contestantID), port, 30*time.Minute)
	rdb.Set(ctx, fmt.Sprintf("sandbox:%s:status", contestantID), "running", 30*time.Minute)

	fmt.Printf("Container started for %s on port %s\n", contestantID, port)
	return port, nil
}

func stopContestantContainer(contestantID string) {
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
			Status:       "Failed",
			Error:        err.Error(),
			ContestantID: contestantID,
		}
		jsonBytes, _ := json.Marshal(result)
		rdb.Publish(ctx, "sandbox_logs", jsonBytes)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// wait for container to be ready
	time.Sleep(2 * time.Second)

	result := ExecutionResult{
		Status:       "running",
		ContestantID: contestantID,
		TargetURL:    fmt.Sprintf("http://localhost:%s", port),
		Output:       "Container started successfully",
	}

	jsonBytes, _ := json.Marshal(result)
	rdb.Publish(ctx, "sandbox_logs", jsonBytes)

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
	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/stop", stopHandler)

	port := ":8080"
	fmt.Printf("Sandbox Engine starting on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}