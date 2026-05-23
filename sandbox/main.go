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

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// Initialize the Redis client to connect to your team's local infrastructure
var rdb = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379", // Default Redis port
	Password: "",               // No password set in local docker-compose
	DB:       0,                // Default DB
})

type ExecutionResult struct {
	Status string `json:"status"`
	Output string `json:"output"`
	Error  string `json:"error,omitempty"`
}

func executeContestantBinary(binaryPath string) ExecutionResult {
	fmt.Printf("Executing %s...\n", binaryPath)
	absPath, _ := filepath.Abs(binaryPath)

	cmd := exec.Command("docker", "run", "--rm",
		"--memory=256m",
		"--cpus=1.0",
		"--network=none",
		"--read-only",
		"--security-opt=no-new-privileges",
		"-v", fmt.Sprintf("%s:/app/contestant_bot:ro", absPath),
		"ubuntu:22.04", 
		"/app/contestant_bot")

	rawOutput, err := cmd.CombinedOutput()
	
	result := ExecutionResult{
		Output: string(rawOutput),
	}

	if err != nil {
		result.Status = "Failed"
		result.Error = err.Error()
	} else {
		result.Status = "Success"
	}

	return result
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

	os.MkdirAll("./uploads", os.ModePerm)
	dstPath := filepath.Join("./uploads", handler.Filename)
	dst, _ := os.Create(dstPath)
	defer dst.Close()
	io.Copy(dst, file)
	os.Chmod(dstPath, 0755)

	// Get the structured result
	result := executeContestantBinary(dstPath)

	// Convert the result into a JSON string
	jsonString, _ := json.Marshal(result)

	// NEW: Publish the JSON string to the "sandbox_logs" Redis channel
	err = rdb.Publish(ctx, "sandbox_logs", jsonString).Err()
	if err != nil {
		fmt.Printf("Failed to publish to Redis: %v\n", err)
	} else {
		fmt.Println("Successfully published execution logs to Redis!")
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonString)
}

func main() {
	http.HandleFunc("/upload", uploadHandler)
	
	port := ":8080"
	fmt.Printf("Sandbox API (Redis Publisher) starting on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
