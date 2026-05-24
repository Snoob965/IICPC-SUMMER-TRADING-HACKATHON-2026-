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

var rdb = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379",
	Password: "",               
	DB:       0,                
})

// UPDATE: Added the Validation struct from validator.go
type ExecutionResult struct {
	Status     string           `json:"status"`
	Output     string           `json:"output"`
	Validation ValidationResult `json:"validation"`
	Error      string           `json:"error,omitempty"`
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
	outputStr := string(rawOutput)
	
	// NEW: Run the algorithmic validation on the raw logs!
	validationScore := ValidateExecutionLogs(outputStr)

	result := ExecutionResult{
		Output:     outputStr,
		Validation: validationScore,
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

	result := executeContestantBinary(dstPath)
	jsonString, _ := json.Marshal(result)

	rdb.Publish(ctx, "sandbox_logs", jsonString)

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonString)
}

func main() {
	http.HandleFunc("/upload", uploadHandler)
	
	port := ":8080"
	fmt.Printf("Sandbox Engine API + Validator starting on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
