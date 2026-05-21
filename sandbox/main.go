package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
)

// We define a struct to format our response cleanly
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

	// Capture the raw output from the Docker container
	rawOutput, err := cmd.CombinedOutput()
	
	// Package the output into our JSON struct
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

	// Run the binary and get the structured result
	result := executeContestantBinary(dstPath)

	// Send the result back to the user as clean JSON!
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func main() {
	http.HandleFunc("/upload", uploadHandler)
	
	port := ":8080"
	fmt.Printf("Sandbox API (JSON Output) starting on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}}
