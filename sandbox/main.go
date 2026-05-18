package main

import (
	"fmt"
	"log"
	"net/http"
	"os/exec"
)

// This function triggers the secure Docker container using your research!
func executeContestantBinary(binaryName string) {
	fmt.Printf("Preparing to run %s in secure Docker container...\n", binaryName)

	// Here are all the flags from your DOCKER_SANDBOX_RESEARCH.md
	cmd := exec.Command("docker", "run", "--rm",
		"--memory=256m",
		"--cpus=1.0",
		"--network=none",
		"--read-only",
		"--security-opt=no-new-privileges",
		"ubuntu:22.04", 
		"echo", "Sandbox secure environment is working!") // We use echo just to test it safely

	// Capture the output from the Docker container
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Container execution failed: %v\nOutput: %s\n", err, string(output))
		return
	}

	// Print the results to the server log
	fmt.Printf("Container Output:\n%s\n", string(output))
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	fmt.Fprintf(w, "Upload endpoint hit! Kickstarting execution...\n")
	
	// Trigger the Docker function
	executeContestantBinary("dummy_bot")
}

func main() {
	http.HandleFunc("/upload", uploadHandler)
	
	port := ":8080"
	fmt.Printf("Sandbox Engine API starting on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
