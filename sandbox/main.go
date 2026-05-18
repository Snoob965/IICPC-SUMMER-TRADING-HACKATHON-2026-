package main

import (
	"fmt"
	"log"
	"net/http"
)

// This function will eventually run the Docker commands you researched
func executeContestantBinary(binaryName string) {
	fmt.Printf("Preparing to run %s in secure Docker container...\n", binaryName)
	// TODO: Implement exec.Command("docker", "run", "--rm", "--memory=256m", ...)
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	// For now, we are just acknowledging the request
	fmt.Fprintf(w, "Upload endpoint hit! Ready to receive binaries.\n")
	
	// Simulate kicking off the execution environment
	executeContestantBinary("dummy_contestant_bot")
}

func main() {
	http.HandleFunc("/upload", uploadHandler)
	
	port := ":8080"
	fmt.Printf("Sandbox Engine API starting on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
