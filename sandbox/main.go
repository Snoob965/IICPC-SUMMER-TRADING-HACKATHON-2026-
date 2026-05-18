package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
)

func executeContestantBinary(binaryPath string) {
	fmt.Printf("Preparing to run %s in secure Docker container...\n", binaryPath)

	// We need the absolute path of the file on your Mac to mount it into Docker
	absPath, err := filepath.Abs(binaryPath)
	if err != nil {
		fmt.Printf("Error getting absolute path: %v\n", err)
		return
	}

	// The '-v' flag mounts the uploaded file into the container at '/app/contestant_bot'
	cmd := exec.Command("docker", "run", "--rm",
		"--memory=256m",
		"--cpus=1.0",
		"--network=none",
		"--read-only",
		"--security-opt=no-new-privileges",
		"-v", fmt.Sprintf("%s:/app/contestant_bot:ro", absPath),
		"ubuntu:22.04", 
		"/app/contestant_bot")

	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Container execution failed: %v\nOutput: %s\n", err, string(output))
		return
	}

	fmt.Printf("Container Output:\n%s\n", string(output))
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1. Parse the incoming file (up to 10 MB limit)
	r.ParseMultipartForm(10 << 20)

	// 2. Retrieve the file from the request
	file, handler, err := r.FormFile("binary")
	if err != nil {
		http.Error(w, "Error retrieving the file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// 3. Create an 'uploads' directory if it doesn't exist yet
	os.MkdirAll("./uploads", os.ModePerm)

	// 4. Create the destination file on your machine
	dstPath := filepath.Join("./uploads", handler.Filename)
	dst, err := os.Create(dstPath)
	if err != nil {
		http.Error(w, "Error saving the file", http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	// 5. Copy the downloaded data into your new file
	if _, err := io.Copy(dst, file); err != nil {
		http.Error(w, "Error writing the file", http.StatusInternalServerError)
		return
	}
	
	// 6. Ensure the file has execution permissions
	os.Chmod(dstPath, 0755)

	fmt.Fprintf(w, "Successfully uploaded %s! Kickstarting execution...\n", handler.Filename)

	// 7. Trigger the Docker container using the real file we just saved!
	executeContestantBinary(dstPath)
}

func main() {
	http.HandleFunc("/upload", uploadHandler)
	
	port := ":8080"
	fmt.Printf("Sandbox Engine API starting on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
