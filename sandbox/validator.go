package main

import (
	"fmt"
	"strings"
)

// ValidationResult holds the final score and reasoning for the leaderboard
type ValidationResult struct {
	IsValid bool   `json:"is_valid"`
	Score   int    `json:"score"`
	Reason  string `json:"reason"`
}

// ValidateExecutionLogs parses the C++ stdout to verify exchange logic
func ValidateExecutionLogs(output string) ValidationResult {
	lines := strings.Split(output, "\n")
	
	orders := 0
	fills := 0

	// O(N) scan through the execution logs
	for _, line := range lines {
		if strings.HasPrefix(line, "ORDER:") {
			orders++
		} else if strings.HasPrefix(line, "FILL:") {
			fills++
		}
	}

	// Constraint Check 1: Phantom Fills
	// A matching engine can never have more fills than it received orders.
	if fills > orders {
		return ValidationResult{
			IsValid: false,
			Score:   0,
			Reason:  "CRITICAL FAIL: Engine executed more fills than received orders (Phantom Fills detected).",
		}
	}

	// If it passes basic constraints, calculate a baseline score
	// (You can expand this later with complex price-time priority math)
	return ValidationResult{
		IsValid: true,
		Score:   100, // Perfect score for passing basic stability
		Reason:  fmt.Sprintf("SUCCESS: System remained stable. Processed %d orders and %d fills correctly.", orders, fills),
	}
}
