// Package jobs contains the built-in job implementations.
//
// Do not modify this file.
package jobs

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"cs4513/project1/types"
)

// ExecuteJob runs one job.
// Exactly one of Output or Err is set in the result.
func ExecuteJob(job types.Job) types.JobResult {
	var output []byte
	var execErr string

	switch job.Spec.Type {
	case "sort":
		output, execErr = runSort(job.Spec.Payload)
	case "wordcount":
		output, execErr = runWordCount(job.Spec.Payload)
	case "checksum":
		output, execErr = runChecksum(job.Spec.Payload)
	case "reverse":
		output, execErr = runReverse(job.Spec.Payload)
	default:
		execErr = fmt.Sprintf("unknown job type %q", job.Spec.Type)
	}

	return types.JobResult{
		JobID:  job.ID,
		Output: output,
		Err:    execErr,
	}
}

// runSort expects {"numbers":[...]} and returns {"sorted":[...]}.
func runSort(payload []byte) ([]byte, string) {
	var input struct {
		Numbers []int `json:"numbers"`
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Sprintf("sort: invalid payload: %v", err)
	}
	sorted := make([]int, len(input.Numbers))
	copy(sorted, input.Numbers)
	sort.Ints(sorted)
	out, _ := json.Marshal(map[string]any{"sorted": sorted})
	return out, ""
}

// runWordCount expects {"text":"..."} and returns {"counts":{...}}.
func runWordCount(payload []byte) ([]byte, string) {
	var input struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Sprintf("wordcount: invalid payload: %v", err)
	}
	counts := make(map[string]int)
	for _, word := range strings.Fields(input.Text) {
		counts[strings.ToLower(word)]++
	}
	out, _ := json.Marshal(map[string]any{"counts": counts})
	return out, ""
}

// runChecksum expects {"data":"..."} and returns {"checksum":"..."}.
func runChecksum(payload []byte) ([]byte, string) {
	var input struct {
		Data string `json:"data"`
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Sprintf("checksum: invalid payload: %v", err)
	}
	sum := sha256.Sum256([]byte(input.Data))
	out, _ := json.Marshal(map[string]any{"checksum": fmt.Sprintf("%x", sum)})
	return out, ""
}

// runReverse expects {"text":"..."} and returns {"result":"..."}.
func runReverse(payload []byte) ([]byte, string) {
	var input struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Sprintf("reverse: invalid payload: %v", err)
	}
	runes := []rune(input.Text)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	out, _ := json.Marshal(map[string]any{"result": string(runes)})
	return out, ""
}
