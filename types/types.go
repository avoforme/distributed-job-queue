// Package types holds the shared RPC types for the project.
//
// Do not modify this file. It defines the graded interface.
package types

import "errors"

// ErrNoWork is returned when there is nothing to hand out.
var ErrNoWork = errors.New("no work available")

// JobID identifies a submitted job.
type JobID string

// WorkerID identifies a registered worker.
type WorkerID string

// JobState tracks where a job is in its lifecycle.
type JobState int

const (
	StatePending JobState = iota // waiting in the queue
	StateRunning                 // assigned to a worker, not yet complete
	StateDone                    // completed successfully
	StateFailed                  // completed with an error
)

func (s JobState) String() string {
	return [...]string{"pending", "running", "done", "failed"}[int(s)]
}

// JobSpec describes one submitted job.
//
// Type must be one of: "sort", "wordcount", "checksum", "reverse".
// Payload is a JSON-encoded input whose schema depends on Type:
//
//	sort:      {"numbers": [3, 1, 4, 1, 5]}
//	wordcount: {"text": "the cat sat on the mat"}
//	checksum:  {"data": "hello world"}
//	reverse:   {"text": "hello"}
type JobSpec struct {
	Type     string
	Payload  []byte
	Priority int // extension: higher values run first; zero is the default
}

// Job is sent from the coordinator to a worker.
type Job struct {
	ID   JobID
	Spec JobSpec
}

// JobResult is sent back by a worker.
// If Err is non-empty, the job failed.
//
// Output is a JSON-encoded result whose schema depends on job type:
//
//	sort:      {"sorted": [1, 1, 3, 4, 5]}
//	wordcount: {"counts": {"cat": 1, "sat": 1, "the": 2}}
//	checksum:  {"checksum": "b94d27b9..."}
//	reverse:   {"result": "olleh"}
type JobResult struct {
	JobID    JobID
	WorkerID WorkerID
	Output   []byte
	Err      string // empty string means success
}

// JobStatus is returned by QueryJob.
type JobStatus struct {
	ID       JobID
	State    JobState
	WorkerID WorkerID // set once the job is StateRunning or later
	Output   []byte   // set when StateDone
	Err      string   // set when StateFailed
}

// JobSummary is the shorter form returned by ListJobs.
type JobSummary struct {
	ID    JobID
	Type  string
	State JobState
}

// WorkerInfo is sent when a worker registers.
// You can add optional debug fields if you want, but none are required.
type WorkerInfo struct{}

// IsNoWork checks whether err is the RPC form of ErrNoWork.
// After an RPC call, errors come back as strings, so errors.Is will not work.
func IsNoWork(err error) bool {
	return err != nil && err.Error() == ErrNoWork.Error()
}
