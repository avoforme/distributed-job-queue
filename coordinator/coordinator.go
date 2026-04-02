// Package coordinator implements the RPC coordinator server.
package coordinator

// Add what you need here.
import (
	"cs4513/project1/types"
)

// jobRecord is the coordinator's private copy of a job and its status.
type jobRecord struct {
	// TODO: add fields
}

// Coordinator is the RPC server.
// Keep shared state here and protect it with a mutex.
type Coordinator struct {
	// TODO: add fields
}

// New returns an initialized Coordinator.
func New() *Coordinator {
	// TODO: implement
	return &Coordinator{}
}

// Start creates a Coordinator, registers it, and starts listening on addr.
func Start(addr string) error {
	// TODO: implement
	return nil
}

// SubmitJob adds a new job and returns its ID.
func (c *Coordinator) SubmitJob(spec types.JobSpec, reply *types.JobID) error {
	// TODO: implement
	return nil
}

// QueryJob returns the current status of a job.
func (c *Coordinator) QueryJob(id types.JobID, reply *types.JobStatus) error {
	// TODO: implement
	return nil
}

// ListJobs returns a summary of every known job.
func (c *Coordinator) ListJobs(_ struct{}, reply *[]types.JobSummary) error {
	// TODO: implement
	return nil
}

// Register assigns a WorkerID to a new worker.
func (c *Coordinator) Register(_ types.WorkerInfo, reply *types.WorkerID) error {
	// TODO: implement
	return nil
}

// RequestJob hands out the next pending job.
func (c *Coordinator) RequestJob(workerID types.WorkerID, reply *types.Job) error {
	// TODO: implement
	return nil
}

// ReportResult stores the result of a finished job.
func (c *Coordinator) ReportResult(result types.JobResult, _ *struct{}) error {
	// TODO: implement
	return nil
}
