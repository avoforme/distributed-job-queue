// Package coordinator implements the RPC coordinator server.
package coordinator

// Add what you need here.
import (
	"cs4513/project1/types"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

// jobRecord is the coordinator's private copy of a job and its status.
type jobRecord struct {
	// TODO: add fields
	job types.Job
	jobStatus types.JobStatus
}

// Coordinator is the RPC server.
// Keep shared state here and protect it with a mutex.
type Coordinator struct {
	// TODO: add fields
	mu sync.Mutex
	jobs map[types.JobID] *jobRecord
	queue []types.JobID
	workers map[types.JobID] time.Time
	nextJobID int64
	nextWorkerID int64
}

// New returns an initialized Coordinator.
func New() *Coordinator {
	// TODO: implement
	return &Coordinator{
		jobs: make(map[types.JobID]*jobRecord),
		workers: make(map[types.JobID]time.Time),
		queue: []types.JobID{},
		nextJobID: 0,
		nextWorkerID: 0,
	}
}

// Start creates a Coordinator, registers it, and starts listening on addr.
func Start(addr string) error {
	// TODO: implement
	c:= New()

	srv := rpc.NewServer()
	srv.Register(c)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Fail to submit job")
		return err
	}

	for {
		conn, err:= ln.Accept()
		if err != nil {
			log.Fatalf("Fail to submit job")
			return err
		}
		go srv.ServeConn(conn)
	}
	return nil
}

// SubmitJob adds a new job and returns its ID.
func (c *Coordinator) SubmitJob(spec types.JobSpec, reply *types.JobID) error {
	// TODO: implement
	c.mu.Lock()
	newJobID := types.JobID(fmt.Sprintf("job-%d", atomic.AddInt64(&c.nextJobID, 1)))
	newJob := types.Job{
		ID: newJobID,
		Spec: spec,
	}
	newJobStatus := types.JobStatus{
		ID: newJobID,
		State: types.StatePending,
	}
	newJobEntry := jobRecord{
		job: newJob,
		jobStatus: newJobStatus,
	}

	// Go automatically dereferences pointers when accessing fields.
	c.jobs[newJobID] = &newJobEntry
	c.queue = append(c.queue, newJobID)

	*reply = newJobID
	c.mu.Unlock()
	return nil
}

// QueryJob returns the current status of a job.
func (c *Coordinator) QueryJob(id types.JobID, reply *types.JobStatus) error {
	// TODO: implement
	jobRecord, err := c.jobs[id]
	*reply = jobRecord.jobStatus

	if !err {
		return fmt.Errorf("No job with this ID")
	}
	return nil
}

// ListJobs returns a summary of every known job.
func (c *Coordinator) ListJobs(_ struct{}, reply *[]types.JobSummary) error {
	// TODO: implement
	for jobID, jobRecord := range c.jobs {
		jobSummary := types.JobSummary{
			ID: jobID,
			Type: jobRecord.job.Spec.Type,
			State: jobRecord.jobStatus.State,
		}

		*reply = append(*reply, jobSummary)
	}
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
