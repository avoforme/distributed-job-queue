// Package coordinator implements the RPC coordinator server.
package coordinator

// Add what you need here.
import (
	"cs4513/project1/types"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
	"container/heap"
)

type Item struct {
	jobID    types.JobID
	priority int   
	seq int
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less (i, j int) bool {
	if pq[i].priority != pq[j].priority {
		return pq[i].priority > pq[j].priority
	}
	return pq[i].seq < pq[j].seq
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x any) {
	item := x.(*Item)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[0 : n-1]
	return item
}

// jobRecord is the coordinator's private copy of a job and its status.
type jobRecord struct {
	// TODO: add fields
	seq int
	job types.Job
	jobStatus types.JobStatus
}

// Coordinator is the RPC server.
// Keep shared state here and protect it with a mutex.
type Coordinator struct {
	// TODO: add fields
	mu sync.Mutex
	jobs map[types.JobID] *jobRecord
	// queue []types.JobID
	queue PriorityQueue
	workers map[types.WorkerID] time.Time
	nextJobID int64
	nextWorkerID int64
}

// New returns an initialized Coordinator.
func New() *Coordinator {
	// TODO: implement
	c := Coordinator{
		jobs: make(map[types.JobID]*jobRecord),
		workers: make(map[types.WorkerID]time.Time),
		// queue: []types.JobID{},
		queue: PriorityQueue{},
		nextJobID: 0,
		nextWorkerID: 0,
	}
	heap.Init(&c.queue)
	return &c
}

// Start creates a Coordinator, registers it, and starts listening on addr.
func Start(addr string) error {
	// TODO: implement
	c:= New()

	srv := rpc.NewServer()
	srv.Register(c)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("Failed to start coordinator listener: %w", err)
	}

	for {
		conn, err:= ln.Accept()
		if err != nil {
			return fmt.Errorf("Fail to accept connection: %w", err)
		}
		go srv.ServeConn(conn)
	}
}

// SubmitJob adds a new job and returns its ID.
func (c *Coordinator) SubmitJob(spec types.JobSpec, reply *types.JobID) error {
	// TODO: implement
	c.mu.Lock()
	defer c.mu.Unlock()
	newSequence := atomic.AddInt64(&c.nextJobID, 1)
	newJobID := types.JobID(fmt.Sprintf("job-%d", newSequence))
	newJob := types.Job{
		ID: newJobID,
		Spec: spec,
	}
	newJobStatus := types.JobStatus{
		ID: newJobID,
		State: types.StatePending,
	}
	newJobEntry := jobRecord{
		seq: int(newSequence),
		job: newJob,
		jobStatus: newJobStatus,
	}

	// Go automatically dereferences pointers when accessing fields.
	c.jobs[newJobID] = &newJobEntry
	// c.queue = append(c.queue, newJobID)
	heap.Push(&c.queue, &Item{
		jobID: newJobID,
		priority: spec.Priority,
		seq: int(newSequence),
	})

	*reply = newJobID
	return nil
}

// QueryJob returns the current status of a job.
func (c *Coordinator) QueryJob(id types.JobID, reply *types.JobStatus) error {
	// TODO: implement
	c.mu.Lock()
	defer c.mu.Unlock()
	jobRecord, ok := c.jobs[id]
	if !ok {
		return fmt.Errorf("No job with this ID")
	}
	*reply = jobRecord.jobStatus
	return nil
}

// ListJobs returns a summary of every known job.
func (c *Coordinator) ListJobs(_ struct{}, reply *[]types.JobSummary) error {
	// TODO: implement
	c.mu.Lock()
	defer c.mu.Unlock()
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
	c.mu.Lock()
	defer c.mu.Unlock()
	newWorkerID := types.WorkerID(fmt.Sprintf("worker-%d", atomic.AddInt64(&c.nextWorkerID, 1)))
	c.workers[newWorkerID] = time.Time{}
	*reply = newWorkerID

	return nil
}

// RequestJob hands out the next pending job.
func (c *Coordinator) RequestJob(workerID types.WorkerID, reply *types.Job) error {
	// TODO: implement
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok :=  c.workers[workerID]
	if !ok {
		return fmt.Errorf("Worker not registered")
	}

	if len(c.queue) == 0 {                
   		return types.ErrNoWork
	}
	
	// job := c.jobs[c.queue[0]]
	// c.queue = c.queue[1:]
	item := heap.Pop(&c.queue).(*Item)
	job := c.jobs[item.jobID]

	job.jobStatus.State = types.StateRunning
	job.jobStatus.WorkerID = workerID
	
	*reply = job.job
	return nil
}

// JobID    JobID
// WorkerID WorkerID
// Output   []byte
// Err      string
// ReportResult stores the result of a finished job.
func (c *Coordinator) ReportResult(result types.JobResult, _ *struct{}) error {
	// TODO: implement
	c.mu.Lock()
	defer c.mu.Unlock()
	job, ok :=  c.jobs[result.JobID]
	if !ok {
		return fmt.Errorf("No job found")
	}

	if (result.Err == "") {
		job.jobStatus.State = types.StateDone
		job.jobStatus.Output = result.Output

	} else {
		job.jobStatus.State = types.StateFailed
		job.jobStatus.Err = result.Err
	}
	return nil
}
