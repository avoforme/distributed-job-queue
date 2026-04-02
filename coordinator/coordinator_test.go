package coordinator_test

// These tests use net.Pipe(), so they stay fast and do not need a real TCP
// listener unless the test is specifically about concurrency.
//
//	go test ./coordinator/
//	go test -race ./coordinator/
//	go test -run TestSubmitJob_ReturnsUniqueIDs ./coordinator/

import (
	"bytes"
	"net"
	"net/rpc"
	"sort"
	"testing"

	"cs4513/project1/coordinator"
	"cs4513/project1/types"
)

// newTestClient returns an RPC client connected to a fresh coordinator.
func newTestClient(t *testing.T) *rpc.Client {
	t.Helper()

	c := coordinator.New()
	srv := rpc.NewServer()
	if err := srv.Register(c); err != nil {
		t.Fatalf("register coordinator: %v", err)
	}

	// net.Pipe() returns two ends of a synchronous in-memory connection.
	serverConn, clientConn := net.Pipe()

	// Serve the coordinator on one end.
	go srv.ServeConn(serverConn)

	client := rpc.NewClient(clientConn)
	t.Cleanup(func() { client.Close() })
	return client
}

// call fails the test right away if the RPC returns an error.
func call(t *testing.T, client *rpc.Client, method string, args, reply any) {
	t.Helper()
	if err := client.Call(method, args, reply); err != nil {
		t.Fatalf("RPC %s: %v", method, err)
	}
}

// Checkpoint 1

// SubmitJob should return a new non-empty ID each time.
func TestSubmitJob_ReturnsUniqueIDs(t *testing.T) {
	client := newTestClient(t)

	spec := types.JobSpec{Type: "reverse", Payload: []byte(`{"text":"hello"}`)}

	var id1, id2, id3 types.JobID
	call(t, client, "Coordinator.SubmitJob", spec, &id1)
	call(t, client, "Coordinator.SubmitJob", spec, &id2)
	call(t, client, "Coordinator.SubmitJob", spec, &id3)

	if id1 == "" || id2 == "" || id3 == "" {
		t.Errorf("got empty job ID: %q %q %q", id1, id2, id3)
	}
	if id1 == id2 || id2 == id3 || id1 == id3 {
		t.Errorf("job IDs are not unique: %q %q %q", id1, id2, id3)
	}
}

// A new job should start in StatePending with no worker assigned.
func TestQueryJob_PendingAfterSubmit(t *testing.T) {
	client := newTestClient(t)

	spec := types.JobSpec{Type: "sort", Payload: []byte(`{"numbers":[3,1,2]}`)}
	var id types.JobID
	call(t, client, "Coordinator.SubmitJob", spec, &id)

	var status types.JobStatus
	call(t, client, "Coordinator.QueryJob", id, &status)

	if status.ID != id {
		t.Errorf("QueryJob returned wrong ID: got %q, want %q", status.ID, id)
	}
	if status.State != types.StatePending {
		t.Errorf("expected StatePending, got %s", status.State)
	}
	if status.WorkerID != "" {
		t.Errorf("WorkerID should be empty for a pending job, got %q", status.WorkerID)
	}
}

// Querying an unknown job ID should return an error.
func TestQueryJob_UnknownID(t *testing.T) {
	client := newTestClient(t)

	var status types.JobStatus
	err := client.Call("Coordinator.QueryJob", types.JobID("job-999"), &status)
	if err == nil {
		t.Error("expected an error for unknown job ID, got nil")
	}
}

// ListJobs should return no entries when nothing has been submitted.
//
// net/rpc uses gob, and gob does not distinguish nil and empty slices here.
// Only the length matters.
func TestListJobs_Empty(t *testing.T) {
	client := newTestClient(t)

	var summaries []types.JobSummary
	call(t, client, "Coordinator.ListJobs", struct{}{}, &summaries)

	if len(summaries) != 0 {
		t.Errorf("expected 0 jobs, got %d", len(summaries))
	}
}

// ListJobs should include every submitted job with the right type and state.
func TestListJobs_ReflectsSubmissions(t *testing.T) {
	client := newTestClient(t)

	specs := []types.JobSpec{
		{Type: "sort", Payload: []byte(`{"numbers":[1,2,3]}`)},
		{Type: "wordcount", Payload: []byte(`{"text":"hello world"}`)},
		{Type: "checksum", Payload: []byte(`{"data":"abc"}`)},
	}

	submittedIDs := make(map[types.JobID]string) // id -> type
	for _, spec := range specs {
		var id types.JobID
		call(t, client, "Coordinator.SubmitJob", spec, &id)
		submittedIDs[id] = spec.Type
	}

	var summaries []types.JobSummary
	call(t, client, "Coordinator.ListJobs", struct{}{}, &summaries)

	if len(summaries) != len(specs) {
		t.Fatalf("expected %d summaries, got %d", len(specs), len(summaries))
	}

	for _, s := range summaries {
		wantType, ok := submittedIDs[s.ID]
		if !ok {
			t.Errorf("ListJobs returned unknown job ID %q", s.ID)
			continue
		}
		if s.Type != wantType {
			t.Errorf("job %s: got type %q, want %q", s.ID, s.Type, wantType)
		}
		if s.State != types.StatePending {
			t.Errorf("job %s: expected StatePending, got %s", s.ID, s.State)
		}
	}
}

// Checkpoint 2

// Register should return a new non-empty worker ID each time.
func TestRegister_ReturnsUniqueWorkerIDs(t *testing.T) {
	client := newTestClient(t)

	var w1, w2 types.WorkerID
	call(t, client, "Coordinator.Register", types.WorkerInfo{}, &w1)
	call(t, client, "Coordinator.Register", types.WorkerInfo{}, &w2)

	if w1 == "" || w2 == "" {
		t.Errorf("got empty worker ID: %q %q", w1, w2)
	}
	if w1 == w2 {
		t.Errorf("Register returned the same ID twice: %q", w1)
	}
}

// RequestJob should return ErrNoWork when the queue is empty.
func TestRequestJob_NoWork(t *testing.T) {
	client := newTestClient(t)

	var workerID types.WorkerID
	call(t, client, "Coordinator.Register", types.WorkerInfo{}, &workerID)

	var job types.Job
	err := client.Call("Coordinator.RequestJob", workerID, &job)
	if !types.IsNoWork(err) {
		t.Errorf("expected ErrNoWork, got %v", err)
	}
}

// RequestJob should reject an unknown worker ID.
func TestRequestJob_UnregisteredWorker(t *testing.T) {
	client := newTestClient(t)

	var job types.Job
	err := client.Call("Coordinator.RequestJob", types.WorkerID("ghost-worker"), &job)
	if err == nil {
		t.Error("expected an error for unregistered worker, got nil")
	}
}

// RequestJob should return the submitted job unchanged.
func TestRequestJob_ReturnsCorrectJob(t *testing.T) {
	client := newTestClient(t)

	spec := types.JobSpec{Type: "reverse", Payload: []byte(`{"text":"hello"}`)}
	var submittedID types.JobID
	call(t, client, "Coordinator.SubmitJob", spec, &submittedID)

	var workerID types.WorkerID
	call(t, client, "Coordinator.Register", types.WorkerInfo{}, &workerID)

	var job types.Job
	call(t, client, "Coordinator.RequestJob", workerID, &job)

	if job.ID != submittedID {
		t.Errorf("got job ID %q, want %q", job.ID, submittedID)
	}
	if job.Spec.Type != spec.Type {
		t.Errorf("got job type %q, want %q", job.Spec.Type, spec.Type)
	}
	if !bytes.Equal(job.Spec.Payload, spec.Payload) {
		t.Errorf("got payload %q, want %q", job.Spec.Payload, spec.Payload)
	}
}

// RequestJob should move the job to StateRunning and record the worker ID.
func TestRequestJob_TransitionsToRunning(t *testing.T) {
	client := newTestClient(t)

	spec := types.JobSpec{Type: "sort", Payload: []byte(`{"numbers":[2,1]}`)}
	var id types.JobID
	call(t, client, "Coordinator.SubmitJob", spec, &id)

	var workerID types.WorkerID
	call(t, client, "Coordinator.Register", types.WorkerInfo{}, &workerID)

	var job types.Job
	call(t, client, "Coordinator.RequestJob", workerID, &job)

	var status types.JobStatus
	call(t, client, "Coordinator.QueryJob", id, &status)

	if status.State != types.StateRunning {
		t.Errorf("expected StateRunning, got %s", status.State)
	}
	if status.WorkerID != workerID {
		t.Errorf("expected WorkerID %q, got %q", workerID, status.WorkerID)
	}
}

// Basic success path: submit, request, report, done.
func TestFullPipeline_Success(t *testing.T) {
	client := newTestClient(t)

	spec := types.JobSpec{Type: "reverse", Payload: []byte(`{"text":"hello"}`)}
	var id types.JobID
	call(t, client, "Coordinator.SubmitJob", spec, &id)

	var workerID types.WorkerID
	call(t, client, "Coordinator.Register", types.WorkerInfo{}, &workerID)

	var job types.Job
	call(t, client, "Coordinator.RequestJob", workerID, &job)

	result := types.JobResult{
		JobID:    job.ID,
		WorkerID: workerID,
		Output:   []byte(`{"result":"olleh"}`),
		Err:      "",
	}
	var ignored struct{}
	call(t, client, "Coordinator.ReportResult", result, &ignored)

	var status types.JobStatus
	call(t, client, "Coordinator.QueryJob", id, &status)

	if status.State != types.StateDone {
		t.Errorf("expected StateDone, got %s", status.State)
	}
	if string(status.Output) != `{"result":"olleh"}` {
		t.Errorf("unexpected output: %s", status.Output)
	}
	if status.Err != "" {
		t.Errorf("expected no error, got %q", status.Err)
	}
}

// A non-empty Err should move the job to StateFailed.
func TestFullPipeline_Failure(t *testing.T) {
	client := newTestClient(t)

	spec := types.JobSpec{Type: "sort", Payload: []byte(`{"numbers":"oops"}`)}
	var id types.JobID
	call(t, client, "Coordinator.SubmitJob", spec, &id)

	var workerID types.WorkerID
	call(t, client, "Coordinator.Register", types.WorkerInfo{}, &workerID)

	var job types.Job
	call(t, client, "Coordinator.RequestJob", workerID, &job)

	result := types.JobResult{
		JobID:    job.ID,
		WorkerID: workerID,
		Err:      "sort: invalid payload",
	}
	var ignored struct{}
	call(t, client, "Coordinator.ReportResult", result, &ignored)

	var status types.JobStatus
	call(t, client, "Coordinator.QueryJob", id, &status)

	if status.State != types.StateFailed {
		t.Errorf("expected StateFailed, got %s", status.State)
	}
	if status.Err != "sort: invalid payload" {
		t.Errorf("unexpected Err: %q", status.Err)
	}
}

// Two workers should be able to drain the queue without deadlocking.
//
// Run this one with -race too:
//
//	go test -race ./coordinator/ -run TestConcurrentWorkers
func TestConcurrentWorkers(t *testing.T) {
	const numJobs = 10
	const numWorkers = 2

	// This test needs a real listener because multiple clients share one
	// coordinator instance through separate connections.
	c := coordinator.New()
	srv := rpc.NewServer()
	if err := srv.Register(c); err != nil {
		t.Fatalf("register: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0") // port 0 = OS picks one
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go srv.ServeConn(conn)
		}
	}()

	addr := ln.Addr().String()

	submitter, err := rpc.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer submitter.Close()

	spec := types.JobSpec{Type: "reverse", Payload: []byte(`{"text":"test"}`)}
	submitted := make([]types.JobID, numJobs)
	for i := range submitted {
		call(t, submitter, "Coordinator.SubmitJob", spec, &submitted[i])
	}

	// Start workers. Each one stops after hitting ErrNoWork a few times.
	done := make(chan []types.JobID, numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func() {
			wc, err := rpc.Dial("tcp", addr)
			if err != nil {
				done <- nil
				return
			}
			defer wc.Close()

			var workerID types.WorkerID
			if err := wc.Call("Coordinator.Register", types.WorkerInfo{}, &workerID); err != nil {
				done <- nil
				return
			}

			var completed []types.JobID
			noWorkStreak := 0
			for noWorkStreak < 3 {
				var job types.Job
				err := wc.Call("Coordinator.RequestJob", workerID, &job)
				if types.IsNoWork(err) {
					noWorkStreak++
					continue
				}
				if err != nil {
					break
				}
				noWorkStreak = 0

				result := types.JobResult{
					JobID:    job.ID,
					WorkerID: workerID,
					Output:   []byte(`{"result":"tset"}`),
				}
				var ignored struct{}
				_ = wc.Call("Coordinator.ReportResult", result, &ignored)
				completed = append(completed, job.ID)
			}
			done <- completed
		}()
	}

	allCompleted := make(map[types.JobID]bool)
	for w := 0; w < numWorkers; w++ {
		ids := <-done
		for _, id := range ids {
			allCompleted[id] = true
		}
	}

	if len(allCompleted) != numJobs {
		t.Errorf("expected %d completed jobs, got %d", numJobs, len(allCompleted))
	}

	var summaries []types.JobSummary
	call(t, submitter, "Coordinator.ListJobs", struct{}{}, &summaries)

	states := make(map[types.JobID]types.JobState)
	for _, s := range summaries {
		states[s.ID] = s.State
	}

	sort.Slice(submitted, func(i, j int) bool { return submitted[i] < submitted[j] })
	for _, id := range submitted {
		if states[id] != types.StateDone {
			t.Errorf("job %s: expected StateDone, got %s", id, states[id])
		}
	}
}

// Extension: priority queue

// Higher-priority work should be dispatched first.
func TestPriority_HighBeforeLow(t *testing.T) {
	client := newTestClient(t)

	low := types.JobSpec{Type: "reverse", Payload: []byte(`{"text":"low"}`), Priority: 1}
	high := types.JobSpec{Type: "reverse", Payload: []byte(`{"text":"high"}`), Priority: 10}

	var lowID, highID types.JobID
	call(t, client, "Coordinator.SubmitJob", low, &lowID)
	call(t, client, "Coordinator.SubmitJob", high, &highID)

	var workerID types.WorkerID
	call(t, client, "Coordinator.Register", types.WorkerInfo{}, &workerID)

	var first types.Job
	call(t, client, "Coordinator.RequestJob", workerID, &first)
	if first.ID != highID {
		t.Errorf("expected high-priority job %s first, got %s", highID, first.ID)
	}

	var ignored struct{}
	call(t, client, "Coordinator.ReportResult", types.JobResult{
		JobID: first.ID, WorkerID: workerID, Output: []byte(`{"result":"high"}`),
	}, &ignored)

	var second types.Job
	call(t, client, "Coordinator.RequestJob", workerID, &second)
	if second.ID != lowID {
		t.Errorf("expected low-priority job %s second, got %s", lowID, second.ID)
	}
}

// Equal priorities should still come out FIFO.
func TestPriority_FIFOWithinEqualPriority(t *testing.T) {
	client := newTestClient(t)

	spec := types.JobSpec{Type: "reverse", Payload: []byte(`{"text":"x"}`), Priority: 5}

	ids := make([]types.JobID, 3)
	for i := range ids {
		call(t, client, "Coordinator.SubmitJob", spec, &ids[i])
	}

	var workerID types.WorkerID
	call(t, client, "Coordinator.Register", types.WorkerInfo{}, &workerID)

	got := make([]types.JobID, 3)
	for i := range got {
		var job types.Job
		call(t, client, "Coordinator.RequestJob", workerID, &job)
		got[i] = job.ID
		var ignored struct{}
		call(t, client, "Coordinator.ReportResult", types.JobResult{
			JobID: job.ID, WorkerID: workerID, Output: []byte(`{"result":"x"}`),
		}, &ignored)
	}

	for i, id := range ids {
		if got[i] != id {
			t.Errorf("position %d: expected %s, got %s (FIFO violated)", i, id, got[i])
		}
	}
}

// Mixed priorities should come out highest to lowest.
func TestPriority_MixedPriorities(t *testing.T) {
	client := newTestClient(t)

	specs := []struct {
		priority int
		label    string
	}{
		{1, "p1"},
		{3, "p3"},
		{2, "p2"},
	}

	ids := make(map[int]types.JobID) // priority -> job ID
	for _, s := range specs {
		spec := types.JobSpec{
			Type:     "reverse",
			Payload:  []byte(`{"text":"` + s.label + `"}`),
			Priority: s.priority,
		}
		var id types.JobID
		call(t, client, "Coordinator.SubmitJob", spec, &id)
		ids[s.priority] = id
	}

	var workerID types.WorkerID
	call(t, client, "Coordinator.Register", types.WorkerInfo{}, &workerID)

	wantOrder := []int{3, 2, 1}
	for _, wantPriority := range wantOrder {
		var job types.Job
		call(t, client, "Coordinator.RequestJob", workerID, &job)
		if job.ID != ids[wantPriority] {
			t.Errorf("expected job with priority %d (%s), got %s",
				wantPriority, ids[wantPriority], job.ID)
		}
		var ignored struct{}
		call(t, client, "Coordinator.ReportResult", types.JobResult{
			JobID: job.ID, WorkerID: workerID, Output: []byte(`{"result":"ok"}`),
		}, &ignored)
	}
}
