package worker_test

// Worker tests run a real coordinator on a loopback port and then start
// worker.Run in the background.
//
// These depend on Part B. Get the coordinator tests passing first.
//
//	go test ./coordinator/
//	go test ./worker/
//	go test -race ./worker/

import (
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"

	"cs4513/project1/coordinator"
	"cs4513/project1/types"
	"cs4513/project1/worker"
)

// testServer starts a coordinator on a random loopback port and returns an
// RPC client plus a cleanup function.
//
// cleanup closes the listener and any accepted connections. The disconnect
// test needs both.
func testServer(t *testing.T) (addr string, client *rpc.Client, cleanup func()) {
	t.Helper()

	c := coordinator.New()
	srv := rpc.NewServer()
	if err := srv.Register(c); err != nil {
		t.Fatalf("register coordinator: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	// Track accepted connections so cleanup can close them too.
	var (
		connsMu sync.Mutex
		conns   []net.Conn
	)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			connsMu.Lock()
			conns = append(conns, conn)
			connsMu.Unlock()
			go srv.ServeConn(conn)
		}
	}()

	addr = ln.Addr().String()

	client, err = rpc.Dial("tcp", addr)
	if err != nil {
		ln.Close()
		t.Fatalf("dial coordinator: %v", err)
	}

	cleanup = func() {
		ln.Close()
		connsMu.Lock()
		for _, conn := range conns {
			conn.Close()
		}
		connsMu.Unlock()
		client.Close()
	}
	return addr, client, cleanup
}

// call fails the test immediately if the RPC returns an error.
func call(t *testing.T, client *rpc.Client, method string, args, reply any) {
	t.Helper()
	if err := client.Call(method, args, reply); err != nil {
		t.Fatalf("RPC %s: %v", method, err)
	}
}

// submitJob submits one job and returns its ID.
func submitJob(t *testing.T, client *rpc.Client, jobType, payload string) types.JobID {
	t.Helper()
	var id types.JobID
	call(t, client, "Coordinator.SubmitJob", types.JobSpec{
		Type:    jobType,
		Payload: []byte(payload),
	}, &id)
	return id
}

// waitForState polls QueryJob until the job reaches wantState.
func waitForState(t *testing.T, client *rpc.Client, id types.JobID, wantState types.JobState, timeout time.Duration) types.JobStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status types.JobStatus
		call(t, client, "Coordinator.QueryJob", id, &status)
		if status.State == wantState {
			return status
		}
		time.Sleep(50 * time.Millisecond)
	}
	var status types.JobStatus
	call(t, client, "Coordinator.QueryJob", id, &status)
	t.Fatalf("job %s: timed out waiting for %s, current state: %s", id, wantState, status.State)
	return status
}

// runWorker starts worker.Run in the background.
func runWorker(addr string) <-chan error {
	ch := make(chan error, 1)
	go func() {
		ch <- worker.Run(addr)
	}()
	return ch
}

// A worker should register and then be able to pick up work.
func TestWorker_RegistersOnConnect(t *testing.T) {
	addr, client, cleanup := testServer(t)
	defer cleanup()

	runWorker(addr)
	time.Sleep(200 * time.Millisecond)

	id := submitJob(t, client, "reverse", `{"text":"hi"}`)
	waitForState(t, client, id, types.StateDone, 3*time.Second)
}

// A single job should complete with the expected output.
func TestWorker_ProcessesSingleJob(t *testing.T) {
	addr, client, cleanup := testServer(t)
	defer cleanup()

	runWorker(addr)

	id := submitJob(t, client, "reverse", `{"text":"hello"}`)
	status := waitForState(t, client, id, types.StateDone, 3*time.Second)

	if status.Err != "" {
		t.Errorf("expected no error, got %q", status.Err)
	}
	want := `{"result":"olleh"}`
	if string(status.Output) != want {
		t.Errorf("output: got %s, want %s", status.Output, want)
	}
}

// The worker should handle every supported job type.
func TestWorker_ProcessesAllJobTypes(t *testing.T) {
	addr, client, cleanup := testServer(t)
	defer cleanup()

	runWorker(addr)

	cases := []struct {
		jobType string
		payload string
	}{
		{"sort", `{"numbers":[3,1,4,1,5]}`},
		{"wordcount", `{"text":"the cat sat on the mat"}`},
		{"checksum", `{"data":"hello world"}`},
		{"reverse", `{"text":"golang"}`},
	}

	ids := make([]types.JobID, len(cases))
	for i, tc := range cases {
		ids[i] = submitJob(t, client, tc.jobType, tc.payload)
	}

	for i, id := range ids {
		status := waitForState(t, client, id, types.StateDone, 3*time.Second)
		if status.Err != "" {
			t.Errorf("job %s (%s): unexpected error: %s", id, cases[i].jobType, status.Err)
		}
		if len(status.Output) == 0 {
			t.Errorf("job %s (%s): output is empty", id, cases[i].jobType)
		}
	}
}

// One worker should be able to process several jobs in a row.
func TestWorker_ProcessesMultipleJobsSequentially(t *testing.T) {
	const numJobs = 5
	addr, client, cleanup := testServer(t)
	defer cleanup()

	runWorker(addr)

	ids := make([]types.JobID, numJobs)
	for i := range ids {
		ids[i] = submitJob(t, client, "reverse", `{"text":"test"}`)
	}

	for _, id := range ids {
		waitForState(t, client, id, types.StateDone, 5*time.Second)
	}
}

// Completed jobs should keep the worker ID.
func TestWorker_SetsWorkerIDOnResult(t *testing.T) {
	addr, client, cleanup := testServer(t)
	defer cleanup()

	runWorker(addr)

	id := submitJob(t, client, "reverse", `{"text":"abc"}`)
	status := waitForState(t, client, id, types.StateDone, 3*time.Second)

	if status.WorkerID == "" {
		t.Error("WorkerID is empty on completed job")
	}
}

// A bad payload should fail the job, but the worker should keep going.
func TestWorker_HandlesInvalidPayloadAsFailure(t *testing.T) {
	addr, client, cleanup := testServer(t)
	defer cleanup()

	runWorker(addr)

	badID := submitJob(t, client, "sort", `{"numbers":"not-an-array"}`)
	goodID := submitJob(t, client, "reverse", `{"text":"hello"}`)

	badStatus := waitForState(t, client, badID, types.StateFailed, 3*time.Second)
	if badStatus.Err == "" {
		t.Error("expected a non-empty Err for the bad job")
	}

	waitForState(t, client, goodID, types.StateDone, 3*time.Second)
}

// Two workers should be able to drain the queue together.
//
// Run this one with -race too:
//
//	go test -race ./worker/ -run TestWorker_TwoWorkersDrainQueue
func TestWorker_TwoWorkersDrainQueue(t *testing.T) {
	const numJobs = 10
	addr, client, cleanup := testServer(t)
	defer cleanup()

	runWorker(addr)
	runWorker(addr)

	ids := make([]types.JobID, numJobs)
	for i := range ids {
		ids[i] = submitJob(t, client, "reverse", `{"text":"parallel"}`)
	}

	for _, id := range ids {
		waitForState(t, client, id, types.StateDone, 5*time.Second)
	}

	var summaries []types.JobSummary
	call(t, client, "Coordinator.ListJobs", struct{}{}, &summaries)
	for _, s := range summaries {
		if s.State != types.StateDone {
			t.Errorf("job %s still in state %s after all workers finished", s.ID, s.State)
		}
	}
}

// worker.Run should return an error if the coordinator disconnects.
func TestWorker_ExitsOnCoordinatorDisconnect(t *testing.T) {
	addr, _, cleanup := testServer(t)

	workerDone := runWorker(addr)

	time.Sleep(200 * time.Millisecond)

	cleanup()

	select {
	case err := <-workerDone:
		if err == nil {
			t.Error("worker.Run returned nil; expected a non-nil error on disconnect")
		}
	case <-time.After(4 * time.Second):
		t.Error("worker.Run did not return within 4s after coordinator disconnected")
	}
}
