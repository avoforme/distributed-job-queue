package main

// Tests for the client CLI helpers.
//
// These use a mock RPC server, so you can work on Part A without running the
// real coordinator.
//
//	go test ./cmd/client/
//	go test -race ./cmd/client/

import (
	"bytes"
	"io"
	"net"
	"net/rpc"
	"os"
	"strings"
	"testing"

	"cs4513/project1/types"
)

// mockCoordinator is just enough of the coordinator to test the client.
// Set the exported fields to control the RPC replies.
type mockCoordinator struct {
	SubmitReply types.JobID
	QueryReply  types.JobStatus
	ListReply   []types.JobSummary
}

func (m *mockCoordinator) SubmitJob(_ types.JobSpec, reply *types.JobID) error {
	*reply = m.SubmitReply
	return nil
}

func (m *mockCoordinator) QueryJob(_ types.JobID, reply *types.JobStatus) error {
	*reply = m.QueryReply
	return nil
}

func (m *mockCoordinator) ListJobs(_ struct{}, reply *[]types.JobSummary) error {
	*reply = m.ListReply
	return nil
}

// newMockClient returns an RPC client connected to the mock coordinator.
// The service name has to be "Coordinator" because the client calls methods
// like "Coordinator.SubmitJob".
func newMockClient(t *testing.T, mock *mockCoordinator) *rpc.Client {
	t.Helper()

	srv := rpc.NewServer()
	if err := srv.RegisterName("Coordinator", mock); err != nil {
		t.Fatalf("register mock coordinator: %v", err)
	}

	serverConn, clientConn := net.Pipe()
	go srv.ServeConn(serverConn)

	client := rpc.NewClient(clientConn)
	t.Cleanup(func() { client.Close() })
	return client
}

// captureStdout runs f and returns what it printed to stdout.
func captureStdout(t *testing.T, f func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	old := os.Stdout
	os.Stdout = w

	f()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}

// cmdSubmit should print "Submitted" and the job ID.
func TestCmdSubmit_PrintsSubmittedAndJobID(t *testing.T) {
	mock := &mockCoordinator{SubmitReply: "job-42"}
	client := newMockClient(t, mock)

	out := captureStdout(t, func() {
		cmdSubmit(client, []string{"sort", `{"numbers":[3,1,4,1,5]}`})
	})

	if !strings.Contains(out, "Submitted") {
		t.Errorf("expected 'Submitted' in output, got: %q", out)
	}
	if !strings.Contains(out, "job-42") {
		t.Errorf("expected job ID 'job-42' in output, got: %q", out)
	}
}

// A done job should print its state, worker ID, and output.
func TestCmdQuery_DoneJob_PrintsWorkerAndOutput(t *testing.T) {
	mock := &mockCoordinator{
		QueryReply: types.JobStatus{
			ID:       "job-1",
			State:    types.StateDone,
			WorkerID: "worker-1",
			Output:   []byte(`{"sorted":[1,1,3,4,5]}`),
		},
	}
	client := newMockClient(t, mock)

	out := captureStdout(t, func() {
		cmdQuery(client, []string{"job-1"})
	})

	if !strings.Contains(out, "done") {
		t.Errorf("expected 'done' in output, got: %q", out)
	}
	if !strings.Contains(out, "(worker-1)") {
		t.Errorf("expected worker ID in parentheses in output, got: %q", out)
	}
	if !strings.Contains(out, `{"sorted":[1,1,3,4,5]}`) {
		t.Errorf("expected job output in output, got: %q", out)
	}
}

// A pending job should not print a worker ID.
func TestCmdQuery_PendingJob_OmitsWorker(t *testing.T) {
	mock := &mockCoordinator{
		QueryReply: types.JobStatus{
			ID:    "job-1",
			State: types.StatePending,
		},
	}
	client := newMockClient(t, mock)

	out := captureStdout(t, func() {
		cmdQuery(client, []string{"job-1"})
	})

	if !strings.Contains(out, "pending") {
		t.Errorf("expected 'pending' in output, got: %q", out)
	}
	if strings.Contains(out, "(") {
		t.Errorf("expected no worker ID for pending job, got: %q", out)
	}
}

// cmdList should print one line per job.
func TestCmdList_PrintsOneLinePerJob(t *testing.T) {
	mock := &mockCoordinator{
		ListReply: []types.JobSummary{
			{ID: "job-1", Type: "sort", State: types.StateDone},
			{ID: "job-2", Type: "wordcount", State: types.StatePending},
			{ID: "job-3", Type: "reverse", State: types.StateRunning},
		},
	}
	client := newMockClient(t, mock)

	out := captureStdout(t, func() {
		cmdList(client)
	})

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 lines of output, got %d:\n%s", len(lines), out)
	}
	for _, want := range []string{"job-1", "sort", "done", "job-2", "wordcount", "pending", "job-3", "reverse", "running"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output, got:\n%s", want, out)
		}
	}
}
