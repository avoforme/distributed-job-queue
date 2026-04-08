// Package worker implements the worker process.
//
// A worker connects over RPC, registers once, then loops:
// request a job, run it locally, report the result.
package worker

import (
	"cs4513/project1/jobs"
	"cs4513/project1/types"
	"fmt"
	"net/rpc"
	"time"
)

// Run connects to the coordinator and processes jobs until it hits an error.
func Run(addr string) error {
	// TODO: implement
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer client.Close()
	
	var reply types.WorkerID
	err = client.Call("Coordinator.Register", types.WorkerInfo{}, &reply)
	if err != nil {
		return fmt.Errorf("Fail to register worker: %w", err)
	}
	workerID := reply

	for {
		var newJob types.Job
		err = client.Call("Coordinator.RequestJob", workerID, &newJob)
		if (err != nil) {
			if types.IsNoWork(err) {
    			time.Sleep(1 * time.Second)
				continue
			} else { return fmt.Errorf("Worker not registerd: %w", err) }
		}

		result := jobs.ExecuteJob(newJob)
		result.WorkerID = workerID
		if result.Err != "" {
			print(result.Err)
		}
		
		err = client.Call("Coordinator.ReportResult", result, &struct{}{})
		if err != nil {
			return fmt.Errorf("Job not found: %w", err)
		}

	}

}
