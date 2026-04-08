# CS 4513 Project 1 Design Questions

## Q1: Worker crash between `RequestJob` and `ReportResult` (4 points)

In the current implementation, a worker crashes after calling `RequestJob` but before calling `ReportResult`.
What state is the job left in right after the crash? 

Assume the coordinator may optionally use timeout reaping: if a job stays in
`running` too long without hearing back from its worker, the coordinator puts
it back in the pending queue. 

Without timeout reaping, what happens to the job? 

With timeout reaping, what happens instead? 

What risk do you create if the timeout is too short?

*Your answer here.*
The job is stuck in running. Without reaping, the job will be stuck in running state forever, and the output will never get returned to the coordinator. With timeout reaping, the coordinator eventually returns the job to pending after some defined time, and the job will be assigned/run again. But if the timeout is too short, another worker will be assigned that job again, and two workers end up running the same job, creating duplicates.

---

## Q2: Error wrapping with `fmt.Errorf("...: %w", err)` (2 points)

Show one place in your code where you return an error using
`fmt.Errorf("...: %w", err)`. Explain what extra debugging value the wrapping adds.

*Your answer here.*
One place this can be used in my code is in the Start function when setting up the listener: 
`ln, err := net.Listen("tcp", addr)
if err != nil {
	return fmt.Errorf("failed to start coordinator listener: %w", err)
}`
Using fmt.Errorf("...: %w", err) wraps the original error with additional context. The original error (err) is preserved, so it can still be inspected later. The added message ("Failed to start coordinator listener") gives higher-level context about where and why the error occurred.