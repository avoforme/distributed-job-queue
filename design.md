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

---

## Q2: Error wrapping with `fmt.Errorf("...: %w", err)` (2 points)

Show one place in your code where you return an error using
`fmt.Errorf("...: %w", err)`. Explain what extra debugging value the wrapping adds.

*Your answer here.*
