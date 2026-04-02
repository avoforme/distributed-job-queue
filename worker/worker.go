// Package worker implements the worker process.
//
// A worker connects over RPC, registers once, then loops:
// request a job, run it locally, report the result.
package worker

// Run connects to the coordinator and processes jobs until it hits an error.
func Run(addr string) error {
	// TODO: implement
	return nil
}
