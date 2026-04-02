// worker connects to the coordinator and processes jobs.
//
//	go run ./cmd/worker -addr localhost:8080
package main

import (
	"flag"
	"log"

	"cs4513/project1/worker"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "coordinator address (host:port)")
	flag.Parse()

	log.Printf("worker connecting to coordinator at %s", *addr)

	if err := worker.Run(*addr); err != nil {
		log.Fatalf("worker: %v", err)
	}
}
