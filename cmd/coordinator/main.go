// coordinator starts the RPC server.
//
//	go run ./cmd/coordinator -port 8080
package main

import (
	"flag"
	"fmt"
	"log"

	"cs4513/project1/coordinator"
)

func main() {
	port := flag.Int("port", 8080, "TCP port to listen on")
	flag.Parse()

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("coordinator starting on %s", addr)

	if err := coordinator.Start(addr); err != nil {
		log.Fatalf("coordinator: %v", err)
	}
}
