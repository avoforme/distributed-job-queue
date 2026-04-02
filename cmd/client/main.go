// client is a small CLI for submit/query/list.
//
//	go run ./cmd/client -addr localhost:8080 submit <type> <payload-json>
//	go run ./cmd/client -addr localhost:8080 query <job-id>
//	go run ./cmd/client -addr localhost:8080 list
package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "coordinator address (host:port)")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: client -addr <host:port> <submit|query|list> [args...]")
		os.Exit(1)
	}

	client, err := rpc.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("client: connect to %s: %v", *addr, err)
	}
	defer client.Close()

	switch args[0] {
	case "submit":
		cmdSubmit(client, args[1:])
	case "query":
		cmdQuery(client, args[1:])
	case "list":
		cmdList(client)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", args[0])
		os.Exit(1)
	}
}

// cmdSubmit submits a job and prints the returned ID.
func cmdSubmit(client *rpc.Client, args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: client submit <type> <payload-json>")
		os.Exit(1)
	}
	// TODO: implement
}

// cmdQuery prints the current state of one job.
func cmdQuery(client *rpc.Client, args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: client query <job-id>")
		os.Exit(1)
	}
	// TODO: implement
}

// cmdList prints all jobs known to the coordinator.
func cmdList(client *rpc.Client) {
	// TODO: implement
}
