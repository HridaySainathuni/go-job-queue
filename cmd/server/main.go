// Command server is the entry point for the go-job-queue node.
//
// Milestone 2 wires the queue to disk persistence: jobs survive a
// restart of the server. There is no network API or replication yet.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/HridaySainathuni/go-job-queue/internal/queue"
)

func main() {
	dataDir := flag.String("data", "data", "directory holding the queue database")
	flag.Parse()

	q, err := queue.Open(*dataDir, queue.DefaultLeaseTimeout)
	if err != nil {
		log.Fatalf("open queue: %v", err)
	}
	defer func() {
		if err := q.Close(); err != nil {
			log.Printf("close queue: %v", err)
		}
	}()

	log.Printf("go-job-queue node ready (single-node, persisted in %s, lease timeout %s)", *dataDir, queue.DefaultLeaseTimeout)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")
}
