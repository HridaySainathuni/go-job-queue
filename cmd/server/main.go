// Command server is the entry point for the go-job-queue node.
//
// Milestone 1 only wires up the single-node in-memory queue. There is no
// network API, persistence, or replication yet.
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/HridaySainathuni/go-job-queue/internal/queue"
)

func main() {
	q := queue.New(queue.DefaultLeaseTimeout)
	defer q.Stop()

	log.Printf("go-job-queue node ready (single-node, in-memory, lease timeout %s)", queue.DefaultLeaseTimeout)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")
}
