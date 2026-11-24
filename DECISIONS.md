# Decisions

A log of design decisions for go-job-queue. Newest first.

## 2026-11-24: Module and project layout (Milestone 1)

- Module: `github.com/HridaySainathuni/go-job-queue`, Go 1.23.
- Layout: `cmd/server` holds the node entry point; `internal/queue` holds
  the queue itself. The queue is an internal package because it is an
  implementation detail of this service, not a library for other modules.
- The server binary in this milestone only starts the in-memory queue and
  waits for a shutdown signal. The gRPC API, persistence, and replication
  land in later milestones and will build on the same `Queue` type.
