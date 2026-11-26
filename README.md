# go-job-queue

A distributed job queue in Go: a gRPC API (enqueue, lease, ack, nack),
replication across 3 nodes with hashicorp/raft, at-least-once delivery with
lease timeouts and idempotency keys, a dead-letter queue after N failures,
persistence with BoltDB or Pebble, Prometheus metrics with a Grafana
dashboard, load and chaos tests, and Docker Compose for a local 3-node
cluster.

## Status

Milestone 2 is done: jobs persist to disk and survive a restart. The
queue keeps the same API from Milestone 1.

## Persistence

- Backend: BoltDB (`go.etcd.io/bbolt` v1.4.0), a single embedded
  key/value store. No separate database process is needed.
- Data lives in `queue.db` inside the data directory. By default the
  server uses a `data` directory in the working directory; pass
  `-data <dir>` to use another one.
- Every mutation (enqueue, lease, ack, nack) is written to disk before
  it returns, so a crash loses no acknowledged job.
- A restart resumes the queue: on startup the server reloads all
  persisted jobs from `queue.db`, including lease state, and continues
  with the same FIFO order, idempotency keys, and lease timeouts.

## What is built so far

- `internal/queue`: a thread-safe FIFO queue with `Enqueue`, `Lease`,
  `Ack`, and `Nack`.
  - Idempotency keys: enqueueing the same key twice returns the same job
    and creates no duplicate.
  - At-least-once delivery: a leased job that is not acked before its
    lease expires becomes available for leasing again.
- `cmd/server`: a server entry point that starts the queue and waits for a
  shutdown signal.

## Planned milestones

1. Single-node queue (done)
2. Persistence (done)
3. Raft replication
4. Leases and retries
5. Metrics
6. Load and chaos tests
7. Design writeup

## Run it

Requires Go 1.23 or later.

```sh
go run ./cmd/server
```

## Test it

```sh
go test ./...
```
