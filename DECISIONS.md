# Decisions

A log of design decisions for go-job-queue. Newest first.

## 2026-11-25: Persistence backend is BoltDB (Milestone 2)

- Choice: jobs are stored in a BoltDB file (`go.etcd.io/bbolt` v1.4.0),
  one file (`queue.db`) in the server data directory.
- Alternatives considered: Pebble (a Log-Structured Merge-tree store from
  CockroachDB) and keeping the in-memory queue with a write-ahead log.
- Why BoltDB: it is a single-file embedded key/value store with
  transactions, so the queue needs no separate database process and no
  extra deployment. Pebble is built for high-write LSM workloads and
  brings more moving parts than a single-node queue needs right now.
  A write-ahead log on top of the in-memory queue would have meant
  building and testing a second storage layer for the same durability.
- Tradeoff accepted: BoltDB writes are serialized through a single
  writer transaction, so write throughput is bounded by one writer at a
  time. That is fine for a single-node queue at this stage; replication
  and higher write concurrency are later milestones.

## 2026-11-25: Persist on every mutation, reload on startup (Milestone 2)

- Choice: every mutation (enqueue, lease, ack, nack) is written to the
  BoltDB file inside a transaction before the call returns. On startup
  the queue loads all persisted jobs from the file and resumes with the
  same state, including lease state.
- Alternatives considered: periodic snapshots of the in-memory queue,
  and write-behind (acknowledge first, flush later).
- Why persist on every mutation: the acceptance criterion is that no
  acknowledged job is lost in a crash, and only a synchronous write
  gives that guarantee. Snapshots would lose every job enqueued since
  the last snapshot, and write-behind would lose jobs accepted just
  before a crash. The cost is one disk transaction per mutation, which
  is acceptable for a single-node queue.
- Reload on startup keeps the existing `Queue` API unchanged: callers
  see the same FIFO order, idempotency keys, and lease timeouts whether
  the queue was just created or reloaded from disk.

## 2026-11-24: Module and project layout (Milestone 1)

- Module: `github.com/HridaySainathuni/go-job-queue`, Go 1.23.
- Layout: `cmd/server` holds the node entry point; `internal/queue` holds
  the queue itself. The queue is an internal package because it is an
  implementation detail of this service, not a library for other modules.
- The server binary in this milestone only starts the in-memory queue and
  waits for a shutdown signal. The gRPC API, persistence, and replication
  land in later milestones and will build on the same `Queue` type.
