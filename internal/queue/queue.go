// Package queue implements a single-node job queue with disk persistence.
//
// This is Milestone 2 of the go-job-queue project. Jobs are stored in a
// BoltDB file and reloaded on startup, so a restarted server resumes with
// its queue intact. There is no replication or network API yet. Delivery
// is at-least-once: a leased job that is not acknowledged before its
// lease expires becomes available for leasing again.
package queue

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

// Job is a unit of work held by the queue.
type Job struct {
	// ID uniquely identifies the job within this queue.
	ID string
	// IdempotencyKey is the caller-supplied key used to deduplicate
	// enqueues. Empty means no deduplication for this job.
	IdempotencyKey string
	// Payload is the opaque job body supplied at enqueue time.
	Payload []byte
	// Attempts counts how many times the job has been leased.
	Attempts int
}

// ErrNotFound is returned when an ack or nack names an unknown lease.
var ErrNotFound = errors.New("queue: lease not found")

// DefaultLeaseTimeout is the lease duration used by New when the caller
// passes a non-positive timeout.
const DefaultLeaseTimeout = 30 * time.Second

type jobState struct {
	job       Job
	leased    bool
	leaseID   string
	deadline  time.Time
	completed bool
	readySeq  uint64 // FIFO position while ready; 0 when leased
}

// Queue is a thread-safe, single-node job queue. When opened with Open,
// jobs are persisted to disk on every mutation and reloaded on startup;
// a queue built with New keeps jobs in memory only.
type Queue struct {
	mu           sync.Mutex
	leaseTimeout time.Duration
	sweepEvery   time.Duration
	db           *bbolt.DB // nil for in-memory queues

	byID         map[string]*jobState
	byKey        map[string]string // idempotency key -> job ID
	ready        []string          // job IDs waiting to be leased, FIFO order
	leased       map[string]string // lease ID -> job ID
	nextReadySeq uint64

	stopCh chan struct{}
	doneCh chan struct{}
}

// New returns an empty in-memory Queue. A non-positive leaseTimeout
// selects DefaultLeaseTimeout. The queue runs a background sweeper that
// returns expired leases to the ready list; call Stop to shut it down.
func New(leaseTimeout time.Duration) *Queue {
	return newQueue(leaseTimeout, nil)
}

// newQueue builds a Queue, optionally backed by db. The caller must hold
// no lock; Open loads stored jobs after this returns.
func newQueue(leaseTimeout time.Duration, db *bbolt.DB) *Queue {
	if leaseTimeout <= 0 {
		leaseTimeout = DefaultLeaseTimeout
	}
	q := &Queue{
		leaseTimeout: leaseTimeout,
		sweepEvery:   leaseTimeout / 2,
		db:           db,
		byID:         make(map[string]*jobState),
		byKey:        make(map[string]string),
		leased:       make(map[string]string),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
	if q.sweepEvery < 10*time.Millisecond {
		q.sweepEvery = 10 * time.Millisecond
	}
	go q.sweepLoop()
	return q
}

// Stop shuts down the background lease sweeper.
func (q *Queue) Stop() {
	close(q.stopCh)
	<-q.doneCh
}

// Enqueue adds a job carrying payload to the queue and returns it. When
// idempotencyKey is non-empty and a job with that key already exists, the
// existing job is returned and no duplicate is created.
func (q *Queue) Enqueue(payload []byte, idempotencyKey string) Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	if idempotencyKey != "" {
		if id, ok := q.byKey[idempotencyKey]; ok {
			return q.byID[id].job
		}
	}

	id := newID()
	job := Job{
		ID:             id,
		IdempotencyKey: idempotencyKey,
		Payload:        payload,
	}
	st := &jobState{job: job, readySeq: q.nextReadySeq}
	q.nextReadySeq++
	q.byID[id] = st
	if idempotencyKey != "" {
		q.byKey[idempotencyKey] = id
	}
	q.ready = append(q.ready, id)
	q.persistLocked(st)
	return job
}

// EnqueueApplied inserts a job whose ID was already assigned, as applied
// from the replicated log. Every replica applies the same command, so
// every replica stores the same job ID. Like Enqueue it is idempotent:
// a job with the same idempotency key or the same ID is returned
// unchanged instead of being stored twice.
func (q *Queue) EnqueueApplied(job Job) Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	if job.IdempotencyKey != "" {
		if id, ok := q.byKey[job.IdempotencyKey]; ok {
			return q.byID[id].job
		}
	}
	if st, ok := q.byID[job.ID]; ok {
		return st.job
	}

	st := &jobState{job: job, readySeq: q.nextReadySeq}
	q.nextReadySeq++
	q.byID[job.ID] = st
	if job.IdempotencyKey != "" {
		q.byKey[job.IdempotencyKey] = job.ID
	}
	q.ready = append(q.ready, job.ID)
	q.persistLocked(st)
	return job
}

// Lease returns the oldest ready job and a lease ID for it. The job is
// reserved until the lease expires or the caller acks or nacks it. When
// no job is ready, ok is false.
func (q *Queue) Lease() (job Job, leaseID string, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.reclaimExpiredLocked(time.Now())
	if len(q.ready) == 0 {
		return Job{}, "", false
	}

	id := q.ready[0]
	q.ready = q.ready[1:]
	st := q.byID[id]
	st.leased = true
	st.leaseID = newID()
	st.deadline = time.Now().Add(q.leaseTimeout)
	st.readySeq = 0
	st.job.Attempts++
	q.leased[st.leaseID] = id
	q.persistLocked(st)
	return st.job, st.leaseID, true
}

// Ack removes the job held under leaseID from the queue. It returns
// ErrNotFound when no live lease matches.
func (q *Queue) Ack(leaseID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	id, ok := q.leased[leaseID]
	if !ok {
		return ErrNotFound
	}
	delete(q.leased, leaseID)
	st := q.byID[id]
	st.completed = true
	st.leased = false
	delete(q.byID, id)
	if st.job.IdempotencyKey != "" {
		delete(q.byKey, st.job.IdempotencyKey)
	}
	q.deleteLocked(id)
	return nil
}

// Nack returns the job held under leaseID to the ready list so it can be
// leased again. It returns ErrNotFound when no live lease matches.
func (q *Queue) Nack(leaseID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	id, ok := q.leased[leaseID]
	if !ok {
		return ErrNotFound
	}
	delete(q.leased, leaseID)
	st := q.byID[id]
	st.leased = false
	st.leaseID = ""
	st.deadline = time.Time{}
	st.readySeq = q.nextReadySeq
	q.nextReadySeq++
	q.ready = append(q.ready, id)
	q.persistLocked(st)
	return nil
}

// Get returns the job with the given ID, reporting whether it exists.
// Leased jobs are returned too; completed jobs are gone.
func (q *Queue) Get(id string) (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	st, ok := q.byID[id]
	if !ok {
		return Job{}, false
	}
	return st.job, true
}

// Len returns the number of jobs currently in the queue, ready or leased.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.byID)
}

// reclaimExpiredLocked returns leased jobs whose deadline has passed to
// the ready list. The caller must hold q.mu.
func (q *Queue) reclaimExpiredLocked(now time.Time) {
	for leaseID, id := range q.leased {
		st := q.byID[id]
		if now.After(st.deadline) {
			delete(q.leased, leaseID)
			st.leased = false
			st.leaseID = ""
			st.deadline = time.Time{}
			st.readySeq = q.nextReadySeq
			q.nextReadySeq++
			q.ready = append(q.ready, id)
			q.persistLocked(st)
		}
	}
}

func (q *Queue) sweepLoop() {
	defer close(q.doneCh)
	t := time.NewTicker(q.sweepEvery)
	defer t.Stop()
	for {
		select {
		case <-q.stopCh:
			return
		case now := <-t.C:
			q.mu.Lock()
			q.reclaimExpiredLocked(now)
			q.mu.Unlock()
		}
	}
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
