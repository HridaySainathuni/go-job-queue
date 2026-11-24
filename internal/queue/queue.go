// Package queue implements a single-node, in-memory job queue.
//
// This is Milestone 1 of the go-job-queue project. Jobs live only in
// memory: there is no persistence, replication, or network API yet.
// Delivery is at-least-once: a leased job that is not acknowledged
// before its lease expires becomes available for leasing again.
package queue

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
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
}

// Queue is a thread-safe, single-node, in-memory FIFO job queue.
type Queue struct {
	mu           sync.Mutex
	leaseTimeout time.Duration
	sweepEvery   time.Duration

	byID   map[string]*jobState
	byKey  map[string]string // idempotency key -> job ID
	ready  []string          // job IDs waiting to be leased, FIFO order
	leased map[string]string // lease ID -> job ID

	stopCh chan struct{}
	doneCh chan struct{}
}

// New returns an empty Queue. A non-positive leaseTimeout selects
// DefaultLeaseTimeout. The queue runs a background sweeper that returns
// expired leases to the ready list; call Stop to shut it down.
func New(leaseTimeout time.Duration) *Queue {
	if leaseTimeout <= 0 {
		leaseTimeout = DefaultLeaseTimeout
	}
	q := &Queue{
		leaseTimeout: leaseTimeout,
		sweepEvery:   leaseTimeout / 2,
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
	q.byID[id] = &jobState{job: job}
	if idempotencyKey != "" {
		q.byKey[idempotencyKey] = id
	}
	q.ready = append(q.ready, id)
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
	st.job.Attempts++
	q.leased[st.leaseID] = id
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
	q.ready = append(q.ready, id)
	return nil
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
			q.ready = append(q.ready, id)
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
