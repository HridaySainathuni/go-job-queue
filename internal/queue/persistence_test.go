package queue

import (
	"testing"
	"time"
)

// reopen closes q and opens a fresh Queue over the same data directory.
func reopen(t *testing.T, q *Queue, dir string, leaseTimeout time.Duration) *Queue {
	t.Helper()
	if err := q.Close(); err != nil {
		t.Fatalf("close queue: %v", err)
	}
	q2, err := Open(dir, leaseTimeout)
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	return q2
}

func TestEnqueueSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir, time.Second)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	enqueued := q.Enqueue([]byte("do work"), "key-1")

	q = reopen(t, q, dir, time.Second)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	if got := q.Len(); got != 1 {
		t.Fatalf("Len after restart = %d, want 1", got)
	}
	job, _, ok := q.Lease()
	if !ok {
		t.Fatal("no job available after restart")
	}
	if job.ID != enqueued.ID || string(job.Payload) != "do work" {
		t.Fatalf("leased job after restart = %+v, want ID %s payload %q", job, enqueued.ID, "do work")
	}
	// Idempotency survives the restart too: the same key returns the
	// stored job instead of creating a duplicate.
	if dup := q.Enqueue([]byte("other"), "key-1"); dup.ID != enqueued.ID {
		t.Fatalf("enqueue with duplicate key after restart created job %s, want %s", dup.ID, enqueued.ID)
	}
}

func TestAckDoesNotResurrectAfterRestart(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir, time.Second)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	q.Enqueue([]byte("do work"), "")
	_, leaseID, ok := q.Lease()
	if !ok {
		t.Fatal("no job to lease")
	}
	if err := q.Ack(leaseID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	q = reopen(t, q, dir, time.Second)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	if got := q.Len(); got != 0 {
		t.Fatalf("Len after restart = %d, want 0 (acked job resurrected)", got)
	}
	if _, _, ok := q.Lease(); ok {
		t.Fatal("leased a job after restart, want none")
	}
}

func TestLeasedJobSurvivesRestartAndReleasesAfterTimeout(t *testing.T) {
	dir := t.TempDir()
	leaseTimeout := 100 * time.Millisecond
	q, err := Open(dir, leaseTimeout)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	enqueued := q.Enqueue([]byte("do work"), "")
	if _, _, ok := q.Lease(); !ok {
		t.Fatal("no job to lease")
	}

	// Restart while the job is still leased: it must not be leasable
	// yet, and it must not be lost.
	q = reopen(t, q, dir, leaseTimeout)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	if got := q.Len(); got != 1 {
		t.Fatalf("Len after restart = %d, want 1 (leased job lost)", got)
	}
	if _, _, ok := q.Lease(); ok {
		t.Fatal("leased a job before its lease expired")
	}

	// After the lease timeout the job becomes available again.
	deadline := time.Now().Add(2 * time.Second)
	for {
		job, _, ok := q.Lease()
		if ok {
			if job.ID != enqueued.ID {
				t.Fatalf("leased job %s, want %s", job.ID, enqueued.ID)
			}
			if job.Attempts != 2 {
				t.Fatalf("Attempts = %d, want 2", job.Attempts)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("leased job never became available after its lease timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReadyOrderSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir, time.Second)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	first := q.Enqueue([]byte("first"), "")
	second := q.Enqueue([]byte("second"), "")

	q = reopen(t, q, dir, time.Second)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	job, _, ok := q.Lease()
	if !ok || job.ID != first.ID {
		t.Fatalf("first lease after restart = %+v, want job %s", job, first.ID)
	}
	job, _, ok = q.Lease()
	if !ok || job.ID != second.ID {
		t.Fatalf("second lease after restart = %+v, want job %s", job, second.ID)
	}
}
