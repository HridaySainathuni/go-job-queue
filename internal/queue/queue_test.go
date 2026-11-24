package queue

import (
	"testing"
	"time"
)

func TestEnqueueAndLeaseRoundTrip(t *testing.T) {
	q := New(0)
	defer q.Stop()

	enqueued := q.Enqueue([]byte("do work"), "key-1")
	if enqueued.ID == "" {
		t.Fatal("enqueue returned a job with an empty ID")
	}

	leased, leaseID, ok := q.Lease()
	if !ok {
		t.Fatal("expected a job to be available for lease")
	}
	if leased.ID != enqueued.ID {
		t.Fatalf("leased job %q, want %q", leased.ID, enqueued.ID)
	}
	if string(leased.Payload) != "do work" {
		t.Fatalf("leased payload %q, want %q", leased.Payload, "do work")
	}
	if leaseID == "" {
		t.Fatal("lease returned an empty lease ID")
	}
	if leased.Attempts != 1 {
		t.Fatalf("leased job attempts = %d, want 1", leased.Attempts)
	}

	if _, _, ok := q.Lease(); ok {
		t.Fatal("leased job must not be available again while its lease is live")
	}
}

func TestAckRemovesJob(t *testing.T) {
	q := New(0)
	defer q.Stop()

	q.Enqueue([]byte("task"), "key-ack")
	_, leaseID, ok := q.Lease()
	if !ok {
		t.Fatal("expected a job to be available for lease")
	}
	if err := q.Ack(leaseID); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
	if q.Len() != 0 {
		t.Fatalf("queue length = %d after ack, want 0", q.Len())
	}
	if _, _, ok := q.Lease(); ok {
		t.Fatal("acked job must not be leasable again")
	}
	if err := q.Ack(leaseID); err != ErrNotFound {
		t.Fatalf("second ack error = %v, want ErrNotFound", err)
	}
}

func TestNackMakesJobAvailableAgain(t *testing.T) {
	q := New(0)
	defer q.Stop()

	enqueued := q.Enqueue([]byte("retry me"), "key-nack")
	_, leaseID, ok := q.Lease()
	if !ok {
		t.Fatal("expected a job to be available for lease")
	}
	if err := q.Nack(leaseID); err != nil {
		t.Fatalf("nack failed: %v", err)
	}

	leased, _, ok := q.Lease()
	if !ok {
		t.Fatal("nacked job must be available for lease again")
	}
	if leased.ID != enqueued.ID {
		t.Fatalf("leased job %q after nack, want %q", leased.ID, enqueued.ID)
	}
	if leased.Attempts != 2 {
		t.Fatalf("leased job attempts = %d after nack, want 2", leased.Attempts)
	}
}

func TestDuplicateIdempotencyKeyDoesNotDuplicate(t *testing.T) {
	q := New(0)
	defer q.Stop()

	first := q.Enqueue([]byte("payload"), "dup-key")
	second := q.Enqueue([]byte("different payload"), "dup-key")
	if second.ID != first.ID {
		t.Fatalf("duplicate key created a second job %q, want %q", second.ID, first.ID)
	}
	if string(second.Payload) != "payload" {
		t.Fatalf("duplicate enqueue returned payload %q, want the original %q", second.Payload, "payload")
	}
	if q.Len() != 1 {
		t.Fatalf("queue length = %d after duplicate enqueue, want 1", q.Len())
	}

	leased, leaseID, ok := q.Lease()
	if !ok || leased.ID != first.ID {
		t.Fatal("expected the single deduplicated job to be leasable")
	}
	if err := q.Ack(leaseID); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	// After the original job is acked, the key is free again.
	third := q.Enqueue([]byte("new payload"), "dup-key")
	if third.ID == first.ID {
		t.Fatal("acked key must not still map to the old job")
	}
}

func TestLeaseTimeoutRequeuesJob(t *testing.T) {
	q := New(50 * time.Millisecond)
	defer q.Stop()

	enqueued := q.Enqueue([]byte("slow task"), "key-timeout")
	_, _, ok := q.Lease()
	if !ok {
		t.Fatal("expected a job to be available for lease")
	}
	if _, _, ok := q.Lease(); ok {
		t.Fatal("leased job must not be available before its lease expires")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		leased, _, ok := q.Lease()
		if ok {
			if leased.ID != enqueued.ID {
				t.Fatalf("re-leased job %q, want %q", leased.ID, enqueued.ID)
			}
			if leased.Attempts != 2 {
				t.Fatalf("re-leased job attempts = %d, want 2", leased.Attempts)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("job was not re-queued after its lease expired")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
