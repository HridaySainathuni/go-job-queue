// Disk persistence for the queue, backed by BoltDB (go.etcd.io/bbolt).
//
// Every mutation writes the affected job record to a single bucket in
// dir/queue.db before the mutation is visible to callers, and Open loads
// all stored records back so a restarted server resumes with its queue
// intact. A record holds the job, its state (pending or leased), the
// live lease ID and deadline when leased, and a ReadySeq number that
// preserves FIFO order across restarts.
//
// States "done" and "dead" have no stored form yet: Ack deletes the
// record, and there is no retry limit or dead-letter handling in this
// milestone, so no job can be dead. That changes when retries land.
package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"go.etcd.io/bbolt"
)

var jobsBucket = []byte("jobs")

// storedJob is the on-disk form of one jobState.
type storedJob struct {
	ID             string
	IdempotencyKey string
	Payload        []byte
	Attempts       int
	State          string // "pending" or "leased"
	LeaseID        string
	Deadline       time.Time
	ReadySeq       uint64 // position in the ready list; 0 when not ready
}

// Open returns a Queue backed by the BoltDB file in dir, loading any
// jobs stored there by an earlier run. A non-positive leaseTimeout
// selects DefaultLeaseTimeout. The queue runs a background sweeper that
// returns expired leases to the ready list; call Close to shut it down
// and close the database.
func Open(dir string, leaseTimeout time.Duration) (*Queue, error) {
	if dir == "" {
		return nil, errors.New("queue: data directory must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("queue: create data directory: %w", err)
	}
	db, err := bbolt.Open(filepath.Join(dir, "queue.db"), 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("queue: open database: %w", err)
	}
	q := newQueue(leaseTimeout, db)
	q.mu.Lock()
	err = q.loadLocked()
	q.mu.Unlock()
	if err != nil {
		db.Close()
		return nil, err
	}
	return q, nil
}

// Close shuts down the background lease sweeper and closes the database.
// It is a no-op for the database when the queue is in-memory.
func (q *Queue) Close() error {
	q.Stop()
	if q.db == nil {
		return nil
	}
	return q.db.Close()
}

// loadLocked rebuilds the in-memory indexes from the stored records.
// The caller must hold q.mu. Leased jobs whose deadline already passed
// while the server was down become ready again.
func (q *Queue) loadLocked() error {
	if q.db == nil {
		return nil
	}
	return q.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		if b == nil {
			return nil
		}
		var records []storedJob
		if err := b.ForEach(func(_, v []byte) error {
			var rec storedJob
			if err := json.Unmarshal(v, &rec); err != nil {
				return fmt.Errorf("queue: decode stored job: %w", err)
			}
			records = append(records, rec)
			return nil
		}); err != nil {
			return err
		}

		// Restore ready jobs in their stored FIFO order first, so
		// expired leases rejoin behind them.
		ready := make([]storedJob, 0, len(records))
		leased := make([]storedJob, 0, len(records))
		for _, rec := range records {
			switch rec.State {
			case "pending":
				ready = append(ready, rec)
			case "leased":
				leased = append(leased, rec)
			default:
				return fmt.Errorf("queue: unknown stored state %q for job %s", rec.State, rec.ID)
			}
		}
		sort.Slice(ready, func(i, j int) bool { return ready[i].ReadySeq < ready[j].ReadySeq })
		sort.Slice(leased, func(i, j int) bool { return leased[i].Deadline.Before(leased[j].Deadline) })

		now := time.Now()
		for _, rec := range ready {
			q.restoreLocked(rec, false)
		}
		for _, rec := range leased {
			if now.After(rec.Deadline) {
				// The lease expired while the server was down: the job
				// is ready again, behind the jobs that were ready.
				rec.State = "pending"
				rec.LeaseID = ""
				rec.Deadline = time.Time{}
				q.restoreLocked(rec, true)
				q.persistLocked(q.byID[rec.ID])
			} else {
				q.restoreLocked(rec, false)
			}
		}
		return nil
	})
}

// restoreLocked inserts one stored record into the in-memory indexes.
// When requeue is true the job joins the back of the ready list with a
// fresh sequence number; otherwise its stored ReadySeq is kept.
func (q *Queue) restoreLocked(rec storedJob, requeue bool) {
	job := Job{
		ID:             rec.ID,
		IdempotencyKey: rec.IdempotencyKey,
		Payload:        rec.Payload,
		Attempts:       rec.Attempts,
	}
	st := &jobState{
		job:      job,
		leased:   rec.State == "leased",
		leaseID:  rec.LeaseID,
		deadline: rec.Deadline,
		readySeq: rec.ReadySeq,
	}
	q.byID[rec.ID] = st
	if rec.IdempotencyKey != "" {
		q.byKey[rec.IdempotencyKey] = rec.ID
	}
	switch rec.State {
	case "pending":
		if requeue {
			st.readySeq = q.nextReadySeq
			q.nextReadySeq++
		} else if rec.ReadySeq >= q.nextReadySeq {
			q.nextReadySeq = rec.ReadySeq + 1
		}
		q.ready = append(q.ready, rec.ID)
	case "leased":
		q.leased[rec.LeaseID] = rec.ID
	}
}

// persistLocked writes one job's current record to disk. It is a no-op
// for in-memory queues. The caller must hold q.mu. A failed write
// panics: a queue that promises durability cannot silently drop it.
func (q *Queue) persistLocked(st *jobState) {
	if q.db == nil {
		return
	}
	rec := storedJob{
		ID:             st.job.ID,
		IdempotencyKey: st.job.IdempotencyKey,
		Payload:        st.job.Payload,
		Attempts:       st.job.Attempts,
		LeaseID:        st.leaseID,
		Deadline:       st.deadline,
		ReadySeq:       st.readySeq,
	}
	if st.leased {
		rec.State = "leased"
	} else {
		rec.State = "pending"
	}
	key := []byte(st.job.ID)
	val, err := json.Marshal(rec)
	if err != nil {
		panic(fmt.Sprintf("queue: encode job %s: %v", st.job.ID, err))
	}
	if err := q.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(jobsBucket)
		if err != nil {
			return err
		}
		return b.Put(key, val)
	}); err != nil {
		panic(fmt.Sprintf("queue: persist job %s: %v", st.job.ID, err))
	}
}

// deleteLocked removes one job's record from disk. It is a no-op for
// in-memory queues. The caller must hold q.mu.
func (q *Queue) deleteLocked(id string) {
	if q.db == nil {
		return
	}
	if err := q.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		if b == nil {
			return nil
		}
		return b.Delete([]byte(id))
	}); err != nil {
		panic(fmt.Sprintf("queue: delete job %s: %v", id, err))
	}
}
