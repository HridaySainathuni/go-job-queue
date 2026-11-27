// Package cluster replicates the job queue across nodes with Raft.
//
// A Node wraps a queue.Queue and a hashicorp/raft instance. Enqueue
// commands go through the Raft log, and the FSM applies each committed
// command to the local queue, so every node stores the same jobs with
// the same IDs. Leases, acks, and nacks stay local to each node for now;
// only enqueue is replicated in this milestone.
package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/HridaySainathuni/go-job-queue/internal/queue"
)

// ErrNotLeader is returned by Enqueue when the node is not the Raft leader.
var ErrNotLeader = errors.New("cluster: not the leader")

// applyTimeout bounds how long Enqueue waits for its command to commit.
const applyTimeout = 10 * time.Second

// opEnqueue is the log command that adds a job to the queue.
const opEnqueue = "enqueue"

// command is one entry in the Raft log. The leader assigns the job ID
// before appending, so every replica applies the same job.
type command struct {
	Op  string    `json:"op"`
	Job queue.Job `json:"job"`
}

func (c command) encode() ([]byte, error) {
	return json.Marshal(c)
}

func decodeCommand(data []byte) (command, error) {
	var cmd command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}

// Config configures one Raft node.
type Config struct {
	// ID is this node's Raft server ID.
	ID raft.ServerID
	// Address is this node's Raft server address. It must match the
	// address the Transport listens on.
	Address raft.ServerAddress
	// DataDir holds the Raft log and stable store on disk. Empty means
	// in-memory stores, for tests.
	DataDir string
	// Transport carries Raft RPCs. Nil selects a TCP transport bound to
	// Address; tests pass an in-memory transport instead.
	Transport raft.Transport
	// Bootstrap lists the servers the node bootstraps with. Empty means
	// the node joins an existing cluster instead of bootstrapping one.
	Bootstrap []raft.Server
}

// Node is one Raft-replicated queue node.
type Node struct {
	id    raft.ServerID
	raft  *raft.Raft
	queue *queue.Queue
}

// New boots a Raft node wrapping q. When cfg.Bootstrap is non-empty the
// node bootstraps a cluster with those servers; otherwise it starts
// unconfigured and must join a cluster before it can elect a leader.
func New(cfg Config, q *queue.Queue) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("cluster: node ID must not be empty")
	}
	if cfg.Transport == nil && cfg.Address == "" {
		return nil, errors.New("cluster: address must not be empty without a transport")
	}
	if q == nil {
		return nil, errors.New("cluster: queue must not be nil")
	}

	var logStore raft.LogStore
	var stableStore raft.StableStore
	var snapshotStore raft.SnapshotStore
	if cfg.DataDir != "" {
		if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
			return nil, fmt.Errorf("cluster: create data directory: %w", err)
		}
		store, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
		if err != nil {
			return nil, fmt.Errorf("cluster: open raft store: %w", err)
		}
		logStore = store
		stableStore = store
		snaps, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
		if err != nil {
			return nil, fmt.Errorf("cluster: create snapshot store: %w", err)
		}
		snapshotStore = snaps
	} else {
		store := raft.NewInmemStore()
		logStore = store
		stableStore = store
		snapshotStore = raft.NewInmemSnapshotStore()
	}

	transport := cfg.Transport
	if transport == nil {
		t, err := raft.NewTCPTransport(string(cfg.Address), nil, 3, 10*time.Second, os.Stderr)
		if err != nil {
			return nil, fmt.Errorf("cluster: create transport: %w", err)
		}
		transport = t
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = cfg.ID

	r, err := raft.NewRaft(raftCfg, &fsm{q: q}, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("cluster: start raft: %w", err)
	}

	if len(cfg.Bootstrap) > 0 {
		if err := r.BootstrapCluster(raft.Configuration{Servers: cfg.Bootstrap}).Error(); err != nil {
			return nil, fmt.Errorf("cluster: bootstrap: %w", err)
		}
	}

	return &Node{id: cfg.ID, raft: r, queue: q}, nil
}

// ID returns this node's Raft server ID.
func (n *Node) ID() raft.ServerID {
	return n.id
}

// Leader returns the Raft server ID of the current leader, or "" when
// no leader is known.
func (n *Node) Leader() raft.ServerID {
	addr := n.raft.Leader()
	if addr == "" {
		return ""
	}
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return ""
	}
	for _, s := range future.Configuration().Servers {
		if s.Address == addr {
			return s.ID
		}
	}
	return ""
}

// IsLeader reports whether this node is the current Raft leader.
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// Queue returns the queue this node replicates.
func (n *Node) Queue() *queue.Queue {
	return n.queue
}

// Enqueue replicates an enqueue of payload through the Raft log and
// returns the job once the leader has applied it. Only the leader can
// enqueue; followers return ErrNotLeader.
func (n *Node) Enqueue(payload []byte, idempotencyKey string) (queue.Job, error) {
	if !n.IsLeader() {
		return queue.Job{}, ErrNotLeader
	}
	job := queue.Job{
		ID:             newID(),
		IdempotencyKey: idempotencyKey,
		Payload:        payload,
	}
	data, err := command{Op: opEnqueue, Job: job}.encode()
	if err != nil {
		return queue.Job{}, fmt.Errorf("cluster: encode enqueue command: %w", err)
	}
	if err := n.raft.Apply(data, applyTimeout).Error(); err != nil {
		return queue.Job{}, fmt.Errorf("cluster: replicate enqueue: %w", err)
	}
	return job, nil
}

// Shutdown stops the Raft instance. It does not stop the queue.
func (n *Node) Shutdown() error {
	return n.raft.Shutdown().Error()
}

// fsm applies committed log entries to the queue.
type fsm struct {
	q *queue.Queue
}

// Apply applies one committed log entry to the queue.
func (f *fsm) Apply(l *raft.Log) interface{} {
	cmd, err := decodeCommand(l.Data)
	if err != nil {
		return err
	}
	switch cmd.Op {
	case opEnqueue:
		return f.q.EnqueueApplied(cmd.Job)
	default:
		return fmt.Errorf("cluster: unknown log command %q", cmd.Op)
	}
}

// Snapshot returns an empty snapshot. Log compaction is not implemented
// in this milestone, so snapshots carry no state yet.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{}, nil
}

// Restore discards a snapshot. It is a no-op until snapshots carry state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	return rc.Close()
}

// fsmSnapshot is an empty snapshot; see fsm.Snapshot.
type fsmSnapshot struct{}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
