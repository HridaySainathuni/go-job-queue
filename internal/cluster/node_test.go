package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/HridaySainathuni/go-job-queue/internal/queue"
)

// waitFor polls cond until it is true or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitForLeader returns the first node that becomes leader.
func waitForLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	var leader *Node
	waitFor(t, 10*time.Second, "a leader to be elected", func() bool {
		for _, n := range nodes {
			if n.IsLeader() {
				leader = n
				return true
			}
		}
		return false
	})
	return leader
}

// waitForJob waits until every node's queue holds a job with the given
// ID and payload.
func waitForJob(t *testing.T, nodes []*Node, job queue.Job) {
	t.Helper()
	waitFor(t, 10*time.Second, "the job to replicate to every node", func() bool {
		for _, n := range nodes {
			if !nodeHasJob(n, job) {
				return false
			}
		}
		return true
	})
}

// nodeHasJob reports whether the node's queue holds a job with the same
// ID and payload.
func nodeHasJob(n *Node, want queue.Job) bool {
	got, ok := n.Queue().Get(want.ID)
	return ok && bytes.Equal(got.Payload, want.Payload)
}

func TestSingleNodeElectsLeaderAndReplicatesEnqueue(t *testing.T) {
	q := queue.New(0)
	defer q.Stop()

	addr, transport := raft.NewInmemTransport(raft.NewInmemAddr())
	node, err := New(Config{
		ID:        "node1",
		Address:   addr,
		DataDir:   t.TempDir(),
		Transport: transport,
		Bootstrap: []raft.Server{{ID: "node1", Address: addr}},
	}, q)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer node.Shutdown()

	waitFor(t, 10*time.Second, "the single node to elect itself leader", node.IsLeader)
	if got := node.Leader(); got != "node1" {
		t.Fatalf("Leader() = %q, want %q", got, "node1")
	}

	job, err := node.Enqueue([]byte("hello"), "key-1")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if job.ID == "" {
		t.Fatal("Enqueue returned a job with an empty ID")
	}
	waitForJob(t, []*Node{node}, job)
}

func TestThreeNodeClusterReplicatesEnqueueToAll(t *testing.T) {
	const count = 3

	transports := make([]*raft.InmemTransport, count)
	addrs := make([]raft.ServerAddress, count)
	for i := range transports {
		addrs[i], transports[i] = raft.NewInmemTransport(raft.NewInmemAddr())
	}
	for i := range transports {
		for j := range transports {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}

	servers := make([]raft.Server, count)
	for i := range servers {
		servers[i] = raft.Server{ID: raft.ServerID(fmt.Sprintf("node%d", i+1)), Address: addrs[i]}
	}

	nodes := make([]*Node, count)
	for i := range nodes {
		q := queue.New(0)
		defer q.Stop()
		node, err := New(Config{
			ID:        servers[i].ID,
			Address:   addrs[i],
			Transport: transports[i],
			Bootstrap: servers,
		}, q)
		if err != nil {
			t.Fatalf("New node %d: %v", i, err)
		}
		defer node.Shutdown()
		nodes[i] = node
	}

	leader := waitForLeader(t, nodes)

	followers := make([]*Node, 0, count-1)
	for _, n := range nodes {
		if n != leader {
			followers = append(followers, n)
		}
	}
	for _, f := range followers {
		if _, err := f.Enqueue([]byte("nope"), ""); !errors.Is(err, ErrNotLeader) {
			t.Errorf("follower Enqueue error = %v, want ErrNotLeader", err)
		}
	}

	job, err := leader.Enqueue([]byte("replicated"), "key-9")
	if err != nil {
		t.Fatalf("leader Enqueue: %v", err)
	}

	waitForJob(t, nodes, job)
}
