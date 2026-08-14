package raftsim

import (
	"bytes"
	"slices"
	"testing"
	"time"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/raft"
)

func TestClusterElectsAndReplicates(t *testing.T) {
	t.Parallel()

	cluster := mustCluster(t, testConfig("a", "b", "c"))
	runUntil(t, cluster, 2*time.Second)
	leader, ok := cluster.Leader()
	if !ok {
		t.Fatalf("no unique leader: %+v", cluster.Statuses())
	}
	if err := cluster.Propose([]byte("set x=1")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+time.Second)

	for _, id := range cluster.Members() {
		store, err := cluster.Store(id)
		if err != nil {
			t.Fatalf("Store(%q): %v", id, err)
		}
		if store.State.HardState.CommitIndex < 2 || store.AppliedIndex < 2 {
			t.Fatalf("node %q did not commit/apply: %+v", id, store)
		}
		if len(store.Applied) < 2 || store.Applied[0].Type != raft.EntryNoop || string(store.Applied[1].Data) != "set x=1" {
			t.Fatalf("node %q applied entries = %+v", id, store.Applied)
		}
	}
	if status, err := cluster.Status(leader); err != nil || status.Role != raft.Leader {
		t.Fatalf("leader %q status=%+v err=%v", leader, status, err)
	}
}

func TestPartitionElectsMajorityLeaderAndHeals(t *testing.T) {
	t.Parallel()

	cluster := mustCluster(t, testConfig("a", "b", "c"))
	runUntil(t, cluster, 2*time.Second)
	oldLeader, ok := cluster.Leader()
	if !ok {
		t.Fatalf("no initial leader: %+v", cluster.Statuses())
	}
	majority := make([]raft.NodeID, 0, 2)
	for _, id := range cluster.Members() {
		if id != oldLeader {
			majority = append(majority, id)
		}
	}
	if err := cluster.Partition([]raft.NodeID{oldLeader}, majority); err != nil {
		t.Fatalf("Partition: %v", err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+2*time.Second)

	majorityHasLeader := false
	for _, id := range majority {
		status, err := cluster.Status(id)
		if err != nil {
			t.Fatal(err)
		}
		majorityHasLeader = majorityHasLeader || status.Role == raft.Leader
	}
	if !majorityHasLeader {
		t.Fatalf("majority failed to elect: %+v", cluster.Statuses())
	}

	cluster.Heal()
	runUntil(t, cluster, cluster.Simulator().Now()+time.Second)
	if _, ok := cluster.Leader(); !ok {
		t.Fatalf("cluster did not converge after healing: %+v", cluster.Statuses())
	}
}

func TestCrashBeforePersistenceLosesTransition(t *testing.T) {
	t.Parallel()

	config := singleNodeCrashConfig()
	cluster := mustCluster(t, config)
	if _, err := cluster.ScheduleCrash(15*time.Millisecond, "a"); err != nil {
		t.Fatalf("ScheduleCrash: %v", err)
	}
	runUntil(t, cluster, 15*time.Millisecond)
	store, _ := cluster.Store("a")
	if store.State.HardState.CurrentTerm != 0 || len(store.State.Log) != 0 {
		t.Fatalf("uncompleted transition became durable: %+v", store)
	}
	if err := cluster.Restart("a"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	runUntil(t, cluster, 45*time.Millisecond)
	status, err := cluster.Status("a")
	if err != nil || status.Role != raft.Leader || status.Term != 1 {
		t.Fatalf("restarted status=%+v err=%v", status, err)
	}
}

func TestCrashAfterPersistenceRetainsTransitionButReleasesNoEffects(t *testing.T) {
	t.Parallel()

	cluster := mustCluster(t, singleNodeCrashConfig())
	if err := cluster.CrashAfterNextPersist("a"); err != nil {
		t.Fatalf("CrashAfterNextPersist: %v", err)
	}
	runUntil(t, cluster, 25*time.Millisecond)
	store, _ := cluster.Store("a")
	if store.State.HardState.CurrentTerm != 1 || store.State.HardState.VotedFor != "a" || len(store.State.Log) != 1 {
		t.Fatalf("completed transition was lost: %+v", store)
	}
	if store.AppliedIndex != 0 {
		t.Fatalf("post-persist effect escaped before acknowledgement: %+v", store)
	}
	if err := cluster.Restart("a"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	store, _ = cluster.Store("a")
	if store.AppliedIndex != 1 || len(store.Applied) != 1 || store.Applied[0].Type != raft.EntryNoop {
		t.Fatalf("restart did not recover committed apply: %+v", store)
	}
}

func TestClusterSeedReproducesStateAndTrace(t *testing.T) {
	t.Parallel()

	run := func() ([]raft.Status, []byte) {
		var output bytes.Buffer
		config := testConfig("a", "b", "c")
		recorder := sim.NewJSONLRecorder(&output)
		config.Trace = recorder
		cluster := mustCluster(t, config)
		runUntil(t, cluster, 2*time.Second)
		if err := recorder.Err(); err != nil {
			t.Fatalf("trace: %v", err)
		}
		return cluster.Statuses(), slices.Clone(output.Bytes())
	}
	leftStatus, leftTrace := run()
	rightStatus, rightTrace := run()
	if !statusesEqual(leftStatus, rightStatus) || !bytes.Equal(leftTrace, rightTrace) {
		t.Fatal("equal seeds produced different cluster executions")
	}
}

func testConfig(members ...raft.NodeID) Config {
	config := DefaultConfig(members...)
	config.Seed = 0x5eed
	config.Network = sim.LinkConfig{MinLatency: time.Millisecond, MaxLatency: 5 * time.Millisecond}
	config.ElectionTimeoutMin = 40 * time.Millisecond
	config.ElectionTimeoutMax = 80 * time.Millisecond
	config.HeartbeatInterval = 10 * time.Millisecond
	config.StorageLatency = time.Millisecond
	return config
}

func singleNodeCrashConfig() Config {
	config := DefaultConfig("a")
	config.ElectionTimeoutMin = 10 * time.Millisecond
	config.ElectionTimeoutMax = 10 * time.Millisecond
	config.HeartbeatInterval = 2 * time.Millisecond
	config.StorageLatency = 10 * time.Millisecond
	return config
}

func mustCluster(t *testing.T, config Config) *Cluster {
	t.Helper()
	cluster, err := New(config)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return cluster
}

func runUntil(t *testing.T, cluster *Cluster, end time.Duration) {
	t.Helper()
	if _, err := cluster.RunUntil(end); err != nil {
		t.Fatalf("RunUntil(%s): %v", end, err)
	}
}

func statusesEqual(left, right []raft.Status) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].ID != right[index].ID || left[index].Role != right[index].Role || left[index].Term != right[index].Term || left[index].CommitIndex != right[index].CommitIndex || left[index].AppliedIndex != right[index].AppliedIndex || !entriesEqual(left[index].Log, right[index].Log) {
			return false
		}
	}
	return true
}

func entriesEqual(left, right []raft.Entry) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Index != right[index].Index || left[index].Term != right[index].Term || left[index].Type != right[index].Type || !bytes.Equal(left[index].Data, right[index].Data) {
			return false
		}
	}
	return true
}
