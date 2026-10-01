package raftsim

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aminkbi/d-raft/decision"
)

func TestCanonicalStateRetainsCrashAfterPersistWhenFuturesDiverge(t *testing.T) {
	t.Parallel()

	build := func(armed bool) (*Cluster, []byte) {
		t.Helper()
		config := DefaultConfig("a")
		config.ElectionTimeoutMin = 10 * time.Millisecond
		config.ElectionTimeoutMax = config.ElectionTimeoutMin
		config.HeartbeatInterval = 9 * time.Millisecond
		config.Decider = decision.NewSeedDecider(1)
		cluster, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cluster.RunUntil(10 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if armed {
			if err := cluster.CrashAfterNextPersist("a"); err != nil {
				t.Fatal(err)
			}
		}
		state, err := cluster.CanonicalState()
		if err != nil {
			t.Fatal(err)
		}
		return cluster, state
	}
	crashing, armed := build(true)
	surviving, unarmed := build(false)
	if bytes.Equal(armed, unarmed) {
		t.Fatal("canonical state merged armed and unarmed persistence")
	}

	// A deliberately incomplete encoder must falsely merge these frontiers.
	// This establishes that the field matters to future behavior, not just bytes.
	var incomplete clusterState
	if err := json.Unmarshal(armed, &incomplete); err != nil {
		t.Fatal(err)
	}
	if len(incomplete.Processes) != 1 || incomplete.Processes[0].PersistEvent == 0 || !incomplete.Processes[0].CrashAfterPersist {
		t.Fatal("fixture did not reach an armed pending write")
	}
	incomplete.Processes[0].CrashAfterPersist = false
	omitted, err := json.Marshal(incomplete)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(omitted, unarmed) {
		t.Fatal("frontiers differ beyond the omitted crash-after-persist field")
	}

	for _, cluster := range []*Cluster{crashing, surviving} {
		if _, err := cluster.RunUntil(11 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := crashing.Status("a"); !errors.Is(err, ErrNodeDown) {
		t.Fatalf("armed process did not crash after durable completion: %v", err)
	}
	if _, err := surviving.Status("a"); err != nil {
		t.Fatalf("unarmed process did not survive: %v", err)
	}
}
