package etcdraft

import (
	"testing"
	"time"

	"github.com/aminkbi/d-raft/apporacle"
	rootraft "github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/raftsim"
)

func TestReferenceAndEtcdraftPortableCommitmentsAgree(t *testing.T) {
	members := []rootraft.NodeID{"a", "b", "c"}
	application := apporacle.KVConfig()
	referenceConfig := raftsim.DefaultConfig(members...)
	referenceConfig.Application = &application
	referenceConfig.ElectionTimeoutMin = 100 * time.Millisecond
	referenceConfig.ElectionTimeoutMax = 250 * time.Millisecond
	referenceConfig.HeartbeatInterval = 20 * time.Millisecond
	referenceConfig.StorageLatency = time.Millisecond
	reference, err := raftsim.New(referenceConfig)
	if err != nil {
		t.Fatal(err)
	}
	production, err := New(portableConfig(members...))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reference.RunUntil(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, production, 20_000, func() bool {
		_, ok := production.Leader()
		return ok
	})

	commands := [][]byte{
		portableCommand(t, 1, apporacle.Put, "x", "1"),
		portableCommand(t, 2, apporacle.Put, "binary\x00key", "\x00\xff"),
		portableCommand(t, 3, apporacle.Delete, "x", ""),
	}
	for index, command := range commands {
		if err := reference.Propose(command); err != nil {
			t.Fatalf("reference proposal %d: %v", index, err)
		}
		if err := production.Propose(command); err != nil {
			t.Fatalf("etcdraft proposal %d: %v", index, err)
		}
		if _, err := reference.RunUntil(reference.Simulator().Now() + time.Second); err != nil {
			t.Fatal(err)
		}
		wantCommands := apporacle.Uint64(index + 1)
		stepUntil(t, production, 20_000, func() bool {
			for _, member := range members {
				commitment, commitmentErr := production.ApplicationCommitment(member)
				if commitmentErr != nil || commitment.Commands != wantCommands {
					return false
				}
			}
			return true
		})
	}

	for _, clusterCrash := range []func(rootraft.NodeID) error{reference.Crash, production.Crash} {
		if err := clusterCrash("b"); err != nil {
			t.Fatal(err)
		}
	}
	for _, clusterRestart := range []func(rootraft.NodeID) error{reference.Restart, production.Restart} {
		if err := clusterRestart("b"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := reference.RunUntil(reference.Simulator().Now() + time.Second); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, production, 20_000, func() bool {
		_, ok := production.Leader()
		return ok
	})

	want, err := reference.ApplicationCommitment("a")
	if err != nil {
		t.Fatal(err)
	}
	if want.Commands != apporacle.Uint64(len(commands)) {
		t.Fatalf("reference commitment = %+v", want)
	}
	for _, member := range members {
		referenceCommitment, referenceErr := reference.ApplicationCommitment(member)
		productionCommitment, productionErr := production.ApplicationCommitment(member)
		if referenceErr != nil || productionErr != nil {
			t.Fatalf("member %s errors: reference=%v etcdraft=%v", member, referenceErr, productionErr)
		}
		if referenceCommitment != want || productionCommitment != want {
			t.Fatalf("member %s commitments: reference=%+v etcdraft=%+v want=%+v", member, referenceCommitment, productionCommitment, want)
		}
	}
}
