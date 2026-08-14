package etcdraft

import (
	"testing"

	rootraft "github.com/aminkbi/d-raft/raft"
)

func TestChainKnownAnswer(t *testing.T) {
	var chain Chain
	if err := chain.Apply(rootraft.Entry{Index: 1, Term: 1, Type: rootraft.EntryNoop}); err != nil {
		t.Fatal(err)
	}
	if got, want := chain.Digest(), "0f325df3d6d32ff710ed2ea5d5993342325077b8f73150eebad86b575ad32564"; got != want {
		t.Fatalf("genesis digest = %s, want %s", got, want)
	}
	if err := chain.Apply(rootraft.Entry{Index: 2, Term: 3, Type: rootraft.EntryCommand, Data: []byte("set:x=1")}); err != nil {
		t.Fatal(err)
	}
	blocks := chain.Blocks()
	if got, want := blocks[1].DataDigest, "4fa41871bd61a82149e75a3ecfc6874b5936c677f113474160c7000f93c1a55b"; got != want {
		t.Fatalf("data digest = %s, want %s", got, want)
	}
	if got, want := chain.Digest(), "29236a8bb4f2bef96e4847b9c7e115263847fe72ad34d82061f840aabc701952"; got != want {
		t.Fatalf("chain digest = %s, want %s", got, want)
	}
	blocks[1].Digest = "mutated"
	if chain.Digest() == "mutated" {
		t.Fatal("Blocks returned an alias")
	}
}

func TestChainRejectsGaps(t *testing.T) {
	var chain Chain
	if err := chain.Apply(rootraft.Entry{Index: 2, Term: 1, Type: rootraft.EntryNoop}); err != ErrOracleOrder {
		t.Fatalf("Apply error = %v, want ErrOracleOrder", err)
	}
}
