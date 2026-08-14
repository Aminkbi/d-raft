package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/experiment"
	"github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/raftsim"
)

func TestRunInspectReplayWorkflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.json")
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"run", "--seed", "42", "--duration", "500ms", "--out", path}, &stdout, &stderr); code != 0 {
		t.Fatalf("run code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("artifact mode=%v", info.Mode().Perm())
	}
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"inspect", path}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), `scenario: "steady-cluster"@"1"`) {
		t.Fatalf("inspect code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"replay", path}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "verified") {
		t.Fatalf("replay code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestCanonicalInspectReplayWorkflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "portable-faults.json")
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"canonical", "--seed", "1", "--out", path, experiment.PortableFaultsV1}, &stdout, &stderr); code != 0 {
		t.Fatalf("canonical code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	run, err := readArtifact(path)
	if err != nil {
		t.Fatal(err)
	}
	if run.Scenario.ID != "semantic/portable-faults" || len(run.Scenario.Actions) != 8 || run.Reproducibility.DecisionSeed != 1 {
		t.Fatalf("run = %+v", run)
	}
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"replay", path}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "verified") {
		t.Fatalf("replay code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"canonical", "--out", path, experiment.PortableFaultsV1}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "file exists") {
		t.Fatalf("overwrite code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestCanonicalRejectsDifferentDecisionSeed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "portable-faults.json")
	var output bytes.Buffer
	if code := execute([]string{"canonical", "--seed", "2", "--out", path, experiment.PortableFaultsV1}, &output, &output); code != 2 || !strings.Contains(output.String(), "fixes --seed=1") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("rejected seed artifact exists: %v", err)
	}
}

func TestRunDoesNotOverwriteArtifact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.json")
	var output bytes.Buffer
	if code := execute([]string{"run", "--duration", "100ms", "--out", path}, &output, &output); code != 0 {
		t.Fatalf("first code=%d: %s", code, output.String())
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if code := execute([]string{"run", "--duration", "200ms", "--out", path}, &output, &output); code != 2 {
		t.Fatalf("second code=%d: %s", code, output.String())
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(original, after) {
		t.Fatalf("artifact changed err=%v", err)
	}
}

func TestCLIRejectsDuplicateMembers(t *testing.T) {
	var output bytes.Buffer
	if code := execute([]string{"run", "--members", "a,a"}, &output, &output); code != 2 || !strings.Contains(output.String(), "duplicate") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestExploreCommandIsBounded(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := execute([]string{"explore", "--duration", "100ms", "--max-runs", "10", "--depth", "1"}, &stdout, &stderr)
	if code != 0 || !strings.Contains(stdout.String(), "runs:") || !strings.Contains(stdout.String(), "run-budget truncated:") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestInspectShowsInitialRolesAndMembershipActions(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "membership.json")
	config := raftsim.DefaultConfig("a", "b", "c", "d")
	config.Voters = []raft.NodeID{"a", "b", "c"}
	config.Learners = []raft.NodeID{"d"}
	digest, err := artifact.DigestJSON(map[string]string{"state": "test"})
	if err != nil {
		t.Fatal(err)
	}
	run := artifact.Run{
		Schema:  artifact.SchemaVersion,
		Adapter: artifact.Adapter{ID: artifact.ReferenceAdapterID, Version: artifact.ReferenceAdapterCurrent},
		Scenario: artifact.Scenario{ID: "membership", Version: "1", DurationNS: int64(time.Second), MaxSteps: 100,
			Actions: []artifact.Action{
				{Kind: artifact.ActionBeginMembership, Voters: []raft.NodeID{"b", "c", "d"}, Learners: []raft.NodeID{"a"}},
				{Kind: artifact.ActionFinalizeMembership},
			},
		},
		Configuration:   artifact.ConfigurationFrom(config),
		Reproducibility: artifact.NewReproducibility(1),
		Decisions:       decision.Tape{Schema: decision.SchemaVersion},
		Outcome:         artifact.Outcome{Status: artifact.OutcomeCompleted, EndNS: int64(time.Second), ObservationDigest: digest},
	}
	if err := writeArtifact(path, run); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"inspect", path}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	for _, expected := range []string{`voters: "a","b","c"`, `learners: "d"`, `kind="begin_membership"`, `voters="b","c","d"`, `learners="a"`, `kind="finalize_membership"`} {
		if !strings.Contains(stdout.String(), expected) {
			t.Fatalf("inspect output does not contain %q:\n%s", expected, stdout.String())
		}
	}
}
