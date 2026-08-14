package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/experiment"
	rootraft "github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/semanticplan"
)

func TestRunPublishesStrictCrossAdapterArtifactsWithoutOverwrite(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.json")
	writeSourceRun(t, sourcePath, commandTestScenario(nil))
	planPath := filepath.Join(directory, "plan.json")
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"derive", "--source-run", sourcePath, "--fallback-seed", "11", "--out", planPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("derive = %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}
	prefix := filepath.Join(directory, "result")
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"run", "--plan", planPath, "--source-run", sourcePath, "--out", prefix}, &stdout, &stderr); code != 0 {
		t.Fatalf("execute = %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "execution boundary: both_reached") || !strings.Contains(stdout.String(), "application: agree") {
		t.Fatalf("stdout = %s", stdout.String())
	}
	paths := bundlePaths(prefix)
	for _, path := range paths {
		if info, err := os.Stat(path); err != nil || info.Size() == 0 {
			t.Fatalf("output %s: info=%v err=%v", path, info, err)
		}
	}
	decodeCapabilities(t, paths[0])
	decodeCapabilities(t, paths[1])
	decodeExecution(t, paths[2])
	decodeExecution(t, paths[3])
	decodeOutcome(t, paths[4])
	decodeOutcome(t, paths[5])
	file, err := os.Open(paths[6])
	if err != nil {
		t.Fatal(err)
	}
	comparison, decodeErr := semanticplan.DecodeNormalizedComparison(file)
	closeErr := file.Close()
	if decodeErr != nil || closeErr != nil {
		t.Fatalf("decode comparison = %#v, %v / %v", comparison, decodeErr, closeErr)
	}
	if comparison.Application != semanticplan.ApplicationAgree || comparison.ExecutionBoundary != semanticplan.ExecutionBothReached {
		t.Fatalf("comparison = %#v", comparison)
	}
	for _, path := range []string{paths[2], paths[3]} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("execution permissions %s: %v", path, err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("execution permissions %s: mode=%v", path, info.Mode())
		}
	}
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"verify", "--plan", planPath, "--source-run", sourcePath, "--in", prefix}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "verified") {
		t.Fatalf("verify = %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"run", "--plan", planPath, "--source-run", sourcePath, "--out", prefix}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "already exists") {
		t.Fatalf("overwrite execute = %d, stderr=%s", code, stderr.String())
	}
}

func TestDeriveRejectsNonReplayableSourceEvidence(t *testing.T) {
	directory := t.TempDir()
	validPath := filepath.Join(directory, "valid.json")
	valid := writeSourceRun(t, validPath, commandTestScenario(nil)).run

	tests := map[string]func(*artifact.Run){
		"empty tape": func(run *artifact.Run) {
			run.Decisions.Entries = []decision.Entry{}
		},
		"unused tape entry": func(run *artifact.Run) {
			run.Decisions = decision.CloneTape(run.Decisions)
			run.Decisions.Entries = append(run.Decisions.Entries, run.Decisions.Entries[len(run.Decisions.Entries)-1])
		},
		"tampered tape selection": func(run *artifact.Run) {
			run.Decisions = decision.CloneTape(run.Decisions)
			for index := range run.Decisions.Entries {
				entry := &run.Decisions.Entries[index]
				if entry.Selection.Number != nil && entry.Choice.Min != nil && entry.Choice.Max != nil && *entry.Choice.Min < *entry.Choice.Max {
					value := *entry.Choice.Min
					if *entry.Selection.Number == value {
						value = *entry.Choice.Max
					}
					entry.Selection.Number = &value
					return
				}
			}
			t.Fatal("source has no variable ranged selection")
		},
		"stale outcome": func(run *artifact.Run) {
			if run.Outcome.ObservationDigest[0] == '0' {
				run.Outcome.ObservationDigest = "1" + run.Outcome.ObservationDigest[1:]
			} else {
				run.Outcome.ObservationDigest = "0" + run.Outcome.ObservationDigest[1:]
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			run := valid
			run.Decisions = decision.CloneTape(valid.Decisions)
			mutate(&run)
			sourcePath := filepath.Join(directory, strings.ReplaceAll(name, " ", "-")+".json")
			writeRun(t, sourcePath, run)
			var stdout, stderr bytes.Buffer
			code := execute([]string{"derive", "--source-run", sourcePath, "--out", sourcePath + ".plan"}, &stdout, &stderr)
			if code != 1 || !strings.Contains(stderr.String(), "source replay") {
				t.Fatalf("derive = %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestVerifyRejectsForgedDerivedEvidenceWithMatchingManifest(t *testing.T) {
	directory := t.TempDir()
	sourcePath, planPath, prefix := createTestBundle(t, directory, "forged")
	comparisonPath := resultPaths(prefix)[6]
	raw, err := os.ReadFile(comparisonPath)
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := semanticplan.DecodeNormalizedComparison(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	comparison.Application = semanticplan.ApplicationStateDivergence
	forged, err := encodeDocument(comparison)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(comparisonPath, forged, 0o600); err != nil {
		t.Fatal(err)
	}
	manifestRaw, err := os.ReadFile(manifestPath(prefix))
	if err != nil {
		t.Fatal(err)
	}
	var manifest bundleManifest
	if err := decodeStrictDocument(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(forged)
	manifest.Files[6].SHA256 = hex.EncodeToString(digest[:])
	manifest.Files[6].Bytes = artifact.Uint64(len(forged))
	manifestRaw, err = encodeDocument(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath(prefix), manifestRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"verify", "--plan", planPath, "--source-run", sourcePath, "--in", prefix}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "comparison does not match") {
		t.Fatalf("verify = %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}
}

func TestIncompleteBundleIsDetectableAndRecoverable(t *testing.T) {
	directory := t.TempDir()
	sourcePath, planPath, prefix := createTestBundle(t, directory, "interrupted")
	if err := os.Remove(manifestPath(prefix)); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"verify", "--plan", planPath, "--source-run", sourcePath, "--in", prefix}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "incomplete") {
		t.Fatalf("verify incomplete = %d, stderr=%s", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"recover", "--in", prefix}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "removed 7") {
		t.Fatalf("recover = %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}
	for _, path := range bundlePaths(prefix) {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("recovered target %s remains: %v", path, err)
		}
	}
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"run", "--plan", planPath, "--source-run", sourcePath, "--out", prefix}, &stdout, &stderr); code != 0 {
		t.Fatalf("rerun after recovery = %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}
}

func TestVerifyRejectsCommittedBundleWithMissingDataFile(t *testing.T) {
	directory := t.TempDir()
	sourcePath, planPath, prefix := createTestBundle(t, directory, "missing")
	if err := os.Remove(resultPaths(prefix)[4]); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"verify", "--plan", planPath, "--source-run", sourcePath, "--in", prefix}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "reference_outcome") {
		t.Fatalf("verify missing = %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}
}

func TestFailedProjectionTapeVerificationRequiresExactSuccessfulPrefix(t *testing.T) {
	minimum, maximum := int64(1), int64(2)
	first := decision.Choice{ID: "first", Kind: decision.ElectionTimeout, Min: &minimum, Max: &maximum}
	entry, err := decision.NewEntry(first, decision.Selection{Number: &minimum})
	if err != nil {
		t.Fatal(err)
	}
	execution := semanticplan.SemanticExecution{
		Projection: semanticplan.ProjectionReport{Fidelity: semanticplan.ProjectionFailed},
		Decisions:  decision.Tape{Schema: decision.SchemaVersion, Entries: []decision.Entry{entry}},
	}
	executePrefix := func(decider decision.Decider) (artifact.Outcome, error) {
		if _, err := decider.Choose(first); err != nil {
			return artifact.Outcome{}, err
		}
		second := first
		second.ID = "second"
		_, err := decider.Choose(second)
		return artifact.Outcome{}, err
	}
	if err := verifyLocalTape(execution, executePrefix); err != nil {
		t.Fatalf("exact failed prefix rejected: %v", err)
	}
	if err := verifyLocalTape(execution, func(decider decision.Decider) (artifact.Outcome, error) {
		if _, err := decider.Choose(first); err != nil {
			return artifact.Outcome{}, err
		}
		return artifact.Outcome{Status: artifact.OutcomeCompleted}, nil
	}); err == nil || !strings.Contains(err.Error(), "next unrecorded choice") {
		t.Fatalf("completed failed prefix accepted: %v", err)
	}
}

func TestRunRejectsIneligibleBeforePublishing(t *testing.T) {
	directory := t.TempDir()
	scenario := commandTestScenario([]artifact.Action{{
		AtNS: int64(100 * time.Millisecond), Kind: artifact.ActionSnapshot,
		Node: "a", Data: []byte("opaque"),
	}})
	sourcePath := filepath.Join(directory, "ineligible-source.json")
	source := writeSourceRun(t, sourcePath, scenario)
	plan := planFromSource(t, source, 11)
	planPath := filepath.Join(directory, "ineligible.json")
	writePlan(t, planPath, plan)
	prefix := filepath.Join(directory, "rejected")
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"run", "--plan", planPath, "--source-run", sourcePath, "--out", prefix}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "ineligible") {
		t.Fatalf("execute = %d, stderr=%s", code, stderr.String())
	}
	for _, path := range resultPaths(prefix) {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("ineligible run published %s: %v", path, err)
		}
	}
}

func TestCommandErrorsAndVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"version"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "draft-cross") {
		t.Fatalf("version = %d, %q", code, stdout.String())
	}
	stdout.Reset()
	if code := execute([]string{"unknown"}, &stdout, &stderr); code != 2 {
		t.Fatalf("unknown = %d", code)
	}
}

func commandTestScenario(actions []artifact.Action) artifact.Scenario {
	if actions == nil {
		actions = []artifact.Action{}
	}
	return artifact.Scenario{
		ID: "semantic/command-smoke", Version: "1",
		DurationNS: int64(time.Second), MaxSteps: 100_000,
		Actions: actions,
	}
}

func commandTestConfiguration() artifact.Configuration {
	return artifact.Configuration{
		Members: []rootraft.NodeID{"a", "b", "c"}, InfrastructureSeed: 5,
		NetworkMinLatencyNS: int64(time.Millisecond), NetworkMaxLatencyNS: int64(5 * time.Millisecond),
		ElectionTimeoutMinNS: int64(100 * time.Millisecond), ElectionTimeoutMaxNS: int64(200 * time.Millisecond),
		HeartbeatIntervalNS: int64(20 * time.Millisecond), StorageLatencyNS: int64(time.Millisecond),
		StopOnViolation: false,
	}
}

func planFromSource(t *testing.T, source sourceRun, fallbackSeed uint64) semanticplan.Plan {
	t.Helper()
	directives, err := semanticplan.DirectivesFromTape(source.run.Decisions)
	if err != nil {
		t.Fatal(err)
	}
	workloadEnd := int64(0)
	for _, action := range source.run.Scenario.Actions {
		workloadEnd = max(workloadEnd, action.AtNS)
	}
	return semanticplan.Plan{
		Schema: semanticplan.SemanticPlanSchema, Scenario: source.run.Scenario,
		Configuration: source.run.Configuration, Application: apporacle.KVConfig(),
		Convergence: semanticplan.Convergence{
			WorkloadEndNS: workloadEnd, ComparisonBoundaryNS: source.run.Scenario.DurationNS,
		},
		Source: semanticplan.Source{
			Adapter: source.run.Adapter, RunSHA256: source.sha256,
		},
		FallbackSeed: artifact.Uint64(fallbackSeed), Directives: directives,
	}
}

func writeSourceRun(t *testing.T, path string, scenario artifact.Scenario) sourceRun {
	t.Helper()
	configuration := commandTestConfiguration()
	const seed = uint64(13)
	recorder := decision.NewRecorder(decision.NewSeedDecider(seed))
	outcome, err := experiment.Execute(scenario, configuration, recorder)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Err(); err != nil {
		t.Fatal(err)
	}
	run := artifact.Run{
		Schema: artifact.SchemaVersion, Scenario: scenario,
		Adapter:       artifact.Adapter{ID: artifact.ReferenceAdapterID, Version: artifact.ReferenceAdapterCurrent},
		Configuration: configuration, Reproducibility: artifact.NewReproducibility(seed),
		Decisions: recorder.Tape(), Outcome: outcome,
	}
	writeRun(t, path, run)
	source, err := readSourceRun(path)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func writeRun(t *testing.T, path string, run artifact.Run) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	encodeErr := artifact.Encode(file, run)
	closeErr := file.Close()
	if encodeErr != nil || closeErr != nil {
		t.Fatalf("write source run: %v / %v", encodeErr, closeErr)
	}
}

func createTestBundle(t *testing.T, directory, name string) (sourcePath, planPath, prefix string) {
	t.Helper()
	sourcePath = filepath.Join(directory, name+"-source.json")
	writeSourceRun(t, sourcePath, commandTestScenario(nil))
	planPath = filepath.Join(directory, name+"-plan.json")
	prefix = filepath.Join(directory, name+"-result")
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"derive", "--source-run", sourcePath, "--out", planPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("derive = %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"run", "--plan", planPath, "--source-run", sourcePath, "--out", prefix}, &stdout, &stderr); code != 0 {
		t.Fatalf("run = %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}
	return sourcePath, planPath, prefix
}

func writePlan(t *testing.T, path string, plan semanticplan.Plan) {
	t.Helper()
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

func decodeCapabilities(t *testing.T, path string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, decodeErr := semanticplan.DecodeCapabilities(file)
	closeErr := file.Close()
	if decodeErr != nil || closeErr != nil {
		t.Fatalf("decode capabilities: %v / %v", decodeErr, closeErr)
	}
}

func decodeExecution(t *testing.T, path string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, decodeErr := semanticplan.DecodeSemanticExecution(file)
	closeErr := file.Close()
	if decodeErr != nil || closeErr != nil {
		t.Fatalf("decode execution: %v / %v", decodeErr, closeErr)
	}
}

func decodeOutcome(t *testing.T, path string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, decodeErr := semanticplan.DecodeNormalizedOutcome(file)
	closeErr := file.Close()
	if decodeErr != nil || closeErr != nil {
		t.Fatalf("decode outcome: %v / %v", decodeErr, closeErr)
	}
}
