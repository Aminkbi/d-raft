package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aminkbi/d-raft/evaluation"
)

func TestVersionAndErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"--version"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), evaluation.SchemaVersion) {
		t.Fatalf("version = %d, %q / %q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"unexpected"}, &stdout, &stderr); code != 2 {
		t.Fatalf("unexpected = %d", code)
	}
}

func TestVerifyPublishedResult(t *testing.T) {
	result := testResult(t)
	result.Environment = cleanEnvironment(t)
	path := filepath.Join(t.TempDir(), "result.json")
	if err := writeResult(path, result); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"--verify", path}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), result.Environment.GitRevision) {
		t.Fatalf("verify = %d, %q / %q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"--verify", path, "--trials", "3"}, &stdout, &stderr); code != 2 {
		t.Fatalf("verify conflict = %d, %q", code, stderr.String())
	}
	local := testResult(t)
	setEnvironmentProvenance(&local.Environment, "unknown", false)
	localPath := filepath.Join(t.TempDir(), "local.json")
	if err := writeResult(localPath, local); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"--verify", localPath}, &stdout, &stderr); code != 2 {
		t.Fatalf("local verify = %d, %q", code, stderr.String())
	}
}

func TestWriteResultDoesNotOverwrite(t *testing.T) {
	config := evaluation.DefaultConfig(3)
	config.RunnerInvocationBudget = 4
	config.Search.MaxDepth = 1
	result, err := evaluation.Run(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "result.json")
	if err := writeResult(path, result); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeResult(path, result); err == nil {
		t.Fatal("overwrite accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(original, after) {
		t.Fatalf("result changed: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("result mode = %v", info.Mode().Perm())
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := evaluation.Decode(file); err != nil {
		t.Fatalf("published result does not decode: %v", err)
	}
}

func TestExecuteRejectsOutputAndProvenanceBeforeRun(t *testing.T) {
	environment := cleanEnvironment(t)
	originalCollect, originalRun := collectEnvironment, runEvaluation
	t.Cleanup(func() { collectEnvironment, runEvaluation = originalCollect, originalRun })
	collectEnvironment = func() (evaluation.Environment, error) { return environment, nil }
	var invoked atomic.Bool
	runEvaluation = func(evaluation.RunConfig, evaluation.Environment) (evaluation.Result, error) {
		invoked.Store(true)
		return evaluation.Result{}, errors.New("must not run")
	}

	directory := t.TempDir()
	existing := filepath.Join(directory, "existing.json")
	if err := os.WriteFile(existing, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"--trials", "3", "--out", existing}, &stdout, &stderr); code != 2 || invoked.Load() {
		t.Fatalf("existing output: code=%d invoked=%t stderr=%q", code, invoked.Load(), stderr.String())
	}

	dirty := environment
	setEnvironmentProvenance(&dirty, strings.Repeat("b", 40), true)
	collectEnvironment = func() (evaluation.Environment, error) { return dirty, nil }
	stderr.Reset()
	output := filepath.Join(directory, "dirty.json")
	if code := execute([]string{"--trials", "3", "--out", output}, &stdout, &stderr); code != 2 || invoked.Load() {
		t.Fatalf("dirty provenance: code=%d invoked=%t stderr=%q", code, invoked.Load(), stderr.String())
	}
	if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dirty publication created output: %v", err)
	}

	unknown := environment
	setEnvironmentProvenance(&unknown, "unknown", false)
	collectEnvironment = func() (evaluation.Environment, error) { return unknown, nil }
	stderr.Reset()
	if code := execute([]string{"--trials", "3", "--out", filepath.Join(directory, "unknown.json")}, &stdout, &stderr); code != 2 || invoked.Load() {
		t.Fatalf("unknown provenance: code=%d invoked=%t stderr=%q", code, invoked.Load(), stderr.String())
	}
}

func TestPreparedOutputCleansUpRunFailure(t *testing.T) {
	environment := cleanEnvironment(t)
	originalCollect, originalRun := collectEnvironment, runEvaluation
	t.Cleanup(func() { collectEnvironment, runEvaluation = originalCollect, originalRun })
	collectEnvironment = func() (evaluation.Environment, error) { return environment, nil }
	runEvaluation = func(evaluation.RunConfig, evaluation.Environment) (evaluation.Result, error) {
		return evaluation.Result{}, errors.New("injected run failure")
	}
	directory := t.TempDir()
	output := filepath.Join(directory, "result.json")
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"--trials", "3", "--out", output}, &stdout, &stderr); code != 2 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging cleanup = %v, err=%v", entries, err)
	}
}

func TestExecuteCollectsEnvironmentOnceAndPublishesIt(t *testing.T) {
	environment := cleanEnvironment(t)
	result := testResult(t)
	originalCollect, originalRun := collectEnvironment, runEvaluation
	t.Cleanup(func() { collectEnvironment, runEvaluation = originalCollect, originalRun })
	var collections, runs atomic.Int32
	collectEnvironment = func() (evaluation.Environment, error) {
		collections.Add(1)
		return environment, nil
	}
	runEvaluation = func(_ evaluation.RunConfig, received evaluation.Environment) (evaluation.Result, error) {
		runs.Add(1)
		if !reflect.DeepEqual(received, environment) {
			t.Fatalf("runner environment changed: got=%+v want=%+v", received, environment)
		}
		result.Environment = received
		return result, nil
	}
	path := filepath.Join(t.TempDir(), "result.json")
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"--trials", "3", "--out", path}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if collections.Load() != 1 || runs.Load() != 1 {
		t.Fatalf("collections/runs = %d/%d", collections.Load(), runs.Load())
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoded, err := evaluation.DecodePublication(file)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.Environment, environment) {
		t.Fatalf("published environment changed: got=%+v want=%+v", decoded.Environment, environment)
	}
}

func TestDirectorySyncFailureReportsCommittedResult(t *testing.T) {
	result := testResult(t)
	originalSync := syncOutputDirectory
	t.Cleanup(func() { syncOutputDirectory = originalSync })
	path := filepath.Join(t.TempDir(), "result.json")
	publisher, err := prepareResult(path)
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.abort()
	syncOutputDirectory = func(string) error { return errors.New("injected sync failure") }
	err = publisher.publish(result)
	if err == nil || !strings.Contains(err.Error(), "result exists") {
		t.Fatalf("publish error = %v", err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("committed result missing: %v", statErr)
	}
}

func TestConcurrentPublishIsNoClobber(t *testing.T) {
	result := testResult(t)
	path := filepath.Join(t.TempDir(), "result.json")
	var successes atomic.Int32
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if writeResult(path, result) == nil {
				successes.Add(1)
			}
		}()
	}
	wait.Wait()
	if successes.Load() != 1 {
		t.Fatalf("successful publishers = %d", successes.Load())
	}
}

func TestPrepareResultRejectsInvalidParents(t *testing.T) {
	directory := t.TempDir()
	file := filepath.Join(directory, "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"", filepath.Join(directory, "missing", "result.json"), filepath.Join(file, "result.json")} {
		if publisher, err := prepareResult(path); err == nil {
			publisher.abort()
			t.Fatalf("invalid path accepted: %q", path)
		}
	}
}

func testResult(t *testing.T) evaluation.Result {
	t.Helper()
	config := evaluation.DefaultConfig(3)
	config.RunnerInvocationBudget = 4
	config.Search.MaxDepth = 1
	result, err := evaluation.Run(config)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func cleanEnvironment(t *testing.T) evaluation.Environment {
	t.Helper()
	environment, err := evaluation.CurrentEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	setEnvironmentProvenance(&environment, strings.Repeat("a", 40), false)
	if err := evaluation.ValidatePublicationEnvironment(environment); err != nil {
		t.Fatal(err)
	}
	return environment
}

func setEnvironmentProvenance(environment *evaluation.Environment, revision string, modified bool) {
	environment.GitRevision = revision
	environment.GitModified = modified
	foundVCS, foundRevision, foundModified := false, false, false
	for index := range environment.BuildSettings {
		switch environment.BuildSettings[index].Key {
		case "vcs":
			environment.BuildSettings[index].Value = "git"
			foundVCS = true
		case "vcs.revision":
			environment.BuildSettings[index].Value = revision
			foundRevision = true
		case "vcs.modified":
			foundModified = true
			if modified {
				environment.BuildSettings[index].Value = "true"
			} else {
				environment.BuildSettings[index].Value = "false"
			}
		}
	}
	if !foundVCS {
		environment.BuildSettings = append(environment.BuildSettings, evaluation.BuildSetting{Key: "vcs", Value: "git"})
	}
	if !foundRevision {
		environment.BuildSettings = append(environment.BuildSettings, evaluation.BuildSetting{Key: "vcs.revision", Value: revision})
	}
	if !foundModified {
		value := "false"
		if modified {
			value = "true"
		}
		environment.BuildSettings = append(environment.BuildSettings, evaluation.BuildSetting{Key: "vcs.modified", Value: value})
	}
	slices.SortFunc(environment.BuildSettings, func(left, right evaluation.BuildSetting) int { return strings.Compare(left.Key, right.Key) })
}
