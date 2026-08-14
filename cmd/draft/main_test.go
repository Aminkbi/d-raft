package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
