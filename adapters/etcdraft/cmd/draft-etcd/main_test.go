package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunAndReplayArtifact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.json")
	var stdout, stderr bytes.Buffer
	code := execute([]string{"run", "--seed", "42", "--duration", "500ms", "--out", path}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run code=%d stderr=%s", code, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("run emitted stderr: %s", stderr.String())
	}
	if !strings.Contains(stdout.String(), "status: completed") {
		t.Fatalf("run output: %s", stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	code = execute([]string{"replay", path}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("replay code=%d stderr=%s", code, stderr.String())
	}
	if stderr.Len() != 0 || !strings.Contains(stdout.String(), "verified "+path) {
		t.Fatalf("replay stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestRunDoesNotOverwriteArtifact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.json")
	var stdout, stderr bytes.Buffer
	args := []string{"run", "--duration", "100ms", "--out", path}
	if code := execute(args, &stdout, &stderr); code != 0 {
		t.Fatalf("first run code=%d stderr=%s", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := execute(args, &stdout, &stderr); code != 2 {
		t.Fatalf("second run code=%d, want 2", code)
	}
}
