package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aminkbi/d-raft/mutant"
)

func TestExecuteRequiresManifest(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if code := execute(context.Background(), nil, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "manifest is required") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestExecuteStrictlyDecodesManifest(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "manifest.json")
	document := `{"schema":"d-raft.mutant/v1","repository":"example.com/r/raft","base_commit":"` + strings.Repeat("a", 40) + `","mutants":[],"command":"sh"}`
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := execute(context.Background(), []string{"-manifest", path}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "unknown field") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestWriteResultPublishesWithoutOverwrite(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	path := filepath.Join(directory, "result.json")
	result := mutant.Result{Schema: mutant.ResultSchema}
	if err := writeResult(path, io.Discard, result); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(before, []byte(mutant.ResultSchema)) {
		t.Fatalf("result=%q", before)
	}
	if err := writeResult(path, io.Discard, mutant.Result{}); err == nil {
		t.Fatal("writeResult overwrote an existing result")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("existing result changed")
	}
	matches, err := filepath.Glob(filepath.Join(directory, ".draft-mutants-result-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary files=%v err=%v", matches, err)
	}
}
