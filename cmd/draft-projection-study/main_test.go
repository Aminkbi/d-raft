package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateVerifyAndTamper(t *testing.T) {
	var output bytes.Buffer
	if err := run(nil, &output); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "result.json")
	if err := os.WriteFile(path, output.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"--verify", path}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"--verify", path}, &bytes.Buffer{}); err == nil {
		t.Fatal("accepted tampered result")
	}
	if err := run([]string{"unexpected"}, &bytes.Buffer{}); err == nil {
		t.Fatal("accepted unexpected argument")
	}
}
