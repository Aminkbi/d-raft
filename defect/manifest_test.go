package defect

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestPinnedEtcdRaftPR31(t *testing.T) {
	manifest := PinnedEtcdRaftPR31()
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := Digest(manifest); err != nil {
		t.Fatal(err)
	}
}

func TestManifestDecodeIsStrict(t *testing.T) {
	manifest := PinnedEtcdRaftPR31()
	raw, err := os.ReadFile("../defects/etcd-raft-pr31/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeManifest(bytes.NewReader(raw))
	if err != nil || decoded != manifest {
		t.Fatalf("decoded manifest = %#v, %v", decoded, err)
	}
	for _, document := range []string{
		`{"schema":"d-raft.defect/v1","schema":"d-raft.defect/v1"}`,
		`{"schema":"d-raft.defect/v1","extra":true}`,
		string(raw) + `{}`,
	} {
		if _, err := DecodeManifest(bytes.NewBufferString(document)); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("strict decode error = %v", err)
		}
	}
}
