package mutant

import (
	"errors"
	"strings"
	"testing"

	"github.com/aminkbi/d-raft/internal/strictjson"
)

func validManifest() Manifest {
	return Manifest{
		Schema: ManifestSchema, Repository: "example.com/research/raft", BaseCommit: strings.Repeat("a", 40),
		Mutants: []Mutant{{
			ID: "vote-once", Package: "./raft", Test: "TestVoteOnce",
			Invariant:       Invariant{Name: "election-safety", Class: InvariantSafety},
			ActivationPatch: Patch{Path: "patches/vote-test.patch", SHA256: strings.Repeat("1", 64)},
			MutantPatch:     Patch{Path: "patches/vote.patch", SHA256: strings.Repeat("2", 64)},
		}},
	}
}

func validManifestDocument() string {
	return `{"schema":"d-raft.mutant/v1","repository":"example.com/r/raft","base_commit":"` + strings.Repeat("a", 40) + `","mutants":[{"id":"m","package":"./raft","test":"TestM","invariant":{"name":"election-safety","class":"safety"},"activation_patch":{"path":"a.patch","sha256":"` + strings.Repeat("1", 64) + `"},"mutant_patch":{"path":"m.patch","sha256":"` + strings.Repeat("2", 64) + `"}}]}`
}

func TestManifestValidationIsStrict(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		edit func(*Manifest)
	}{
		{"schema", func(m *Manifest) { m.Schema = "d-raft.mutant/v2" }},
		{"commit length", func(m *Manifest) { m.BaseCommit = strings.Repeat("a", 39) }},
		{"commit uppercase", func(m *Manifest) { m.BaseCommit = strings.Repeat("A", 40) }},
		{"repository", func(m *Manifest) { m.Repository = "../raft" }},
		{"target", func(m *Manifest) { m.Mutants[0].Package = "./experiment" }},
		{"test regex", func(m *Manifest) { m.Mutants[0].Test = "Test.*" }},
		{"unsafe patch", func(m *Manifest) { m.Mutants[0].MutantPatch.Path = "../escape.patch" }},
		{"checksum", func(m *Manifest) { m.Mutants[0].MutantPatch.SHA256 = strings.Repeat("F", 64) }},
		{"invariant class", func(m *Manifest) { m.Mutants[0].Invariant.Class = "availability" }},
		{"same patch", func(m *Manifest) { m.Mutants[0].MutantPatch.Path = m.Mutants[0].ActivationPatch.Path }},
		{"duplicate id", func(m *Manifest) { m.Mutants = append(m.Mutants, m.Mutants[0]) }},
		{"unsorted ids", func(m *Manifest) {
			second := m.Mutants[0]
			second.ID = "earlier"
			m.Mutants = append(m.Mutants, second)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			manifest := validManifest()
			test.edit(&manifest)
			if err := manifest.Validate(); err == nil {
				t.Fatal("Validate accepted invalid manifest")
			}
		})
	}
}

func TestManifestAcceptsCanonicalInvariantID(t *testing.T) {
	t.Parallel()
	manifest := validManifest()
	manifest.Mutants[0].Invariant.Name = "raft/election-certificate"
	if err := manifest.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestDecodeManifestRejectsUnknownFieldsAndTrailingValues(t *testing.T) {
	t.Parallel()
	base := validManifestDocument()
	for _, document := range []string{
		strings.Replace(base, `"schema":`, `"unknown":true,"schema":`, 1),
		base + `{}`,
	} {
		if _, err := DecodeManifest(strings.NewReader(document)); err == nil {
			t.Fatalf("DecodeManifest accepted %q", document)
		}
	}
}

func TestDecodeManifestRejectsDuplicateNames(t *testing.T) {
	t.Parallel()

	base := validManifestDocument()
	if _, err := DecodeManifest(strings.NewReader(base)); err != nil {
		t.Fatalf("valid manifest: %v", err)
	}
	tests := []struct {
		name        string
		old         string
		replacement string
	}{
		{"top level", `"schema":`, `"schema":"d-raft.mutant/v1","schema":`},
		{"escaped nested", `"class":"safety"`, `"class":"safety","cla\u0073s":"safety"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			document := strings.Replace(base, test.old, test.replacement, 1)
			if document == base {
				t.Fatalf("replacement %q did not alter document", test.old)
			}
			if _, err := DecodeManifest(strings.NewReader(document)); !errors.Is(err, strictjson.ErrDuplicateName) {
				t.Fatalf("duplicate-name error = %v", err)
			}
		})
	}
}

func TestDecodeManifestRejectsNilReader(t *testing.T) {
	t.Parallel()

	if _, err := DecodeManifest(nil); err == nil || !strings.Contains(err.Error(), "nil reader") {
		t.Fatalf("nil-reader error = %v", err)
	}
}
