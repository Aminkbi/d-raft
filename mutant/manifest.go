// Package mutant executes a versioned, repository-pinned corpus of source
// mutants. It deliberately exposes no manifest-controlled command execution.
package mutant

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"unicode"
)

const (
	// ManifestSchema is the only corpus manifest schema accepted by this version.
	ManifestSchema = "d-raft.mutant/v1"
	// ResultSchema is the result document schema emitted by this version.
	ResultSchema = "d-raft.mutant-result/v1"
	// MaximumMutants bounds the amount of work described by one manifest.
	MaximumMutants = 64
)

var (
	hex40Pattern      = regexp.MustCompile(`^[0-9a-f]{40}$`)
	hex64Pattern      = regexp.MustCompile(`^[0-9a-f]{64}$`)
	idPattern         = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	repositoryPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)+$`)
	testPattern       = regexp.MustCompile(`^Test[A-Za-z0-9_]*[A-Za-z0-9]$`)
	invariantPattern  = regexp.MustCompile(`^[a-z][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*)*$`)
)

// Manifest describes a bounded corpus at one exact Git commit. Repository is
// the module path found in go.mod at BaseCommit, not a local filesystem path.
type Manifest struct {
	Schema     string   `json:"schema"`
	Repository string   `json:"repository"`
	BaseCommit string   `json:"base_commit"`
	Mutants    []Mutant `json:"mutants"`
}

// Mutant declares the only two patches and the only test the runner may use.
type Mutant struct {
	ID              string    `json:"id"`
	Package         string    `json:"package"`
	Test            string    `json:"test"`
	Invariant       Invariant `json:"invariant"`
	ActivationPatch Patch     `json:"activation_patch"`
	MutantPatch     Patch     `json:"mutant_patch"`
}

// Invariant identifies the property that must be reported by the test before
// a failure is attributed as a targeted mutant kill.
type Invariant struct {
	Name  string         `json:"name"`
	Class InvariantClass `json:"class"`
}

// InvariantClass separates protocol safety claims from conformance claims.
type InvariantClass string

const (
	InvariantSafety      InvariantClass = "safety"
	InvariantConformance InvariantClass = "conformance"
)

// Patch is a corpus-relative patch and its lowercase SHA-256 digest.
type Patch struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// DecodeManifest strictly decodes and validates one manifest. Unknown fields,
// multiple JSON values, and unsupported schema versions are rejected.
func DecodeManifest(r io.Reader) (Manifest, error) {
	data, err := io.ReadAll(io.LimitReader(r, (4<<20)+1))
	if err != nil {
		return Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	if len(data) > 4<<20 {
		return Manifest{}, errors.New("manifest exceeds 4194304 bytes")
	}
	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Manifest{}, errors.New("decode manifest: multiple JSON values")
		}
		return Manifest{}, fmt.Errorf("decode manifest trailer: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// Validate checks all closed-vocabulary and bounded manifest fields.
func (m Manifest) Validate() error {
	if m.Schema != ManifestSchema {
		return fmt.Errorf("manifest schema %q is unsupported", m.Schema)
	}
	if !repositoryPattern.MatchString(m.Repository) || strings.Contains(m.Repository, "..") {
		return fmt.Errorf("repository %q is not a module path", m.Repository)
	}
	if !hex40Pattern.MatchString(m.BaseCommit) {
		return errors.New("base_commit must be exactly 40 lowercase hexadecimal characters")
	}
	if len(m.Mutants) == 0 || len(m.Mutants) > MaximumMutants {
		return fmt.Errorf("mutants must contain between 1 and %d entries", MaximumMutants)
	}
	seen := make(map[string]struct{}, len(m.Mutants))
	for i, mutant := range m.Mutants {
		if err := mutant.validate(); err != nil {
			return fmt.Errorf("mutants[%d]: %w", i, err)
		}
		if _, ok := seen[mutant.ID]; ok {
			return fmt.Errorf("mutants[%d]: duplicate id %q", i, mutant.ID)
		}
		if i > 0 && m.Mutants[i-1].ID >= mutant.ID {
			return errors.New("mutants must be sorted by strictly increasing id")
		}
		seen[mutant.ID] = struct{}{}
	}
	return nil
}

func (m Mutant) validate() error {
	if err := m.validateTarget(); err != nil {
		return err
	}
	if err := m.ActivationPatch.validate(); err != nil {
		return fmt.Errorf("activation_patch: %w", err)
	}
	if err := m.MutantPatch.validate(); err != nil {
		return fmt.Errorf("mutant_patch: %w", err)
	}
	if m.ActivationPatch.Path == m.MutantPatch.Path {
		return errors.New("activation_patch and mutant_patch must be different files")
	}
	return nil
}

func (m Mutant) validateTarget() error {
	if !idPattern.MatchString(m.ID) {
		return fmt.Errorf("invalid id %q", m.ID)
	}
	switch m.Package {
	case "./raft", "./check", "./raftsim":
	default:
		return fmt.Errorf("package %q is not an allowed target", m.Package)
	}
	if !testPattern.MatchString(m.Test) {
		return fmt.Errorf("test %q must be one exact exported Test name", m.Test)
	}
	if !invariantPattern.MatchString(m.Invariant.Name) {
		return fmt.Errorf("invalid invariant name %q", m.Invariant.Name)
	}
	if m.Invariant.Class != InvariantSafety && m.Invariant.Class != InvariantConformance {
		return fmt.Errorf("invalid invariant class %q", m.Invariant.Class)
	}
	return nil
}

func (p Patch) validate() error {
	if p.Path == "" || len(p.Path) > 4096 || strings.Contains(p.Path, `\`) || strings.IndexFunc(p.Path, unicode.IsControl) >= 0 || path.IsAbs(p.Path) || path.Clean(p.Path) != p.Path || p.Path == "." || strings.HasPrefix(p.Path, "../") {
		return fmt.Errorf("path %q is not a safe corpus-relative path", p.Path)
	}
	if !hex64Pattern.MatchString(p.SHA256) {
		return errors.New("sha256 must be exactly 64 lowercase hexadecimal characters")
	}
	return nil
}

// ActivationMarker is the exact line fragment an activated test must emit.
func ActivationMarker(id string) string { return "DRAFT_MUTANT_ACTIVATED:" + id }

// InvariantMarker is the exact line fragment a targeted detection must emit.
func InvariantMarker(name string) string { return "DRAFT_MUTANT_INVARIANT:" + name }
