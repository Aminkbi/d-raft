// Package defect defines metadata for pinned production-defect replay cases.
package defect

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/aminkbi/d-raft/internal/strictjson"
)

const ManifestSchema = "d-raft.defect/v1"

const maxManifestBytes = 16 << 10

var (
	ErrInvalidManifest = errors.New("defect: invalid manifest")
	revisionPattern    = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

type Manifest struct {
	Schema             string `json:"schema"`
	ID                 string `json:"id"`
	UpstreamModule     string `json:"upstream_module"`
	IssueURL           string `json:"issue_url"`
	FixURL             string `json:"fix_url"`
	VulnerableRevision string `json:"vulnerable_revision"`
	FixedRevision      string `json:"fixed_revision"`
	Fixture            string `json:"fixture"`
	FixtureSHA256      string `json:"fixture_sha256"`
	FailurePredicate   string `json:"failure_predicate"`
	MappingPolicy      string `json:"mapping_policy"`
}

func (m Manifest) Validate() error {
	if m.Schema != ManifestSchema || m.ID == "" || m.UpstreamModule == "" || m.IssueURL == "" || m.FixURL == "" || m.Fixture == "" || m.FailurePredicate == "" || m.MappingPolicy == "" {
		return fmt.Errorf("%w: required field missing", ErrInvalidManifest)
	}
	for field, value := range map[string]string{
		"id": m.ID, "upstream_module": m.UpstreamModule, "issue_url": m.IssueURL,
		"fix_url": m.FixURL, "fixture": m.Fixture, "failure_predicate": m.FailurePredicate,
		"mapping_policy": m.MappingPolicy,
	} {
		if len(value) > maxManifestBytes || strings.TrimSpace(value) != value || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("%w: %s is malformed or exceeds resource budget", ErrInvalidManifest, field)
		}
	}
	if !revisionPattern.MatchString(m.VulnerableRevision) || !revisionPattern.MatchString(m.FixedRevision) || m.VulnerableRevision == m.FixedRevision {
		return fmt.Errorf("%w: revisions must be distinct full lowercase Git IDs", ErrInvalidManifest)
	}
	if len(m.FixtureSHA256) != sha256.Size*2 || strings.ToLower(m.FixtureSHA256) != m.FixtureSHA256 {
		return fmt.Errorf("%w: fixture_sha256 must be lowercase SHA-256", ErrInvalidManifest)
	}
	if _, err := hex.DecodeString(m.FixtureSHA256); err != nil {
		return fmt.Errorf("%w: fixture_sha256: %v", ErrInvalidManifest, err)
	}
	encoded, err := json.Marshal(m)
	if err != nil || len(encoded) > maxManifestBytes {
		return fmt.Errorf("%w: encoded manifest exceeds resource budget", ErrInvalidManifest)
	}
	return nil
}

// DecodeManifest strictly decodes one bounded defect manifest. Manifests are
// review inputs, so duplicate names, unknown fields, and trailing values are
// rejected before any replay command uses their metadata.
func DecodeManifest(reader io.Reader) (Manifest, error) {
	if reader == nil {
		return Manifest{}, fmt.Errorf("%w: nil reader", ErrInvalidManifest)
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxManifestBytes+1))
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	if len(data) > maxManifestBytes {
		return Manifest{}, fmt.Errorf("%w: document exceeds size limit", ErrInvalidManifest)
	}
	if err := strictjson.RejectDuplicateNames(data); err != nil {
		return Manifest{}, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Manifest{}, fmt.Errorf("%w: multiple JSON values", ErrInvalidManifest)
		}
		return Manifest{}, fmt.Errorf("%w: trailing JSON: %v", ErrInvalidManifest, err)
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func PinnedEtcdRaftPR31() Manifest {
	return Manifest{
		Schema: ManifestSchema, ID: "etcd-raft/pr-31-log-truncation-panic", UpstreamModule: "go.etcd.io/etcd/raft/v3",
		IssueURL: "https://github.com/etcd-io/raft/pull/31", FixURL: "https://github.com/etcd-io/raft/commit/d086538f5647fb518b8dbba26a424aea9d782c78",
		VulnerableRevision: "42419da55f51f7a0f0fb22b0e15892fb5657d191", FixedRevision: "d086538f5647fb518b8dbba26a424aea9d782c78",
		Fixture: "defects/etcd-raft-pr31/slow_follower_after_compaction.txt", FixtureSHA256: "bdf15b0b725fca9114922c7f5d0e56aa3005f6eea2555c9edd14efe3142fb4a9",
		FailurePredicate: "vulnerable execution panics while processing slow_follower_after_compaction.txt; fixed execution reaches the fixture boundary without panic",
		MappingPolicy:    "network-loss directives bind all MsgApp attempts carrying one stable application/log operation; conflicting batches are ineligible",
	}
}

func Digest(m Manifest) (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
