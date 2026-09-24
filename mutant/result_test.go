package mutant

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aminkbi/d-raft/internal/strictjson"
)

func validResultDocument(t *testing.T) string {
	t.Helper()

	result := Result{
		Schema: ResultSchema, ManifestSchema: ManifestSchema,
		ManifestSHA256: strings.Repeat("3", 64), Repository: "example.com/r/raft",
		BaseCommit: strings.Repeat("a", 40), BaseTree: strings.Repeat("b", 40),
		Environment: Environment{
			RunnerRevision: strings.Repeat("c", 40), TargetHead: strings.Repeat("d", 40),
			GoVersion: "go1.26.6", GOOS: "linux", GOARCH: "amd64",
		},
		Results: []MutantResult{{
			ID: "m", Package: "./raft", Test: "TestM",
			Invariant:        Invariant{Name: "election-safety", Class: InvariantSafety},
			ActivationSHA256: strings.Repeat("1", 64), MutationSHA256: strings.Repeat("2", 64),
			Classification: OperationalError,
		}},
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func TestClassify(t *testing.T) {
	t.Parallel()
	valid := ClassificationInput{Invariant: Invariant{Name: "election-safety", Class: InvariantSafety}, Eligible: true, BaselineRan: true, BaselinePassed: true, BaselineMarked: true, MutantRan: true, MutantMarked: true}
	tests := []struct {
		name string
		edit func(*ClassificationInput)
		want Classification
	}{
		{"safety kill", func(in *ClassificationInput) { in.TargetMarked = true }, SafetyKill},
		{"conformance kill", func(in *ClassificationInput) { in.Invariant.Class = InvariantConformance; in.TargetMarked = true }, ConformanceKill},
		{"unattributed failure", func(in *ClassificationInput) {}, NonSafetyDetection},
		{"survived", func(in *ClassificationInput) { in.MutantPassed = true }, Survived},
		{"baseline marker", func(in *ClassificationInput) { in.BaselineMarked = false }, NotActivated},
		{"mutant marker", func(in *ClassificationInput) { in.MutantMarked = false }, NotActivated},
		{"baseline failure", func(in *ClassificationInput) { in.BaselinePassed = false }, BaselineFailed},
		{"ineligible", func(in *ClassificationInput) { in.Eligible = false }, Ineligible},
		{"operational", func(in *ClassificationInput) { in.OperationalFail = true }, OperationalError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			input := valid
			test.edit(&input)
			if got := Classify(input); got != test.want {
				t.Fatalf("Classify()=%q want %q", got, test.want)
			}
		})
	}
}

func TestDecodeResultRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	base := validResultDocument(t)
	if _, err := DecodeResult(strings.NewReader(base)); err != nil {
		t.Fatalf("valid result: %v", err)
	}
	document := strings.Replace(base, `{"schema":`, `{"unknown":true,"schema":`, 1)
	if document == base {
		t.Fatal("unknown-field replacement did not alter document")
	}
	if _, err := DecodeResult(strings.NewReader(document)); err == nil {
		t.Fatal("DecodeResult accepted an unknown field")
	}
}

func TestDecodeResultRejectsDuplicateNames(t *testing.T) {
	t.Parallel()

	base := validResultDocument(t)
	tests := []struct {
		name        string
		old         string
		replacement string
	}{
		{"top level", `"schema":`, `"schema":"d-raft.mutant-result/v1","schema":`},
		{"escaped nested", `"class":"safety"`, `"class":"safety","cla\u0073s":"safety"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			document := strings.Replace(base, test.old, test.replacement, 1)
			if document == base {
				t.Fatalf("replacement %q did not alter document", test.old)
			}
			if _, err := DecodeResult(strings.NewReader(document)); !errors.Is(err, strictjson.ErrDuplicateName) {
				t.Fatalf("duplicate-name error = %v", err)
			}
		})
	}
}

func TestDecodeResultRejectsNilReader(t *testing.T) {
	t.Parallel()

	if _, err := DecodeResult(nil); err == nil || !strings.Contains(err.Error(), "nil reader") {
		t.Fatalf("nil-reader error = %v", err)
	}
}

func TestResultValidationRejectsMissingProvenance(t *testing.T) {
	t.Parallel()
	result := Result{
		Schema: ResultSchema, ManifestSchema: ManifestSchema,
		Repository: "example.com/r/raft", BaseCommit: strings.Repeat("a", 40),
		Results: []MutantResult{{
			ID: "m", Package: "./raft", Test: "TestM",
			Invariant:      Invariant{Name: "raft/election-certificate", Class: InvariantSafety},
			Classification: OperationalError,
		}},
	}
	if err := result.Validate(); err == nil {
		t.Fatal("Validate accepted missing result provenance")
	}
}
