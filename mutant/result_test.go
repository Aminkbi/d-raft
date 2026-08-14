package mutant

import (
	"strings"
	"testing"
)

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
		test := test
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
	document := `{"schema":"d-raft.mutant-result/v1","manifest_schema":"d-raft.mutant/v1","repository":"example.com/r/raft","base_commit":"` + strings.Repeat("a", 40) + `","results":[],"unknown":true}`
	if _, err := DecodeResult(strings.NewReader(document)); err == nil {
		t.Fatal("DecodeResult accepted an unknown field")
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
