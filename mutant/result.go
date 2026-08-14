package mutant

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
)

// Classification is the closed outcome vocabulary for one mutant.
type Classification string

const (
	SafetyKill         Classification = "safety_kill"
	ConformanceKill    Classification = "conformance_kill"
	NonSafetyDetection Classification = "non_safety_detection"
	Survived           Classification = "survived"
	NotActivated       Classification = "not_activated"
	BaselineFailed     Classification = "baseline_failed"
	Ineligible         Classification = "ineligible"
	OperationalError   Classification = "operational_error"
)

var classifications = []Classification{
	SafetyKill, ConformanceKill, NonSafetyDetection, Survived,
	NotActivated, BaselineFailed, Ineligible, OperationalError,
}

// Result is the strict JSON-compatible output for a complete corpus run.
type Result struct {
	Schema         string         `json:"schema"`
	ManifestSchema string         `json:"manifest_schema"`
	ManifestSHA256 string         `json:"manifest_sha256"`
	Repository     string         `json:"repository"`
	BaseCommit     string         `json:"base_commit"`
	BaseTree       string         `json:"base_tree"`
	Environment    Environment    `json:"environment"`
	Results        []MutantResult `json:"results"`
}

// Environment identifies the runner source and Go platform that produced a
// result. RunnerModified is true when the target checkout was not clean.
type Environment struct {
	RunnerRevision string `json:"runner_revision"`
	RunnerModified bool   `json:"runner_modified"`
	TargetHead     string `json:"target_head"`
	TargetModified bool   `json:"target_modified"`
	GoVersion      string `json:"go_version"`
	GOOS           string `json:"goos"`
	GOARCH         string `json:"goarch"`
}

// MutantResult records attribution plus bounded command evidence.
type MutantResult struct {
	ID               string         `json:"id"`
	Package          string         `json:"package"`
	Test             string         `json:"test"`
	Invariant        Invariant      `json:"invariant"`
	ActivationSHA256 string         `json:"activation_sha256"`
	MutationSHA256   string         `json:"mutation_sha256"`
	Classification   Classification `json:"classification"`
	Detail           string         `json:"detail"`
	Baseline         *CommandResult `json:"baseline"`
	Mutant           *CommandResult `json:"mutant"`
}

// CommandResult is a bounded record of one fixed go test invocation.
type CommandResult struct {
	ExitCode        int    `json:"exit_code"`
	DurationMS      int64  `json:"duration_ms"`
	Output          string `json:"output"`
	OutputTruncated bool   `json:"output_truncated"`
}

// ClassificationInput contains only the evidence needed for attribution.
type ClassificationInput struct {
	Invariant       Invariant
	BaselineRan     bool
	BaselinePassed  bool
	BaselineMarked  bool
	MutantRan       bool
	MutantPassed    bool
	MutantMarked    bool
	TargetMarked    bool
	Eligible        bool
	OperationalFail bool
}

// DecodeResult strictly decodes and validates one result document.
func DecodeResult(r io.Reader) (Result, error) {
	data, err := io.ReadAll(io.LimitReader(r, (64<<20)+1))
	if err != nil {
		return Result{}, fmt.Errorf("read result: %w", err)
	}
	if len(data) > 64<<20 {
		return Result{}, errors.New("result exceeds 67108864 bytes")
	}
	var result Result
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return Result{}, fmt.Errorf("decode result: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Result{}, errors.New("decode result: multiple JSON values")
		}
		return Result{}, fmt.Errorf("decode result trailer: %w", err)
	}
	if err := result.Validate(); err != nil {
		return Result{}, err
	}
	return result, nil
}

// Classify applies the outcome precedence used by Runner. In particular, a
// failing test is a targeted kill only when its invariant marker is present.
func Classify(in ClassificationInput) Classification {
	if in.OperationalFail {
		return OperationalError
	}
	if !in.Eligible {
		return Ineligible
	}
	if !in.BaselineRan || !in.BaselinePassed {
		return BaselineFailed
	}
	if !in.BaselineMarked || (in.MutantRan && !in.MutantMarked) {
		return NotActivated
	}
	if !in.MutantRan {
		return OperationalError
	}
	if in.MutantPassed {
		return Survived
	}
	if !in.TargetMarked {
		return NonSafetyDetection
	}
	if in.Invariant.Class == InvariantSafety {
		return SafetyKill
	}
	if in.Invariant.Class == InvariantConformance {
		return ConformanceKill
	}
	return OperationalError
}

// Validate verifies that a result document uses the closed schemas and
// classification vocabulary. It is useful to consumers before aggregation.
func (r Result) Validate() error {
	if r.Schema != ResultSchema || r.ManifestSchema != ManifestSchema {
		return fmt.Errorf("unsupported result schemas %q and %q", r.Schema, r.ManifestSchema)
	}
	if !repositoryPattern.MatchString(r.Repository) || !hex40Pattern.MatchString(r.BaseCommit) || !hex40Pattern.MatchString(r.BaseTree) || !hex64Pattern.MatchString(r.ManifestSHA256) {
		return fmt.Errorf("invalid result repository or base commit")
	}
	if !hex40Pattern.MatchString(r.Environment.RunnerRevision) || !hex40Pattern.MatchString(r.Environment.TargetHead) || r.Environment.GoVersion == "" || r.Environment.GOOS == "" || r.Environment.GOARCH == "" {
		return errors.New("invalid result environment")
	}
	if len(r.Results) == 0 || len(r.Results) > MaximumMutants {
		return fmt.Errorf("results must contain between 1 and %d entries", MaximumMutants)
	}
	seen := make(map[string]struct{}, len(r.Results))
	for i, result := range r.Results {
		if !idPattern.MatchString(result.ID) || !slices.Contains(classifications, result.Classification) {
			return fmt.Errorf("results[%d] has invalid id or classification", i)
		}
		declaration := Mutant{ID: result.ID, Package: result.Package, Test: result.Test, Invariant: result.Invariant}
		if err := declaration.validateTarget(); err != nil {
			return fmt.Errorf("results[%d]: %w", i, err)
		}
		if !hex64Pattern.MatchString(result.ActivationSHA256) || !hex64Pattern.MatchString(result.MutationSHA256) {
			return fmt.Errorf("results[%d] has invalid patch digests", i)
		}
		if len(result.Detail) > 16<<10 {
			return fmt.Errorf("results[%d] detail is too large", i)
		}
		if result.Classification == SafetyKill && result.Invariant.Class != InvariantSafety {
			return fmt.Errorf("results[%d] safety_kill has non-safety invariant", i)
		}
		if result.Classification == ConformanceKill && result.Invariant.Class != InvariantConformance {
			return fmt.Errorf("results[%d] conformance_kill has non-conformance invariant", i)
		}
		for _, command := range []*CommandResult{result.Baseline, result.Mutant} {
			if command != nil && (command.DurationMS < 0 || len(command.Output) > maximumOutput) {
				return fmt.Errorf("results[%d] has invalid command evidence", i)
			}
		}
		if err := result.validateEvidence(); err != nil {
			return fmt.Errorf("results[%d]: %w", i, err)
		}
		if _, ok := seen[result.ID]; ok {
			return fmt.Errorf("results[%d] has duplicate id %q", i, result.ID)
		}
		if i > 0 && r.Results[i-1].ID >= result.ID {
			return errors.New("results must be sorted by strictly increasing id")
		}
		seen[result.ID] = struct{}{}
	}
	return nil
}

func (r MutantResult) validateEvidence() error {
	baselineMarked := r.Baseline != nil && hasExactMarker(r.Baseline.Output, ActivationMarker(r.ID))
	mutantMarked := r.Mutant != nil && hasExactMarker(r.Mutant.Output, ActivationMarker(r.ID))
	targetMarked := r.Mutant != nil && hasExactMarker(r.Mutant.Output, InvariantMarker(r.Invariant.Name))
	switch r.Classification {
	case SafetyKill, ConformanceKill:
		if r.Baseline == nil || r.Baseline.ExitCode != 0 || r.Mutant == nil || r.Mutant.ExitCode == 0 || !baselineMarked || !mutantMarked || !targetMarked {
			return errors.New("targeted kill lacks consistent command evidence")
		}
	case NonSafetyDetection:
		if r.Baseline == nil || r.Baseline.ExitCode != 0 || r.Mutant == nil || r.Mutant.ExitCode == 0 || !baselineMarked || !mutantMarked || targetMarked {
			return errors.New("non-safety detection lacks consistent command evidence")
		}
	case Survived:
		if r.Baseline == nil || r.Baseline.ExitCode != 0 || r.Mutant == nil || r.Mutant.ExitCode != 0 || !baselineMarked || !mutantMarked {
			return errors.New("survivor lacks consistent command evidence")
		}
	case BaselineFailed:
		if r.Baseline == nil || r.Baseline.ExitCode == 0 || r.Mutant != nil {
			return errors.New("baseline failure lacks consistent command evidence")
		}
	case NotActivated:
		if r.Baseline == nil || r.Baseline.ExitCode != 0 || (baselineMarked && (r.Mutant == nil || mutantMarked)) {
			return errors.New("not-activated result lacks consistent command evidence")
		}
	}
	return nil
}
