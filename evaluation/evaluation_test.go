package evaluation

import (
	"bytes"
	"encoding/json"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/aminkbi/d-raft/artifact"
)

func TestRunEncodeDecode(t *testing.T) {
	config := DefaultConfig(3)
	config.RunnerInvocationBudget = 8
	config.Search.MaxDepth = 2
	config.Cache.MaxEntries = 100
	config.Cache.MaxBytes = 1 << 20
	result, err := Run(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Trials) != 9 || len(result.Summaries) != 3 || len(result.Contrasts) != 1 {
		t.Fatalf("result shape = %d/%d", len(result.Trials), len(result.Summaries))
	}
	var encoded bytes.Buffer
	if err := Encode(&encoded, result); err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Schema != SchemaVersion || len(decoded.Trials) != len(result.Trials) {
		t.Fatalf("decoded = %+v", decoded)
	}
}

func TestOutcomeStatusAccounting(t *testing.T) {
	var trial Trial
	for _, status := range []artifact.OutcomeStatus{
		artifact.OutcomeCompleted,
		artifact.OutcomeViolation,
		artifact.OutcomeError,
		artifact.OutcomeBudgetExhausted,
	} {
		recordOutcome(&trial, artifact.Outcome{Status: status})
	}
	if trial.TerminalExecutions != 4 || trial.CompletedRuns != 1 || trial.ViolatingRuns != 1 || trial.ErrorRuns != 1 || trial.BudgetExhaustedRuns != 1 {
		t.Fatalf("status accounting = %+v", trial)
	}
}

func TestBootstrapOpenProcessesNoSimulatorEvent(t *testing.T) {
	config := DefaultConfig(3)
	config.RunnerInvocationBudget = 1
	config.Search.MaxDepth = 1
	trial, err := runTrial(config, MethodCacheOff, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if trial.RunnerInvocations != 1 || trial.OpenChoices != 1 || trial.TerminalExecutions != 0 || trial.ProcessedSimulatorEvents != 0 {
		t.Fatalf("bootstrap accounting = %+v", trial)
	}
}

func TestValidationRejectsForgedAccountingAndStatistics(t *testing.T) {
	base := smallResult(t)
	mutations := map[string]func(*Result){
		"status sum":        func(result *Result) { result.Trials[0].ErrorRuns++ },
		"invocation budget": func(result *Result) { result.Trials[0].RunnerInvocations++ },
		"rotation":          func(result *Result) { result.Trials[0].Order = 1 },
		"dfs equation": func(result *Result) {
			trial := trialFor(result, MethodCacheOff)
			trial.PrefixPruned++
		},
		"cache equation": func(result *Result) {
			trial := trialFor(result, MethodCacheOn)
			trial.CacheHits++
		},
		"cache bytes": func(result *Result) {
			trial := trialFor(result, MethodCacheOn)
			trial.CacheBytes = artifact.Uint64(result.Config.Cache.MaxBytes + 1)
		},
		"cache bytes impossible minimum": func(result *Result) {
			trial := trialFor(result, MethodCacheOn)
			trial.CacheBytes = artifact.Uint64(uint64(trial.UniqueStates)*minimumCacheIdentityBytes - 1)
		},
		"cache bytes without states": func(result *Result) {
			trial := trialFor(result, MethodCacheOn)
			trial.CacheBudgetSkips += trial.UniqueStates
			trial.UniqueStates = 0
			trial.CacheBytes = 1
		},
		"cache collision maximum": func(result *Result) {
			trial := trialFor(result, MethodCacheOn)
			lookups := uint64(trial.CacheLookups)
			trial.HashCollisions = artifact.Uint64(lookups*(lookups-1)/2 + 1)
		},
		"counter overflow":    func(result *Result) { result.Trials[0].CompletedRuns = artifact.Uint64(^uint64(0)) },
		"summary":             func(result *Result) { result.Summaries[0].CompletedRunsTotal++ },
		"contrast":            func(result *Result) { result.Contrasts[0].ElapsedDifferenceNS95.Mean++ },
		"environment control": func(result *Result) { result.Environment.CPUModel = "cpu\x1bmodel" },
		"environment settings order": func(result *Result) {
			result.Environment.BuildSettings = []BuildSetting{{Key: "vcs.revision", Value: result.Environment.GitRevision}, {Key: "vcs.modified", Value: "false"}}
		},
		"environment GOOS disagreement": func(result *Result) {
			result.Environment.BuildSettings = []BuildSetting{{Key: "GOOS", Value: "contradiction"}}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			forged := cloneResult(t, base)
			mutate(&forged)
			if err := forged.Validate(); err == nil {
				t.Fatal("forged result accepted")
			}
		})
	}
}

func TestSummaryRejectsOverflow(t *testing.T) {
	trials := []Trial{
		{Method: MethodRandom, ElapsedNS: 1, ProcessedSimulatorEvents: 1, CompletedRuns: artifact.Uint64(^uint64(0))},
		{Method: MethodRandom, ElapsedNS: 1, ProcessedSimulatorEvents: 1, CompletedRuns: 1},
	}
	if _, err := summarize(trials); err == nil {
		t.Fatal("overflowing summary accepted")
	}
}

func TestPairedContrastUsesTrialIndexAndDeclaredSign(t *testing.T) {
	trials := make([]Trial, 0, 6)
	for index := range 3 {
		trials = append(trials,
			Trial{Index: index, Method: MethodCacheOn, ElapsedNS: artifact.Uint64(2 * 1e9), ProcessedSimulatorEvents: 400},
			Trial{Index: index, Method: MethodCacheOff, ElapsedNS: artifact.Uint64(1e9), ProcessedSimulatorEvents: 100},
		)
	}
	slices.Reverse(trials)
	contrasts, err := pairedContrasts(trials, 3)
	if err != nil {
		t.Fatal(err)
	}
	contrast := contrasts[0]
	if contrast.Left != MethodCacheOn || contrast.Right != MethodCacheOff || contrast.ElapsedDifferenceNS95 != (Interval{Mean: 1e9, Lower: 1e9, Upper: 1e9}) || contrast.EventsPerSecondDifference95 != (Interval{Mean: 100, Lower: 100, Upper: 100}) {
		t.Fatalf("contrast = %+v", contrast)
	}
}

func TestEnvironmentAndPublicationProvenance(t *testing.T) {
	environment, err := CurrentEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateEnvironment(environment); err != nil {
		t.Fatal(err)
	}
	unknown := environment
	setProvenance(&unknown, "unknown", false)
	if err := ValidatePublicationEnvironment(unknown); err == nil {
		t.Fatal("unknown revision accepted for publication")
	}
	dirty := environment
	setProvenance(&dirty, strings.Repeat("a", 40), true)
	if err := ValidatePublicationEnvironment(dirty); err == nil {
		t.Fatal("dirty revision accepted for publication")
	}
	clean := environment
	setProvenance(&clean, strings.Repeat("a", 40), false)
	if err := ValidatePublicationEnvironment(clean); err != nil {
		t.Fatalf("clean provenance rejected: %v", err)
	}
}

func TestBalancedConfigAndStudentTBounds(t *testing.T) {
	if config := DefaultConfig(21); config.Trials != 21 || ValidateConfig(config) != nil {
		t.Fatalf("default config = %+v", config)
	}
	if err := ValidateConfig(DefaultConfig(20)); err == nil {
		t.Fatal("unbalanced trial count accepted")
	}
	if value := tCritical95(29); value != 2.045 {
		t.Fatalf("t(29) = %v", value)
	}
	if !math.IsNaN(tCritical95(31)) {
		t.Fatal("out-of-table t critical silently approximated")
	}
}

func TestDecodeRejectsUnknownAndDuplicateFields(t *testing.T) {
	for _, input := range []string{
		`{"schema":"d-raft.evaluation/v1","schema":"d-raft.evaluation/v1"}`,
		`{"schema":"d-raft.evaluation/v1","unknown":true}`,
	} {
		if _, err := Decode(bytes.NewBufferString(input)); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}
}

func TestDecodeRejectsSizeTrailingAndTruncation(t *testing.T) {
	if _, err := Decode(bytes.NewReader(bytes.Repeat([]byte{' '}, DefaultMaxResultBytes+1))); err == nil {
		t.Fatal("oversized result accepted")
	}
	result := smallResult(t)
	var encoded bytes.Buffer
	if err := Encode(&encoded, result); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"second value": append(slices.Clone(encoded.Bytes()), []byte("{}")...),
		"truncated":    slices.Clone(encoded.Bytes()[:encoded.Len()-8]),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(bytes.NewReader(data)); err == nil {
				t.Fatal("malformed result accepted")
			}
		})
	}
}

func TestDecodePublicationRequiresAndPreservesCleanProvenance(t *testing.T) {
	result := smallResult(t)
	setProvenance(&result.Environment, "unknown", false)
	var local bytes.Buffer
	if err := Encode(&local, result); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodePublication(bytes.NewReader(local.Bytes())); err == nil {
		t.Fatal("local test result accepted as a publication")
	}
	clean := cloneResult(t, result)
	setProvenance(&clean.Environment, strings.Repeat("c", 40), false)
	var published bytes.Buffer
	if err := Encode(&published, clean); err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePublication(bytes.NewReader(published.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Environment.GitRevision != strings.Repeat("c", 40) || decoded.Environment.GitModified {
		t.Fatalf("provenance = %+v", decoded.Environment)
	}
}

func smallResult(t *testing.T) Result {
	t.Helper()
	config := DefaultConfig(3)
	config.RunnerInvocationBudget = 8
	config.Search.MaxDepth = 2
	config.Cache.MaxEntries = 100
	config.Cache.MaxBytes = 1 << 20
	result, err := Run(config)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func cloneResult(t *testing.T, source Result) Result {
	t.Helper()
	data, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var result Result
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func trialFor(result *Result, method Method) *Trial {
	for index := range result.Trials {
		if result.Trials[index].Method == method {
			return &result.Trials[index]
		}
	}
	panic("method missing")
}

func setProvenance(environment *Environment, revision string, modified bool) {
	environment.GitRevision = revision
	environment.GitModified = modified
	foundVCS, foundRevision, foundModified := false, false, false
	for index := range environment.BuildSettings {
		switch environment.BuildSettings[index].Key {
		case "vcs":
			environment.BuildSettings[index].Value = "git"
			foundVCS = true
		case "vcs.revision":
			environment.BuildSettings[index].Value = revision
			foundRevision = true
		case "vcs.modified":
			environment.BuildSettings[index].Value = map[bool]string{false: "false", true: "true"}[modified]
			foundModified = true
		}
	}
	if !foundVCS {
		environment.BuildSettings = append(environment.BuildSettings, BuildSetting{Key: "vcs", Value: "git"})
	}
	if !foundRevision {
		environment.BuildSettings = append(environment.BuildSettings, BuildSetting{Key: "vcs.revision", Value: revision})
	}
	if !foundModified {
		environment.BuildSettings = append(environment.BuildSettings, BuildSetting{Key: "vcs.modified", Value: map[bool]string{false: "false", true: "true"}[modified]})
	}
	slices.SortFunc(environment.BuildSettings, func(left, right BuildSetting) int { return strings.Compare(left.Key, right.Key) })
}
