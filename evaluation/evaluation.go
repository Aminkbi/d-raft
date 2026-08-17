// Package evaluation runs the bounded, repeated empirical comparison shipped
// with d-raft. It measures wall time only at this outer research boundary; the
// simulator and protocol executions remain virtual-time deterministic.
package evaluation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/experiment"
	"github.com/aminkbi/d-raft/explore"
	"github.com/aminkbi/d-raft/internal/strictjson"
	"github.com/aminkbi/d-raft/raft"
)

const (
	SchemaVersion             = "d-raft.evaluation/v1"
	DefaultMaxResultBytes     = 4 << 20
	MaxTrials                 = 30
	minimumCacheIdentityBytes = 64
)

type Method string

const (
	MethodRandom   Method = "random_full_runs"
	MethodCacheOff Method = "bounded_dfs_frontier_cache_off"
	MethodCacheOn  Method = "bounded_dfs_frontier_cache_on"
)

var methods = []Method{MethodRandom, MethodCacheOff, MethodCacheOn}

// RunConfig fixes the matched workload and explicit search/resource bounds.
type RunConfig struct {
	Trials                 int                    `json:"trials"`
	RunnerInvocationBudget int                    `json:"runner_invocation_budget"`
	SeedBase               artifact.Uint64        `json:"seed_base"`
	Scenario               artifact.Scenario      `json:"scenario"`
	Cluster                artifact.Configuration `json:"cluster"`
	Search                 SearchBounds           `json:"search"`
	Cache                  explore.CacheBounds    `json:"cache"`
}

// SearchBounds is the stable JSON form of explore.Bounds used by the study.
type SearchBounds struct {
	MaxDepth             int `json:"max_depth"`
	MaxBranchesPerChoice int `json:"max_branches_per_choice"`
	RangeSamples         int `json:"range_samples"`
}

type Environment struct {
	GitRevision       string           `json:"git_revision"`
	GitModified       bool             `json:"git_modified"`
	GoVersion         string           `json:"go_version"`
	GOOS              string           `json:"goos"`
	GOARCH            string           `json:"goarch"`
	OSVersion         string           `json:"os_version"`
	CPUModel          string           `json:"cpu_model"`
	MemoryBytes       artifact.Uint64  `json:"memory_bytes"`
	LogicalCPUs       int              `json:"logical_cpus"`
	GOMAXPROCS        int              `json:"gomaxprocs"`
	Adapter           artifact.Adapter `json:"adapter"`
	DecisionSchema    string           `json:"decision_schema"`
	CheckerSchema     string           `json:"checker_schema"`
	ObservationSchema string           `json:"observation_schema"`
	MessageCodec      string           `json:"message_codec"`
	BuildSettings     []BuildSetting   `json:"build_settings"`
}

type BuildSetting struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Trial retains every raw observation. ElapsedNS is wall-clock time and must
// not be interpreted as a deterministic artifact field. A processed simulator
// event includes an event that ran only until it opened a semantic choice; it
// excludes bootstrap choices and canonical-state construction work.
type Trial struct {
	Index                    int             `json:"index"`
	Order                    int             `json:"order"`
	Method                   Method          `json:"method"`
	ElapsedNS                artifact.Uint64 `json:"elapsed_ns"`
	RunnerInvocations        artifact.Uint64 `json:"runner_invocations"`
	ProcessedSimulatorEvents artifact.Uint64 `json:"processed_simulator_events"`
	TerminalExecutions       artifact.Uint64 `json:"terminal_executions"`
	CompletedRuns            artifact.Uint64 `json:"completed_runs"`
	ViolatingRuns            artifact.Uint64 `json:"violating_runs"`
	ErrorRuns                artifact.Uint64 `json:"error_runs"`
	BudgetExhaustedRuns      artifact.Uint64 `json:"budget_exhausted_runs"`
	OpenChoices              artifact.Uint64 `json:"open_choices"`
	PrefixPruned             artifact.Uint64 `json:"prefix_pruned"`
	StatePruned              artifact.Uint64 `json:"state_pruned"`
	CacheLookups             artifact.Uint64 `json:"cache_lookups"`
	CacheHits                artifact.Uint64 `json:"cache_hits"`
	CacheMisses              artifact.Uint64 `json:"cache_misses"`
	CacheBytes               artifact.Uint64 `json:"cache_bytes"`
	CacheBudgetSkips         artifact.Uint64 `json:"cache_budget_skips"`
	HashCollisions           artifact.Uint64 `json:"hash_collisions"`
	SampledDomains           artifact.Uint64 `json:"sampled_domains"`
	DepthBoundHits           artifact.Uint64 `json:"depth_bound_hits"`
	UniqueStates             artifact.Uint64 `json:"unique_states"`
	RunBudgetTruncated       bool            `json:"run_budget_truncated"`
}

type Interval struct {
	Mean  float64 `json:"mean"`
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
}

type Summary struct {
	Method                     Method   `json:"method"`
	Trials                     int      `json:"trials"`
	ElapsedNS95                Interval `json:"elapsed_ns_95_t_interval"`
	ProcessedSimulatorEvents95 Interval `json:"processed_simulator_events_95_t_interval"`
	EventsPerSecond95          Interval `json:"events_per_second_95_t_interval"`
	RunnerInvocationsMean      float64  `json:"runner_invocations_mean"`
	TerminalExecutionsMean     float64  `json:"terminal_executions_mean"`
	CompletedRunsTotal         uint64   `json:"completed_runs_total"`
	ViolatingRunsTotal         uint64   `json:"violating_runs_total"`
	ErrorRunsTotal             uint64   `json:"error_runs_total"`
	BudgetExhaustedRunsTotal   uint64   `json:"budget_exhausted_runs_total"`
	OpenChoicesTotal           uint64   `json:"open_choices_total"`
	PrefixPrunedTotal          uint64   `json:"prefix_pruned_total"`
	StatePrunedTotal           uint64   `json:"state_pruned_total"`
	DepthBoundHitsTotal        uint64   `json:"depth_bound_hits_total"`
	SampledDomainsTotal        uint64   `json:"sampled_domains_total"`
	CacheLookupsTotal          uint64   `json:"cache_lookups_total"`
	CacheHitsTotal             uint64   `json:"cache_hits_total"`
	CacheMissesTotal           uint64   `json:"cache_misses_total"`
	CacheBudgetSkipsTotal      uint64   `json:"cache_budget_skips_total"`
	UniqueStatesMean           float64  `json:"unique_states_mean"`
	CacheBytesMean             float64  `json:"cache_bytes_mean"`
	HashCollisionsTotal        uint64   `json:"hash_collisions_total"`
	RunBudgetTruncatedCount    int      `json:"run_budget_truncated_count"`
}

type Contrast struct {
	Name                        string   `json:"name"`
	Left                        Method   `json:"left"`
	Right                       Method   `json:"right"`
	ElapsedDifferenceNS95       Interval `json:"elapsed_difference_ns_95_t_interval"`
	EventsPerSecondDifference95 Interval `json:"events_per_second_difference_95_t_interval"`
}

type Result struct {
	Schema      string      `json:"schema"`
	Environment Environment `json:"environment"`
	Config      RunConfig   `json:"config"`
	Trials      []Trial     `json:"trials"`
	Summaries   []Summary   `json:"summaries"`
	Contrasts   []Contrast  `json:"paired_contrasts"`
}

// DefaultConfig returns the published matched invocation-budget study.
func DefaultConfig(trials int) RunConfig {
	return RunConfig{
		Trials: trials, RunnerInvocationBudget: 244, SeedBase: 20260815,
		Scenario: artifact.Scenario{ID: "evaluation/steady-cluster", Version: "1", DurationNS: int64(300 * time.Millisecond), MaxSteps: 100_000},
		Cluster: artifact.Configuration{
			Members: []raft.NodeID{"a", "b", "c"}, InfrastructureSeed: 19,
			NetworkMinLatencyNS: int64(time.Millisecond), NetworkMaxLatencyNS: int64(5 * time.Millisecond), NetworkLossProbability: 0.02,
			ElectionTimeoutMinNS: int64(100 * time.Millisecond), ElectionTimeoutMaxNS: int64(200 * time.Millisecond),
			HeartbeatIntervalNS: int64(20 * time.Millisecond), StorageLatencyNS: int64(time.Millisecond), StopOnViolation: true,
		},
		Search: SearchBounds{MaxDepth: 6, MaxBranchesPerChoice: 3, RangeSamples: 3},
		Cache:  explore.CacheBounds{MaxEntries: 100_000, MaxBytes: 256 << 20},
	}
}

// Run captures the current environment and executes the study.
func Run(config RunConfig) (Result, error) {
	environment, err := CurrentEnvironment()
	if err != nil {
		return Result{}, err
	}
	return RunWithEnvironment(config, environment)
}

// RunWithEnvironment executes one excluded warm-up per method, rotates
// measured method order, and returns raw trials plus two-sided 95% Student-t
// intervals. Callers that preflight publication provenance can pass the exact
// environment they validated rather than sampling it twice.
func RunWithEnvironment(config RunConfig, environment Environment) (Result, error) {
	if err := validateConfig(config); err != nil {
		return Result{}, err
	}
	environment.BuildSettings = slices.Clone(environment.BuildSettings)
	if err := validateEnvironment(environment); err != nil {
		return Result{}, err
	}
	for _, method := range methods {
		if _, err := runTrial(config, method, -1, 0); err != nil {
			return Result{}, fmt.Errorf("evaluation: warm-up %s: %w", method, err)
		}
	}

	result := Result{
		Schema:      SchemaVersion,
		Environment: environment,
		Config:      cloneConfig(config),
		Trials:      make([]Trial, 0, config.Trials*len(methods)),
	}
	for index := range config.Trials {
		for order := range methods {
			method := methods[(index+order)%len(methods)]
			trial, err := runTrial(config, method, index, order)
			if err != nil {
				return Result{}, fmt.Errorf("evaluation: trial %d %s: %w", index, method, err)
			}
			result.Trials = append(result.Trials, trial)
		}
	}
	summaries, err := summarize(result.Trials)
	if err != nil {
		return Result{}, err
	}
	result.Summaries = summaries
	contrasts, err := pairedContrasts(result.Trials, config.Trials)
	if err != nil {
		return Result{}, err
	}
	result.Contrasts = contrasts
	if err := result.Validate(); err != nil {
		return Result{}, err
	}
	return result, nil
}

func runTrial(config RunConfig, method Method, index, order int) (Trial, error) {
	trial := Trial{Index: index, Order: order, Method: method}
	started := time.Now()
	switch method {
	case MethodRandom:
		for run := range config.RunnerInvocationBudget {
			seed := uint64(config.SeedBase) + uint64(max(index, 0))*uint64(config.RunnerInvocationBudget) + uint64(run)
			outcome, err := experiment.Execute(config.Scenario, config.Cluster, decision.NewSeedDecider(seed))
			if err != nil {
				return Trial{}, err
			}
			trial.RunnerInvocations++
			trial.ProcessedSimulatorEvents += artifact.Uint64(outcome.Steps)
			recordOutcome(&trial, outcome)
		}
	case MethodCacheOff, MethodCacheOn:
		bounds := explore.Bounds{
			MaxRuns: config.RunnerInvocationBudget, MaxDepth: config.Search.MaxDepth,
			MaxBranchesPerChoice: config.Search.MaxBranchesPerChoice, RangeSamples: config.Search.RangeSamples,
			FallbackSeed: uint64(config.SeedBase) + uint64(max(index, 0)), StopOnViolation: false,
		}
		runner := countedRunner(config.Scenario, config.Cluster, &trial)
		var explored explore.Result
		var err error
		if method == MethodCacheOn {
			explored, err = explore.DFSWithCache(runner, bounds, config.Cache)
		} else {
			explored, err = explore.DFS(func(decider decision.Decider) (artifact.Outcome, error) {
				outcome, _, runErr := runner(decider)
				return outcome, runErr
			}, bounds)
		}
		if err != nil {
			return Trial{}, err
		}
		trial.RunnerInvocations = artifact.Uint64(explored.Runs)
		trial.TerminalExecutions = artifact.Uint64(explored.Completed)
		trial.CompletedRuns = artifact.Uint64(explored.CompletedRuns)
		trial.ViolatingRuns = artifact.Uint64(explored.OutcomeViolationRuns)
		trial.ErrorRuns = artifact.Uint64(explored.ErrorRuns)
		trial.BudgetExhaustedRuns = artifact.Uint64(explored.BudgetExhaustedRuns)
		trial.OpenChoices = artifact.Uint64(explored.OpenChoices)
		trial.PrefixPruned = artifact.Uint64(explored.PrunedPrefixes)
		trial.StatePruned = artifact.Uint64(explored.StatePruned)
		trial.CacheLookups = artifact.Uint64(explored.CacheLookups)
		trial.CacheHits = artifact.Uint64(explored.CacheHits)
		trial.CacheMisses = artifact.Uint64(explored.CacheMisses)
		trial.CacheBytes = artifact.Uint64(explored.CacheBytes)
		trial.CacheBudgetSkips = artifact.Uint64(explored.CacheBudgetSkips)
		trial.HashCollisions = artifact.Uint64(explored.HashCollisions)
		trial.SampledDomains = artifact.Uint64(explored.SampledDomains)
		trial.DepthBoundHits = artifact.Uint64(explored.DepthBoundHits)
		trial.UniqueStates = artifact.Uint64(explored.UniqueStates)
		trial.RunBudgetTruncated = explored.Truncated
	default:
		return Trial{}, fmt.Errorf("unknown method %q", method)
	}
	trial.ElapsedNS = artifact.Uint64(time.Since(started).Nanoseconds())
	return trial, nil
}

func recordOutcome(trial *Trial, outcome artifact.Outcome) {
	trial.TerminalExecutions++
	switch outcome.Status {
	case artifact.OutcomeCompleted:
		trial.CompletedRuns++
	case artifact.OutcomeViolation:
		trial.ViolatingRuns++
	case artifact.OutcomeError:
		trial.ErrorRuns++
	case artifact.OutcomeBudgetExhausted:
		trial.BudgetExhaustedRuns++
	}
}

func countedRunner(scenario artifact.Scenario, configuration artifact.Configuration, trial *Trial) explore.StatefulRunner {
	return func(decider decision.Decider) (artifact.Outcome, []byte, error) {
		outcome, frontier, err := experiment.ExecuteWithFrontier(scenario, configuration, decider)
		if errors.Is(err, decision.ErrOpenChoice) {
			var usage struct {
				Schema    string `json:"schema"`
				StepsUsed uint64 `json:"steps_used"`
				PreEvent  struct {
					Bootstrapped bool `json:"bootstrapped"`
				} `json:"pre_event"`
			}
			if decodeErr := json.Unmarshal(frontier, &usage); decodeErr != nil || usage.Schema != experiment.FrontierSchemaVersion {
				return artifact.Outcome{}, nil, errors.New("evaluation: invalid frontier usage")
			}
			processed := usage.StepsUsed
			// An open choice during Bootstrap has not processed a simulator
			// event. Once bootstrapped, the active Step was popped and processed
			// up to the choice and therefore counts as one event.
			if usage.PreEvent.Bootstrapped {
				var ok bool
				processed, ok = addUint64(processed, 1)
				if !ok {
					return artifact.Outcome{}, nil, errors.New("evaluation: processed-event counter overflows")
				}
			}
			total, ok := addUint64(uint64(trial.ProcessedSimulatorEvents), processed)
			if !ok {
				return artifact.Outcome{}, nil, errors.New("evaluation: processed-event counter overflows")
			}
			trial.ProcessedSimulatorEvents = artifact.Uint64(total)
		} else {
			total, ok := addUint64(uint64(trial.ProcessedSimulatorEvents), outcome.Steps)
			if !ok {
				return artifact.Outcome{}, nil, errors.New("evaluation: processed-event counter overflows")
			}
			trial.ProcessedSimulatorEvents = artifact.Uint64(total)
		}
		return outcome, frontier, err
	}
}

func summarize(trials []Trial) ([]Summary, error) {
	result := make([]Summary, 0, len(methods))
	for _, method := range methods {
		var elapsed, events, rates, invocations, terminals, uniqueStates, cacheBytes []float64
		var completed, violations, errorRuns, exhausted, openChoices, prefixPruned uint64
		var statePruned, depthHits, sampledDomains, lookups, hits, misses, budgetSkips, collisions uint64
		truncated := 0
		for _, trial := range trials {
			if trial.Method != method {
				continue
			}
			ns := float64(trial.ElapsedNS)
			eventCount := float64(trial.ProcessedSimulatorEvents)
			elapsed = append(elapsed, ns)
			events = append(events, eventCount)
			rates = append(rates, eventCount/(ns/float64(time.Second)))
			invocations = append(invocations, float64(trial.RunnerInvocations))
			terminals = append(terminals, float64(trial.TerminalExecutions))
			uniqueStates = append(uniqueStates, float64(trial.UniqueStates))
			cacheBytes = append(cacheBytes, float64(trial.CacheBytes))
			var ok bool
			if completed, ok = addUint64(completed, uint64(trial.CompletedRuns)); !ok {
				return nil, errors.New("evaluation: completed-run total overflows")
			}
			if violations, ok = addUint64(violations, uint64(trial.ViolatingRuns)); !ok {
				return nil, errors.New("evaluation: violating-run total overflows")
			}
			if errorRuns, ok = addUint64(errorRuns, uint64(trial.ErrorRuns)); !ok {
				return nil, errors.New("evaluation: error-run total overflows")
			}
			if exhausted, ok = addUint64(exhausted, uint64(trial.BudgetExhaustedRuns)); !ok {
				return nil, errors.New("evaluation: budget-exhausted total overflows")
			}
			if openChoices, ok = addUint64(openChoices, uint64(trial.OpenChoices)); !ok {
				return nil, errors.New("evaluation: open-choice total overflows")
			}
			if prefixPruned, ok = addUint64(prefixPruned, uint64(trial.PrefixPruned)); !ok {
				return nil, errors.New("evaluation: prefix-pruned total overflows")
			}
			if statePruned, ok = addUint64(statePruned, uint64(trial.StatePruned)); !ok {
				return nil, errors.New("evaluation: state-pruned total overflows")
			}
			if depthHits, ok = addUint64(depthHits, uint64(trial.DepthBoundHits)); !ok {
				return nil, errors.New("evaluation: depth-bound total overflows")
			}
			if sampledDomains, ok = addUint64(sampledDomains, uint64(trial.SampledDomains)); !ok {
				return nil, errors.New("evaluation: sampled-domain total overflows")
			}
			if lookups, ok = addUint64(lookups, uint64(trial.CacheLookups)); !ok {
				return nil, errors.New("evaluation: cache-lookup total overflows")
			}
			if hits, ok = addUint64(hits, uint64(trial.CacheHits)); !ok {
				return nil, errors.New("evaluation: cache-hit total overflows")
			}
			if misses, ok = addUint64(misses, uint64(trial.CacheMisses)); !ok {
				return nil, errors.New("evaluation: cache-miss total overflows")
			}
			if budgetSkips, ok = addUint64(budgetSkips, uint64(trial.CacheBudgetSkips)); !ok {
				return nil, errors.New("evaluation: cache-budget-skip total overflows")
			}
			if collisions, ok = addUint64(collisions, uint64(trial.HashCollisions)); !ok {
				return nil, errors.New("evaluation: hash-collision total overflows")
			}
			if trial.RunBudgetTruncated {
				truncated++
			}
		}
		result = append(result, Summary{
			Method: method, Trials: len(elapsed), ElapsedNS95: confidence95(elapsed),
			ProcessedSimulatorEvents95: confidence95(events), EventsPerSecond95: confidence95(rates),
			RunnerInvocationsMean: mean(invocations), TerminalExecutionsMean: mean(terminals),
			CompletedRunsTotal: completed, ViolatingRunsTotal: violations,
			ErrorRunsTotal: errorRuns, BudgetExhaustedRunsTotal: exhausted,
			OpenChoicesTotal: openChoices, PrefixPrunedTotal: prefixPruned,
			StatePrunedTotal: statePruned, DepthBoundHitsTotal: depthHits,
			SampledDomainsTotal: sampledDomains, CacheLookupsTotal: lookups,
			CacheHitsTotal: hits, CacheMissesTotal: misses, CacheBudgetSkipsTotal: budgetSkips,
			UniqueStatesMean: mean(uniqueStates), CacheBytesMean: mean(cacheBytes),
			HashCollisionsTotal: collisions, RunBudgetTruncatedCount: truncated,
		})
	}
	return result, nil
}

func pairedContrasts(trials []Trial, trialCount int) ([]Contrast, error) {
	type pair struct {
		off    Trial
		on     Trial
		hasOff bool
		hasOn  bool
	}
	pairs := make([]pair, trialCount)
	for _, trial := range trials {
		if trial.Index < 0 || trial.Index >= trialCount {
			continue
		}
		switch trial.Method {
		case MethodCacheOff:
			if pairs[trial.Index].hasOff {
				return nil, errors.New("evaluation: duplicate cache-off contrast trial")
			}
			pairs[trial.Index].off = trial
			pairs[trial.Index].hasOff = true
		case MethodCacheOn:
			if pairs[trial.Index].hasOn {
				return nil, errors.New("evaluation: duplicate cache-on contrast trial")
			}
			pairs[trial.Index].on = trial
			pairs[trial.Index].hasOn = true
		}
	}
	elapsedDifferences := make([]float64, 0, trialCount)
	rateDifferences := make([]float64, 0, trialCount)
	for _, pair := range pairs {
		if !pair.hasOff || !pair.hasOn || pair.off.ElapsedNS == 0 || pair.on.ElapsedNS == 0 {
			return nil, errors.New("evaluation: incomplete paired contrast")
		}
		offRate := float64(pair.off.ProcessedSimulatorEvents) / (float64(pair.off.ElapsedNS) / float64(time.Second))
		onRate := float64(pair.on.ProcessedSimulatorEvents) / (float64(pair.on.ElapsedNS) / float64(time.Second))
		elapsedDifferences = append(elapsedDifferences, float64(pair.on.ElapsedNS)-float64(pair.off.ElapsedNS))
		rateDifferences = append(rateDifferences, onRate-offRate)
	}
	return []Contrast{{
		Name:                        "bounded_dfs_frontier_cache_on_minus_bounded_dfs_frontier_cache_off",
		Left:                        MethodCacheOn,
		Right:                       MethodCacheOff,
		ElapsedDifferenceNS95:       confidence95(elapsedDifferences),
		EventsPerSecondDifference95: confidence95(rateDifferences),
	}}, nil
}

func addUint64(left, right uint64) (uint64, bool) {
	if left > ^uint64(0)-right {
		return 0, false
	}
	return left + right, true
}

func multiplyUint64(left, right uint64) (uint64, bool) {
	if left != 0 && right > ^uint64(0)/left {
		return 0, false
	}
	return left * right, true
}

func confidence95(values []float64) Interval {
	average := mean(values)
	if len(values) < 2 {
		return Interval{Mean: average, Lower: average, Upper: average}
	}
	var squares float64
	for _, value := range values {
		delta := value - average
		squares += delta * delta
	}
	standardError := math.Sqrt(squares/float64(len(values)-1)) / math.Sqrt(float64(len(values)))
	margin := tCritical95(len(values)-1) * standardError
	return Interval{Mean: average, Lower: average - margin, Upper: average + margin}
}

func tCritical95(degrees int) float64 {
	values := []float64{0, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042}
	if degrees > 0 && degrees < len(values) {
		return values[degrees]
	}
	return math.NaN()
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var total float64
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

// CurrentEnvironment captures the machine, build, adapter, and schema identity
// needed to interpret performance results.
func CurrentEnvironment() (Environment, error) {
	revision := "unknown"
	modified := false
	settings := make([]BuildSetting, 0)
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if !relevantBuildSetting(setting.Key) {
				continue
			}
			settings = append(settings, BuildSetting{Key: setting.Key, Value: setting.Value})
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				modified = setting.Value == "true"
			}
		}
	}
	slices.SortFunc(settings, func(left, right BuildSetting) int {
		if compared := strings.Compare(left.Key, right.Key); compared != 0 {
			return compared
		}
		return strings.Compare(left.Value, right.Value)
	})

	osVersion := runtime.GOOS
	cpuModel := runtime.GOARCH
	var memoryBytes uint64
	if runtime.GOOS == "linux" {
		var err error
		osVersion, err = readTrimmedFile("/proc/sys/kernel/osrelease", 4096)
		if err != nil {
			return Environment{}, fmt.Errorf("evaluation: read kernel version: %w", err)
		}
		cpuInfo, err := readTrimmedFile("/proc/cpuinfo", 4<<20)
		if err != nil {
			return Environment{}, fmt.Errorf("evaluation: read CPU model: %w", err)
		}
		cpuModel = parseCPUModel(cpuInfo)
		memoryInfo, err := readTrimmedFile("/proc/meminfo", 1<<20)
		if err != nil {
			return Environment{}, fmt.Errorf("evaluation: read memory size: %w", err)
		}
		memoryBytes, err = parseMemoryBytes(memoryInfo)
		if err != nil {
			return Environment{}, err
		}
	}

	environment := Environment{
		GitRevision: revision, GitModified: modified,
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		OSVersion: osVersion, CPUModel: cpuModel, MemoryBytes: artifact.Uint64(memoryBytes),
		LogicalCPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		Adapter:        artifact.Adapter{ID: artifact.ReferenceAdapterID, Version: artifact.ReferenceAdapterCurrent},
		DecisionSchema: decision.SchemaVersion, CheckerSchema: check.SchemaVersion,
		ObservationSchema: artifact.ObservationSchemaCurrent, MessageCodec: artifact.MessageCodecCurrent,
		BuildSettings: settings,
	}
	if err := validateEnvironment(environment); err != nil {
		return Environment{}, err
	}
	return environment, nil
}

func relevantBuildSetting(key string) bool {
	switch key {
	case "-buildmode", "-compiler", "CGO_ENABLED", "GOARCH", "GOOS", "GO386", "GOAMD64", "GOARM", "GOARM64", "GOMIPS", "GOMIPS64", "GOPPC64", "GORISCV64", "GOEXPERIMENT", "vcs", "vcs.revision", "vcs.time", "vcs.modified":
		return true
	default:
		return false
	}
}

func readTrimmedFile(path string, limit int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return "", err
	}
	if int64(len(data)) > limit {
		return "", errors.New("metadata file exceeds size limit")
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", errors.New("metadata file is empty")
	}
	return value, nil
}

func parseCPUModel(cpuInfo string) string {
	for _, preferred := range []string{"model name", "Hardware", "Processor"} {
		for _, line := range strings.Split(cpuInfo, "\n") {
			key, value, found := strings.Cut(line, ":")
			if found && strings.TrimSpace(key) == preferred && strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		}
	}
	return runtime.GOARCH
}

func parseMemoryBytes(memoryInfo string) (uint64, error) {
	for _, line := range strings.Split(memoryInfo, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != "MemTotal:" || fields[2] != "kB" {
			continue
		}
		kilobytes, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("evaluation: parse memory size: %w", err)
		}
		bytes, ok := multiplyUint64(kilobytes, 1024)
		if !ok || bytes == 0 {
			return 0, errors.New("evaluation: invalid memory size")
		}
		return bytes, nil
	}
	return 0, errors.New("evaluation: memory size missing from /proc/meminfo")
}

// ValidateConfig checks the public study configuration without executing it.
func ValidateConfig(config RunConfig) error { return validateConfig(config) }

func validateConfig(config RunConfig) error {
	if config.Trials < len(methods) || config.Trials > MaxTrials || config.Trials%len(methods) != 0 || config.RunnerInvocationBudget <= 0 || config.RunnerInvocationBudget > explore.MaxRunsLimit {
		return errors.New("evaluation: trials must be a multiple of three in [3,30] and the invocation budget must be bounded")
	}
	seedSpan, ok := multiplyUint64(uint64(config.Trials), uint64(config.RunnerInvocationBudget))
	if !ok || uint64(config.SeedBase) > ^uint64(0)-(seedSpan-1) {
		return errors.New("evaluation: seed range overflows")
	}
	if err := artifact.ValidateExperiment(config.Scenario, config.Cluster); err != nil {
		return err
	}
	if config.Search.MaxDepth < 0 || config.Search.MaxDepth > explore.MaxDepthLimit || config.Search.MaxBranchesPerChoice <= 0 || config.Search.MaxBranchesPerChoice > explore.MaxBranchesLimit || config.Search.RangeSamples <= 0 || config.Search.RangeSamples > 3 {
		return explore.ErrInvalidBounds
	}
	if config.Cache.MaxEntries <= 0 || config.Cache.MaxBytes <= 0 {
		return explore.ErrInvalidCacheBounds
	}
	return nil
}

var gitRevisionPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

func validateEnvironment(environment Environment) error {
	stringsToCheck := []string{
		environment.GitRevision, environment.GoVersion, environment.GOOS,
		environment.GOARCH, environment.OSVersion, environment.CPUModel,
	}
	for _, value := range stringsToCheck {
		if value == "" || len(value) > 512 || strings.TrimSpace(value) != value || strings.ContainsFunc(value, unicode.IsControl) {
			return errors.New("evaluation: invalid environment metadata")
		}
	}
	if environment.LogicalCPUs <= 0 || environment.GOMAXPROCS <= 0 || (environment.GOOS == "linux" && environment.MemoryBytes == 0) {
		return errors.New("evaluation: invalid environment resources")
	}
	if environment.Adapter != (artifact.Adapter{ID: artifact.ReferenceAdapterID, Version: artifact.ReferenceAdapterCurrent}) ||
		environment.DecisionSchema != decision.SchemaVersion || environment.CheckerSchema != check.SchemaVersion ||
		environment.ObservationSchema != artifact.ObservationSchemaCurrent || environment.MessageCodec != artifact.MessageCodecCurrent {
		return errors.New("evaluation: invalid adapter or schema identity")
	}
	if len(environment.BuildSettings) > 32 {
		return errors.New("evaluation: too many build settings")
	}
	var previous string
	var revisionSetting string
	var revisionSettingPresent bool
	var modifiedSetting *bool
	for index, setting := range environment.BuildSettings {
		if !relevantBuildSetting(setting.Key) || setting.Key == "" || len(setting.Key) > 64 || len(setting.Value) > 512 || strings.ContainsFunc(setting.Key+setting.Value, unicode.IsControl) || (index > 0 && previous >= setting.Key) {
			return errors.New("evaluation: invalid or unsorted build settings")
		}
		previous = setting.Key
		switch setting.Key {
		case "GOOS":
			if setting.Value != environment.GOOS {
				return errors.New("evaluation: GOOS disagrees with build settings")
			}
		case "GOARCH":
			if setting.Value != environment.GOARCH {
				return errors.New("evaluation: GOARCH disagrees with build settings")
			}
		case "vcs":
			if setting.Value != "git" {
				return errors.New("evaluation: unsupported VCS build setting")
			}
		case "vcs.revision":
			revisionSetting = setting.Value
			revisionSettingPresent = true
		case "vcs.modified":
			if setting.Value != "true" && setting.Value != "false" {
				return errors.New("evaluation: invalid vcs.modified build setting")
			}
			value := setting.Value == "true"
			modifiedSetting = &value
		}
	}
	if revisionSettingPresent && revisionSetting != environment.GitRevision {
		return errors.New("evaluation: Git revision disagrees with build settings")
	}
	if modifiedSetting != nil && *modifiedSetting != environment.GitModified {
		return errors.New("evaluation: Git dirty bit disagrees with build settings")
	}
	return nil
}

// ValidatePublicationEnvironment applies the stricter provenance gate used by
// draft-eval before spending time on a publishable result.
func ValidatePublicationEnvironment(environment Environment) error {
	if err := validateEnvironment(environment); err != nil {
		return err
	}
	if environment.GOOS != "linux" {
		return errors.New("evaluation: publication is currently supported only on Linux")
	}
	if !gitRevisionPattern.MatchString(environment.GitRevision) {
		return errors.New("evaluation: publication requires an exact 40-character lowercase Git revision")
	}
	if environment.GitModified {
		return errors.New("evaluation: publication requires a clean Git build")
	}
	if environment.MemoryBytes == 0 || environment.CPUModel == "unknown" || environment.OSVersion == "unknown" {
		return errors.New("evaluation: publication requires complete machine metadata")
	}
	var hasVCS, hasRevision, hasModified bool
	for _, setting := range environment.BuildSettings {
		switch setting.Key {
		case "vcs":
			hasVCS = true
		case "vcs.revision":
			hasRevision = true
		case "vcs.modified":
			hasModified = true
		}
	}
	if !hasVCS || !hasRevision || !hasModified {
		return errors.New("evaluation: publication requires embedded VCS build settings")
	}
	return nil
}

// Validate checks schema, closed method coverage, raw trial shape, summaries,
// finite statistics, and the embedded experiment bounds.
func (result Result) Validate() error {
	if result.Schema != SchemaVersion {
		return errors.New("evaluation: invalid schema")
	}
	if err := validateEnvironment(result.Environment); err != nil {
		return err
	}
	if err := validateConfig(result.Config); err != nil {
		return err
	}
	if len(result.Trials) != result.Config.Trials*len(methods) || len(result.Summaries) != len(methods) || len(result.Contrasts) != 1 {
		return errors.New("evaluation: incomplete result")
	}
	budget := uint64(result.Config.RunnerInvocationBudget)
	maxEvents, ok := multiplyUint64(budget, result.Config.Scenario.MaxSteps)
	if !ok {
		return errors.New("evaluation: event bound overflows")
	}
	counts := make(map[Method]int, len(methods))
	seen := make(map[[2]int]struct{}, len(result.Trials))
	for _, trial := range result.Trials {
		key := [2]int{trial.Index, trial.Order}
		if !slices.Contains(methods, trial.Method) || trial.Index < 0 || trial.Index >= result.Config.Trials || trial.Order < 0 || trial.Order >= len(methods) || trial.Method != methods[(trial.Index+trial.Order)%len(methods)] || trial.ElapsedNS == 0 || trial.RunnerInvocations == 0 || uint64(trial.RunnerInvocations) > budget || uint64(trial.ProcessedSimulatorEvents) > maxEvents {
			return errors.New("evaluation: invalid raw trial")
		}
		boundedCounters := []artifact.Uint64{
			trial.TerminalExecutions, trial.CompletedRuns, trial.ViolatingRuns,
			trial.ErrorRuns, trial.BudgetExhaustedRuns, trial.OpenChoices,
			trial.PrefixPruned, trial.StatePruned, trial.CacheLookups,
			trial.CacheHits, trial.CacheMisses, trial.CacheBudgetSkips,
			trial.SampledDomains, trial.DepthBoundHits, trial.UniqueStates,
		}
		for _, counter := range boundedCounters {
			if uint64(counter) > budget {
				return errors.New("evaluation: trial counter exceeds invocation budget")
			}
		}
		statusTotal, statusOK := addUint64(uint64(trial.CompletedRuns), uint64(trial.ViolatingRuns))
		if statusOK {
			statusTotal, statusOK = addUint64(statusTotal, uint64(trial.ErrorRuns))
		}
		if statusOK {
			statusTotal, statusOK = addUint64(statusTotal, uint64(trial.BudgetExhaustedRuns))
		}
		if !statusOK || statusTotal != uint64(trial.TerminalExecutions) {
			return errors.New("evaluation: terminal status accounting mismatch")
		}
		switch trial.Method {
		case MethodRandom:
			if uint64(trial.RunnerInvocations) != budget || trial.TerminalExecutions != trial.RunnerInvocations || trial.OpenChoices != 0 || trial.PrefixPruned != 0 || trial.StatePruned != 0 || trial.CacheLookups != 0 || trial.CacheHits != 0 || trial.CacheMisses != 0 || trial.CacheBytes != 0 || trial.CacheBudgetSkips != 0 || trial.HashCollisions != 0 || trial.SampledDomains != 0 || trial.DepthBoundHits != 0 || trial.UniqueStates != 0 || trial.RunBudgetTruncated {
				return errors.New("evaluation: invalid random trial fields")
			}
		case MethodCacheOff, MethodCacheOn:
			dfsTotal, dfsOK := addUint64(uint64(trial.TerminalExecutions), uint64(trial.OpenChoices))
			if dfsOK {
				dfsTotal, dfsOK = addUint64(dfsTotal, uint64(trial.PrefixPruned))
			}
			depthTerminal, depthOK := addUint64(uint64(trial.TerminalExecutions), uint64(trial.PrefixPruned))
			if !dfsOK || dfsTotal != uint64(trial.RunnerInvocations) || !depthOK || uint64(trial.DepthBoundHits) > depthTerminal || trial.StatePruned > trial.OpenChoices || trial.SampledDomains > trial.OpenChoices-trial.StatePruned || (trial.RunBudgetTruncated && uint64(trial.RunnerInvocations) != budget) {
				return errors.New("evaluation: invalid DFS trial accounting")
			}
		}
		switch trial.Method {
		case MethodCacheOff:
			if trial.StatePruned != 0 || trial.CacheLookups != 0 || trial.CacheHits != 0 || trial.CacheMisses != 0 || trial.CacheBytes != 0 || trial.CacheBudgetSkips != 0 || trial.HashCollisions != 0 || trial.UniqueStates != 0 {
				return errors.New("evaluation: invalid cache-off trial fields")
			}
		case MethodCacheOn:
			lookupTotal, lookupOK := addUint64(uint64(trial.CacheHits), uint64(trial.CacheMisses))
			missTotal, missOK := addUint64(uint64(trial.UniqueStates), uint64(trial.CacheBudgetSkips))
			lookupCount := uint64(trial.CacheLookups)
			priorLookups := uint64(0)
			if lookupCount > 0 {
				priorLookups = lookupCount - 1
			}
			collisionProduct, collisionOK := multiplyUint64(lookupCount, priorLookups)
			minimumBytes, bytesOK := multiplyUint64(uint64(trial.UniqueStates), minimumCacheIdentityBytes)
			if !lookupOK || lookupTotal != uint64(trial.CacheLookups) || trial.CacheLookups != trial.OpenChoices || trial.StatePruned != trial.CacheHits || !missOK || missTotal != uint64(trial.CacheMisses) || uint64(trial.UniqueStates) > uint64(result.Config.Cache.MaxEntries) || uint64(trial.CacheBytes) > uint64(result.Config.Cache.MaxBytes) || (trial.UniqueStates == 0) != (trial.CacheBytes == 0) || !bytesOK || uint64(trial.CacheBytes) < minimumBytes || !collisionOK || uint64(trial.HashCollisions) > collisionProduct/2 {
				return errors.New("evaluation: invalid cache-on trial fields")
			}
		}
		if _, exists := seen[key]; exists {
			return errors.New("evaluation: duplicate raw trial")
		}
		seen[key] = struct{}{}
		counts[trial.Method]++
	}
	expectedSummaries, err := summarize(result.Trials)
	if err != nil {
		return err
	}
	for index, summary := range result.Summaries {
		if summary.Method != methods[index] || summary.Trials != result.Config.Trials || counts[summary.Method] != result.Config.Trials || !validInterval(summary.ElapsedNS95) || !validInterval(summary.ProcessedSimulatorEvents95) || !validInterval(summary.EventsPerSecond95) || !finite(summary.RunnerInvocationsMean) || !finite(summary.TerminalExecutionsMean) || !finite(summary.UniqueStatesMean) || !finite(summary.CacheBytesMean) || !reflect.DeepEqual(summary, expectedSummaries[index]) {
			return errors.New("evaluation: invalid summary")
		}
	}
	expectedContrasts, err := pairedContrasts(result.Trials, result.Config.Trials)
	if err != nil {
		return err
	}
	for index, contrast := range result.Contrasts {
		if !validInterval(contrast.ElapsedDifferenceNS95) || !validInterval(contrast.EventsPerSecondDifference95) || !reflect.DeepEqual(contrast, expectedContrasts[index]) {
			return errors.New("evaluation: invalid paired contrast")
		}
	}
	return nil
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

func validInterval(interval Interval) bool {
	return !math.IsNaN(interval.Mean) && !math.IsInf(interval.Mean, 0) && !math.IsNaN(interval.Lower) && !math.IsInf(interval.Lower, 0) && !math.IsNaN(interval.Upper) && !math.IsInf(interval.Upper, 0) && interval.Lower <= interval.Mean && interval.Mean <= interval.Upper
}

func cloneConfig(config RunConfig) RunConfig {
	actions := config.Scenario.Actions
	config.Scenario.Actions = make([]artifact.Action, len(actions))
	for index, action := range actions {
		action.Data = slices.Clone(action.Data)
		action.Voters = slices.Clone(action.Voters)
		action.Learners = slices.Clone(action.Learners)
		groups := action.Groups
		action.Groups = make([][]raft.NodeID, len(groups))
		for group := range groups {
			action.Groups[group] = slices.Clone(groups[group])
		}
		config.Scenario.Actions[index] = action
	}
	config.Cluster.Members = slices.Clone(config.Cluster.Members)
	config.Cluster.Voters = slices.Clone(config.Cluster.Voters)
	config.Cluster.Learners = slices.Clone(config.Cluster.Learners)
	return config
}

func Encode(writer io.Writer, result Result) error {
	if writer == nil {
		return errors.New("evaluation: nil writer")
	}
	if err := result.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > DefaultMaxResultBytes {
		return errors.New("evaluation: result exceeds size limit")
	}
	written, err := writer.Write(data)
	if err != nil {
		return err
	}
	if written != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func Decode(reader io.Reader) (Result, error) {
	if reader == nil {
		return Result{}, errors.New("evaluation: nil reader")
	}
	data, err := io.ReadAll(io.LimitReader(reader, DefaultMaxResultBytes+1))
	if err != nil {
		return Result{}, err
	}
	if len(data) > DefaultMaxResultBytes {
		return Result{}, errors.New("evaluation: result exceeds size limit")
	}
	if err := strictjson.RejectDuplicateNames(data); err != nil {
		return Result{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var result Result
	if err := decoder.Decode(&result); err != nil {
		return Result{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Result{}, errors.New("evaluation: trailing JSON value")
	}
	if err := result.Validate(); err != nil {
		return Result{}, err
	}
	return result, nil
}

// DecodePublication decodes a structurally valid result and additionally
// requires the clean, exact VCS provenance used for published artifacts.
func DecodePublication(reader io.Reader) (Result, error) {
	result, err := Decode(reader)
	if err != nil {
		return Result{}, err
	}
	if err := ValidatePublicationEnvironment(result.Environment); err != nil {
		return Result{}, err
	}
	return result, nil
}
