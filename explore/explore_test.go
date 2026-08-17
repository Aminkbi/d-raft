package explore

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
)

func TestDFSFindsBoundedSemanticFailure(t *testing.T) {
	t.Parallel()

	runner := func(decider decision.Decider) (artifact.Outcome, error) {
		choice := func(id string) (decision.Selection, error) {
			return decider.Choose(decision.Choice{ID: id, Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "deliver", Weight: 1}, {ID: "drop", Weight: 1}}})
		}
		first, err := choice("first")
		if err != nil {
			return artifact.Outcome{}, err
		}
		second, err := choice("second")
		if err != nil {
			return artifact.Outcome{}, err
		}
		outcome := artifact.Outcome{Status: artifact.OutcomeCompleted}
		if first.Option == "drop" && second.Option == "drop" {
			outcome.Status = artifact.OutcomeViolation
			outcome.Violations = []check.Violation{{Fingerprint: "target"}}
		}
		return outcome, nil
	}
	result, err := DFS(runner, Bounds{MaxRuns: 20, MaxDepth: 2, MaxBranchesPerChoice: 2, RangeSamples: 3, StopOnViolation: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.FirstViolation == nil || result.FirstViolation.Outcome.Violations[0].Fingerprint != "target" || len(result.FirstViolation.Tape.Entries) != 2 {
		t.Fatalf("result = %+v", result)
	}
}

func TestDFSFillsSuffixAtDepthBound(t *testing.T) {
	t.Parallel()

	runner := func(decider decision.Decider) (artifact.Outcome, error) {
		for _, id := range []string{"a", "b", "c"} {
			if _, err := decider.Choose(decision.Choice{ID: id, Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "deliver", Weight: 1}, {ID: "drop", Weight: 1}}}); err != nil {
				return artifact.Outcome{}, err
			}
		}
		return artifact.Outcome{Status: artifact.OutcomeCompleted}, nil
	}
	result, err := DFS(runner, Bounds{MaxRuns: 20, MaxDepth: 1, MaxBranchesPerChoice: 2, RangeSamples: 3})
	if err != nil {
		t.Fatal(err)
	}
	if result.Completed != 2 || result.OpenChoices != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestDFSSeparatesOutcomeStatusFromTargetMatching(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		outcome   artifact.Outcome
		completed int
		violating int
		errors    int
		exhausted int
		matching  int
	}{
		{name: "completed", outcome: artifact.Outcome{Status: artifact.OutcomeCompleted}, completed: 1},
		{name: "violation", outcome: artifact.Outcome{Status: artifact.OutcomeViolation, Violations: []check.Violation{{Fingerprint: "other"}}}, violating: 1},
		{name: "error", outcome: artifact.Outcome{Status: artifact.OutcomeError}, errors: 1},
		{name: "budget exhausted", outcome: artifact.Outcome{Status: artifact.OutcomeBudgetExhausted}, exhausted: 1},
		{name: "matching violation", outcome: artifact.Outcome{Status: artifact.OutcomeViolation, Violations: []check.Violation{{Fingerprint: "target"}}}, violating: 1, matching: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := DFS(
				func(decision.Decider) (artifact.Outcome, error) { return test.outcome, nil },
				Bounds{MaxRuns: 1, MaxDepth: 0, MaxBranchesPerChoice: 1, RangeSamples: 1, TargetFingerprint: "target"},
			)
			if err != nil {
				t.Fatal(err)
			}
			if result.Completed != 1 || result.CompletedRuns != test.completed || result.OutcomeViolationRuns != test.violating || result.ErrorRuns != test.errors || result.BudgetExhaustedRuns != test.exhausted || result.ViolatingRuns != test.matching {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestDFSRejectsUnknownOutcomeStatus(t *testing.T) {
	t.Parallel()

	_, err := DFS(
		func(decision.Decider) (artifact.Outcome, error) { return artifact.Outcome{Status: "future"}, nil },
		Bounds{MaxRuns: 1, MaxDepth: 0, MaxBranchesPerChoice: 1, RangeSamples: 1},
	)
	if !errors.Is(err, ErrInvalidOutcomeStatus) {
		t.Fatalf("error = %v", err)
	}
}

func TestStateCacheCopiesBytesAndDefendsAgainstHashCollisions(t *testing.T) {
	t.Parallel()

	cache := newStateCache(CacheBounds{MaxEntries: 10, MaxBytes: 1_000}, constantStateHash)
	first := []byte("first")
	if cache.seen(first) {
		t.Fatal("new state was reported as seen")
	}
	first[0] = 'x'
	if !cache.seen([]byte("first")) {
		t.Fatal("cache retained caller-owned bytes or missed dominated state")
	}
	if cache.seen([]byte("second")) {
		t.Fatal("hash collision was treated as state equality")
	}
	if cache.lookups != 3 || cache.hits != 1 || cache.misses != 2 || cache.collisions != 1 || cache.entries != 2 {
		t.Fatalf("cache metrics = %+v", cache)
	}
}

func TestCacheIdentityRequiresExactRemainingDepth(t *testing.T) {
	t.Parallel()

	choice := decision.Choice{ID: "same", Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "deliver", Weight: 1}}}
	shallow, err := cacheIdentity([]byte("state"), choice, 2)
	if err != nil {
		t.Fatal(err)
	}
	deep, err := cacheIdentity([]byte("state"), choice, 1)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(shallow, deep) {
		t.Fatal("remaining depth was omitted")
	}
	cache := newStateCache(CacheBounds{MaxEntries: 10, MaxBytes: 1_000}, constantStateHash)
	if cache.seen(shallow) || cache.seen(deep) || !cache.seen(shallow) {
		t.Fatalf("exact-depth cache behavior = %+v", cache)
	}
}

func TestDFSWithCacheMergesDiamondState(t *testing.T) {
	t.Parallel()

	runner := diamondRunner()
	bounds := Bounds{MaxRuns: 20, MaxDepth: 2, MaxBranchesPerChoice: 2, RangeSamples: 3}
	result, err := DFSWithCache(runner, bounds, CacheBounds{MaxEntries: 10, MaxBytes: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	if result.Runs != 5 || result.OpenChoices != 3 || result.Completed != 2 || result.StatePruned != 1 || result.CacheLookups != 3 || result.CacheHits != 1 || result.CacheMisses != 2 || result.UniqueStates != 2 || result.HashCollisions != 0 {
		t.Fatalf("result = %+v", result)
	}

	for repetition := 0; repetition < 20; repetition++ {
		again, err := DFSWithCache(runner, bounds, CacheBounds{MaxEntries: 10, MaxBytes: 10_000})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result, again) {
			t.Fatalf("nondeterministic cached result: first=%+v again=%+v", result, again)
		}
	}
}

func TestDFSWithCacheCollisionCannotHideViolation(t *testing.T) {
	t.Parallel()

	routeChoice := decision.Choice{ID: "route", Kind: decision.FaultAction, Options: []decision.Option{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}}}
	faultChoice := decision.Choice{ID: "fault", Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "deliver", Weight: 1}, {ID: "drop", Weight: 1}}}
	runner := internalRunner(func(decider decision.Decider) (artifact.Outcome, []byte, error) {
		route, err := decider.Choose(routeChoice)
		if err != nil {
			return artifact.Outcome{}, []byte("root"), err
		}
		fault, err := decider.Choose(faultChoice)
		if err != nil {
			return artifact.Outcome{}, []byte("state-" + route.Option), err
		}
		outcome := artifact.Outcome{Status: artifact.OutcomeCompleted}
		if route.Option == "b" && fault.Option == "drop" {
			outcome.Status = artifact.OutcomeViolation
			outcome.Violations = []check.Violation{{Fingerprint: "collision-target"}}
		}
		return outcome, nil, nil
	})
	bounds := Bounds{MaxRuns: 20, MaxDepth: 2, MaxBranchesPerChoice: 2, RangeSamples: 3}
	result, err := dfs(runner, bounds, newStateCache(CacheBounds{MaxEntries: 10, MaxBytes: 10_000}, constantStateHash))
	if err != nil {
		t.Fatal(err)
	}
	if result.FirstViolation == nil || result.FirstViolation.Outcome.Violations[0].Fingerprint != "collision-target" || result.StatePruned != 0 || result.HashCollisions == 0 || result.UniqueStates != 3 {
		t.Fatalf("result = %+v", result)
	}
}

func TestDFSWithCacheRequiresStateAtOpenChoice(t *testing.T) {
	t.Parallel()

	runner := func(decider decision.Decider) (artifact.Outcome, []byte, error) {
		_, err := decider.Choose(decision.Choice{ID: "choice", Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "deliver", Weight: 1}}})
		return artifact.Outcome{}, nil, err
	}
	_, err := DFSWithCache(runner, Bounds{MaxRuns: 2, MaxDepth: 1, MaxBranchesPerChoice: 1, RangeSamples: 1}, CacheBounds{MaxEntries: 1, MaxBytes: 100})
	if !errors.Is(err, ErrMissingCanonicalState) {
		t.Fatalf("error = %v", err)
	}
}

func TestDFSWithCacheBudgetExhaustionSafelyBypasses(t *testing.T) {
	t.Parallel()

	result, err := DFSWithCache(
		diamondRunner(),
		Bounds{MaxRuns: 20, MaxDepth: 2, MaxBranchesPerChoice: 2, RangeSamples: 3},
		CacheBounds{MaxEntries: 1, MaxBytes: 10_000},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Runs != 7 || result.Completed != 4 || result.StatePruned != 0 || result.UniqueStates != 1 || result.CacheBudgetSkips != 2 {
		t.Fatalf("result = %+v", result)
	}
}

func TestDFSWithCacheDoesNotUseDepthDominanceAcrossSeededSuffix(t *testing.T) {
	t.Parallel()

	route := decision.Choice{ID: "route", Kind: decision.FaultAction, Options: []decision.Option{{ID: "shallow", Weight: 1}, {ID: "deep", Weight: 1}}}
	detour := decision.Choice{ID: "detour", Kind: decision.ClientAction, Options: []decision.Option{{ID: "continue", Weight: 1}}}
	join := decision.Choice{ID: "join", Kind: decision.ClientAction, Options: []decision.Option{{ID: "continue", Weight: 1}}}
	hazard := decision.Choice{ID: "hazard", Kind: decision.FaultAction, Options: []decision.Option{{ID: "safe-1", Weight: 1}, {ID: "safe-2", Weight: 1}, {ID: "violate", Weight: 1}}}
	runner := func(decider decision.Decider) (artifact.Outcome, []byte, error) {
		selectedRoute, err := decider.Choose(route)
		if err != nil {
			return artifact.Outcome{}, []byte("root"), err
		}
		if selectedRoute.Option == "deep" {
			if _, err := decider.Choose(detour); err != nil {
				return artifact.Outcome{}, []byte("detour"), err
			}
		}
		if _, err := decider.Choose(join); err != nil {
			return artifact.Outcome{}, []byte("joined"), err
		}
		selectedHazard, err := decider.Choose(hazard)
		if err != nil {
			return artifact.Outcome{}, []byte("hazard"), err
		}
		outcome := artifact.Outcome{Status: artifact.OutcomeCompleted}
		if selectedHazard.Option == "violate" {
			outcome.Status = artifact.OutcomeViolation
			outcome.Violations = []check.Violation{{Fingerprint: "depth-target"}}
		}
		return outcome, nil, nil
	}
	bounds := Bounds{MaxRuns: 50, MaxDepth: 3, MaxBranchesPerChoice: 2, RangeSamples: 3, FallbackSeed: 0}
	plain, err := dfs(internalRunner(runner), bounds, nil)
	if err != nil {
		t.Fatal(err)
	}
	cached, err := DFSWithCache(runner, bounds, CacheBounds{MaxEntries: 50, MaxBytes: 50_000})
	if err != nil {
		t.Fatal(err)
	}
	if plain.ViolatingRuns != 1 || cached.ViolatingRuns != plain.ViolatingRuns || cached.StatePruned != 0 {
		t.Fatalf("plain=%+v cached=%+v", plain, cached)
	}
}

func TestCacheIdentityIncludesOpenChoice(t *testing.T) {
	t.Parallel()

	options := []decision.Option{{ID: "deliver", Weight: 1}}
	left, err := cacheIdentity([]byte("same-state"), decision.Choice{ID: "left", Kind: decision.NetworkLoss, Options: options}, 1)
	if err != nil {
		t.Fatal(err)
	}
	right, err := cacheIdentity([]byte("same-state"), decision.Choice{ID: "right", Kind: decision.NetworkLoss, Options: options}, 1)
	if err != nil {
		t.Fatal(err)
	}
	cache := newStateCache(CacheBounds{MaxEntries: 2, MaxBytes: 1_000}, constantStateHash)
	if cache.seen(left) || cache.seen(right) || cache.entries != 2 || cache.collisions != 1 {
		t.Fatalf("different open choices were merged: %+v", cache)
	}
}

func TestCacheIdentityIncludesExactDomainAndContext(t *testing.T) {
	t.Parallel()

	base := decision.Choice{ID: "choice", Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "deliver", Weight: 1}}, Context: []byte(`{"node":"a"}`)}
	differentDomain := base
	differentDomain.Options = []decision.Option{{ID: "deliver", Weight: 2}}
	differentContext := base
	differentContext.Context = []byte(`{"node":"b"}`)
	identities := make([][]byte, 0, 3)
	for _, choice := range []decision.Choice{base, differentDomain, differentContext} {
		identity, err := cacheIdentity([]byte("state"), choice, 1)
		if err != nil {
			t.Fatal(err)
		}
		identities = append(identities, identity)
	}
	if reflect.DeepEqual(identities[0], identities[1]) || reflect.DeepEqual(identities[0], identities[2]) {
		t.Fatal("exact domain or context was omitted from cache identity")
	}
}

func diamondRunner() StatefulRunner {
	routeChoice := decision.Choice{ID: "route", Kind: decision.FaultAction, Options: []decision.Option{{ID: "left", Weight: 1}, {ID: "right", Weight: 1}}}
	joinedChoice := decision.Choice{ID: "joined", Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "deliver", Weight: 1}, {ID: "drop", Weight: 1}}}
	return func(decider decision.Decider) (artifact.Outcome, []byte, error) {
		if _, err := decider.Choose(routeChoice); err != nil {
			return artifact.Outcome{}, []byte("root"), err
		}
		if _, err := decider.Choose(joinedChoice); err != nil {
			return artifact.Outcome{}, []byte("joined"), err
		}
		return artifact.Outcome{Status: artifact.OutcomeCompleted}, nil, nil
	}
}

func constantStateHash([]byte) [32]byte { return [32]byte{1} }
