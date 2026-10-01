package experiment

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/explore"
	"github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/raftsim"
)

func TestReferenceCachePreservesEnumeratedOutcomes(t *testing.T) {
	t.Parallel()

	single := raftsim.DefaultConfig("a")
	single.ElectionTimeoutMin = 10 * time.Millisecond
	// Two nanoseconds are fully enumerated, not sampled from a larger range.
	single.ElectionTimeoutMax = single.ElectionTimeoutMin + time.Nanosecond
	single.HeartbeatInterval = 9 * time.Millisecond
	single.StorageLatency = time.Millisecond
	membership := single
	membership.Members = []raft.NodeID{"a", "b"}
	membership.Voters = []raft.NodeID{"a"}
	membership.Learners = []raft.NodeID{"b"}
	membership.ElectionTimeoutMax = membership.ElectionTimeoutMin
	membership.Network.MinLatency = 100 * time.Microsecond
	membership.Network.MaxLatency = membership.Network.MinLatency

	tests := []struct {
		name      string
		config    raftsim.Config
		end       time.Duration
		actions   []artifact.Action
		wantError bool
	}{
		{
			name: "election-at-proposal-boundary", config: single, end: 15 * time.Millisecond,
			actions: []artifact.Action{
				{AtNS: int64(10 * time.Millisecond), Kind: artifact.ActionPropose, Node: "a", Data: []byte("x=1")},
			},
			wantError: true,
		},
		{
			name: "crash-before-write", config: single, end: 26 * time.Millisecond,
			actions: []artifact.Action{
				{AtNS: int64(11 * time.Millisecond), Kind: artifact.ActionCrash, Node: "a"},
				{AtNS: int64(12 * time.Millisecond), Kind: artifact.ActionRestart, Node: "a"},
			},
		},
		{
			name: "crash-after-write-before-ack", config: single, end: 26 * time.Millisecond,
			actions: []artifact.Action{
				{AtNS: int64(5 * time.Millisecond), Kind: artifact.ActionCrashAfterNextPersist, Node: "a"},
				{AtNS: int64(12 * time.Millisecond), Kind: artifact.ActionRestart, Node: "a"},
			},
		},
		{
			name: "snapshot-recovery", config: single, end: 38 * time.Millisecond,
			actions: []artifact.Action{
				{AtNS: int64(15 * time.Millisecond), Kind: artifact.ActionPropose, Node: "a", Data: []byte("x=1")},
				{AtNS: int64(18 * time.Millisecond), Kind: artifact.ActionSnapshot, Node: "a", Data: []byte("checkpoint")},
				{AtNS: int64(21 * time.Millisecond), Kind: artifact.ActionCrash, Node: "a"},
				{AtNS: int64(22 * time.Millisecond), Kind: artifact.ActionRestart, Node: "a"},
			},
		},
		{
			name: "equal-time-crash-restart", config: single, end: 26 * time.Millisecond,
			actions: []artifact.Action{
				{AtNS: int64(13 * time.Millisecond), Kind: artifact.ActionCrash, Node: "a"},
				{AtNS: int64(13 * time.Millisecond), Kind: artifact.ActionRestart, Node: "a"},
			},
		},
		{
			name: "joint-membership-with-learner", config: membership, end: 26 * time.Millisecond,
			actions: []artifact.Action{
				{AtNS: int64(16 * time.Millisecond), Kind: artifact.ActionBeginMembership, Node: "a", Voters: []raft.NodeID{"a", "b"}},
				{AtNS: int64(20 * time.Millisecond), Kind: artifact.ActionFinalizeMembership, Node: "a"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			scenario := artifact.Scenario{ID: test.name, Version: "1", DurationNS: int64(test.end), MaxSteps: 200, Actions: test.actions}
			configuration := artifact.ConfigurationFrom(test.config)
			bounds := explore.Bounds{MaxRuns: 512, MaxDepth: 64, MaxBranchesPerChoice: 2, RangeSamples: 2}

			runSearch := func(cache *explore.CacheBounds) (explore.Result, []string) {
				t.Helper()
				terminal := make(map[string]struct{})
				record := func(outcome artifact.Outcome, err error) {
					t.Helper()
					if err != nil {
						return // An open choice is a frontier, not a terminal outcome.
					}
					if outcome.Status != artifact.OutcomeCompleted && !(test.wantError && outcome.Status == artifact.OutcomeError) {
						t.Fatalf("unexpected terminal outcome: %+v", outcome)
					}
					if outcome.Status == artifact.OutcomeError && outcome.Error != raft.ErrNotLeader.Error() {
						t.Fatalf("unexpected execution error: %s", outcome.Error)
					}
					encoded, encodeErr := json.Marshal(outcome)
					if encodeErr != nil {
						t.Fatal(encodeErr)
					}
					terminal[string(encoded)] = struct{}{}
				}
				var result explore.Result
				var err error
				if cache == nil {
					result, err = explore.DFS(func(decider decision.Decider) (artifact.Outcome, error) {
						outcome, runErr := Execute(scenario, configuration, decider)
						record(outcome, runErr)
						return outcome, runErr
					}, bounds)
				} else {
					result, err = explore.DFSWithCache(func(decider decision.Decider) (artifact.Outcome, []byte, error) {
						outcome, state, runErr := ExecuteWithFrontier(scenario, configuration, decider)
						record(outcome, runErr)
						return outcome, state, runErr
					}, bounds, *cache)
				}
				if err != nil {
					t.Fatal(err)
				}
				if result.Truncated || result.SampledDomains != 0 || result.DepthBoundHits != 0 || result.BudgetExhaustedRuns != 0 || len(terminal) == 0 {
					t.Fatalf("search was not fully enumerated: %+v", result)
				}
				return result, slices.Sorted(maps.Keys(terminal))
			}

			plain, want := runSearch(nil)
			if plain.CompletedRuns == 0 || (test.wantError && plain.ErrorRuns == 0) {
				t.Fatalf("fixture did not exercise its expected outcomes: %+v", plain)
			}
			if test.config.ElectionTimeoutMin != test.config.ElectionTimeoutMax && plain.Completed < 2 {
				t.Fatalf("fixture did not enumerate both election timings: %+v", plain)
			}
			for _, cache := range []explore.CacheBounds{
				{MaxEntries: 512, MaxBytes: 8 << 20},
				{MaxEntries: 1, MaxBytes: 1}, // Every admission is bypassed.
			} {
				cached, got := runSearch(&cache)
				if !slices.Equal(want, got) {
					t.Fatalf("cache %+v changed terminal outcomes:\nplain: %v\ncached: %v", cache, want, got)
				}
				if cached.CacheLookups == 0 || (cache.MaxBytes == 1 && cached.CacheBudgetSkips == 0) {
					t.Fatalf("cache path was not exercised: %+v", cached)
				}
				if cache.MaxBytes > 1 && cached.UniqueStates == 0 {
					t.Fatalf("cache retained no reference frontiers: %+v", cached)
				}
			}
		})
	}
}
