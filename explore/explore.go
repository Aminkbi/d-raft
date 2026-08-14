// Package explore performs bounded depth-first exploration by clean prefix
// reruns over semantic choices.
package explore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
)

var (
	ErrInvalidBounds         = errors.New("explore: invalid bounds")
	ErrInvalidCacheBounds    = errors.New("explore: invalid cache bounds")
	ErrMissingCanonicalState = errors.New("explore: cached runner returned no canonical state at an open choice")
)

const (
	MaxRunsLimit     = 100_000
	MaxDepthLimit    = 64
	MaxBranchesLimit = 64
)

// Runner executes one clean run with the supplied decider.
type Runner func(decision.Decider) (artifact.Outcome, error)

// StatefulRunner additionally returns a canonical, Markov-complete state at
// an open choice. The state is ignored for completed and failed executions.
// Callers retain ownership of the returned bytes.
type StatefulRunner func(decision.Decider) (artifact.Outcome, []byte, error)

// CacheBounds limits retained canonical states. A full cache safely stops
// storing new states; it never evicts or prunes from a hash match alone.
type CacheBounds struct {
	MaxEntries int
	MaxBytes   int
}

type Bounds struct {
	MaxRuns              int
	MaxDepth             int
	MaxBranchesPerChoice int
	RangeSamples         int
	FallbackSeed         uint64
	StopOnViolation      bool
	TargetFingerprint    string
}

type Execution struct {
	Tape    decision.Tape
	Outcome artifact.Outcome
}

type Result struct {
	Runs             int
	OpenChoices      int
	Completed        int
	PrunedPrefixes   int
	DepthBoundHits   int
	SampledDomains   int
	ViolatingRuns    int
	StatePruned      int
	CacheLookups     int
	CacheHits        int
	CacheMisses      int
	HashCollisions   int
	UniqueStates     int
	CacheBytes       int
	CacheBudgetSkips int
	Truncated        bool
	FirstViolation   *Execution
}

type stateHasher func([]byte) [sha256.Size]byte

type cachedState struct {
	canonical []byte
}

type stateCache struct {
	buckets     map[[sha256.Size]byte][]cachedState
	bounds      CacheBounds
	hash        stateHasher
	lookups     int
	hits        int
	misses      int
	collisions  int
	entries     int
	bytes       int
	budgetSkips int
}

func newStateCache(bounds CacheBounds, hash stateHasher) *stateCache {
	return &stateCache{buckets: make(map[[sha256.Size]byte][]cachedState), bounds: bounds, hash: hash}
}

// seen reports whether this exact state, open choice, and remaining-depth
// identity was already explored.
func (c *stateCache) seen(canonical []byte) bool {
	c.lookups++
	digest := c.hash(canonical)
	bucket := c.buckets[digest]
	for index := range bucket {
		if !bytes.Equal(bucket[index].canonical, canonical) {
			c.collisions++
			continue
		}
		c.hits++
		return true
	}

	c.misses++
	if c.entries >= c.bounds.MaxEntries || len(canonical) > c.bounds.MaxBytes-c.bytes {
		c.budgetSkips++
		return false
	}
	copy := append(make([]byte, 0, len(canonical)), canonical...)
	c.buckets[digest] = append(bucket, cachedState{canonical: copy})
	c.entries++
	c.bytes += len(copy)
	return false
}

type internalRunner func(decision.Decider) (artifact.Outcome, []byte, error)

// DFS explores choice prefixes in deterministic domain order. At MaxDepth it
// completes the suffix with a seeded decider so every explored leaf can yield
// a concrete run artifact.
func DFS(run Runner, bounds Bounds) (Result, error) {
	if run == nil || !validBounds(bounds) {
		return Result{}, ErrInvalidBounds
	}
	return dfs(func(decider decision.Decider) (artifact.Outcome, []byte, error) {
		outcome, err := run(decider)
		return outcome, nil, err
	}, bounds, nil)
}

// DFSWithCache performs DFS with an opt-in, collision-safe canonical-state
// cache. Hash equality only selects a bucket; pruning requires full byte
// equality, including the exact remaining systematic depth.
func DFSWithCache(run StatefulRunner, bounds Bounds, cacheBounds CacheBounds) (Result, error) {
	if run == nil || !validBounds(bounds) {
		return Result{}, ErrInvalidBounds
	}
	if cacheBounds.MaxEntries <= 0 || cacheBounds.MaxBytes <= 0 {
		return Result{}, ErrInvalidCacheBounds
	}
	return dfs(internalRunner(run), bounds, newStateCache(cacheBounds, sha256.Sum256))
}

func validBounds(bounds Bounds) bool {
	return bounds.MaxRuns > 0 && bounds.MaxRuns <= MaxRunsLimit && bounds.MaxDepth >= 0 && bounds.MaxDepth <= MaxDepthLimit && bounds.MaxBranchesPerChoice > 0 && bounds.MaxBranchesPerChoice <= MaxBranchesLimit && bounds.RangeSamples > 0 && bounds.RangeSamples <= 3
}

func dfs(run internalRunner, bounds Bounds, cache *stateCache) (result Result, err error) {
	defer func() {
		if cache == nil {
			return
		}
		result.CacheLookups = cache.lookups
		result.CacheHits = cache.hits
		result.CacheMisses = cache.misses
		result.HashCollisions = cache.collisions
		result.UniqueStates = cache.entries
		result.CacheBytes = cache.bytes
		result.CacheBudgetSkips = cache.budgetSkips
	}()
	stack := []decision.Tape{{Schema: decision.SchemaVersion}}
	for len(stack) > 0 && result.Runs < bounds.MaxRuns {
		prefix := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if len(prefix.Entries) >= bounds.MaxDepth {
			result.DepthBoundHits++
			execution, pruned, err := complete(run, prefix, bounds.FallbackSeed)
			result.Runs++
			if err != nil {
				return result, err
			}
			if pruned {
				result.PrunedPrefixes++
				continue
			}
			result.Completed++
			if outcomeMatches(execution.Outcome, bounds.TargetFingerprint) {
				result.ViolatingRuns++
				if result.FirstViolation == nil {
					copy := execution
					result.FirstViolation = &copy
				}
				if bounds.StopOnViolation {
					result.Truncated = len(stack) > 0
					return result, nil
				}
			}
			continue
		}

		decider, err := decision.NewPrefixDecider(prefix)
		if err != nil {
			return result, err
		}
		outcome, canonical, runErr := run(decider)
		result.Runs++
		var open *decision.OpenChoiceError
		switch {
		case errors.As(runErr, &open):
			if err := decider.Finish(); err != nil {
				return result, err
			}
			result.OpenChoices++
			if cache != nil {
				if canonical == nil {
					return result, ErrMissingCanonicalState
				}
				remainingDepth := bounds.MaxDepth - len(prefix.Entries)
				identity, err := cacheIdentity(canonical, open.Choice, remainingDepth)
				if err != nil {
					return result, err
				}
				if cache.seen(identity) {
					result.StatePruned++
					continue
				}
			}
			selections, sampled, err := candidates(open.Choice, bounds)
			if err != nil {
				return result, err
			}
			if sampled {
				result.SampledDomains++
			}
			for index := len(selections) - 1; index >= 0; index-- {
				entry, err := decision.NewEntry(open.Choice, selections[index])
				if err != nil {
					return result, err
				}
				child := decision.CloneTape(prefix)
				child.Entries = append(child.Entries, entry)
				stack = append(stack, child)
			}
		case runErr != nil:
			return result, runErr
		default:
			if err := decider.Finish(); err != nil {
				if errors.Is(err, decision.ErrTapeNotConsumed) {
					result.PrunedPrefixes++
					continue
				}
				return result, err
			}
			result.Completed++
			execution := Execution{Tape: decision.CloneTape(prefix), Outcome: outcome}
			if outcomeMatches(outcome, bounds.TargetFingerprint) {
				result.ViolatingRuns++
				if result.FirstViolation == nil {
					result.FirstViolation = &execution
				}
				if bounds.StopOnViolation {
					result.Truncated = len(stack) > 0
					return result, nil
				}
			}
		}
	}
	result.Truncated = len(stack) > 0
	return result, nil
}

func complete(run internalRunner, prefix decision.Tape, seed uint64) (Execution, bool, error) {
	combined, err := decision.NewPrefixThenDecider(prefix, decision.NewSeedDecider(seed))
	if err != nil {
		return Execution{}, false, err
	}
	recorder := decision.NewRecorder(combined)
	outcome, _, runErr := run(recorder)
	if runErr != nil {
		return Execution{}, false, runErr
	}
	if err := combined.Finish(); err != nil {
		if errors.Is(err, decision.ErrTapeNotConsumed) {
			return Execution{}, true, nil
		}
		return Execution{}, false, err
	}
	if err := recorder.Err(); err != nil {
		return Execution{}, false, err
	}
	return Execution{Tape: recorder.Tape(), Outcome: outcome}, false, nil
}

func cacheIdentity(state []byte, choice decision.Choice, remainingDepth int) ([]byte, error) {
	domain, err := decision.CanonicalDomain(choice)
	if err != nil {
		return nil, err
	}
	context, err := decision.CanonicalContext(choice)
	if err != nil {
		return nil, err
	}
	identity := make([]byte, 0, len(state)+len(choice.ID)+len(choice.Kind)+len(domain)+len(context)+64)
	identity = appendField(identity, []byte("d-raft.explore-cache/v1"))
	identity = appendField(identity, state)
	identity = appendField(identity, []byte(choice.ID))
	identity = appendField(identity, []byte(choice.Kind))
	identity = appendField(identity, domain)
	identity = appendField(identity, context)
	identity = binary.BigEndian.AppendUint64(identity, uint64(remainingDepth))
	return identity, nil
}

func appendField(destination, value []byte) []byte {
	destination = binary.BigEndian.AppendUint64(destination, uint64(len(value)))
	return append(destination, value...)
}

func candidates(choice decision.Choice, bounds Bounds) ([]decision.Selection, bool, error) {
	if err := decision.ValidateChoice(choice); err != nil {
		return nil, false, err
	}
	if len(choice.Options) > 0 {
		limit := min(len(choice.Options), bounds.MaxBranchesPerChoice)
		result := make([]decision.Selection, limit)
		for index := range limit {
			result[index] = decision.Selection{Option: choice.Options[index].ID}
		}
		return result, limit < len(choice.Options), nil
	}
	minimum, maximum := *choice.Min, *choice.Max
	span := uint64(maximum-minimum) + 1
	count := min(bounds.RangeSamples, bounds.MaxBranchesPerChoice)
	if span <= uint64(count) {
		result := make([]decision.Selection, int(span))
		for index := range result {
			value := minimum + int64(index)
			result[index].Number = &value
		}
		return result, false, nil
	}
	values := []int64{minimum}
	if count >= 3 {
		values = append(values, minimum+(maximum-minimum)/2)
	}
	if count >= 2 {
		values = append(values, maximum)
	}
	result := make([]decision.Selection, len(values))
	for index, source := range values {
		value := source
		result[index].Number = &value
	}
	return result, true, nil
}

func outcomeMatches(outcome artifact.Outcome, target string) bool {
	for _, violation := range outcome.Violations {
		if target == "" || violation.Fingerprint == target {
			return true
		}
	}
	return false
}
