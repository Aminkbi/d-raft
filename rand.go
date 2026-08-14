package sim

import (
	"fmt"
	"math/bits"
	"strconv"
	"time"
)

// Rand is a small, deterministic pseudo-random number generator.
//
// Rand uses SplitMix64. Its output is deliberately implemented in this
// package, rather than delegated to a standard-library generator whose stream
// may change between Go releases. A seed therefore reproduces the same stream
// on every supported platform and package version.
//
// Rand is intended for simulation, randomized testing, and deriving
// independent simulation streams. It is not cryptographically secure.
type Rand struct {
	state      uint64
	trace      TraceSink
	stream     string
	splitCount uint64
}

// RandState is the future-relevant state of one deterministic stream.
type RandState struct {
	State      uint64 `json:"state"`
	SplitCount uint64 `json:"split_count"`
}

// State returns an immutable checkpoint. Trace metadata is deliberately
// excluded because canonical exploration rejects side-effecting trace sinks.
func (r *Rand) State() RandState {
	if r == nil {
		return RandState{}
	}
	return RandState{State: r.state, SplitCount: r.splitCount}
}

// NewRand returns a generator initialized with seed. Every uint64 value,
// including zero, is a valid seed.
func NewRand(seed uint64) *Rand {
	return &Rand{state: seed, stream: "default"}
}

// SetTraceSink sets the synchronous trace destination and stream label.
// Empty labels are recorded as "default". Child generators created by Split
// inherit the sink and receive stable labels below this stream.
func (r *Rand) SetTraceSink(sink TraceSink, stream string) {
	if stream == "" {
		stream = "default"
	}
	r.trace = sink
	r.stream = stream
}

// Uint64 returns the next value in the stream.
func (r *Rand) Uint64() uint64 {
	value := r.nextUint64()
	if r.trace != nil {
		r.recordRandom("uint64", nil, fmt.Sprintf("0x%016x", value))
	}
	return value
}

func (r *Rand) nextUint64() uint64 {
	// SplitMix64 by Steele, Lea, and Flood, using the constants from the
	// reference implementation.
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Uint64N returns a uniformly distributed value in [0, n). It panics if n is
// zero. Rejection sampling avoids modulo bias.
func (r *Rand) Uint64N(n uint64) uint64 {
	if n == 0 {
		panic("sim: invalid argument to Rand.Uint64N: n must be positive")
	}

	value := r.uint64N(n)
	if r.trace != nil {
		r.recordRandom("uint64n", map[string]string{"n": strconv.FormatUint(n, 10)}, strconv.FormatUint(value, 10))
	}
	return value
}

// IntN returns a uniformly distributed value in [0, n). It panics if n is
// not positive.
func (r *Rand) IntN(n int) int {
	if n <= 0 {
		panic("sim: invalid argument to Rand.IntN: n must be positive")
	}
	value := int(r.uint64N(uint64(n)))
	if r.trace != nil {
		r.recordRandom("intn", map[string]string{"n": strconv.Itoa(n)}, strconv.Itoa(value))
	}
	return value
}

// Float64 returns a uniformly distributed value in [0.0, 1.0).
func (r *Rand) Float64() float64 {
	value := r.float64()
	if r.trace != nil {
		r.recordRandom("float64", nil, strconv.FormatFloat(value, 'g', -1, 64))
	}
	return value
}

// Chance reports whether an event with probability p occurs. It panics when
// p is outside [0, 1] or is NaN.
func (r *Rand) Chance(p float64) bool {
	if p < 0 || p > 1 || p != p {
		panic("sim: invalid probability")
	}
	if p == 0 {
		if r.trace != nil {
			r.recordRandom("chance", map[string]string{"p": "0"}, "false")
		}
		return false
	}
	if p == 1 {
		if r.trace != nil {
			r.recordRandom("chance", map[string]string{"p": "1"}, "true")
		}
		return true
	}
	value := r.float64() < p
	if r.trace != nil {
		r.recordRandom("chance", map[string]string{"p": strconv.FormatFloat(p, 'g', -1, 64)}, strconv.FormatBool(value))
	}
	return value
}

// Duration returns a uniformly distributed duration in the inclusive range
// [min, max]. It panics if min is negative or max is less than min.
func (r *Rand) Duration(min, max time.Duration) time.Duration {
	if min < 0 || max < min {
		panic("sim: invalid duration range")
	}
	if min == max {
		if r.trace != nil {
			r.recordRandom("duration", map[string]string{
				"min_ns": strconv.FormatInt(int64(min), 10),
				"max_ns": strconv.FormatInt(int64(max), 10),
			}, strconv.FormatInt(int64(min), 10))
		}
		return min
	}
	span := uint64(max-min) + 1
	value := min + time.Duration(r.uint64N(span))
	if r.trace != nil {
		r.recordRandom("duration", map[string]string{
			"min_ns": strconv.FormatInt(int64(min), 10),
			"max_ns": strconv.FormatInt(int64(max), 10),
		}, strconv.FormatInt(int64(value), 10))
	}
	return value
}

// Split derives a deterministic child stream. Calling Split consumes one
// value from r, so stream allocation order is significant.
func (r *Rand) Split() *Rand {
	seed := r.nextUint64()
	r.splitCount++
	childStream := fmt.Sprintf("%s/%d", r.stream, r.splitCount)
	child := NewRand(seed)
	if r.trace != nil {
		r.recordRandom("split", map[string]string{"child_stream": childStream}, fmt.Sprintf("0x%016x", seed))
		child.SetTraceSink(r.trace, childStream)
	} else {
		child.stream = childStream
	}
	return child
}

func (r *Rand) uint64N(n uint64) uint64 {
	hi, lo := bits.Mul64(r.nextUint64(), n)
	if lo < n {
		threshold := -n % n
		for lo < threshold {
			hi, lo = bits.Mul64(r.nextUint64(), n)
		}
	}
	return hi
}

func (r *Rand) float64() float64 {
	return float64(r.nextUint64()>>11) * (1.0 / (1 << 53))
}

func (r *Rand) recordRandom(operation string, arguments map[string]string, result string) {
	if r.trace == nil {
		return
	}
	r.trace.RecordTrace(TraceEvent{
		Kind:            TraceRandomDraw,
		RandomStream:    r.stream,
		RandomOperation: operation,
		RandomArguments: arguments,
		RandomResult:    result,
	})
}
