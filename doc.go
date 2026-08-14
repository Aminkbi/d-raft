// Package sim provides d-raft's deterministic, single-threaded discrete-event
// simulation runtime for testing distributed systems.
//
// Time advances only when Simulator executes an event. The package never
// starts a goroutine and never waits on wall-clock time. Events scheduled for
// the same instant execute in scheduling order. Rand provides package-owned
// reproducible random streams, and Router provides a typed network with
// latency, loss, endpoint lifecycle, and directed partitions.
//
// Components can write their transitions to a shared JSONLRecorder. The
// resulting versioned JSON Lines stream gives scheduler, random, network, and
// protocol activity one global observational order. Exact execution replay is
// driven separately by the semantic choices in package decision.
package sim
