package sim

import (
	"errors"
	"math"
	"time"
)

var (
	// ErrNegativeTime is returned when an operation is given negative virtual
	// time or a negative delay.
	ErrNegativeTime = errors.New("sim: virtual time and delays must not be negative")
	// ErrPast is returned when an operation would move virtual time backwards.
	ErrPast = errors.New("sim: cannot schedule or advance into the past")
	// ErrTimeOverflow is returned when adding a delay would overflow a
	// time.Duration.
	ErrTimeOverflow = errors.New("sim: virtual time overflow")
	// ErrEventIDExhausted is returned in the practically unreachable case that
	// all uint64 event identifiers have been allocated.
	ErrEventIDExhausted = errors.New("sim: event identifier space exhausted")
)

// EventID identifies a pending event. The zero value is never issued.
type EventID uint64

// Action is invoked synchronously when an event reaches the front of the
// queue. It may schedule or cancel other events.
type Action func(*Simulator)

// Clock exposes the current virtual time. It has no wall-clock behavior.
type Clock struct {
	now time.Duration
}

// Now returns the current virtual time since the start of the simulation.
func (c *Clock) Now() time.Duration {
	return c.now
}

// Simulator is a single-threaded deterministic discrete-event scheduler.
// Its zero value is ready to use.
//
// Simulator is intentionally not safe for concurrent use. Running it from one
// OS goroutine makes event execution, random draws, and network delivery fully
// reproducible.
type Simulator struct {
	clock     Clock
	queue     eventHeap
	pending   map[EventID]*scheduledEvent
	nextID    EventID
	exhausted bool
	trace     TraceSink
}

// New returns an empty simulator at virtual time zero.
func New() *Simulator {
	return &Simulator{nextID: 1}
}

// Clock returns the simulator's read-only virtual clock.
func (s *Simulator) Clock() *Clock {
	return &s.clock
}

// SetTraceSink sets the synchronous machine-readable trace destination.
// Passing nil disables tracing.
func (s *Simulator) SetTraceSink(sink TraceSink) {
	s.trace = sink
}

// Now returns the current virtual time.
func (s *Simulator) Now() time.Duration {
	return s.clock.now
}

// Len returns the number of pending events.
func (s *Simulator) Len() int {
	return len(s.queue)
}

// Schedule adds an action delay after the current virtual time.
func (s *Simulator) Schedule(delay time.Duration, action Action) (EventID, error) {
	if delay < 0 {
		return 0, ErrNegativeTime
	}
	if s.clock.now > time.Duration(math.MaxInt64)-delay {
		return 0, ErrTimeOverflow
	}
	return s.ScheduleAt(s.clock.now+delay, action)
}

// ScheduleAt adds an action at the specified absolute virtual time. Events at
// the same time execute in the order in which they were scheduled.
func (s *Simulator) ScheduleAt(when time.Duration, action Action) (EventID, error) {
	if when < 0 {
		return 0, ErrNegativeTime
	}
	if when < s.clock.now {
		return 0, ErrPast
	}
	if action == nil {
		return 0, errors.New("sim: event action must not be nil")
	}
	if s.exhausted {
		return 0, ErrEventIDExhausted
	}
	if s.nextID == 0 {
		s.nextID = 1 // Make the Simulator zero value useful.
	}

	id := s.nextID
	if id == EventID(math.MaxUint64) {
		s.exhausted = true
	} else {
		s.nextID++
	}
	event := &scheduledEvent{id: id, when: when, order: uint64(id), action: action}
	if s.pending == nil {
		s.pending = make(map[EventID]*scheduledEvent)
	}
	s.pending[id] = event
	s.queue.push(event)
	if s.trace != nil {
		s.recordTrace(TraceEvent{
			Kind:           TraceEventScheduled,
			AtNS:           traceTime(s.clock.now),
			EventID:        id,
			ScheduledForNS: traceTime(when),
		})
	}
	return id, nil
}

// Cancel removes a pending event. It reports false if id is zero, unknown, or
// already executed or canceled.
func (s *Simulator) Cancel(id EventID) bool {
	event, ok := s.pending[id]
	if !ok {
		return false
	}
	delete(s.pending, id)
	s.queue.remove(event.index)
	event.action = nil
	if s.trace != nil {
		s.recordTrace(TraceEvent{
			Kind:           TraceEventCanceled,
			AtNS:           traceTime(s.clock.now),
			EventID:        id,
			ScheduledForNS: traceTime(event.when),
		})
	}
	return true
}

// NextEventTime returns the time of the next event and whether one exists.
func (s *Simulator) NextEventTime() (time.Duration, bool) {
	if len(s.queue) == 0 {
		return 0, false
	}
	return s.queue[0].when, true
}

// Step executes the next event and reports whether an event was available.
func (s *Simulator) Step() bool {
	if len(s.queue) == 0 {
		return false
	}
	event := s.queue.pop()
	delete(s.pending, event.id)
	s.clock.now = event.when
	if s.trace != nil {
		s.recordTrace(TraceEvent{
			Kind:    TraceEventExecuted,
			AtNS:    traceTime(event.when),
			EventID: event.id,
		})
	}
	action := event.action
	event.action = nil
	action(s)
	return true
}

// Run executes events until the queue is empty and returns the number run.
func (s *Simulator) Run() int {
	count := 0
	for s.Step() {
		count++
	}
	return count
}

// RunSteps executes at most limit events and returns the number run. A
// negative limit executes no events.
func (s *Simulator) RunSteps(limit int) int {
	count := 0
	for count < limit && s.Step() {
		count++
	}
	return count
}

// RunUntil executes every event scheduled at or before end, then advances the
// clock to end. Events scheduled for end by another event at end are also run.
func (s *Simulator) RunUntil(end time.Duration) (int, error) {
	if end < 0 {
		return 0, ErrNegativeTime
	}
	if end < s.clock.now {
		return 0, ErrPast
	}

	count := 0
	for len(s.queue) > 0 && s.queue[0].when <= end {
		s.Step()
		count++
	}
	if s.clock.now != end {
		s.clock.now = end
		if s.trace != nil {
			s.recordTrace(TraceEvent{Kind: TraceClockAdvanced, AtNS: traceTime(end)})
		}
	}
	return count, nil
}

func (s *Simulator) recordTrace(event TraceEvent) {
	if s.trace != nil {
		s.trace.RecordTrace(event)
	}
}
