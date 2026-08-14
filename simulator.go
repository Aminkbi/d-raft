package sim

import (
	"encoding/json"
	"errors"
	"math"
	"slices"
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
	// ErrUncacheableState reports hidden callback or trace-sink behavior.
	ErrUncacheableState = errors.New("sim: state is not canonically inspectable")
	// ErrInvalidEventTag reports a malformed canonical callback descriptor.
	ErrInvalidEventTag = errors.New("sim: invalid event tag")
)

const CanonicalStateSchema = "d-raft.sim-state/v1"

// EventKind identifies the closed set of callbacks used by the reference
// experiment executor. EventTag data is a canonical JSON value whose schema
// is owned by the producer of that kind.
type EventKind string

const (
	EventNetworkDelivery   EventKind = "network_delivery"
	EventStorageCompletion EventKind = "storage_completion"
	EventPersistenceAck    EventKind = "persistence_ack"
	EventElectionTimer     EventKind = "election_timer"
	EventHeartbeatTimer    EventKind = "heartbeat_timer"
	EventExternalAction    EventKind = "external_action"
	EventScheduledCrash    EventKind = "scheduled_crash"
	EventScheduledRestart  EventKind = "scheduled_restart"
)

// EventTag is the inspectable semantic replacement for an opaque callback.
type EventTag struct {
	Kind EventKind       `json:"kind"`
	Data json.RawMessage `json:"data"`
}

// PendingEventState is one future callback in execution order.
type PendingEventState struct {
	ID    EventID  `json:"id"`
	AtNS  int64    `json:"at_ns"`
	Order uint64   `json:"order"`
	Tag   EventTag `json:"tag"`
}

// SimulatorState contains every future-relevant scheduler field. Events are
// sorted by execution order rather than heap layout.
type SimulatorState struct {
	Schema    string              `json:"schema"`
	NowNS     int64               `json:"now_ns"`
	NextID    EventID             `json:"next_id"`
	Exhausted bool                `json:"exhausted"`
	Events    []PendingEventState `json:"events"`
}

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

// ScheduleTagged adds an inspectable callback delay after the current time.
func (s *Simulator) ScheduleTagged(delay time.Duration, tag EventTag, action Action) (EventID, error) {
	if delay < 0 {
		return 0, ErrNegativeTime
	}
	if s.clock.now > time.Duration(math.MaxInt64)-delay {
		return 0, ErrTimeOverflow
	}
	return s.ScheduleAtTagged(s.clock.now+delay, tag, action)
}

// ScheduleAt adds an action at the specified absolute virtual time. Events at
// the same time execute in the order in which they were scheduled.
func (s *Simulator) ScheduleAt(when time.Duration, action Action) (EventID, error) {
	return s.scheduleAt(when, EventTag{}, action)
}

// ScheduleAtTagged adds an inspectable callback at an absolute virtual time.
func (s *Simulator) ScheduleAtTagged(when time.Duration, tag EventTag, action Action) (EventID, error) {
	if err := validateEventTag(tag); err != nil {
		return 0, err
	}
	return s.scheduleAt(when, cloneEventTag(tag), action)
}

func (s *Simulator) scheduleAt(when time.Duration, tag EventTag, action Action) (EventID, error) {
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
	event := &scheduledEvent{id: id, when: when, order: uint64(id), tag: tag, action: action}
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

// CanonicalState returns an independent scheduler checkpoint. It refuses to
// guess the meaning of opaque Action closures.
func (s *Simulator) CanonicalState() (SimulatorState, error) {
	if s.trace != nil {
		return SimulatorState{}, ErrUncacheableState
	}
	state := SimulatorState{Schema: CanonicalStateSchema, NowNS: int64(s.clock.now), NextID: s.nextID, Exhausted: s.exhausted}
	state.Events = make([]PendingEventState, 0, len(s.queue))
	for _, event := range s.queue {
		if event.tag.Kind == "" {
			return SimulatorState{}, ErrUncacheableState
		}
		if err := validateEventTag(event.tag); err != nil {
			return SimulatorState{}, err
		}
		state.Events = append(state.Events, PendingEventState{ID: event.id, AtNS: int64(event.when), Order: event.order, Tag: cloneEventTag(event.tag)})
	}
	slices.SortFunc(state.Events, func(left, right PendingEventState) int {
		if left.AtNS < right.AtNS {
			return -1
		}
		if left.AtNS > right.AtNS {
			return 1
		}
		if left.Order < right.Order {
			return -1
		}
		if left.Order > right.Order {
			return 1
		}
		return 0
	})
	return state, nil
}

func validateEventTag(tag EventTag) error {
	switch tag.Kind {
	case EventNetworkDelivery, EventStorageCompletion, EventPersistenceAck, EventElectionTimer, EventHeartbeatTimer, EventExternalAction, EventScheduledCrash, EventScheduledRestart:
	default:
		return ErrInvalidEventTag
	}
	if len(tag.Data) == 0 || !json.Valid(tag.Data) {
		return ErrInvalidEventTag
	}
	return nil
}

func cloneEventTag(tag EventTag) EventTag {
	tag.Data = slices.Clone(tag.Data)
	return tag
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
