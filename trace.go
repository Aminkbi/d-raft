package sim

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// TraceSchemaVersion identifies the JSON Lines trace format emitted by
// JSONLRecorder. It is versioned independently from the Go module.
const TraceSchemaVersion = "d-raft.trace/v1"

// TraceEventKind identifies a deterministic simulator transition.
type TraceEventKind string

const (
	TraceEventScheduled   TraceEventKind = "event_scheduled"
	TraceEventCanceled    TraceEventKind = "event_canceled"
	TraceEventExecuted    TraceEventKind = "event_executed"
	TraceClockAdvanced    TraceEventKind = "clock_advanced"
	TraceRandomDraw       TraceEventKind = "random_draw"
	TraceNodeRegistered   TraceEventKind = "node_registered"
	TraceNodeUnregistered TraceEventKind = "node_unregistered"
	TraceLinkSet          TraceEventKind = "link_set"
	TraceLinkReset        TraceEventKind = "link_reset"
	TracePartitionChanged TraceEventKind = "partition_changed"
	TracePacketScheduled  TraceEventKind = "packet_scheduled"
	TracePacketDelivered  TraceEventKind = "packet_delivered"
	TracePacketDropped    TraceEventKind = "packet_dropped"
	TraceProtocolInput    TraceEventKind = "protocol_input"
	TraceProtocolState    TraceEventKind = "protocol_state"
	TracePersistence      TraceEventKind = "persistence"
	TraceProcessLifecycle TraceEventKind = "process_lifecycle"
	TraceProtocolDrop     TraceEventKind = "protocol_drop"
)

// TraceRoute is one permitted directed route in a partition trace event.
type TraceRoute struct {
	From NodeID `json:"from"`
	To   NodeID `json:"to"`
}

// TraceLinkConfig is the stable, nanosecond-based trace representation of a
// LinkConfig.
type TraceLinkConfig struct {
	MinLatencyNS    int64   `json:"min_latency_ns"`
	MaxLatencyNS    int64   `json:"max_latency_ns"`
	LossProbability float64 `json:"loss_probability"`
}

// TraceEvent is an event before it is assigned a schema version and sequence
// number by a recorder. Fields irrelevant to Kind are omitted from JSON.
// Message must be JSON-encodable when the event is sent to JSONLRecorder.
type TraceEvent struct {
	Kind TraceEventKind `json:"kind"`

	AtNS           *int64  `json:"at_ns,omitempty"`
	EventID        EventID `json:"event_id,omitempty"`
	ScheduledForNS *int64  `json:"scheduled_for_ns,omitempty"`

	RandomStream    string            `json:"random_stream,omitempty"`
	RandomOperation string            `json:"random_operation,omitempty"`
	RandomArguments map[string]string `json:"random_arguments,omitempty"`
	RandomResult    string            `json:"random_result,omitempty"`

	PacketID     uint64 `json:"packet_id,omitempty"`
	From         NodeID `json:"from,omitempty"`
	To           NodeID `json:"to,omitempty"`
	Message      any    `json:"message,omitempty"`
	DeliveryAtNS *int64 `json:"delivery_at_ns,omitempty"`
	DropReason   string `json:"drop_reason,omitempty"`

	Component string `json:"component,omitempty"`
	Action    string `json:"action,omitempty"`
	Details   any    `json:"details,omitempty"`

	Node             NodeID           `json:"node,omitempty"`
	Link             *TraceLinkConfig `json:"link,omitempty"`
	PartitionActive  *bool            `json:"partition_active,omitempty"`
	PartitionNodes   []NodeID         `json:"partition_nodes,omitempty"`
	PartitionAllowed []TraceRoute     `json:"partition_allowed,omitempty"`
}

// TraceRecord is one versioned, globally ordered machine-readable record.
type TraceRecord struct {
	Schema   string `json:"schema"`
	Sequence uint64 `json:"sequence"`
	TraceEvent
}

// TraceSink consumes trace events synchronously. Implementations must not
// call back into the component producing an event because doing so can change
// deterministic scheduling order.
type TraceSink interface {
	RecordTrace(TraceEvent)
}

// TraceFunc adapts a function to TraceSink.
type TraceFunc func(TraceEvent)

// RecordTrace calls f(event).
func (f TraceFunc) RecordTrace(event TraceEvent) {
	f(event)
}

// JSONLRecorder writes one TraceRecord per line. It is intentionally
// synchronous and is not safe for concurrent use. After the first encoding
// error it records no further events; Err reports that error.
type JSONLRecorder struct {
	encoder *json.Encoder
	next    uint64
	err     error
}

// NewJSONLRecorder returns a recorder that writes to writer.
func NewJSONLRecorder(writer io.Writer) *JSONLRecorder {
	if writer == nil {
		return &JSONLRecorder{err: fmt.Errorf("sim: JSONLRecorder requires a writer")}
	}
	return &JSONLRecorder{encoder: json.NewEncoder(writer), next: 1}
}

// RecordTrace implements TraceSink.
func (r *JSONLRecorder) RecordTrace(event TraceEvent) {
	if r.err != nil {
		return
	}
	if r.encoder == nil {
		r.err = fmt.Errorf("sim: JSONLRecorder has no writer")
		return
	}
	record := TraceRecord{
		Schema:     TraceSchemaVersion,
		Sequence:   r.next,
		TraceEvent: event,
	}
	if err := r.encoder.Encode(record); err != nil {
		r.err = err
		return
	}
	r.next++
}

// Err returns the first JSON encoding or writing error observed by r.
func (r *JSONLRecorder) Err() error {
	return r.err
}

func traceTime(value time.Duration) *int64 {
	nanoseconds := int64(value)
	return &nanoseconds
}

func traceBool(value bool) *bool {
	return &value
}
