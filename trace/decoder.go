// Package trace decodes and validates d-raft's observational JSON Lines trace
// without converting full-width protocol integers through float64.
package trace

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"

	sim "github.com/aminkbi/d-raft"
)

const DefaultMaxRecordBytes = 16 << 20

var (
	ErrInvalidRecord  = errors.New("trace: invalid record")
	ErrRecordTooLarge = errors.New("trace: record exceeds size limit")
	ErrSequence       = errors.New("trace: invalid sequence")
	ErrUnknownKind    = errors.New("trace: unknown event kind")
)

// ValidationMode selects forward-compatible or exact known-schema checks.
type ValidationMode uint8

const (
	ValidateCompatible ValidationMode = iota + 1
	ValidateStrict
)

// Record preserves known sim.TraceRecord fields and keeps Message and Details
// as raw JSON until a caller selects their concrete type. Compatible-mode
// fields added by a future schema producer are accepted but not retained.
type Record struct {
	Schema   string             `json:"schema"`
	Sequence uint64             `json:"sequence"`
	Kind     sim.TraceEventKind `json:"kind"`

	AtNS           *int64      `json:"at_ns,omitempty"`
	EventID        sim.EventID `json:"event_id,omitempty"`
	ScheduledForNS *int64      `json:"scheduled_for_ns,omitempty"`

	RandomStream    string            `json:"random_stream,omitempty"`
	RandomOperation string            `json:"random_operation,omitempty"`
	RandomArguments map[string]string `json:"random_arguments,omitempty"`
	RandomResult    string            `json:"random_result,omitempty"`

	PacketID     uint64          `json:"packet_id,omitempty"`
	From         sim.NodeID      `json:"from,omitempty"`
	To           sim.NodeID      `json:"to,omitempty"`
	Message      json.RawMessage `json:"message,omitempty"`
	DeliveryAtNS *int64          `json:"delivery_at_ns,omitempty"`
	DropReason   string          `json:"drop_reason,omitempty"`

	Component string          `json:"component,omitempty"`
	Action    string          `json:"action,omitempty"`
	Details   json.RawMessage `json:"details,omitempty"`

	Node             sim.NodeID           `json:"node,omitempty"`
	Link             *sim.TraceLinkConfig `json:"link,omitempty"`
	PartitionActive  *bool                `json:"partition_active,omitempty"`
	PartitionNodes   []sim.NodeID         `json:"partition_nodes,omitempty"`
	PartitionAllowed []sim.TraceRoute     `json:"partition_allowed,omitempty"`
}

// Decoder reads one bounded JSON object per line.
type Decoder struct {
	reader  *bufio.Reader
	maximum int
	mode    ValidationMode
	next    uint64
	err     error
	line    uint64
}

// Option configures a Decoder.
type Option func(*Decoder)

// WithMaxRecordBytes changes the per-line size limit.
func WithMaxRecordBytes(maximum int) Option {
	return func(decoder *Decoder) { decoder.maximum = maximum }
}

// WithValidation selects compatible or strict validation.
func WithValidation(mode ValidationMode) Option {
	return func(decoder *Decoder) { decoder.mode = mode }
}

// NewDecoder returns a line-aware trace decoder.
func NewDecoder(reader io.Reader, options ...Option) *Decoder {
	decoder := &Decoder{maximum: DefaultMaxRecordBytes, mode: ValidateCompatible, next: 1}
	for _, option := range options {
		option(decoder)
	}
	if reader == nil {
		decoder.err = fmt.Errorf("%w: nil reader", ErrInvalidRecord)
	} else if decoder.maximum <= 0 {
		decoder.err = fmt.Errorf("%w: non-positive size limit", ErrInvalidRecord)
	} else if decoder.mode != ValidateCompatible && decoder.mode != ValidateStrict {
		decoder.err = fmt.Errorf("%w: validation mode %d", ErrInvalidRecord, decoder.mode)
	} else {
		decoder.reader = bufio.NewReader(reader)
	}
	return decoder
}

// Next returns the next record or io.EOF. After a decoding error, subsequent
// calls return the same error.
func (d *Decoder) Next() (Record, error) {
	if d.err != nil {
		return Record{}, d.err
	}
	line, err := d.readLine()
	if err != nil {
		if errors.Is(err, io.EOF) && len(line) == 0 {
			return Record{}, io.EOF
		}
		if !errors.Is(err, io.EOF) {
			d.err = err
			return Record{}, err
		}
	}
	d.line++
	line = bytes.TrimSuffix(line, []byte{'\n'})
	line = bytes.TrimSuffix(line, []byte{'\r'})
	if len(bytes.TrimSpace(line)) == 0 {
		d.err = fmt.Errorf("%w at line %d: blank line", ErrInvalidRecord, d.line)
		return Record{}, d.err
	}

	var record Record
	jsonDecoder := json.NewDecoder(bytes.NewReader(line))
	if d.mode == ValidateStrict {
		jsonDecoder.DisallowUnknownFields()
	}
	if err := jsonDecoder.Decode(&record); err != nil {
		d.err = fmt.Errorf("%w at line %d: %v", ErrInvalidRecord, d.line, err)
		return Record{}, d.err
	}
	var extra any
	if err := jsonDecoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		d.err = fmt.Errorf("%w at line %d: %v", ErrInvalidRecord, d.line, err)
		return Record{}, d.err
	}
	if err := d.validate(record); err != nil {
		d.err = fmt.Errorf("%w at line %d: %w", ErrInvalidRecord, d.line, err)
		return Record{}, d.err
	}
	d.next++
	return record, nil
}

func (d *Decoder) readLine() ([]byte, error) {
	var line []byte
	for {
		fragment, err := d.reader.ReadSlice('\n')
		if len(line)+len(fragment) > d.maximum {
			return nil, fmt.Errorf("%w: maximum %d bytes", ErrRecordTooLarge, d.maximum)
		}
		line = append(line, fragment...)
		switch {
		case err == nil:
			return line, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			return line, io.EOF
		default:
			return nil, err
		}
	}
}

func (d *Decoder) validate(record Record) error {
	if record.Schema != sim.TraceSchemaVersion {
		return fmt.Errorf("schema %q, want %q", record.Schema, sim.TraceSchemaVersion)
	}
	if record.Sequence != d.next {
		return fmt.Errorf("%w: got %d, want %d", ErrSequence, record.Sequence, d.next)
	}
	if record.Kind == "" {
		return errors.New("missing kind")
	}
	for _, value := range []*int64{record.AtNS, record.ScheduledForNS, record.DeliveryAtNS} {
		if value != nil && *value < 0 {
			return errors.New("negative time")
		}
	}
	if record.Link != nil && (record.Link.MinLatencyNS < 0 || record.Link.MaxLatencyNS < record.Link.MinLatencyNS || record.Link.LossProbability < 0 || record.Link.LossProbability > 1 || math.IsNaN(record.Link.LossProbability)) {
		return errors.New("invalid link configuration")
	}
	if !knownKind(record.Kind) {
		if d.mode == ValidateStrict {
			return fmt.Errorf("%w %q", ErrUnknownKind, record.Kind)
		}
		return nil
	}
	switch record.Kind {
	case sim.TraceEventScheduled:
		if record.AtNS == nil || record.ScheduledForNS == nil || record.EventID == 0 {
			return errors.New("scheduled event is missing time or event_id")
		}
	case sim.TraceEventCanceled:
		if record.AtNS == nil || record.EventID == 0 {
			return errors.New("canceled event is missing time or event_id")
		}
	case sim.TraceEventExecuted:
		if record.AtNS == nil || record.EventID == 0 {
			return errors.New("executed event is missing time or event_id")
		}
	case sim.TraceClockAdvanced:
		if record.AtNS == nil {
			return errors.New("clock advance is missing time")
		}
	case sim.TraceRandomDraw:
		if record.RandomStream == "" || record.RandomOperation == "" || record.RandomResult == "" {
			return errors.New("random draw is incomplete")
		}
	case sim.TraceNodeRegistered, sim.TraceNodeUnregistered:
		if record.AtNS == nil || record.Node == "" {
			return errors.New("node lifecycle event is incomplete")
		}
	case sim.TraceLinkSet:
		if record.AtNS == nil || record.From == "" || record.To == "" || record.Link == nil {
			return errors.New("link event is incomplete")
		}
	case sim.TraceLinkReset:
		if record.AtNS == nil || record.From == "" || record.To == "" {
			return errors.New("link reset is incomplete")
		}
	case sim.TracePartitionChanged:
		if record.AtNS == nil || record.PartitionActive == nil {
			return errors.New("partition event is incomplete")
		}
	case sim.TracePacketScheduled, sim.TracePacketDelivered:
		if record.AtNS == nil || record.PacketID == 0 || record.From == "" || record.To == "" || record.DeliveryAtNS == nil {
			return errors.New("packet event is incomplete")
		}
	case sim.TracePacketDropped:
		if record.AtNS == nil || record.PacketID == 0 || record.From == "" || record.To == "" || (record.DropReason != "loss" && record.DropReason != "partition" && record.DropReason != "target_unavailable") {
			return errors.New("packet drop is incomplete")
		}
	case sim.TraceProtocolInput, sim.TraceProtocolState, sim.TracePersistence, sim.TraceProcessLifecycle, sim.TraceProtocolDrop:
		if record.AtNS == nil || record.Component == "" || record.Action == "" {
			return errors.New("protocol event is incomplete")
		}
	}
	return nil
}

func knownKind(kind sim.TraceEventKind) bool {
	switch kind {
	case sim.TraceEventScheduled, sim.TraceEventCanceled, sim.TraceEventExecuted, sim.TraceClockAdvanced,
		sim.TraceRandomDraw, sim.TraceNodeRegistered, sim.TraceNodeUnregistered, sim.TraceLinkSet,
		sim.TraceLinkReset, sim.TracePartitionChanged, sim.TracePacketScheduled, sim.TracePacketDelivered,
		sim.TracePacketDropped, sim.TraceProtocolInput, sim.TraceProtocolState, sim.TracePersistence,
		sim.TraceProcessLifecycle, sim.TraceProtocolDrop:
		return true
	default:
		return false
	}
}

// DecodeMessage decodes a record's message without lossy generic-number
// conversion.
func DecodeMessage[M any](record Record) (M, error) {
	var message M
	if len(record.Message) == 0 {
		return message, errors.New("trace: record has no message")
	}
	if err := json.Unmarshal(record.Message, &message); err != nil {
		return message, err
	}
	return message, nil
}

// DecodeDetails decodes a record's protocol details.
func DecodeDetails[D any](record Record) (D, error) {
	var details D
	if len(record.Details) == 0 {
		return details, errors.New("trace: record has no details")
	}
	if err := json.Unmarshal(record.Details, &details); err != nil {
		return details, err
	}
	return details, nil
}
