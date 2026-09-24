package trace

import (
	"errors"
	"io"
	"strings"
	"testing"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/internal/strictjson"
)

func TestDecoderPreservesFullWidthMessageIntegers(t *testing.T) {
	t.Parallel()

	input := `{"schema":"d-raft.trace/v1","sequence":1,"kind":"packet_delivered","at_ns":5,"packet_id":1,"from":"a","to":"b","message":{"term":18446744073709551615},"delivery_at_ns":5}` + "\n"
	decoder := NewDecoder(strings.NewReader(input), WithValidation(ValidateStrict))
	record, err := decoder.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	message, err := DecodeMessage[struct {
		Term uint64 `json:"term"`
	}](record)
	if err != nil || message.Term != ^uint64(0) {
		t.Fatalf("message=%+v err=%v", message, err)
	}
	if _, err := decoder.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("final Next error = %v", err)
	}
}

func TestDecoderRejectsSequenceGap(t *testing.T) {
	t.Parallel()

	input := `{"schema":"d-raft.trace/v1","sequence":2,"kind":"clock_advanced","at_ns":1}` + "\n"
	decoder := NewDecoder(strings.NewReader(input))
	if _, err := decoder.Next(); !errors.Is(err, ErrInvalidRecord) || !errors.Is(err, ErrSequence) {
		t.Fatalf("sequence error = %v", err)
	}
}

func TestDecoderCompatibleModeAcceptsUnknownKind(t *testing.T) {
	t.Parallel()

	input := `{"schema":"d-raft.trace/v1","sequence":1,"kind":"future_event","future":true}` + "\n"
	if _, err := NewDecoder(strings.NewReader(input)).Next(); err != nil {
		t.Fatalf("compatible Next: %v", err)
	}
	if _, err := NewDecoder(strings.NewReader(input), WithValidation(ValidateStrict)).Next(); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("strict error = %v", err)
	}
}

func TestDecoderHandlesLargeRecordAndLimit(t *testing.T) {
	t.Parallel()

	payload := strings.Repeat("x", 100_000)
	input := `{"schema":"d-raft.trace/v1","sequence":1,"kind":"protocol_state","at_ns":0,"component":"raft/a","action":"large","details":{"payload":"` + payload + `"}}` + "\n"
	if _, err := NewDecoder(strings.NewReader(input)).Next(); err != nil {
		t.Fatalf("large Next: %v", err)
	}
	if _, err := NewDecoder(strings.NewReader(input), WithMaxRecordBytes(1024)).Next(); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("size error = %v", err)
	}
}

func TestDecoderRejectsMultipleValues(t *testing.T) {
	t.Parallel()

	input := `{"schema":"d-raft.trace/v1","sequence":1,"kind":"clock_advanced","at_ns":1} {}` + "\n"
	if _, err := NewDecoder(strings.NewReader(input)).Next(); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("multiple-value error = %v", err)
	}
}

func TestDecoderRejectsDuplicateNames(t *testing.T) {
	t.Parallel()

	base := `{"schema":"d-raft.trace/v1","sequence":1,"kind":"clock_advanced","at_ns":1}`
	tests := []struct {
		name        string
		old         string
		replacement string
	}{
		{"top level", `"schema":`, `"schema":"d-raft.trace/v1","schema":`},
		{"escaped nested", `"at_ns":1`, `"at_ns":1,"details":{"tag":1,"ta\u0067":2}`},
	}
	modes := []struct {
		name string
		mode ValidationMode
	}{
		{"compatible", ValidateCompatible},
		{"strict", ValidateStrict},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			t.Parallel()
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()
					document := strings.Replace(base, test.old, test.replacement, 1)
					if document == base {
						t.Fatalf("replacement %q did not alter document", test.old)
					}
					_, err := NewDecoder(strings.NewReader(document), WithValidation(mode.mode)).Next()
					if !errors.Is(err, ErrInvalidRecord) || !errors.Is(err, strictjson.ErrDuplicateName) {
						t.Fatalf("duplicate-name error = %v", err)
					}
				})
			}
		})
	}
}

func TestDecoderRejectsNilReader(t *testing.T) {
	t.Parallel()

	decoder := NewDecoder(nil)
	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := decoder.Next(); !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("attempt %d error = %v", attempt, err)
		}
	}
}

func TestKnownKindCoverage(t *testing.T) {
	t.Parallel()

	kinds := []sim.TraceEventKind{
		sim.TraceEventScheduled, sim.TraceEventCanceled, sim.TraceEventExecuted, sim.TraceClockAdvanced,
		sim.TraceRandomDraw, sim.TraceNodeRegistered, sim.TraceNodeUnregistered, sim.TraceLinkSet,
		sim.TraceLinkReset, sim.TracePartitionChanged, sim.TracePacketScheduled, sim.TracePacketDelivered,
		sim.TracePacketDropped, sim.TraceProtocolInput, sim.TraceProtocolState, sim.TracePersistence,
		sim.TraceProcessLifecycle, sim.TraceProtocolDrop,
	}
	for _, kind := range kinds {
		if !knownKind(kind) {
			t.Errorf("knownKind(%q) = false", kind)
		}
	}
	if knownKind("not_real") {
		t.Fatal("known-kind table accepted an unknown kind")
	}
}
