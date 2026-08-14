package trace

import (
	"errors"
	"io"
	"strings"
	"testing"

	sim "github.com/aminkbi/d-raft"
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

func TestKnownKindCoverage(t *testing.T) {
	t.Parallel()

	if !knownKind(sim.TraceProtocolInput) || knownKind("not_real") {
		t.Fatal("known-kind table is incorrect")
	}
}
