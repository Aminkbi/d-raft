package sim

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"
)

func TestJSONLTraceCapturesOrderedSimulation(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	recorder := NewJSONLRecorder(&output)
	simulator := New()
	random := NewRand(99)
	router := mustRouter[map[string]int](t, simulator, random, LinkConfig{
		MinLatency: 2 * time.Millisecond,
		MaxLatency: 2 * time.Millisecond,
	}, func(message map[string]int) map[string]int {
		clone := make(map[string]int, len(message))
		for key, value := range message {
			clone[key] = value
		}
		return clone
	})
	simulator.SetTraceSink(recorder)
	random.SetTraceSink(recorder, "network")
	router.SetTraceSink(recorder)

	register(t, router, "a", func(Packet[map[string]int]) {})
	register(t, router, "b", func(Packet[map[string]int]) {})
	partition, err := NewPartitions([]NodeID{"a"}, []NodeID{"b"})
	if err != nil {
		t.Fatalf("NewPartitions: %v", err)
	}
	router.SetPartition(partition)
	if _, err := router.Send("a", "b", map[string]int{"term": 3}); err != nil {
		t.Fatalf("partitioned Send: %v", err)
	}
	router.SetPartition(nil)
	if _, err := router.Send("a", "b", map[string]int{"term": 4}); err != nil {
		t.Fatalf("connected Send: %v", err)
	}
	simulator.Run()

	if err := recorder.Err(); err != nil {
		t.Fatalf("trace recorder: %v", err)
	}
	records := decodeTraceRecords(t, output.Bytes())
	if len(records) == 0 {
		t.Fatal("trace is empty")
	}
	for index, record := range records {
		if record.Schema != TraceSchemaVersion {
			t.Fatalf("record %d schema = %q", index, record.Schema)
		}
		if record.Sequence != uint64(index+1) {
			t.Fatalf("record %d sequence = %d", index, record.Sequence)
		}
	}

	wantKinds := []TraceEventKind{
		TraceNodeRegistered,
		TraceNodeRegistered,
		TracePartitionChanged,
		TracePacketDropped,
		TracePartitionChanged,
		TraceRandomDraw,
		TraceRandomDraw,
		TraceEventScheduled,
		TracePacketScheduled,
		TraceEventExecuted,
		TracePacketDelivered,
	}
	gotKinds := make([]TraceEventKind, len(records))
	for index := range records {
		gotKinds[index] = records[index].Kind
	}
	if !slicesEqual(gotKinds, wantKinds) {
		t.Fatalf("trace kinds = %v, want %v", gotKinds, wantKinds)
	}

	dropped := records[3]
	message, ok := dropped.Message.(map[string]any)
	if !ok || message["term"] != float64(3) || dropped.DropReason != "partition" {
		t.Fatalf("dropped packet trace = %+v", dropped)
	}
	scheduled := records[8]
	if scheduled.DeliveryAtNS == nil || *scheduled.DeliveryAtNS != int64(2*time.Millisecond) {
		t.Fatalf("scheduled delivery = %v", scheduled.DeliveryAtNS)
	}
}

func TestRandTraceLabelsSplitStreams(t *testing.T) {
	t.Parallel()

	var events []TraceEvent
	random := NewRand(42)
	random.SetTraceSink(TraceFunc(func(event TraceEvent) { events = append(events, event) }), "node-1")
	child := random.Split()
	child.IntN(10)

	if len(events) != 2 {
		t.Fatalf("trace event count = %d, want 2", len(events))
	}
	if events[0].RandomOperation != "split" || events[0].RandomArguments["child_stream"] != "node-1/1" {
		t.Fatalf("split trace = %+v", events[0])
	}
	if events[1].RandomStream != "node-1/1" || events[1].RandomOperation != "intn" {
		t.Fatalf("child trace = %+v", events[1])
	}
}

func TestJSONLRecorderReportsUnsupportedMessage(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	recorder := NewJSONLRecorder(&output)
	recorder.RecordTrace(TraceEvent{Kind: TracePacketDropped, Message: make(chan int)})
	if recorder.Err() == nil {
		t.Fatal("recorder accepted a channel message")
	}
	if output.Len() != 0 {
		t.Fatalf("encoding failure wrote %d bytes", output.Len())
	}
}

func TestJSONLRecorderRejectsNilWriter(t *testing.T) {
	t.Parallel()

	recorder := NewJSONLRecorder(nil)
	recorder.RecordTrace(TraceEvent{Kind: TraceClockAdvanced})
	if recorder.Err() == nil {
		t.Fatal("recorder accepted a nil writer")
	}
}

func TestJSONLRecorderReportsShortWrite(t *testing.T) {
	t.Parallel()

	recorder := NewJSONLRecorder(shortWriter{})
	recorder.RecordTrace(TraceEvent{Kind: TraceClockAdvanced})
	if !errors.Is(recorder.Err(), io.ErrShortWrite) {
		t.Fatalf("short write error = %v", recorder.Err())
	}
}

type shortWriter struct{}

func (shortWriter) Write(data []byte) (int, error) {
	return len(data) / 2, nil
}

func decodeTraceRecords(t *testing.T, data []byte) []TraceRecord {
	t.Helper()
	var records []TraceRecord
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var record TraceRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode trace: %v", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan trace: %v", err)
	}
	return records
}
