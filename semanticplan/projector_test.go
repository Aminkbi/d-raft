package semanticplan

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
)

func TestPortableKeyKnownAnswersAcrossAdapters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		kind      decision.Kind
		reference string
		etcd      string
		want      PortableKey
	}{
		{
			name: "election", kind: decision.ElectionTimeout,
			reference: `{"node":"node-a","incarnation":18446744073709551615,"generation":42}`,
			etcd:      `{"generation":42,"node":"node-a","incarnation":18446744073709551615}`,
			want:      PortableKey{Kind: decision.ElectionTimeout, Node: "node-a", Incarnation: artifact.Uint64(math.MaxUint64), Occurrence: 42},
		},
		{
			name: "storage", kind: decision.StorageLatency,
			reference: `{"node":"node-a","incarnation":7,"generation":9,"token":88}`,
			etcd:      `{"node":"node-a","incarnation":7,"generation":9}`,
			want:      PortableKey{Kind: decision.StorageLatency, Node: "node-a", Incarnation: 7, Occurrence: 9},
		},
		{
			name: "loss", kind: decision.NetworkLoss,
			reference: `{"from":"node-a","to":"node-b","sender_incarnation":7,"send_sequence":9,"message":{"type":"append_entries","term":44,"prev_log_index":13},"min_latency_ns":1,"max_latency_ns":5,"loss_probability":0.25}`,
			etcd:      `{"from":"node-a","to":"node-b","sender_incarnation":7,"send_sequence":9,"message_protobuf":"CAEQAQ==","min_latency_ns":1,"max_latency_ns":5,"loss_probability":0.25}`,
			want:      PortableKey{Kind: decision.NetworkLoss, From: "node-a", To: "node-b", Incarnation: 7, Occurrence: 9},
		},
		{
			name: "latency", kind: decision.NetworkLatency,
			reference: `{"from":"node-a","to":"node-b","sender_incarnation":7,"send_sequence":9,"message":{"type":"request_vote","term":99,"last_log_index":31},"min_latency_ns":1,"max_latency_ns":5,"loss_probability":0.25}`,
			etcd:      `{"from":"node-a","to":"node-b","sender_incarnation":7,"send_sequence":9,"message_protobuf":"CP8B","min_latency_ns":1,"max_latency_ns":5,"loss_probability":0.25}`,
			want:      PortableKey{Kind: decision.NetworkLatency, From: "node-a", To: "node-b", Incarnation: 7, Occurrence: 9},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reference, err := PortableKeyForChoice(choiceFor(test.kind, test.reference, 1, 5))
			if err != nil {
				t.Fatalf("reference key: %v", err)
			}
			etcd, err := PortableKeyForChoice(choiceFor(test.kind, test.etcd, 1, 5))
			if err != nil {
				t.Fatalf("etcd key: %v", err)
			}
			if reference != test.want || etcd != test.want {
				t.Fatalf("keys differ:\nreference=%+v\netcd=%+v\nwant=%+v", reference, etcd, test.want)
			}
		})
	}

	raw, err := json.Marshal(tests[0].want)
	if err != nil {
		t.Fatal(err)
	}
	const wantJSON = `{"kind":"election_timeout","node":"node-a","incarnation":"18446744073709551615","occurrence":"42"}`
	if string(raw) != wantJSON {
		t.Fatalf("portable key JSON = %s, want %s", raw, wantJSON)
	}
}

func TestDirectivesFromTapeUsesFullIndexesAndOmitsFixedChoices(t *testing.T) {
	t.Parallel()

	fixed := choiceFor(decision.StorageLatency, `{"node":"a","incarnation":1,"generation":1,"token":1}`, 3, 3)
	election := choiceFor(decision.ElectionTimeout, `{"node":"a","incarnation":1,"generation":2}`, 4, 9)
	loss := choiceFor(decision.NetworkLoss, `{"from":"a","to":"b","sender_incarnation":1,"send_sequence":7,"message":{}}`, 0, 0)
	entries := []decision.Entry{
		mustEntry(t, fixed, numberSelection(3)),
		mustEntry(t, election, numberSelection(8)),
		mustEntry(t, loss, decision.Selection{Option: "drop"}),
	}
	directives, err := DirectivesFromTape(decision.Tape{Schema: decision.SchemaVersion, Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	want := []Directive{
		{Key: PortableKey{Kind: decision.ElectionTimeout, Node: "a", Incarnation: 1, Occurrence: 2}, SourceIndex: 1, Selection: numberSelection(8)},
		{Key: PortableKey{Kind: decision.NetworkLoss, From: "a", To: "b", Incarnation: 1, Occurrence: 7}, SourceIndex: 2, Selection: decision.Selection{Option: "drop"}},
	}
	if !reflect.DeepEqual(directives, want) {
		t.Fatalf("directives = %#v, want %#v", directives, want)
	}

	bad := decision.Tape{Schema: "wrong"}
	if _, err := DirectivesFromTape(bad); !errors.Is(err, ErrInvalidProjection) {
		t.Fatalf("invalid tape error = %v", err)
	}
}

func TestProjectorExactAcrossAdapterContextsAndFixedChoice(t *testing.T) {
	t.Parallel()

	directives := []Directive{
		{Key: PortableKey{Kind: decision.ElectionTimeout, Node: "a", Incarnation: 1, Occurrence: 2}, SourceIndex: 4, Selection: numberSelection(8)},
		{Key: PortableKey{Kind: decision.NetworkLoss, From: "a", To: "b", Incarnation: 1, Occurrence: 7}, SourceIndex: 9, Selection: decision.Selection{Option: "drop"}},
	}
	projector, err := NewProjector(directives, 123)
	if err != nil {
		t.Fatal(err)
	}
	election := choiceFor(decision.ElectionTimeout, `{"generation":2,"incarnation":1,"node":"a"}`, 4, 9)
	if got, err := projector.Choose(election); err != nil || !selectionEqual(got, numberSelection(8)) {
		t.Fatalf("election selection = %#v, %v", got, err)
	}
	fixed := choiceFor(decision.StorageLatency, `{"node":"a","incarnation":1,"generation":3}`, 5, 5)
	if got, err := projector.Choose(fixed); err != nil || !selectionEqual(got, numberSelection(5)) {
		t.Fatalf("fixed selection = %#v, %v", got, err)
	}
	loss := choiceFor(decision.NetworkLoss, `{"from":"a","to":"b","sender_incarnation":1,"send_sequence":7,"message_protobuf":"CAE="}`, 0, 0)
	if got, err := projector.Choose(loss); err != nil || got.Option != "drop" {
		t.Fatalf("loss selection = %#v, %v", got, err)
	}
	report := projector.Finish()
	if report.Fidelity != ProjectionExact || report.Directives != 2 || report.Projected != 2 || report.Fixed != 1 || len(report.Additional) != 0 || len(report.Unmatched) != 0 {
		t.Fatalf("report = %+v", report)
	}
}

func TestProjectorPartialUsesPinnedFallbackAndSortsCoverage(t *testing.T) {
	t.Parallel()

	unmatchedKey := PortableKey{Kind: decision.NetworkLoss, From: "z", To: "a", Incarnation: 2, Occurrence: 3}
	matchedKey := PortableKey{Kind: decision.ElectionTimeout, Node: "a", Incarnation: 1, Occurrence: 2}
	directives := []Directive{
		{Key: unmatchedKey, SourceIndex: 2, Selection: decision.Selection{Option: "deliver"}},
		{Key: matchedKey, SourceIndex: 8, Selection: numberSelection(7)},
	}
	projector, err := NewProjector(directives, 991)
	if err != nil {
		t.Fatal(err)
	}
	matched := choiceFor(decision.ElectionTimeout, `{"node":"a","incarnation":1,"generation":2}`, 5, 9)
	if _, err := projector.Choose(matched); err != nil {
		t.Fatal(err)
	}
	additionalB := choiceFor(decision.NetworkLatency, `{"from":"b","to":"c","sender_incarnation":1,"send_sequence":2}`, 10, 99)
	additionalA := choiceFor(decision.ElectionTimeout, `{"node":"b","incarnation":1,"generation":1}`, 10, 99)
	fallback := decision.NewSeedDecider(991)
	for _, choice := range []decision.Choice{additionalB, additionalA} {
		want, err := fallback.Choose(choice)
		if err != nil {
			t.Fatal(err)
		}
		got, err := projector.Choose(choice)
		if err != nil || !selectionEqual(got, want) {
			t.Fatalf("fallback = %#v, %v, want %#v", got, err, want)
		}
	}
	report := projector.Finish()
	wantAdditional := []PortableKey{
		{Kind: decision.ElectionTimeout, Node: "b", Incarnation: 1, Occurrence: 1},
		{Kind: decision.NetworkLatency, From: "b", To: "c", Incarnation: 1, Occurrence: 2},
	}
	if report.Fidelity != ProjectionPartial || report.Projected != 1 || !reflect.DeepEqual(report.Additional, wantAdditional) || len(report.Unmatched) != 1 || report.Unmatched[0].Key != unmatchedKey {
		t.Fatalf("report = %+v", report)
	}

	// Finish returns independent slices.
	report.Additional[0].Node = "changed"
	if projector.Finish().Additional[0].Node == "changed" {
		t.Fatal("Finish exposed internal additional slice")
	}
}

func TestProjectorFixedChoiceBypassesDirective(t *testing.T) {
	t.Parallel()

	key := PortableKey{Kind: decision.StorageLatency, Node: "a", Incarnation: 1, Occurrence: 1}
	projector, err := NewProjector([]Directive{{Key: key, SourceIndex: 0, Selection: numberSelection(99)}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	choice := choiceFor(decision.StorageLatency, `{"node":"a","incarnation":1,"generation":1}`, 5, 5)
	got, err := projector.Choose(choice)
	if err != nil || !selectionEqual(got, numberSelection(5)) {
		t.Fatalf("fixed = %#v, %v", got, err)
	}
	report := projector.Finish()
	if report.Fidelity != ProjectionPartial || report.Fixed != 1 || report.Projected != 0 || len(report.Unmatched) != 1 {
		t.Fatalf("report = %+v", report)
	}
	*report.Unmatched[0].Selection.Number = 0
	if got := *projector.Finish().Unmatched[0].Selection.Number; got != 99 {
		t.Fatalf("Finish exposed internal directive selection: %d", got)
	}
}

func TestProjectorFailsClosed(t *testing.T) {
	t.Parallel()

	key := PortableKey{Kind: decision.ElectionTimeout, Node: "a", Incarnation: 1, Occurrence: 1}
	t.Run("duplicate directives", func(t *testing.T) {
		directive := Directive{Key: key, SourceIndex: 1, Selection: numberSelection(2)}
		other := directive
		other.SourceIndex = 2
		if _, err := NewProjector([]Directive{directive, other}, 1); !errors.Is(err, ErrInvalidProjection) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("source order", func(t *testing.T) {
		directives := []Directive{
			{Key: key, SourceIndex: 2, Selection: numberSelection(2)},
			{Key: PortableKey{Kind: decision.StorageLatency, Node: "a", Incarnation: 1, Occurrence: 2}, SourceIndex: 1, Selection: numberSelection(2)},
		}
		if err := ValidateDirectives(directives); !errors.Is(err, ErrInvalidProjection) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("source index beyond tape bound", func(t *testing.T) {
		directive := Directive{Key: key, SourceIndex: artifact.Uint64(artifact.MaxDecisions), Selection: numberSelection(2)}
		if err := ValidateDirectives([]Directive{directive}); !errors.Is(err, ErrInvalidProjection) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("out of domain", func(t *testing.T) {
		projector, err := NewProjector([]Directive{{Key: key, SourceIndex: 0, Selection: numberSelection(9)}}, 1)
		if err != nil {
			t.Fatal(err)
		}
		choice := choiceFor(decision.ElectionTimeout, `{"node":"a","incarnation":1,"generation":1}`, 1, 2)
		if _, err := projector.Choose(choice); !errors.Is(err, ErrInvalidProjection) {
			t.Fatalf("error = %v", err)
		}
		if projector.Finish().Fidelity != ProjectionFailed || projector.Err() == nil {
			t.Fatalf("failed projector report = %+v, err = %v", projector.Finish(), projector.Err())
		}
		if _, err := projector.Choose(choice); err != projector.Err() {
			t.Fatalf("retry error = %v, want first error %v", err, projector.Err())
		}
	})
	t.Run("duplicate target", func(t *testing.T) {
		projector, err := NewProjector(nil, 1)
		if err != nil {
			t.Fatal(err)
		}
		choice := choiceFor(decision.StorageLatency, `{"node":"a","incarnation":1,"generation":1}`, 2, 2)
		if _, err := projector.Choose(choice); err != nil {
			t.Fatal(err)
		}
		if _, err := projector.Choose(choice); !errors.Is(err, ErrDuplicateTarget) {
			t.Fatalf("error = %v", err)
		}
		if projector.Finish().Fidelity != ProjectionFailed {
			t.Fatalf("report = %+v", projector.Finish())
		}
	})
}

func TestPortableKeyRejectsMalformedOrNoncanonicalContexts(t *testing.T) {
	t.Parallel()

	tests := []string{
		`null`,
		`{"node":"a","incarnation":1}`,
		`{"node":"a","incarnation":1,"generation":0}`,
		`{"node":"a","incarnation":"1","generation":1}`,
		`{"node":"a","incarnation":1e0,"generation":1}`,
		`{"node":"a","incarnation":01,"generation":1}`,
		`{"node":"a","incarnation":1,"incarnation":2,"generation":1}`,
		`{"node":"a","incarnation":1,"generation":1,"diagnostic":{"name":1,"name":2}}`,
		`{"node":"a","incarnation":1,"sender_incarnation":1,"generation":1}`,
		`{"node":"a","from":"b","incarnation":1,"generation":1}`,
		`{"node":"\u0000","incarnation":1,"generation":1}`,
		`{"node":"a","incarnation":18446744073709551616,"generation":1}`,
		`{"node":"a","incarnation":1,"generation":1} true`,
	}
	for _, context := range tests {
		choice := choiceFor(decision.ElectionTimeout, context, 1, 2)
		if _, err := PortableKeyForChoice(choice); !errors.Is(err, ErrInvalidProjection) {
			t.Errorf("context %s: error = %v", context, err)
		}
	}

	unsupported := choiceFor(decision.ClientAction, `{"node":"a","incarnation":1,"generation":1}`, 1, 2)
	if _, err := PortableKeyForChoice(unsupported); !errors.Is(err, ErrInvalidProjection) {
		t.Fatalf("unsupported error = %v", err)
	}
	badDomain := choiceFor(decision.ElectionTimeout, `{"node":"a","incarnation":1,"generation":1}`, 1, 2)
	badDomain.Min, badDomain.Max = nil, nil
	badDomain.Options = []decision.Option{{ID: "x", Weight: 1}}
	if _, err := PortableKeyForChoice(badDomain); !errors.Is(err, ErrInvalidProjection) {
		t.Fatalf("domain error = %v", err)
	}
}

func choiceFor(kind decision.Kind, context string, minimum, maximum int64) decision.Choice {
	choice := decision.Choice{ID: "choice", Kind: kind, Context: json.RawMessage(context)}
	if kind == decision.NetworkLoss {
		choice.Options = []decision.Option{{ID: "drop", Weight: 1}, {ID: "deliver", Weight: 3}}
	} else {
		choice.Min, choice.Max = &minimum, &maximum
	}
	return choice
}

func numberSelection(value int64) decision.Selection {
	return decision.Selection{Number: &value}
}

func mustEntry(t *testing.T, choice decision.Choice, selection decision.Selection) decision.Entry {
	t.Helper()
	entry, err := decision.NewEntry(choice, selection)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func selectionEqual(left, right decision.Selection) bool {
	if left.Option != right.Option || (left.Number == nil) != (right.Number == nil) {
		return false
	}
	return left.Number == nil || *left.Number == *right.Number
}

var _ decision.Decider = (*Projector)(nil)
var _ raft.NodeID = PortableKey{}.Node
