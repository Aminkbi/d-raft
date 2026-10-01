package semanticplan

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
)

func TestCausalProjectorPreservesOperationDisposition(t *testing.T) {
	id := "00000000000000000000000000000001"
	p, err := NewCausalProjector([]CausalDirective{{OperationID: id, SourceIndices: []artifact.Uint64{0}, Selection: decision.Selection{Option: "drop"}}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	choice := lossChoice(t, []string{id, "00000000000000000000000000000002"})
	if _, err := p.Choose(choice); !errors.Is(err, ErrCausalConflict) {
		t.Fatalf("mixed drop batch error = %v", err)
	}
}

func TestCausalProjectorHandlesFixedLossDomains(t *testing.T) {
	unknown := "00000000000000000000000000000001"
	p, err := NewCausalProjector(nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	choice := decision.Choice{ID: "fixed-drop", Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "drop", Weight: 1}}, Context: mustContext(t, []string{unknown})}
	selection, err := p.Choose(choice)
	if err != nil || selection.Option != "drop" {
		t.Fatalf("fixed drop selection = %#v, %v", selection, err)
	}
	if report := p.Finish(); report.Fidelity != ProjectionPartial || len(report.Additional) != 1 {
		t.Fatalf("fixed drop report = %#v", report)
	}
}

func mustContext(t *testing.T, ids []string) []byte {
	t.Helper()
	context, err := json.Marshal(struct {
		Causal       bool     `json:"causal"`
		OperationIDs []string `json:"operation_ids"`
	}{true, ids})
	if err != nil {
		t.Fatal(err)
	}
	return context
}

func TestCausalDirectivesRejectConflictingSource(t *testing.T) {
	id := "00000000000000000000000000000001"
	makeTape := func(first, second string) decision.Tape {
		tape := decision.Tape{Schema: decision.SchemaVersion, Entries: []decision.Entry{}}
		for index, option := range []string{first, second} {
			choice := lossChoice(t, []string{id})
			selection := decision.Selection{Option: option}
			recorder := decision.NewRecorder(&fixedDecider{selection: selection})
			_, _ = recorder.Choose(choice)
			entry := recorder.Tape().Entries[0]
			_ = index
			tape.Entries = append(tape.Entries, entry)
		}
		return tape
	}
	if _, err := CausalDirectivesFromTape(makeTape("drop", "deliver")); !errors.Is(err, ErrCausalConflict) {
		t.Fatalf("conflict error = %v", err)
	}
}

func TestCausalSourceRejectsOrdinaryTape(t *testing.T) {
	id := "00000000000000000000000000000001"
	context, err := json.Marshal(struct {
		OperationIDs []string `json:"operation_ids"`
	}{[]string{id}})
	if err != nil {
		t.Fatal(err)
	}
	choice := decision.Choice{ID: "loss", Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "deliver", Weight: 1}}, Context: context}
	recorder := decision.NewRecorder(&fixedDecider{selection: decision.Selection{Option: "deliver"}})
	if _, err := recorder.Choose(choice); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCausalTape(recorder.Tape()); !errors.Is(err, ErrInvalidCausal) {
		t.Fatalf("ordinary source error = %v", err)
	}
}

func TestCausalProjectionReportStrictRoundTrip(t *testing.T) {
	report := CausalProjectionReport{
		Schema: CausalReplaySchema, Fidelity: ProjectionPartial, Directives: 1,
		Projected: 1, Additional: []string{"network/extra"}, Unmatched: []CausalDirective{},
	}
	if err := report.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCausalProjectionReport(bytes.NewReader(raw))
	if err != nil || !reflect.DeepEqual(decoded, report) {
		t.Fatalf("decoded report = %#v, %v", decoded, err)
	}
	for _, document := range []string{
		`{"schema":"d-raft.causal-replay/v1","fidelity":"partial","directives":1,"projected":1,"additional":[],"unmatched":[],"extra":true}`,
		string(raw) + `{}`,
	} {
		if _, err := DecodeCausalProjectionReport(bytes.NewBufferString(document)); !errors.Is(err, ErrInvalidCausal) {
			t.Fatalf("invalid report error = %v", err)
		}
	}
}

type fixedDecider struct{ selection decision.Selection }

func (d *fixedDecider) Choose(decision.Choice) (decision.Selection, error) { return d.selection, nil }

func lossChoice(t *testing.T, ids []string) decision.Choice {
	t.Helper()
	return decision.Choice{ID: "loss", Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "drop", Weight: 1}, {ID: "deliver", Weight: 1}}, Context: mustContext(t, ids)}
}
