package decision

import (
	"errors"
	"testing"
)

func TestPrefixDeciderOpensAfterExactPrefix(t *testing.T) {
	t.Parallel()

	minimum, maximum := int64(1), int64(3)
	first := Choice{ID: "first", Kind: ElectionTimeout, Min: &minimum, Max: &maximum}
	second := Choice{ID: "second", Kind: NetworkLoss, Options: []Option{{ID: "drop", Weight: 1}, {ID: "deliver", Weight: 1}}}
	entry, err := NewEntry(first, Selection{Number: &minimum})
	if err != nil {
		t.Fatal(err)
	}
	decider, err := NewPrefixDecider(Tape{Schema: SchemaVersion, Entries: []Entry{entry}})
	if err != nil {
		t.Fatal(err)
	}
	if selection, err := decider.Choose(first); err != nil || selection.Number == nil || *selection.Number != minimum {
		t.Fatalf("selection=%+v err=%v", selection, err)
	}
	_, err = decider.Choose(second)
	var open *OpenChoiceError
	if !errors.As(err, &open) || open.Index != 1 || open.Choice.ID != second.ID {
		t.Fatalf("open error = %v", err)
	}
	if err := decider.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestRecorderSuffixIsIndependent(t *testing.T) {
	t.Parallel()

	choice := Choice{ID: "suffix", Kind: NetworkLoss, Options: []Option{{ID: "deliver", Weight: 1}}, Context: []byte(`{"node":"a"}`)}
	recorder := NewRecorder(fixedDecider{selection: Selection{Option: "deliver"}})
	if _, err := recorder.Choose(choice); err != nil {
		t.Fatal(err)
	}
	if recorder.Len() != 1 {
		t.Fatalf("recorder length = %d, want 1", recorder.Len())
	}
	suffix := recorder.Suffix(0)
	if len(suffix) != 1 {
		t.Fatalf("suffix length = %d, want 1", len(suffix))
	}
	suffix[0].Choice.Options[0].ID = "changed"
	suffix[0].Choice.Context[0] = '['
	suffix[0].Selection.Option = "changed"
	tape := recorder.Tape()
	if tape.Entries[0].Choice.Options[0].ID != "deliver" || string(tape.Entries[0].Choice.Context) != `{"node":"a"}` || tape.Entries[0].Selection.Option != "deliver" {
		t.Fatalf("suffix mutation escaped recorder: %+v", tape.Entries[0])
	}
	if empty := recorder.Suffix(1); empty == nil || len(empty) != 0 {
		t.Fatalf("empty suffix = %#v", empty)
	}
	if suffix := recorder.Suffix(-1); suffix != nil {
		t.Fatalf("negative suffix = %#v", suffix)
	}
	if suffix := recorder.Suffix(2); suffix != nil {
		t.Fatalf("out-of-range suffix = %#v", suffix)
	}
}

func TestPrefixThenAndGuidedFallback(t *testing.T) {
	t.Parallel()

	choice := Choice{ID: "loss", Kind: NetworkLoss, Options: []Option{{ID: "drop", Weight: 1}, {ID: "deliver", Weight: 1}}}
	entry, _ := NewEntry(choice, Selection{Option: "deliver"})
	tape := Tape{Schema: SchemaVersion, Entries: []Entry{entry}}
	fallback := fixedDecider{selection: Selection{Option: "drop"}}
	prefix, err := NewPrefixThenDecider(tape, fallback)
	if err != nil {
		t.Fatal(err)
	}
	if selection, err := prefix.Choose(choice); err != nil || selection.Option != "deliver" {
		t.Fatalf("prefix selection=%+v err=%v", selection, err)
	}
	if selection, err := prefix.Choose(Choice{ID: "other", Kind: NetworkLoss, Options: choice.Options}); err != nil || selection.Option != "drop" {
		t.Fatalf("fallback selection=%+v err=%v", selection, err)
	}
	if err := prefix.Finish(); err != nil {
		t.Fatal(err)
	}

	guided, err := NewGuidedDecider(tape, fallback)
	if err != nil {
		t.Fatal(err)
	}
	if selection, _ := guided.Choose(choice); selection.Option != "deliver" {
		t.Fatalf("guided selection=%+v", selection)
	}
	drifted := choice
	drifted.Context = []byte(`{"to":"new"}`)
	if selection, _ := guided.Choose(drifted); selection.Option != "drop" {
		t.Fatalf("drift fallback=%+v", selection)
	}
}

type fixedDecider struct{ selection Selection }

func (d fixedDecider) Choose(Choice) (Selection, error) { return d.selection, nil }
