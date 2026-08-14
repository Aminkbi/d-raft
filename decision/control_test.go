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
