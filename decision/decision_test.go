package decision

import (
	"errors"
	"reflect"
	"testing"
)

func TestRecordAndReplay(t *testing.T) {
	t.Parallel()

	minValue, maxValue := int64(10), int64(20)
	choices := []Choice{
		{ID: "timer/a/1", Kind: ElectionTimeout, Min: &minValue, Max: &maxValue},
		{ID: "network/a/1/loss", Kind: NetworkLoss, Options: []Option{{ID: "deliver", Weight: 3}, {ID: "drop", Weight: 1}}},
	}
	recorder := NewRecorder(NewSeedDecider(42))
	var selected []Selection
	for _, choice := range choices {
		selection, err := recorder.Choose(choice)
		if err != nil {
			t.Fatalf("record Choose: %v", err)
		}
		selected = append(selected, selection)
	}
	replay, err := NewTapeDecider(recorder.Tape())
	if err != nil {
		t.Fatalf("NewTapeDecider: %v", err)
	}
	var replayed []Selection
	for _, choice := range choices {
		selection, err := replay.Choose(choice)
		if err != nil {
			t.Fatalf("replay Choose: %v", err)
		}
		replayed = append(replayed, selection)
	}
	if err := replay.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if !reflect.DeepEqual(selected, replayed) {
		t.Fatalf("selected=%+v replayed=%+v", selected, replayed)
	}
}

func TestTapeRejectsDomainDrift(t *testing.T) {
	t.Parallel()

	choice := Choice{ID: "fault/1", Kind: FaultAction, Options: []Option{{ID: "none", Weight: 1}, {ID: "crash", Weight: 1}}}
	recorder := NewRecorder(NewSeedDecider(1))
	if _, err := recorder.Choose(choice); err != nil {
		t.Fatal(err)
	}
	replay, _ := NewTapeDecider(recorder.Tape())
	drifted := choice
	drifted.Options[1].Weight = 2
	if _, err := replay.Choose(drifted); !errors.Is(err, ErrTapeMismatch) {
		t.Fatalf("domain drift error = %v", err)
	}
}

func TestTapeRejectsContextDrift(t *testing.T) {
	t.Parallel()

	choice := Choice{ID: "network/a/1/loss", Kind: NetworkLoss, Options: []Option{{ID: "deliver", Weight: 1}}, Context: []byte(`{"to":"b","term":4}`)}
	recorder := NewRecorder(NewSeedDecider(1))
	if _, err := recorder.Choose(choice); err != nil {
		t.Fatal(err)
	}
	replay, _ := NewTapeDecider(recorder.Tape())
	drifted := choice
	drifted.Context = []byte(`{"to":"c","term":4}`)
	if _, err := replay.Choose(drifted); !errors.Is(err, ErrTapeMismatch) {
		t.Fatalf("context drift error = %v", err)
	}
}

func TestContextDigestIgnoresWhitespace(t *testing.T) {
	t.Parallel()

	left := Choice{ID: "context", Kind: ClientAction, Options: []Option{{ID: "run", Weight: 1}}, Context: []byte(`{"term":4}`)}
	right := left
	right.Context = []byte(" { \n \t \"term\" : 4 } ")
	leftDigest, leftErr := ContextDigest(left)
	rightDigest, rightErr := ContextDigest(right)
	if leftErr != nil || rightErr != nil || leftDigest != rightDigest {
		t.Fatalf("left=%q/%v right=%q/%v", leftDigest, leftErr, rightDigest, rightErr)
	}
}

func TestSeedDeciderWeightedBoundaries(t *testing.T) {
	t.Parallel()

	only := Choice{ID: "loss", Kind: NetworkLoss, Options: []Option{{ID: "deliver", Weight: 1}}}
	decider := NewSeedDecider(0)
	for range 100 {
		selection, err := decider.Choose(only)
		if err != nil || selection.Option != "deliver" {
			t.Fatalf("selection=%+v err=%v", selection, err)
		}
	}
}

func TestSeedDeciderDoesNotDrawForSingletonDomains(t *testing.T) {
	t.Parallel()

	minimum, maximum := int64(10), int64(20)
	fixed := int64(7)
	variable := Choice{ID: "variable", Kind: ElectionTimeout, Min: &minimum, Max: &maximum}
	direct, err := NewSeedDecider(99).Choose(variable)
	if err != nil {
		t.Fatal(err)
	}
	decider := NewSeedDecider(99)
	if _, err := decider.Choose(Choice{ID: "only", Kind: NetworkLoss, Options: []Option{{ID: "deliver", Weight: 1}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := decider.Choose(Choice{ID: "fixed", Kind: StorageLatency, Min: &fixed, Max: &fixed}); err != nil {
		t.Fatal(err)
	}
	afterSingletons, err := decider.Choose(variable)
	if err != nil || !reflect.DeepEqual(direct, afterSingletons) {
		t.Fatalf("direct=%+v after=%+v err=%v", direct, afterSingletons, err)
	}
}

func TestChoiceValidation(t *testing.T) {
	t.Parallel()

	if err := ValidateChoice(Choice{}); !errors.Is(err, ErrInvalidChoice) {
		t.Fatalf("empty choice error = %v", err)
	}
	zero := int64(0)
	if err := ValidateChoice(Choice{ID: "both", Kind: ClientAction, Options: []Option{{ID: "x", Weight: 1}}, Min: &zero, Max: &zero}); !errors.Is(err, ErrInvalidChoice) {
		t.Fatalf("mixed domain error = %v", err)
	}
}
