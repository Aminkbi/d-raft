package decision

import (
	"errors"
	"fmt"
)

var ErrOpenChoice = errors.New("decision: open choice")

// OpenChoiceError reports the first semantic choice beyond a replay prefix.
type OpenChoiceError struct {
	Index  int
	Choice Choice
}

func (e *OpenChoiceError) Error() string {
	return fmt.Sprintf("%s at choice %d: %s", ErrOpenChoice, e.Index, e.Choice.ID)
}

func (e *OpenChoiceError) Unwrap() error { return ErrOpenChoice }

// NewEntry validates and constructs one self-describing tape entry.
func NewEntry(choice Choice, selection Selection) (Entry, error) {
	if err := ValidateSelection(choice, selection); err != nil {
		return Entry{}, err
	}
	domain, err := DomainDigest(choice)
	if err != nil {
		return Entry{}, err
	}
	context, err := ContextDigest(choice)
	if err != nil {
		return Entry{}, err
	}
	return Entry{Choice: cloneChoice(choice), DomainDigest: domain, ContextDigest: context, Selection: cloneSelection(selection)}, nil
}

// PrefixDecider exactly consumes a fixed tape prefix and returns OpenChoiceError
// for the next choice. It is intended for clean-rerun systematic exploration.
type PrefixDecider struct {
	replay *TapeDecider
	length int
}

func NewPrefixDecider(prefix Tape) (*PrefixDecider, error) {
	replay, err := NewTapeDecider(prefix)
	if err != nil {
		return nil, err
	}
	return &PrefixDecider{replay: replay, length: len(prefix.Entries)}, nil
}

func (d *PrefixDecider) Choose(choice Choice) (Selection, error) {
	if d.replay.index < d.length {
		return d.replay.Choose(choice)
	}
	if err := ValidateChoice(choice); err != nil {
		return Selection{}, err
	}
	return Selection{}, &OpenChoiceError{Index: d.length, Choice: cloneChoice(choice)}
}

func (d *PrefixDecider) Finish() error { return d.replay.Finish() }

// PrefixThenDecider consumes an exact prefix and delegates every later choice
// to fallback. Finish still requires the entire prefix to have been consumed.
type PrefixThenDecider struct {
	replay   *TapeDecider
	length   int
	fallback Decider
}

func NewPrefixThenDecider(prefix Tape, fallback Decider) (*PrefixThenDecider, error) {
	if fallback == nil {
		return nil, errors.New("decision: prefix fallback is nil")
	}
	replay, err := NewTapeDecider(prefix)
	if err != nil {
		return nil, err
	}
	return &PrefixThenDecider{replay: replay, length: len(prefix.Entries), fallback: fallback}, nil
}

func (d *PrefixThenDecider) Choose(choice Choice) (Selection, error) {
	if d.replay.index < d.length {
		return d.replay.Choose(choice)
	}
	return d.fallback.Choose(choice)
}

func (d *PrefixThenDecider) Finish() error { return d.replay.Finish() }

// GuidedDecider reuses a recorded selection only when the complete semantic
// choice identity matches. Unmatched choices are delegated to fallback.
type GuidedDecider struct {
	entries  map[string]Entry
	fallback Decider
}

func NewGuidedDecider(guide Tape, fallback Decider) (*GuidedDecider, error) {
	if fallback == nil {
		return nil, errors.New("decision: guided fallback is nil")
	}
	if _, err := NewTapeDecider(guide); err != nil {
		return nil, err
	}
	entries := make(map[string]Entry, len(guide.Entries))
	for _, entry := range guide.Entries {
		key := entryKey(entry)
		if _, exists := entries[key]; exists {
			return nil, fmt.Errorf("%w: duplicate guided choice %q", ErrTapeMismatch, entry.Choice.ID)
		}
		entries[key] = cloneTape(Tape{Entries: []Entry{entry}}).Entries[0]
	}
	return &GuidedDecider{entries: entries, fallback: fallback}, nil
}

func (d *GuidedDecider) Choose(choice Choice) (Selection, error) {
	key, err := choiceKey(choice)
	if err != nil {
		return Selection{}, err
	}
	if entry, exists := d.entries[key]; exists {
		return cloneSelection(entry.Selection), nil
	}
	return d.fallback.Choose(choice)
}

func choiceKey(choice Choice) (string, error) {
	domain, err := DomainDigest(choice)
	if err != nil {
		return "", err
	}
	context, err := ContextDigest(choice)
	if err != nil {
		return "", err
	}
	return choice.ID + "\x00" + string(choice.Kind) + "\x00" + domain + "\x00" + context, nil
}

func entryKey(entry Entry) string {
	return entry.Choice.ID + "\x00" + string(entry.Choice.Kind) + "\x00" + entry.DomainDigest + "\x00" + entry.ContextDigest
}
