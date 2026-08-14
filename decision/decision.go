// Package decision defines semantic choices, seeded selection, recording, and
// exact tape replay for deterministic distributed-system executions.
package decision

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"

	sim "github.com/aminkbi/d-raft"
)

const SchemaVersion = "d-raft.decisions/v1"

var (
	ErrInvalidChoice   = errors.New("decision: invalid choice")
	ErrTapeExhausted   = errors.New("decision: tape exhausted")
	ErrTapeMismatch    = errors.New("decision: tape choice mismatch")
	ErrTapeNotConsumed = errors.New("decision: tape has unconsumed decisions")
)

// Kind identifies the meaning of a semantic choice.
type Kind string

const (
	ElectionTimeout Kind = "election_timeout"
	NetworkLoss     Kind = "network_loss"
	NetworkLatency  Kind = "network_latency"
	StorageLatency  Kind = "storage_latency"
	FaultAction     Kind = "fault_action"
	ClientAction    Kind = "client_action"
)

// Option is one weighted discrete alternative. SeedDecider chooses in
// proportion to Weight. Weights must be positive.
type Option struct {
	ID     string `json:"id"`
	Weight uint64 `json:"weight"`
}

// Choice is either a discrete option domain or an inclusive non-negative
// integer range. Context does not alter the legal domain, but exact replay
// verifies its semantic digest.
type Choice struct {
	ID      string          `json:"id"`
	Kind    Kind            `json:"kind"`
	Options []Option        `json:"options,omitempty"`
	Min     *int64          `json:"min,omitempty"`
	Max     *int64          `json:"max,omitempty"`
	Context json.RawMessage `json:"context,omitempty"`
}

// Selection contains either Option or Number according to the Choice domain.
type Selection struct {
	Option string `json:"option,omitempty"`
	Number *int64 `json:"number,omitempty"`
}

// Entry is one replayable choice result.
type Entry struct {
	Choice        Choice    `json:"choice"`
	DomainDigest  string    `json:"domain_digest"`
	ContextDigest string    `json:"context_digest"`
	Selection     Selection `json:"selection"`
}

// Tape is an ordered semantic decision sequence.
type Tape struct {
	Schema  string  `json:"schema"`
	Entries []Entry `json:"entries"`
}

// Decider selects one valid alternative for each encountered Choice.
type Decider interface {
	Choose(Choice) (Selection, error)
}

// SeedDecider makes stable weighted and ranged selections.
type SeedDecider struct {
	random *sim.Rand
}

// NewSeedDecider returns a deterministic semantic decider.
func NewSeedDecider(seed uint64) *SeedDecider {
	return &SeedDecider{random: sim.NewRand(seed)}
}

// Choose implements Decider.
func (d *SeedDecider) Choose(choice Choice) (Selection, error) {
	if err := ValidateChoice(choice); err != nil {
		return Selection{}, err
	}
	if len(choice.Options) > 0 {
		if len(choice.Options) == 1 {
			return Selection{Option: choice.Options[0].ID}, nil
		}
		var total uint64
		for _, option := range choice.Options {
			if math.MaxUint64-total < option.Weight {
				return Selection{}, fmt.Errorf("%w: option weight overflow", ErrInvalidChoice)
			}
			total += option.Weight
		}
		draw := d.random.Uint64N(total)
		for _, option := range choice.Options {
			if draw < option.Weight {
				return Selection{Option: option.ID}, nil
			}
			draw -= option.Weight
		}
		panic("decision: unreachable weighted selection")
	}
	if *choice.Min == *choice.Max {
		value := *choice.Min
		return Selection{Number: &value}, nil
	}
	span := uint64(*choice.Max-*choice.Min) + 1
	value := *choice.Min + int64(d.random.Uint64N(span))
	return Selection{Number: &value}, nil
}

// Recorder decorates a Decider and records validated choices.
type Recorder struct {
	base    Decider
	entries []Entry
	err     error
}

// NewRecorder returns a recording decider.
func NewRecorder(base Decider) *Recorder {
	return &Recorder{base: base}
}

// Choose implements Decider.
func (r *Recorder) Choose(choice Choice) (Selection, error) {
	if r.err != nil {
		return Selection{}, r.err
	}
	if r.base == nil {
		r.err = errors.New("decision: recorder requires a base decider")
		return Selection{}, r.err
	}
	selection, err := r.base.Choose(choice)
	if err != nil {
		r.err = err
		return Selection{}, err
	}
	if err := ValidateSelection(choice, selection); err != nil {
		r.err = err
		return Selection{}, err
	}
	digest, err := DomainDigest(choice)
	if err != nil {
		r.err = err
		return Selection{}, err
	}
	contextDigest, err := ContextDigest(choice)
	if err != nil {
		r.err = err
		return Selection{}, err
	}
	r.entries = append(r.entries, Entry{Choice: cloneChoice(choice), DomainDigest: digest, ContextDigest: contextDigest, Selection: cloneSelection(selection)})
	return selection, nil
}

// Tape returns an independent snapshot of the recorded decisions.
func (r *Recorder) Tape() Tape {
	return cloneTape(Tape{Schema: SchemaVersion, Entries: r.entries})
}

// Err returns the first selection or recording error.
func (r *Recorder) Err() error {
	return r.err
}

// TapeDecider replays and validates an exact semantic tape.
type TapeDecider struct {
	tape  Tape
	index int
}

// NewTapeDecider validates tape metadata and returns a replay decider.
func NewTapeDecider(tape Tape) (*TapeDecider, error) {
	if tape.Schema != SchemaVersion {
		return nil, fmt.Errorf("%w: schema %q", ErrTapeMismatch, tape.Schema)
	}
	for index, entry := range tape.Entries {
		domainDigest, err := DomainDigest(entry.Choice)
		if err != nil || domainDigest != entry.DomainDigest {
			return nil, fmt.Errorf("%w at choice %d: invalid stored domain", ErrTapeMismatch, index)
		}
		contextDigest, err := ContextDigest(entry.Choice)
		if err != nil || contextDigest != entry.ContextDigest {
			return nil, fmt.Errorf("%w at choice %d: invalid stored context", ErrTapeMismatch, index)
		}
		if err := ValidateSelection(entry.Choice, entry.Selection); err != nil {
			return nil, fmt.Errorf("%w at choice %d: invalid stored selection: %v", ErrTapeMismatch, index, err)
		}
	}
	return &TapeDecider{tape: cloneTape(tape)}, nil
}

// Choose implements Decider and rejects the first identity, kind, domain, or
// selection mismatch.
func (d *TapeDecider) Choose(choice Choice) (Selection, error) {
	if d.index >= len(d.tape.Entries) {
		return Selection{}, fmt.Errorf("%w at choice %d: %s", ErrTapeExhausted, d.index, choice.ID)
	}
	entry := d.tape.Entries[d.index]
	digest, err := DomainDigest(choice)
	if err != nil {
		return Selection{}, err
	}
	contextDigest, err := ContextDigest(choice)
	if err != nil {
		return Selection{}, err
	}
	if entry.Choice.ID != choice.ID || entry.Choice.Kind != choice.Kind || entry.DomainDigest != digest || entry.ContextDigest != contextDigest {
		return Selection{}, fmt.Errorf("%w at choice %d: got id=%q kind=%q domain=%q context=%q, want id=%q kind=%q domain=%q context=%q", ErrTapeMismatch, d.index, choice.ID, choice.Kind, digest, contextDigest, entry.Choice.ID, entry.Choice.Kind, entry.DomainDigest, entry.ContextDigest)
	}
	if err := ValidateSelection(choice, entry.Selection); err != nil {
		return Selection{}, fmt.Errorf("%w at choice %d: %v", ErrTapeMismatch, d.index, err)
	}
	d.index++
	return cloneSelection(entry.Selection), nil
}

// Finish verifies that every recorded decision was consumed.
func (d *TapeDecider) Finish() error {
	if d.index != len(d.tape.Entries) {
		return fmt.Errorf("%w: consumed %d of %d", ErrTapeNotConsumed, d.index, len(d.tape.Entries))
	}
	return nil
}

// ValidateChoice verifies domain shape and identifiers.
func ValidateChoice(choice Choice) error {
	if choice.ID == "" || choice.Kind == "" {
		return fmt.Errorf("%w: empty identity or kind", ErrInvalidChoice)
	}
	if len(choice.Context) > 0 && !json.Valid(choice.Context) {
		return fmt.Errorf("%w: choice %q has invalid JSON context", ErrInvalidChoice, choice.ID)
	}
	hasOptions := len(choice.Options) > 0
	hasRange := choice.Min != nil || choice.Max != nil
	if hasOptions == hasRange || hasRange && (choice.Min == nil || choice.Max == nil || *choice.Min < 0 || *choice.Max < *choice.Min) {
		return fmt.Errorf("%w: choice %q must have exactly one valid domain", ErrInvalidChoice, choice.ID)
	}
	seen := make(map[string]struct{}, len(choice.Options))
	for _, option := range choice.Options {
		if option.ID == "" || option.Weight == 0 {
			return fmt.Errorf("%w: invalid option in %q", ErrInvalidChoice, choice.ID)
		}
		if _, exists := seen[option.ID]; exists {
			return fmt.Errorf("%w: duplicate option %q", ErrInvalidChoice, option.ID)
		}
		seen[option.ID] = struct{}{}
	}
	return nil
}

// ValidateSelection verifies that selection belongs to choice's domain.
func ValidateSelection(choice Choice, selection Selection) error {
	if err := ValidateChoice(choice); err != nil {
		return err
	}
	if len(choice.Options) > 0 {
		if selection.Number != nil || selection.Option == "" {
			return fmt.Errorf("%w: choice %q requires an option", ErrInvalidChoice, choice.ID)
		}
		for _, option := range choice.Options {
			if selection.Option == option.ID {
				return nil
			}
		}
		return fmt.Errorf("%w: option %q is outside choice %q", ErrInvalidChoice, selection.Option, choice.ID)
	}
	if selection.Number == nil || selection.Option != "" || *selection.Number < *choice.Min || *selection.Number > *choice.Max {
		return fmt.Errorf("%w: ranged selection is outside choice %q", ErrInvalidChoice, choice.ID)
	}
	return nil
}

// DomainDigest returns the stable identity of a choice's kind and domain.
func DomainDigest(choice Choice) (string, error) {
	if err := ValidateChoice(choice); err != nil {
		return "", err
	}
	canonical := struct {
		Kind    Kind     `json:"kind"`
		Options []Option `json:"options,omitempty"`
		Min     *int64   `json:"min,omitempty"`
		Max     *int64   `json:"max,omitempty"`
	}{Kind: choice.Kind, Options: choice.Options, Min: choice.Min, Max: choice.Max}
	raw, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// ContextDigest returns the stable identity of a choice's diagnostic semantic
// context. Insignificant JSON whitespace does not affect the digest. Producers
// should marshal structured contexts so object key order is canonical.
func ContextDigest(choice Choice) (string, error) {
	if err := ValidateChoice(choice); err != nil {
		return "", err
	}
	context := choice.Context
	if len(context) == 0 {
		context = json.RawMessage("null")
	}
	buffer := bytes.NewBuffer(nil)
	if err := json.Compact(buffer, context); err != nil {
		return "", fmt.Errorf("%w: choice %q context: %v", ErrInvalidChoice, choice.ID, err)
	}
	digest := sha256.Sum256(buffer.Bytes())
	return hex.EncodeToString(digest[:]), nil
}

func cloneChoice(choice Choice) Choice {
	choice.Options = slices.Clone(choice.Options)
	choice.Context = slices.Clone(choice.Context)
	if choice.Min != nil {
		value := *choice.Min
		choice.Min = &value
	}
	if choice.Max != nil {
		value := *choice.Max
		choice.Max = &value
	}
	return choice
}

func cloneSelection(selection Selection) Selection {
	if selection.Number != nil {
		value := *selection.Number
		selection.Number = &value
	}
	return selection
}

func cloneTape(tape Tape) Tape {
	result := Tape{Schema: tape.Schema, Entries: make([]Entry, len(tape.Entries))}
	for index, entry := range tape.Entries {
		entry.Choice = cloneChoice(entry.Choice)
		entry.Selection = cloneSelection(entry.Selection)
		result.Entries[index] = entry
	}
	return result
}
