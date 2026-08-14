// Package semanticplan projects adapter-neutral semantic directives onto
// adapter-local decision streams.
package semanticplan

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/internal/strictjson"
	"github.com/aminkbi/d-raft/raft"
)

const maximumContextBytes = 16 << 20

var (
	// ErrInvalidProjection reports an invalid directive, context, or choice.
	ErrInvalidProjection = errors.New("semanticplan: invalid projection")
	// ErrDuplicateTarget reports a second target choice with the same portable
	// identity. Reusing a directive would otherwise make projection depend on
	// adapter-local call ordering.
	ErrDuplicateTarget = errors.New("semanticplan: duplicate target choice")
)

// ProjectionFidelity classifies how completely a plan described one target
// execution's variable portable choices.
type ProjectionFidelity string

const (
	ProjectionExact   ProjectionFidelity = "exact"
	ProjectionPartial ProjectionFidelity = "partial"
	ProjectionFailed  ProjectionFidelity = "failed"
)

// PortableKey identifies a choice without adapter-local message encodings,
// terms, log indexes, or message types. Incarnation and Occurrence are
// canonical unsigned decimal strings, which preserves their full uint64 range
// in language-neutral JSON.
type PortableKey struct {
	Kind        decision.Kind   `json:"kind"`
	Node        raft.NodeID     `json:"node,omitempty"`
	From        raft.NodeID     `json:"from,omitempty"`
	To          raft.NodeID     `json:"to,omitempty"`
	Incarnation artifact.Uint64 `json:"incarnation"`
	Occurrence  artifact.Uint64 `json:"occurrence"`
}

// Validate enforces the closed portable-key vocabulary and canonical field
// forms.
func (k PortableKey) Validate() error {
	if !supportedKind(k.Kind) {
		return fmt.Errorf("%w: unsupported kind %q", ErrInvalidProjection, k.Kind)
	}
	if k.Incarnation == 0 || k.Occurrence == 0 {
		return fmt.Errorf("%w: incarnation and occurrence must be nonzero canonical unsigned decimals", ErrInvalidProjection)
	}
	switch k.Kind {
	case decision.ElectionTimeout, decision.StorageLatency:
		if !validEndpoint(string(k.Node)) || k.From != "" || k.To != "" {
			return fmt.Errorf("%w: %s key requires only node", ErrInvalidProjection, k.Kind)
		}
	case decision.NetworkLoss, decision.NetworkLatency:
		if k.Node != "" || !validEndpoint(string(k.From)) || !validEndpoint(string(k.To)) {
			return fmt.Errorf("%w: %s key requires only from and to", ErrInvalidProjection, k.Kind)
		}
	}
	return nil
}

// Directive pins the selection for one variable portable choice. Selection is
// represented with the decision package's closed option-or-number union.
type Directive struct {
	Key         PortableKey        `json:"key"`
	SourceIndex artifact.Uint64    `json:"source_index"`
	Selection   decision.Selection `json:"selection"`
}

// ProjectionReport is an immutable summary of choices observed so far.
// Additional is sorted by portable identity; Unmatched retains canonical
// source-index order.
type ProjectionReport struct {
	Fidelity   ProjectionFidelity `json:"fidelity"`
	Directives int                `json:"directives"`
	Projected  int                `json:"projected"`
	Fixed      int                `json:"fixed"`
	Additional []PortableKey      `json:"additional"`
	Unmatched  []Directive        `json:"unmatched"`
}

// Projector consumes every directive and target identity at most once. It is
// deliberately stateful and intended for one clean execution.
type Projector struct {
	directives map[PortableKey]Directive
	consumed   map[PortableKey]struct{}
	seen       map[PortableKey]struct{}
	fallback   *decision.SeedDecider
	additional []PortableKey
	projected  int
	fixed      int
	failed     error
}

// NewProjector validates a plan's directives and pins the fallback to
// decision.NewSeedDecider(fallbackSeed).
func NewProjector(directives []Directive, fallbackSeed artifact.Uint64) (*Projector, error) {
	if err := ValidateDirectives(directives); err != nil {
		return nil, err
	}
	projector := &Projector{
		directives: make(map[PortableKey]Directive, len(directives)),
		consumed:   make(map[PortableKey]struct{}, len(directives)),
		seen:       make(map[PortableKey]struct{}),
		fallback:   decision.NewSeedDecider(uint64(fallbackSeed)),
		additional: make([]PortableKey, 0),
	}
	for _, directive := range directives {
		directive.Selection = cloneSelection(directive.Selection)
		projector.directives[directive.Key] = directive
	}
	return projector, nil
}

// ValidateDirectives verifies canonical source order, unique portable keys,
// and the kind-specific directive selection shape. Source index zero is valid
// because it denotes the first entry of the full source tape.
func ValidateDirectives(directives []Directive) error {
	seen := make(map[PortableKey]struct{}, len(directives))
	for index, directive := range directives {
		if uint64(directive.SourceIndex) >= artifact.MaxDecisions {
			return fmt.Errorf("directives[%d]: %w: source_index must be less than %d", index, ErrInvalidProjection, artifact.MaxDecisions)
		}
		if index > 0 && directives[index-1].SourceIndex >= directive.SourceIndex {
			return fmt.Errorf("directives[%d]: %w: source indexes must be strictly increasing", index, ErrInvalidProjection)
		}
		if err := directive.Key.Validate(); err != nil {
			return fmt.Errorf("directives[%d]: %w", index, err)
		}
		if err := validateDirectiveSelection(directive); err != nil {
			return fmt.Errorf("directives[%d]: %w", index, err)
		}
		if _, exists := seen[directive.Key]; exists {
			return fmt.Errorf("directives[%d]: %w: duplicate key %s", index, ErrInvalidProjection, formatKey(directive.Key))
		}
		seen[directive.Key] = struct{}{}
	}
	return nil
}

// DirectivesFromTape extracts only variable portable choices from a validated
// exact source tape. SourceIndex always refers to the entry's position in the
// full tape, including fixed entries that do not become directives. Entries
// outside the closed portable-kind vocabulary are rejected.
func DirectivesFromTape(tape decision.Tape) ([]Directive, error) {
	if _, err := decision.NewTapeDecider(tape); err != nil {
		return nil, fmt.Errorf("%w: invalid source tape: %v", ErrInvalidProjection, err)
	}
	directives := make([]Directive, 0, len(tape.Entries))
	seen := make(map[PortableKey]struct{}, len(tape.Entries))
	for index, entry := range tape.Entries {
		if err := validatePortableChoice(entry.Choice); err != nil {
			return nil, fmt.Errorf("%w: source entry %d: %v", ErrInvalidProjection, index, err)
		}
		key, err := PortableKeyForChoice(entry.Choice)
		if err != nil {
			return nil, fmt.Errorf("source entry %d: %w", index, err)
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("%w: source entry %d repeats %s", ErrDuplicateTarget, index, formatKey(key))
		}
		seen[key] = struct{}{}
		if _, fixed := fixedSelection(entry.Choice); fixed {
			continue
		}
		directives = append(directives, Directive{
			Key: key, SourceIndex: artifact.Uint64(index), Selection: cloneSelection(entry.Selection),
		})
	}
	if err := ValidateDirectives(directives); err != nil {
		return nil, err
	}
	return directives, nil
}

// Choose implements decision.Decider. Invalid input permanently fails the
// projector, preventing a caller from retrying into a different projection.
func (p *Projector) Choose(choice decision.Choice) (decision.Selection, error) {
	if p == nil {
		return decision.Selection{}, fmt.Errorf("%w: nil projector", ErrInvalidProjection)
	}
	if p.failed != nil {
		return decision.Selection{}, p.failed
	}
	if err := validatePortableChoice(choice); err != nil {
		return decision.Selection{}, p.fail(err)
	}
	key, err := PortableKeyForChoice(choice)
	if err != nil {
		return decision.Selection{}, p.fail(err)
	}
	if _, exists := p.seen[key]; exists {
		return decision.Selection{}, p.fail(fmt.Errorf("%w: %s", ErrDuplicateTarget, formatKey(key)))
	}
	p.seen[key] = struct{}{}

	if selection, fixed := fixedSelection(choice); fixed {
		p.fixed++
		return selection, nil
	}
	if directive, exists := p.directives[key]; exists {
		if err := decision.ValidateSelection(choice, directive.Selection); err != nil {
			return decision.Selection{}, p.fail(fmt.Errorf("%w: directive for %s is outside the target domain: %v", ErrInvalidProjection, formatKey(key), err))
		}
		// Only successfully recorded choices count as consumed. A rejected
		// directive remains unmatched, allowing the exact successful tape prefix
		// to independently reproduce the failed report's accounting.
		p.consumed[key] = struct{}{}
		p.projected++
		return cloneSelection(directive.Selection), nil
	}

	selection, err := p.fallback.Choose(choice)
	if err != nil {
		return decision.Selection{}, p.fail(fmt.Errorf("%w: fallback for %s: %v", ErrInvalidProjection, formatKey(key), err))
	}
	p.additional = append(p.additional, key)
	return selection, nil
}

// Finish returns a stable snapshot. A failed Choose dominates partial or exact
// coverage; otherwise any unmatched directive or additional variable target
// choice makes the projection partial.
func (p *Projector) Finish() ProjectionReport {
	if p == nil {
		return ProjectionReport{Fidelity: ProjectionFailed, Additional: []PortableKey{}, Unmatched: []Directive{}}
	}
	unmatched := make([]Directive, 0, len(p.directives)-len(p.consumed))
	for key, directive := range p.directives {
		if _, consumed := p.consumed[key]; !consumed {
			directive.Selection = cloneSelection(directive.Selection)
			unmatched = append(unmatched, directive)
		}
	}
	additional := slices.Clone(p.additional)
	sortKeys(additional)
	slices.SortFunc(unmatched, compareDirectives)
	fidelity := ProjectionExact
	if p.failed != nil {
		fidelity = ProjectionFailed
	} else if len(additional) != 0 || len(unmatched) != 0 {
		fidelity = ProjectionPartial
	}
	return ProjectionReport{
		Fidelity: fidelity, Directives: len(p.directives), Projected: p.projected,
		Fixed: p.fixed, Additional: additional, Unmatched: unmatched,
	}
}

// Err returns the first permanent projection failure, if any.
func (p *Projector) Err() error {
	if p == nil {
		return fmt.Errorf("%w: nil projector", ErrInvalidProjection)
	}
	return p.failed
}

// PortableKeyForChoice extracts the adapter-neutral identity from the current
// reference or etcd/raft context shape.
func PortableKeyForChoice(choice decision.Choice) (PortableKey, error) {
	if err := validatePortableChoice(choice); err != nil {
		return PortableKey{}, err
	}
	fields, err := decodeContextObject(choice.Context)
	if err != nil {
		return PortableKey{}, fmt.Errorf("%w: choice %q context: %v", ErrInvalidProjection, choice.ID, err)
	}
	key := PortableKey{Kind: choice.Kind}
	switch choice.Kind {
	case decision.ElectionTimeout, decision.StorageLatency:
		if err = rejectFields(fields, "from", "to", "sender_incarnation", "send_sequence"); err != nil {
			break
		}
		var node string
		node, err = requiredString(fields, "node")
		key.Node = raft.NodeID(node)
		if err == nil {
			var incarnation uint64
			incarnation, err = requiredUint(fields, "incarnation")
			key.Incarnation = artifact.Uint64(incarnation)
		}
		if err == nil {
			var occurrence uint64
			occurrence, err = requiredUint(fields, "generation")
			key.Occurrence = artifact.Uint64(occurrence)
		}
	case decision.NetworkLoss, decision.NetworkLatency:
		if err = rejectFields(fields, "node", "incarnation", "generation"); err != nil {
			break
		}
		var from string
		from, err = requiredString(fields, "from")
		key.From = raft.NodeID(from)
		if err == nil {
			var to string
			to, err = requiredString(fields, "to")
			key.To = raft.NodeID(to)
		}
		if err == nil {
			var incarnation uint64
			incarnation, err = requiredUint(fields, "sender_incarnation")
			key.Incarnation = artifact.Uint64(incarnation)
		}
		if err == nil {
			var occurrence uint64
			occurrence, err = requiredUint(fields, "send_sequence")
			key.Occurrence = artifact.Uint64(occurrence)
		}
	}
	if err != nil {
		return PortableKey{}, fmt.Errorf("%w: choice %q context: %v", ErrInvalidProjection, choice.ID, err)
	}
	if err := key.Validate(); err != nil {
		return PortableKey{}, fmt.Errorf("choice %q: %w", choice.ID, err)
	}
	return key, nil
}

func validatePortableChoice(choice decision.Choice) error {
	if err := decision.ValidateChoice(choice); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidProjection, err)
	}
	if !supportedKind(choice.Kind) {
		return fmt.Errorf("%w: choice %q has unsupported kind %q", ErrInvalidProjection, choice.ID, choice.Kind)
	}
	if choice.Kind == decision.NetworkLoss {
		if len(choice.Options) == 0 {
			return fmt.Errorf("%w: network_loss choice %q must use options", ErrInvalidProjection, choice.ID)
		}
		for _, option := range choice.Options {
			if option.ID != "deliver" && option.ID != "drop" {
				return fmt.Errorf("%w: network_loss choice %q has option %q", ErrInvalidProjection, choice.ID, option.ID)
			}
		}
	} else if choice.Min == nil || choice.Max == nil {
		return fmt.Errorf("%w: %s choice %q must use a range", ErrInvalidProjection, choice.Kind, choice.ID)
	}
	return nil
}

func validateDirectiveSelection(directive Directive) error {
	switch directive.Key.Kind {
	case decision.NetworkLoss:
		if directive.Selection.Number != nil || (directive.Selection.Option != "deliver" && directive.Selection.Option != "drop") {
			return fmt.Errorf("%w: network_loss directive must select deliver or drop", ErrInvalidProjection)
		}
	default:
		if directive.Selection.Number == nil || directive.Selection.Option != "" || *directive.Selection.Number < 0 {
			return fmt.Errorf("%w: duration directive must select one nonnegative number", ErrInvalidProjection)
		}
	}
	return nil
}

func fixedSelection(choice decision.Choice) (decision.Selection, bool) {
	if len(choice.Options) == 1 {
		return decision.Selection{Option: choice.Options[0].ID}, true
	}
	if choice.Min != nil && choice.Max != nil && *choice.Min == *choice.Max {
		value := *choice.Min
		return decision.Selection{Number: &value}, true
	}
	return decision.Selection{}, false
}

func supportedKind(kind decision.Kind) bool {
	switch kind {
	case decision.ElectionTimeout, decision.StorageLatency, decision.NetworkLoss, decision.NetworkLatency:
		return true
	default:
		return false
	}
}

func validEndpoint(value string) bool {
	if value == "" || len(value) > 4096 || !utf8.ValidString(value) {
		return false
	}
	return strings.IndexFunc(value, unicode.IsControl) < 0
}

func decodeContextObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maximumContextBytes || !utf8.Valid(raw) {
		return nil, errors.New("missing, oversized, or non-UTF-8 JSON")
	}
	if err := strictjson.RejectDuplicateNames(raw); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return nil, errors.New("must be a JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := token.(string)
		if !ok {
			return nil, errors.New("object member name is not a string")
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, fmt.Errorf("duplicate field %q", name)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[name] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	var trailer any
	if err := decoder.Decode(&trailer); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	return fields, nil
}

func requiredString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", fmt.Errorf("missing field %q", name)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || !validEndpoint(value) {
		return "", fmt.Errorf("field %q must be a nonempty control-free string", name)
	}
	return value, nil
}

func requiredUint(fields map[string]json.RawMessage, names ...string) (uint64, error) {
	var found string
	for _, name := range names {
		if _, ok := fields[name]; ok {
			if found != "" {
				return 0, fmt.Errorf("fields %q and %q are mutually exclusive", found, name)
			}
			found = name
		}
	}
	if found == "" {
		return 0, fmt.Errorf("missing field %q", names[0])
	}
	text := string(fields[found])
	if text == "" || text == "0" || text[0] == '0' {
		return 0, fmt.Errorf("field %q must be a nonzero canonical unsigned integer", found)
	}
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("field %q must be a nonzero canonical unsigned integer", found)
	}
	return value, nil
}

func rejectFields(fields map[string]json.RawMessage, names ...string) error {
	for _, name := range names {
		if _, exists := fields[name]; exists {
			return fmt.Errorf("field %q is not valid for this choice kind", name)
		}
	}
	return nil
}

func (p *Projector) fail(err error) error {
	if p.failed == nil {
		p.failed = err
	}
	return p.failed
}

func cloneSelection(selection decision.Selection) decision.Selection {
	if selection.Number != nil {
		value := *selection.Number
		selection.Number = &value
	}
	return selection
}

func formatKey(key PortableKey) string {
	raw, _ := json.Marshal(key)
	return string(raw)
}

func sortKeys(keys []PortableKey) {
	slices.SortFunc(keys, func(left, right PortableKey) int {
		for _, comparison := range []int{
			cmp.Compare(left.Kind, right.Kind), cmp.Compare(left.Node, right.Node),
			cmp.Compare(left.From, right.From), cmp.Compare(left.To, right.To),
			cmp.Compare(left.Incarnation, right.Incarnation), cmp.Compare(left.Occurrence, right.Occurrence),
		} {
			if comparison != 0 {
				return comparison
			}
		}
		return 0
	})
}

func compareDirectives(left, right Directive) int {
	if comparison := cmp.Compare(left.SourceIndex, right.SourceIndex); comparison != 0 {
		return comparison
	}
	return compareKeys(left.Key, right.Key)
}

func compareKeys(left, right PortableKey) int {
	for _, comparison := range []int{
		cmp.Compare(left.Kind, right.Kind), cmp.Compare(left.Node, right.Node),
		cmp.Compare(left.From, right.From), cmp.Compare(left.To, right.To),
		cmp.Compare(left.Incarnation, right.Incarnation), cmp.Compare(left.Occurrence, right.Occurrence),
	} {
		if comparison != 0 {
			return comparison
		}
	}
	return 0
}
