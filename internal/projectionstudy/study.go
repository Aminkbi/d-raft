// Package projectionstudy runs a closed, synthetic causal-alignment experiment.
// It is not a Raft adapter or a production failure-detection benchmark.
package projectionstudy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/semanticplan"
)

const Schema = "d-raft.projection-study/v1"

// Message is an experiment-owned logical replication envelope. Operations are
// ground truth supplied by the fixture, not inferred from a protocol payload.
type Message struct {
	Sequence uint64   `json:"sequence"`
	At       int      `json:"at"`
	Ops      []string `json:"operations"`
}

type Case struct {
	ID       string    `json:"id"`
	Messages []Message `json:"messages"`
}

type Delivery struct {
	Message
	Selection string `json:"selection"`
}

// Witness describes only the deliberately faulty two-replica toy model: x is
// acknowledged by a volatile primary before replication; after its crash x is
// lost exactly when no delivered envelope persisted x on the backup.
type Witness struct {
	Invariant string `json:"invariant"`
	Operation string `json:"operation"`
}

type Observation struct {
	Case                 string                         `json:"case"`
	Method               string                         `json:"method"`
	Status               string                         `json:"status"`
	Reason               string                         `json:"reason,omitempty"`
	Projection           *semanticplan.ProjectionReport `json:"projection,omitempty"`
	Deliveries           []Delivery                     `json:"deliveries"`
	DurableOperations    []string                       `json:"durable_operations"`
	MisalignedOperations int                            `json:"misaligned_operations"`
	Witness              *Witness                       `json:"witness,omitempty"`
	FailurePreserved     bool                           `json:"failure_preserved"`
	LocalReplayVerified  bool                           `json:"local_replay_verified"`
	Tape                 decision.Tape                  `json:"tape"`
}

type Report struct {
	Schema       string        `json:"schema"`
	Model        string        `json:"model"`
	FallbackSeed uint64        `json:"fallback_seed"`
	Source       Case          `json:"source"`
	SourceTape   decision.Tape `json:"source_tape"`
	Cases        []Case        `json:"cases"`
	Observations []Observation `json:"observations"`
}

// Fixtures is a versioned development set. No case is held-out evidence.
func Fixtures() []Case {
	return []Case{
		{ID: "control", Messages: []Message{{1, 10, []string{"x"}}, {2, 20, []string{"y"}}}},
		{ID: "insert-irrelevant", Messages: []Message{{1, 10, []string{"noise"}}, {2, 11, []string{"x"}}, {3, 20, []string{"y"}}}},
		{ID: "reorder", Messages: []Message{{1, 10, []string{"y"}}, {2, 20, []string{"x"}}}},
		{ID: "shift-time", Messages: []Message{{1, 11, []string{"x"}}, {2, 21, []string{"y"}}}},
		{ID: "merge-conflicting-batch", Messages: []Message{{1, 10, []string{"x", "y"}}}},
		{ID: "split-retry", Messages: []Message{{1, 10, []string{"x"}}, {2, 11, []string{"x"}}, {3, 20, []string{"y"}}}},
	}
}

func choice(m Message) decision.Choice {
	context, _ := json.Marshal(struct {
		From        string   `json:"from"`
		To          string   `json:"to"`
		Incarnation uint64   `json:"sender_incarnation"`
		Sequence    uint64   `json:"send_sequence"`
		Operations  []string `json:"operations"`
	}{"a", "b", 1, m.Sequence, m.Ops})
	return decision.Choice{
		ID: fmt.Sprintf("study/send/%d/loss", m.Sequence), Kind: decision.NetworkLoss,
		Options: []decision.Option{{ID: "drop", Weight: 1}, {ID: "deliver", Weight: 1}}, Context: context,
	}
}

// Run compares a time interval, the real v1 occurrence projector, and an
// experimental operation-identity policy on the exact same target envelopes.
func Run() (Report, error) {
	cases := Fixtures()
	source := cases[0]
	tape := decision.Tape{Schema: decision.SchemaVersion, Entries: []decision.Entry{}}
	for i, message := range source.Messages {
		selection := "deliver"
		if i == 0 {
			selection = "drop"
		}
		entry, err := decision.NewEntry(choice(message), decision.Selection{Option: selection})
		if err != nil {
			return Report{}, err
		}
		tape.Entries = append(tape.Entries, entry)
	}
	directives, err := semanticplan.DirectivesFromTape(tape)
	if err != nil {
		return Report{}, err
	}
	const seed = 1
	report := Report{Schema: Schema, Model: "synthetic/ack-before-replication-v1", FallbackSeed: seed, Source: source, SourceTape: tape, Cases: cases, Observations: []Observation{}}
	for _, fixture := range cases {
		for _, method := range []string{"fault-interval", "occurrence-v1", "causal-operation-prototype"} {
			observation, err := execute(fixture, method, directives, seed)
			if err != nil {
				return Report{}, fmt.Errorf("%s/%s: %w", fixture.ID, method, err)
			}
			report.Observations = append(report.Observations, observation)
		}
	}
	return report, nil
}

func execute(fixture Case, method string, directives []semanticplan.Directive, seed uint64) (Observation, error) {
	result := Observation{Case: fixture.ID, Method: method, Status: "completed", Deliveries: []Delivery{}, DurableOperations: []string{}, Tape: decision.Tape{Schema: decision.SchemaVersion, Entries: []decision.Entry{}}}
	causal := make(map[string]string)
	if method == "causal-operation-prototype" {
		// Bind logical operations to source directives before touching the target.
		// A real adapter must supply these identities independently of the bug.
		for _, directive := range directives {
			for _, op := range Fixtures()[0].Messages[int(directive.SourceIndex)].Ops {
				causal[op] = directive.Selection.Option
			}
		}
		// One physical envelope cannot be both dropped and delivered. Reject the
		// entire case before execution instead of selecting a favorable policy.
		for _, message := range fixture.Messages {
			if _, err := causalSelection(message, causal); err != nil {
				result.Status, result.Reason = "ineligible", err.Error()
				return result, nil
			}
		}
	}
	projector, err := semanticplan.NewProjector(directives, artifact.Uint64(seed))
	if err != nil {
		return Observation{}, err
	}
	for _, message := range fixture.Messages {
		selection := decision.Selection{Option: "deliver"}
		switch method {
		case "fault-interval":
			if message.At >= 10 && message.At < 11 {
				selection.Option = "drop"
			}
		case "occurrence-v1":
			selection, err = projector.Choose(choice(message))
		case "causal-operation-prototype":
			selection.Option, err = causalSelection(message, causal)
		default:
			return Observation{}, fmt.Errorf("unknown method %q", method)
		}
		if err != nil {
			return Observation{}, err
		}
		entry, err := decision.NewEntry(choice(message), selection)
		if err != nil {
			return Observation{}, err
		}
		result.Tape.Entries = append(result.Tape.Entries, entry)
		result.Deliveries = append(result.Deliveries, Delivery{Message: message, Selection: selection.Option})
	}
	if method == "occurrence-v1" {
		projection := projector.Finish()
		result.Projection = &projection
	}
	result.DurableOperations, result.MisalignedOperations, result.Witness = assess(result.Deliveries)
	result.FailurePreserved = result.Witness != nil
	// Replay the stored local tape through the toy model, checking choices and
	// final state/witness independently of the projecting policy.
	replay, err := decision.NewTapeDecider(result.Tape)
	if err != nil {
		return Observation{}, err
	}
	var deliveries []Delivery
	for _, message := range fixture.Messages {
		selection, err := replay.Choose(choice(message))
		if err != nil {
			return Observation{}, err
		}
		deliveries = append(deliveries, Delivery{Message: message, Selection: selection.Option})
	}
	if err := replay.Finish(); err != nil {
		return Observation{}, err
	}
	durable, mismatches, witness := assess(deliveries)
	if !slices.Equal(durable, result.DurableOperations) || mismatches != result.MisalignedOperations || !sameWitness(witness, result.Witness) {
		return Observation{}, fmt.Errorf("local replay outcome mismatch")
	}
	result.LocalReplayVerified = true
	return result, nil
}

func causalSelection(message Message, directives map[string]string) (string, error) {
	selected := ""
	for _, op := range message.Ops {
		want, exists := directives[op]
		if !exists {
			want = "deliver" // explicit neutral policy for new logical operations
		}
		if selected != "" && selected != want {
			return "", fmt.Errorf("conflicting source dispositions in target batch %d", message.Sequence)
		}
		selected = want
	}
	return selected, nil
}

func assess(deliveries []Delivery) ([]string, int, *Witness) {
	durable := []string{}
	misaligned := 0
	for _, delivery := range deliveries {
		for _, op := range delivery.Ops {
			want := "deliver"
			if op == "x" {
				want = "drop"
			}
			if delivery.Selection != want {
				misaligned++
			}
			if delivery.Selection == "deliver" && !slices.Contains(durable, op) {
				durable = append(durable, op)
			}
		}
	}
	slices.Sort(durable)
	if !slices.Contains(durable, "x") {
		return durable, misaligned, &Witness{Invariant: "toy/acknowledged-write-lost", Operation: "x"}
	}
	return durable, misaligned, nil
}

func sameWitness(a, b *Witness) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// Encode produces deterministic bytes; host timings are deliberately absent.
func Encode(report Report) ([]byte, error) {
	raw, err := json.MarshalIndent(report, "", "  ")
	return append(raw, '\n'), err
}

// Verify regenerates every observation and requires exact bytes, rejecting
// duplicate/unknown fields, altered results, and trailing data without trusting
// any derived field in the supplied document.
func Verify(raw []byte) error {
	report, err := Run()
	if err != nil {
		return err
	}
	want, err := Encode(report)
	if err != nil {
		return err
	}
	if !bytes.Equal(raw, want) {
		return fmt.Errorf("projection study differs from deterministic regeneration")
	}
	return nil
}
