package artifact

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/raftsim"
)

func TestArtifactRoundTrip(t *testing.T) {
	t.Parallel()

	run := validRun()
	var output bytes.Buffer
	if err := Encode(&output, run); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := Decode(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.Schema != SchemaVersion || decoded.Scenario.ID != "steady" || decoded.Outcome.ObservationDigest != run.Outcome.ObservationDigest {
		t.Fatalf("decoded = %+v", decoded)
	}
}

func TestArtifactDecoderRejectsUnknownAndTrailingFields(t *testing.T) {
	t.Parallel()

	run := validRun()
	var output bytes.Buffer
	if err := Encode(&output, run); err != nil {
		t.Fatal(err)
	}
	unknown := strings.Replace(output.String(), `"schema":"`+SchemaVersion+`",`, `"schema":"`+SchemaVersion+`","future":true,`, 1)
	if _, err := Decode(strings.NewReader(unknown)); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("unknown field error = %v", err)
	}
	if _, err := Decode(strings.NewReader(output.String() + `{}`)); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("trailing value error = %v", err)
	}
}

func TestArtifactRejectsInvalidTape(t *testing.T) {
	t.Parallel()

	run := validRun()
	run.Decisions.Schema = "wrong"
	if err := run.Validate(); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("Validate error = %v", err)
	}
}

func TestArtifactEncodeAndDecodeUseSameSizeLimit(t *testing.T) {
	t.Parallel()

	run := validRun()
	run.Scenario.Actions = []Action{{Kind: ActionPropose, Data: bytes.Repeat([]byte("x"), 2_048)}}
	var output bytes.Buffer
	if err := encodeWithLimit(&output, run, 1_024); !errors.Is(err, ErrArtifactTooLarge) {
		t.Fatalf("encode size error = %v", err)
	}
	if err := encodeWithLimit(&output, run, 1<<20); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeWithLimit(bytes.NewReader(output.Bytes()), 1_024); !errors.Is(err, ErrArtifactTooLarge) {
		t.Fatalf("decode size error = %v", err)
	}
}

func TestArtifactEncodingEnforcesExactExpandedLimitWithoutPartialOutput(t *testing.T) {
	t.Parallel()

	run := validRun()
	choice := decision.Choice{ID: strings.Repeat("\x01", 100), Kind: decision.ClientAction, Options: []decision.Option{{ID: "x", Weight: 1}}}
	domainDigest, err := decision.DomainDigest(choice)
	if err != nil {
		t.Fatal(err)
	}
	contextDigest, err := decision.ContextDigest(choice)
	if err != nil {
		t.Fatal(err)
	}
	run.Decisions.Entries = []decision.Entry{{Choice: choice, DomainDigest: domainDigest, ContextDigest: contextDigest, Selection: decision.Selection{Option: "x"}}}
	var output bytes.Buffer
	if err := encodeWithLimit(&output, run, 1_024); !errors.Is(err, ErrArtifactTooLarge) {
		t.Fatalf("expanded size error = %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("partial output length = %d", output.Len())
	}
}

func TestArtifactUsesFullWidthStringSeeds(t *testing.T) {
	t.Parallel()

	run := validRun()
	run.Configuration.InfrastructureSeed = Uint64(^uint64(0))
	run.Reproducibility.DecisionSeed = Uint64(^uint64(0))
	var output bytes.Buffer
	if err := Encode(&output, run); err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), `"18446744073709551615"`) != 2 {
		t.Fatalf("full-width seeds were not strings: %s", output.String())
	}
	decoded, err := Decode(bytes.NewReader(output.Bytes()))
	if err != nil || uint64(decoded.Reproducibility.DecisionSeed) != ^uint64(0) {
		t.Fatalf("decoded seed=%d err=%v", decoded.Reproducibility.DecisionSeed, err)
	}
}

func TestArtifactRejectsNoncanonicalAndUnsupportedMetadata(t *testing.T) {
	t.Parallel()

	run := validRun()
	run.Configuration.Members = []raft.NodeID{"b", "a"}
	if err := run.Validate(); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("membership error = %v", err)
	}
	run = validRun()
	run.Reproducibility.MessageCodec = "unsupported"
	if err := run.Validate(); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("codec error = %v", err)
	}
	run = validRun()
	run.Adapter = Adapter{ID: "external", Version: "1"}
	run.Reproducibility.CheckerSchema = ""
	if err := run.Validate(); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("missing checker schema error = %v", err)
	}
	run = validRun()
	run.Scenario.ID = "bad\x1b]8;;"
	if err := run.Validate(); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("identifier error = %v", err)
	}
}

func TestArtifactRejectsAggregatePayloadBeforeEncoding(t *testing.T) {
	t.Parallel()

	run := validRun()
	context := append([]byte{'"'}, bytes.Repeat([]byte("x"), MaxDecisionContextBytes-2)...)
	context = append(context, '"')
	run.Decisions.Entries = make([]decision.Entry, 33)
	for index := range run.Decisions.Entries {
		run.Decisions.Entries[index].Choice.Context = context
	}
	if err := Encode(&bytes.Buffer{}, run); !errors.Is(err, ErrArtifactTooLarge) {
		t.Fatalf("aggregate size error = %v", err)
	}
}

func TestArtifactValidatesPublishedReferenceV1Metadata(t *testing.T) {
	t.Parallel()

	run := validRun()
	run.Schema = SchemaV1
	run.Adapter.Version = ReferenceAdapterV1
	run.Reproducibility.MessageCodec = MessageCodecV1
	run.Reproducibility.CheckerSchema = check.SchemaV1
	run.Reproducibility.ObservationSchema = ObservationSchemaV1
	if err := run.Validate(); err != nil {
		t.Fatalf("Validate legacy v1: %v", err)
	}
	var encoded bytes.Buffer
	if err := Encode(&encoded, run); err != nil {
		t.Fatalf("Encode legacy v1: %v", err)
	}
	if decoded, err := Decode(bytes.NewReader(encoded.Bytes())); err != nil || decoded.Schema != SchemaV1 {
		t.Fatalf("Decode legacy v1: schema=%q err=%v", decoded.Schema, err)
	}
}

func TestPublishedReferenceV1RejectsV2Witness(t *testing.T) {
	t.Parallel()

	run := validRun()
	run.Schema = SchemaV1
	run.Adapter.Version = ReferenceAdapterV1
	run.Reproducibility.MessageCodec = MessageCodecV1
	run.Reproducibility.CheckerSchema = check.SchemaV1
	run.Reproducibility.ObservationSchema = ObservationSchemaV1
	evidence := []byte(`{"index":1}`)
	fingerprint, err := check.Fingerprint(check.SnapshotConflict, []raft.NodeID{"a"}, evidence)
	if err != nil {
		t.Fatal(err)
	}
	run.Outcome.Status = OutcomeViolation
	run.Outcome.Violations = []check.Violation{{ID: check.SnapshotConflict, Nodes: []raft.NodeID{"a"}, Evidence: evidence, Fingerprint: fingerprint}}
	if err := run.Validate(); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("v1 witness error = %v", err)
	}
}

func TestLegacyV1RetainsHistoricalDecisionLimits(t *testing.T) {
	t.Parallel()

	run := validRun()
	run.Schema = SchemaV1
	run.Adapter.Version = ReferenceAdapterV1
	run.Reproducibility.MessageCodec = MessageCodecV1
	run.Reproducibility.CheckerSchema = check.SchemaV1
	run.Reproducibility.ObservationSchema = ObservationSchemaV1
	context := append([]byte{'"'}, bytes.Repeat([]byte("x"), MaxDecisionContextBytes)...)
	context = append(context, '"')
	choice := decision.Choice{ID: "legacy-large-context", Kind: decision.ClientAction, Options: []decision.Option{{ID: "keep", Weight: 1}}, Context: context}
	domainDigest, err := decision.DomainDigest(choice)
	if err != nil {
		t.Fatal(err)
	}
	contextDigest, err := decision.ContextDigest(choice)
	if err != nil {
		t.Fatal(err)
	}
	run.Decisions.Entries = []decision.Entry{{Choice: choice, DomainDigest: domainDigest, ContextDigest: contextDigest, Selection: decision.Selection{Option: "keep"}}}
	var encoded bytes.Buffer
	if err := Encode(&encoded, run); err != nil {
		t.Fatalf("Encode legacy v1: %v", err)
	}
	if _, err := Decode(bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatalf("Decode legacy v1: %v", err)
	}
}

func TestSnapshotActionRequiresRunV2(t *testing.T) {
	t.Parallel()

	run := validRun()
	run.Scenario.Actions = []Action{{Kind: ActionSnapshot, Node: "a", Data: []byte("checkpoint")}}
	if err := run.Validate(); err != nil {
		t.Fatalf("Validate v2 snapshot: %v", err)
	}
	run.Schema = SchemaV1
	run.Adapter.Version = ReferenceAdapterV1
	run.Reproducibility.MessageCodec = MessageCodecV1
	run.Reproducibility.CheckerSchema = check.SchemaV1
	run.Reproducibility.ObservationSchema = ObservationSchemaV1
	if err := run.Validate(); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("v1 snapshot error = %v", err)
	}
}

func validRun() Run {
	config := raftsim.DefaultConfig("a", "b", "c")
	digest, _ := DigestJSON(map[string]any{"state": "ok"})
	return Run{
		Schema:          SchemaVersion,
		Scenario:        Scenario{ID: "steady", Version: "1", DurationNS: int64(time.Second), MaxSteps: 10_000},
		Adapter:         Adapter{ID: ReferenceAdapterID, Version: ReferenceAdapterCurrent},
		Configuration:   ConfigurationFrom(config),
		Reproducibility: Reproducibility{DecisionSeed: 1, GitRevision: "test", GoVersion: "go-test", DecisionSchema: decision.SchemaVersion, CheckerSchema: check.SchemaVersion, MessageCodec: MessageCodecCurrent, ObservationSchema: ObservationSchemaCurrent},
		Decisions:       decision.Tape{Schema: decision.SchemaVersion},
		Outcome:         Outcome{Status: OutcomeCompleted, EndNS: int64(time.Second), ObservationDigest: digest},
	}
}
