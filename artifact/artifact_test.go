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

func TestArtifactValidatesPublishedReferenceV2Metadata(t *testing.T) {
	t.Parallel()

	run := validRun()
	run.Schema = SchemaV2
	run.Adapter.Version = ReferenceAdapterV2
	run.Reproducibility.MessageCodec = MessageCodecV2
	run.Reproducibility.CheckerSchema = check.SchemaV2
	run.Reproducibility.ObservationSchema = ObservationSchemaV2
	var encoded bytes.Buffer
	if err := Encode(&encoded, run); err != nil {
		t.Fatalf("Encode legacy v2: %v", err)
	}
	if decoded, err := Decode(bytes.NewReader(encoded.Bytes())); err != nil || decoded.Schema != SchemaV2 {
		t.Fatalf("Decode legacy v2: schema=%q err=%v", decoded.Schema, err)
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
		t.Fatalf("Validate current snapshot: %v", err)
	}
	run.Schema = SchemaV2
	run.Adapter.Version = ReferenceAdapterV2
	run.Reproducibility.MessageCodec = MessageCodecV2
	run.Reproducibility.CheckerSchema = check.SchemaV2
	run.Reproducibility.ObservationSchema = ObservationSchemaV2
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

func TestMembershipActionsRequireRunV3(t *testing.T) {
	t.Parallel()

	run := validRun()
	run.Configuration.Members = []raft.NodeID{"a", "b", "c", "d"}
	run.Configuration.Voters = []raft.NodeID{"a", "b", "c"}
	run.Configuration.Learners = []raft.NodeID{"d"}
	run.Scenario.Actions = []Action{
		{Kind: ActionBeginMembership, Voters: []raft.NodeID{"b", "c", "d"}, Learners: []raft.NodeID{"a"}},
		{Kind: ActionFinalizeMembership},
	}
	if err := run.Validate(); err != nil {
		t.Fatalf("Validate v3 membership: %v", err)
	}
	run.Schema = SchemaV2
	run.Adapter.Version = ReferenceAdapterV2
	run.Reproducibility.MessageCodec = MessageCodecV2
	run.Reproducibility.CheckerSchema = check.SchemaV2
	run.Reproducibility.ObservationSchema = ObservationSchemaV2
	if err := run.Validate(); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("v2 membership error = %v", err)
	}
}

func TestLegacyArtifactsRejectPresentV3RoleFields(t *testing.T) {
	t.Parallel()

	run := validRun()
	run.Schema = SchemaV2
	run.Adapter.Version = ReferenceAdapterV2
	run.Reproducibility.MessageCodec = MessageCodecV2
	run.Reproducibility.CheckerSchema = check.SchemaV2
	run.Reproducibility.ObservationSchema = ObservationSchemaV2
	run.Scenario.Actions = []Action{{Kind: ActionSnapshot, Node: "a", Data: []byte("checkpoint")}}
	var encoded bytes.Buffer
	if err := Encode(&encoded, run); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		json string
	}{
		{"configuration empty", strings.Replace(encoded.String(), `"configuration":{`, `"configuration":{"voters":[],`, 1)},
		{"configuration null", strings.Replace(encoded.String(), `"configuration":{`, `"configuration":{"learners":null,`, 1)},
		{"action empty", strings.Replace(encoded.String(), `"kind":"snapshot"`, `"kind":"snapshot","voters":[]`, 1)},
		{"action null", strings.Replace(encoded.String(), `"kind":"snapshot"`, `"kind":"snapshot","learners":null`, 1)},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Decode(strings.NewReader(test.json)); !errors.Is(err, ErrInvalidArtifact) {
				t.Fatalf("Decode error = %v", err)
			}
		})
	}
}

func TestReferenceSchemaTuplesAreAtomic(t *testing.T) {
	t.Parallel()

	types := []struct {
		schema      string
		adapter     string
		codec       string
		checker     string
		observation string
	}{
		{SchemaV1, ReferenceAdapterV1, MessageCodecV1, check.SchemaV1, ObservationSchemaV1},
		{SchemaV2, ReferenceAdapterV2, MessageCodecV2, check.SchemaV2, ObservationSchemaV2},
		{SchemaVersion, ReferenceAdapterV3, MessageCodecV3, check.SchemaVersion, ObservationSchemaV3},
	}
	configure := func(run *Run, tuple struct {
		schema      string
		adapter     string
		codec       string
		checker     string
		observation string
	}) {
		run.Schema = tuple.schema
		run.Adapter.Version = tuple.adapter
		run.Reproducibility.MessageCodec = tuple.codec
		run.Reproducibility.CheckerSchema = tuple.checker
		run.Reproducibility.ObservationSchema = tuple.observation
	}
	for index, tuple := range types {
		base := validRun()
		configure(&base, tuple)
		if err := base.Validate(); err != nil {
			t.Fatalf("coherent tuple %s: %v", tuple.schema, err)
		}
		other := types[(index+1)%len(types)]
		mutations := []struct {
			name   string
			mutate func(*Run)
		}{
			{"schema", func(run *Run) { run.Schema = other.schema }},
			{"adapter", func(run *Run) { run.Adapter.Version = other.adapter }},
			{"codec", func(run *Run) { run.Reproducibility.MessageCodec = other.codec }},
			{"checker", func(run *Run) { run.Reproducibility.CheckerSchema = other.checker }},
			{"observation", func(run *Run) { run.Reproducibility.ObservationSchema = other.observation }},
		}
		for _, mutation := range mutations {
			run := base
			mutation.mutate(&run)
			if err := run.Validate(); !errors.Is(err, ErrInvalidArtifact) {
				t.Fatalf("tuple %s accepted mismatched %s: %v", tuple.schema, mutation.name, err)
			}
		}
	}
}

func TestResourceBudgetAccountsForRoleSets(t *testing.T) {
	t.Parallel()

	minimum := func(run Run) int {
		for limit := 1; limit < 100_000; limit++ {
			if validateResourceBudget(run, limit) == nil {
				return limit
			}
		}
		t.Fatal("no passing resource limit")
		return 0
	}
	baseConfiguration := validRun()
	roleConfiguration := baseConfiguration
	roleConfiguration.Configuration.Voters = []raft.NodeID{"a", "b"}
	roleConfiguration.Configuration.Learners = []raft.NodeID{"c"}
	if baseMinimum, roleMinimum := minimum(baseConfiguration), minimum(roleConfiguration); roleMinimum <= baseMinimum {
		t.Fatalf("configuration base minimum=%d role minimum=%d", baseMinimum, roleMinimum)
	}
	baseAction := validRun()
	baseAction.Scenario.Actions = []Action{{Kind: ActionPropose}}
	roleAction := baseAction
	roleAction.Scenario.Actions = []Action{{Kind: ActionPropose, Voters: []raft.NodeID{"a", "b"}, Learners: []raft.NodeID{"c"}}}
	if baseMinimum, roleMinimum := minimum(baseAction), minimum(roleAction); roleMinimum <= baseMinimum {
		t.Fatalf("action base minimum=%d role minimum=%d", baseMinimum, roleMinimum)
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
