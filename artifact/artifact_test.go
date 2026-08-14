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
	unknown := strings.Replace(output.String(), `"schema": "d-raft.run/v1",`, `"schema": "d-raft.run/v1", "future": true,`, 1)
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
	run.Scenario.ID = "bad\x1b]8;;"
	if err := run.Validate(); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("identifier error = %v", err)
	}
}

func validRun() Run {
	config := raftsim.DefaultConfig("a", "b", "c")
	digest, _ := DigestJSON(map[string]any{"state": "ok"})
	return Run{
		Schema:          SchemaVersion,
		Scenario:        Scenario{ID: "steady", Version: "1", DurationNS: int64(time.Second), MaxSteps: 10_000},
		Adapter:         Adapter{ID: ReferenceAdapterID, Version: ReferenceAdapterV1},
		Configuration:   ConfigurationFrom(config),
		Reproducibility: Reproducibility{DecisionSeed: 1, GitRevision: "test", GoVersion: "go-test", DecisionSchema: decision.SchemaVersion, CheckerSchema: check.SchemaVersion, MessageCodec: MessageCodecV1, ObservationSchema: ObservationSchemaV1},
		Decisions:       decision.Tape{Schema: decision.SchemaVersion},
		Outcome:         Outcome{Status: OutcomeCompleted, EndNS: int64(time.Second), ObservationDigest: digest},
	}
}
