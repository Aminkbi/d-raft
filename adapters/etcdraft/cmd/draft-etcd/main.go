package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/aminkbi/d-raft/adapters/etcdraft"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	rootraft "github.com/aminkbi/d-raft/raft"
)

var version = "devel"

func main() { os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr)) }

func execute(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	switch args[0] {
	case "run":
		return runCommand(args[1:], stdout, stderr)
	case "replay":
		return replayCommand(args[1:], stdout, stderr)
	case "version":
		fmt.Fprintf(stdout, "draft-etcd %s (%s@%s adapter-schema=%s)\n", version, etcdraft.AdapterID, etcdraft.UpstreamVersion, etcdraft.AdapterSchemaVersion)
		return 0
	case "help", "-h", "--help":
		usage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "draft-etcd: unknown command %q\n", args[0])
		usage(stderr)
		return 2
	}
}

func runCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("draft-etcd run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	output := flags.String("out", "d-raft-etcd-run.json", "artifact output path")
	seed := flags.Uint64("seed", 1, "semantic decision seed")
	duration := flags.Duration("duration", 2*time.Second, "virtual run duration")
	membersText := flags.String("members", "a,b,c", "comma-separated member IDs")
	loss := flags.Float64("loss", 0, "network loss probability")
	minLatency := flags.Duration("network-min", time.Millisecond, "minimum network latency")
	maxLatency := flags.Duration("network-max", 5*time.Millisecond, "maximum network latency")
	electionMin := flags.Duration("election-min", 150*time.Millisecond, "minimum semantic election timeout")
	electionMax := flags.Duration("election-max", 300*time.Millisecond, "maximum semantic election timeout")
	heartbeat := flags.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat interval")
	storage := flags.Duration("storage", time.Millisecond, "atomic storage latency")
	maxSteps := flags.Uint64("max-steps", 1_000_000, "maximum simulator events")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "draft-etcd run: unexpected positional arguments")
		return 2
	}
	members, err := parseMembers(*membersText)
	if err != nil {
		fmt.Fprintf(stderr, "draft-etcd run: %v\n", err)
		return 2
	}
	configuration := artifact.Configuration{
		Members: members, InfrastructureSeed: artifact.Uint64(*seed),
		NetworkMinLatencyNS: int64(*minLatency), NetworkMaxLatencyNS: int64(*maxLatency), NetworkLossProbability: *loss,
		ElectionTimeoutMinNS: int64(*electionMin), ElectionTimeoutMaxNS: int64(*electionMax),
		HeartbeatIntervalNS: int64(*heartbeat), StorageLatencyNS: int64(*storage), StopOnViolation: true,
	}
	scenario := artifact.Scenario{ID: "etcdraft-steady-cluster", Version: "1", DurationNS: int64(*duration), MaxSteps: *maxSteps}
	recorder := decision.NewRecorder(decision.NewSeedDecider(*seed))
	outcome, err := etcdraft.Execute(scenario, configuration, recorder)
	if err != nil {
		fmt.Fprintf(stderr, "draft-etcd run: %v\n", err)
		return 2
	}
	if err := recorder.Err(); err != nil {
		fmt.Fprintf(stderr, "draft-etcd run: %v\n", err)
		return 2
	}
	run := artifact.Run{
		Schema: artifact.SchemaVersion, Scenario: scenario,
		Adapter: artifact.Adapter{ID: etcdraft.AdapterID, Version: etcdraft.AdapterVersion}, Configuration: configuration,
		Reproducibility: reproducibility(*seed), Decisions: recorder.Tape(), Outcome: outcome,
	}
	if err := writeArtifact(*output, run); err != nil {
		fmt.Fprintf(stderr, "draft-etcd run: %v\n", err)
		return 2
	}
	fmt.Fprintf(stdout, "wrote %s\nstatus: %s\ndecisions: %d\nobservation: %s\n", *output, outcome.Status, len(run.Decisions.Entries), outcome.ObservationDigest)
	if outcome.Status != artifact.OutcomeCompleted {
		return 1
	}
	return 0
}

func replayCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("draft-etcd replay", flag.ContinueOnError)
	flags.SetOutput(stderr)
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: draft-etcd replay ARTIFACT")
		return 2
	}
	file, err := os.Open(flags.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "draft-etcd replay: %v\n", err)
		return 2
	}
	run, decodeErr := artifact.Decode(file)
	closeErr := file.Close()
	if decodeErr != nil {
		fmt.Fprintf(stderr, "draft-etcd replay: %v\n", decodeErr)
		return 2
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "draft-etcd replay: %v\n", closeErr)
		return 2
	}
	if run.Adapter.ID != etcdraft.AdapterID || run.Adapter.Version != etcdraft.AdapterVersion || run.Reproducibility.CheckerSchema != etcdraft.CheckerProfile || run.Reproducibility.MessageCodec != etcdraft.MessageCodecVersion || run.Reproducibility.ObservationSchema != etcdraft.ObservationSchemaVersion {
		fmt.Fprintln(stderr, "draft-etcd replay: incompatible adapter or normalization schema")
		return 2
	}
	replay, err := decision.NewTapeDecider(run.Decisions)
	if err != nil {
		fmt.Fprintf(stderr, "draft-etcd replay: %v\n", err)
		return 2
	}
	outcome, err := etcdraft.Execute(run.Scenario, run.Configuration, replay)
	if err != nil {
		fmt.Fprintf(stderr, "draft-etcd replay: %v\n", err)
		return 2
	}
	if err := replay.Finish(); err != nil {
		fmt.Fprintf(stderr, "draft-etcd replay: %v\n", err)
		return 1
	}
	if !artifact.OutcomesEqual(run.Outcome, outcome) {
		fmt.Fprintln(stderr, "draft-etcd replay: outcome mismatch")
		return 1
	}
	fmt.Fprintf(stdout, "verified %s\nstatus: %s\ndecisions: %d\nobservation: %s\n", flags.Arg(0), outcome.Status, len(run.Decisions.Entries), outcome.ObservationDigest)
	return 0
}

func reproducibility(seed uint64) artifact.Reproducibility {
	result := artifact.NewReproducibility(seed)
	result.CheckerSchema = etcdraft.CheckerProfile
	result.MessageCodec = etcdraft.MessageCodecVersion
	result.ObservationSchema = etcdraft.ObservationSchemaVersion
	return result
}

func parseMembers(value string) ([]rootraft.NodeID, error) {
	parts := strings.Split(value, ",")
	result := make([]rootraft.NodeID, 0, len(parts))
	seen := make(map[rootraft.NodeID]struct{}, len(parts))
	for _, part := range parts {
		member := rootraft.NodeID(strings.TrimSpace(part))
		if member == "" {
			return nil, errors.New("member IDs must not be empty")
		}
		if _, exists := seen[member]; exists {
			return nil, fmt.Errorf("duplicate member %q", member)
		}
		seen[member] = struct{}{}
		result = append(result, member)
	}
	slices.Sort(result)
	return result, nil
}

func writeArtifact(path string, run artifact.Run) (err error) {
	if path == "" {
		return errors.New("empty output path")
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := artifact.Encode(temporary, run); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Link(temporaryPath, path)
}

func usage(writer io.Writer) {
	fmt.Fprintln(writer, "d-raft experimental etcd/raft production-core adapter")
	fmt.Fprintln(writer, "usage: draft-etcd <run|replay|version> [options]")
}
