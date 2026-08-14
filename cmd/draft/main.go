package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/experiment"
	"github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/raftsim"
)

var version = "devel"

func main() {
	os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr))
}

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
	case "inspect":
		return inspectCommand(args[1:], stdout, stderr)
	case "version":
		fmt.Fprintf(stdout, "draft %s (%s)\n", version, artifact.SchemaVersion)
		return 0
	case "help", "-h", "--help":
		usage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "draft: unknown command %q\n", args[0])
		usage(stderr)
		return 2
	}
}

func runCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("draft run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	output := flags.String("out", "d-raft-run.json", "artifact output path")
	seed := flags.Uint64("seed", 1, "semantic decision seed")
	duration := flags.Duration("duration", 2*time.Second, "virtual run duration")
	membersText := flags.String("members", "a,b,c", "comma-separated member IDs")
	loss := flags.Float64("loss", 0, "network loss probability")
	minLatency := flags.Duration("network-min", time.Millisecond, "minimum network latency")
	maxLatency := flags.Duration("network-max", 5*time.Millisecond, "maximum network latency")
	electionMin := flags.Duration("election-min", 150*time.Millisecond, "minimum election timeout")
	electionMax := flags.Duration("election-max", 300*time.Millisecond, "maximum election timeout")
	heartbeat := flags.Duration("heartbeat", 50*time.Millisecond, "heartbeat interval")
	storage := flags.Duration("storage", time.Millisecond, "storage latency")
	maxSteps := flags.Uint64("max-steps", 1_000_000, "maximum simulator events")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "draft run: unexpected positional arguments")
		return 2
	}
	members, err := parseMembers(*membersText)
	if err != nil {
		reportError(stderr, "draft run", err)
		return 2
	}
	if *duration < 0 {
		fmt.Fprintln(stderr, "draft run: duration must be non-negative")
		return 2
	}
	config := raftsim.Config{
		Members: members, Seed: *seed,
		Network:            sim.LinkConfig{MinLatency: *minLatency, MaxLatency: *maxLatency, LossProbability: *loss},
		ElectionTimeoutMin: *electionMin, ElectionTimeoutMax: *electionMax,
		HeartbeatInterval: *heartbeat, StorageLatency: *storage, StopOnViolation: true,
	}
	scenario := artifact.Scenario{ID: "steady-cluster", Version: "1", DurationNS: int64(*duration), MaxSteps: *maxSteps}
	recorder := decision.NewRecorder(decision.NewSeedDecider(*seed))
	outcome, err := experiment.Execute(scenario, artifact.ConfigurationFrom(config), recorder)
	if err != nil {
		reportError(stderr, "draft run", err)
		return 2
	}
	if err := recorder.Err(); err != nil {
		reportError(stderr, "draft run: record decisions", err)
		return 2
	}
	run := artifact.Run{
		Schema:          artifact.SchemaVersion,
		Scenario:        scenario,
		Adapter:         artifact.Adapter{ID: artifact.ReferenceAdapterID, Version: artifact.ReferenceAdapterV1},
		Configuration:   artifact.ConfigurationFrom(config),
		Reproducibility: artifact.NewReproducibility(*seed),
		Decisions:       recorder.Tape(),
		Outcome:         outcome,
	}
	if err := writeArtifact(*output, run); err != nil {
		reportError(stderr, "draft run: write artifact", err)
		return 2
	}
	fmt.Fprintf(stdout, "wrote %s\nstatus: %s\ndecisions: %d\nobservation: %s\n", safeText(*output, 4096), outcome.Status, len(run.Decisions.Entries), outcome.ObservationDigest)
	if outcome.Status != artifact.OutcomeCompleted {
		return 1
	}
	return 0
}

func replayCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("draft replay", flag.ContinueOnError)
	flags.SetOutput(stderr)
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: draft replay ARTIFACT")
		return 2
	}
	run, err := readArtifact(flags.Arg(0))
	if err != nil {
		reportError(stderr, "draft replay", err)
		return 2
	}
	if run.Adapter.ID != artifact.ReferenceAdapterID || run.Adapter.Version != artifact.ReferenceAdapterV1 {
		fmt.Fprintf(stderr, "draft replay: unsupported adapter %s@%s\n", safeText(run.Adapter.ID, 128), safeText(run.Adapter.Version, 64))
		return 2
	}
	replay, err := decision.NewTapeDecider(run.Decisions)
	if err != nil {
		reportError(stderr, "draft replay", err)
		return 2
	}
	outcome, err := experiment.Execute(run.Scenario, run.Configuration, replay)
	if err != nil {
		reportError(stderr, "draft replay: execute", err)
		return 2
	}
	if err := replay.Finish(); err != nil {
		reportError(stderr, "draft replay", err)
		return 1
	}
	if !artifact.OutcomesEqual(run.Outcome, outcome) {
		fmt.Fprintf(stderr, "draft replay: outcome mismatch\nrecorded: status=%s steps=%d end=%s digest=%s\nreplayed: status=%s steps=%d end=%s digest=%s\n", run.Outcome.Status, run.Outcome.Steps, time.Duration(run.Outcome.EndNS), run.Outcome.ObservationDigest, outcome.Status, outcome.Steps, time.Duration(outcome.EndNS), outcome.ObservationDigest)
		return 1
	}
	fmt.Fprintf(stdout, "verified %s\nstatus: %s\ndecisions: %d\nobservation: %s\n", safeText(flags.Arg(0), 4096), outcome.Status, len(run.Decisions.Entries), outcome.ObservationDigest)
	return 0
}

func inspectCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("draft inspect", flag.ContinueOnError)
	flags.SetOutput(stderr)
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: draft inspect ARTIFACT")
		return 2
	}
	run, err := readArtifact(flags.Arg(0))
	if err != nil {
		reportError(stderr, "draft inspect", err)
		return 2
	}
	fmt.Fprintf(stdout, "schema: %s\nscenario: %s@%s\nadapter: %s@%s\nmembers: %s\nduration: %s\nmax steps: %d\nactions: %d\ninfrastructure seed: %d\ndecision seed: %d\nnetwork: %s..%s loss=%g\nelection: %s..%s\nheartbeat: %s\nstorage: %s\nsource: %s modified=%t\ngo: %s\ndecision schema: %s\nchecker schema: %s\nobservation schema: %s\nmessage codec: %s\ndecisions: %d\nstatus: %s\nsteps: %d\nend: %s\nobservation: %s\n", safeText(run.Schema, 128), safeText(run.Scenario.ID, 128), safeText(run.Scenario.Version, 64), safeText(run.Adapter.ID, 128), safeText(run.Adapter.Version, 64), joinMembers(run.Configuration.Members), time.Duration(run.Scenario.DurationNS), run.Scenario.MaxSteps, len(run.Scenario.Actions), run.Configuration.InfrastructureSeed, run.Reproducibility.DecisionSeed, time.Duration(run.Configuration.NetworkMinLatencyNS), time.Duration(run.Configuration.NetworkMaxLatencyNS), run.Configuration.NetworkLossProbability, time.Duration(run.Configuration.ElectionTimeoutMinNS), time.Duration(run.Configuration.ElectionTimeoutMaxNS), time.Duration(run.Configuration.HeartbeatIntervalNS), time.Duration(run.Configuration.StorageLatencyNS), safeText(run.Reproducibility.GitRevision, 128), run.Reproducibility.GitModified, safeText(run.Reproducibility.GoVersion, 64), safeText(run.Reproducibility.DecisionSchema, 128), safeText(run.Reproducibility.CheckerSchema, 128), safeText(run.Reproducibility.ObservationSchema, 128), safeText(run.Reproducibility.MessageCodec, 128), len(run.Decisions.Entries), run.Outcome.Status, run.Outcome.Steps, time.Duration(run.Outcome.EndNS), run.Outcome.ObservationDigest)
	for index, action := range run.Scenario.Actions {
		fmt.Fprintf(stdout, "action %d: at=%s kind=%s node=%s data_bytes=%d groups=%s\n", index, time.Duration(action.AtNS), safeText(string(action.Kind), 64), safeText(string(action.Node), 64), len(action.Data), formatGroups(action.Groups))
	}
	for _, violation := range run.Outcome.Violations {
		fmt.Fprintf(stdout, "violation: %s [%s] nodes=%s\n", safeText(violation.ID, 128), violation.Fingerprint, joinMembers(violation.Nodes))
		fmt.Fprintf(stdout, "evidence: %s\n", safeText(string(violation.Evidence), 4096))
	}
	if run.Outcome.Error != "" {
		fmt.Fprintf(stdout, "error: %s\n", safeText(run.Outcome.Error, artifact.MaxOutcomeErrorBytes))
	}
	return 0
}

func parseMembers(value string) ([]raft.NodeID, error) {
	parts := strings.Split(value, ",")
	members := make([]raft.NodeID, 0, len(parts))
	seen := make(map[raft.NodeID]struct{}, len(parts))
	for _, part := range parts {
		member := raft.NodeID(strings.TrimSpace(part))
		if member == "" {
			return nil, errors.New("member IDs must not be empty")
		}
		if _, exists := seen[member]; exists {
			return nil, fmt.Errorf("duplicate member %q", member)
		}
		seen[member] = struct{}{}
		members = append(members, member)
	}
	if len(members) == 0 {
		return nil, errors.New("membership must not be empty")
	}
	slices.Sort(members)
	return members, nil
}

func joinMembers(members []raft.NodeID) string {
	parts := make([]string, len(members))
	for index, member := range members {
		parts[index] = safeText(string(member), 64)
	}
	return strings.Join(parts, ",")
}

func formatGroups(groups [][]raft.NodeID) string {
	formatted := make([]string, len(groups))
	for index, group := range groups {
		formatted[index] = "[" + joinMembers(group) + "]"
	}
	return "[" + strings.Join(formatted, ",") + "]"
}

func readArtifact(path string) (artifact.Run, error) {
	file, err := os.Open(path)
	if err != nil {
		return artifact.Run{}, err
	}
	defer file.Close()
	return artifact.Decode(file)
}

func writeArtifact(path string, run artifact.Run) (err error) {
	if path == "" {
		return errors.New("empty output path")
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	closed := false
	defer func() {
		if !closed {
			if closeErr := temporary.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}
		if err != nil {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err = artifact.Encode(temporary, run); err != nil {
		return err
	}
	if err = temporary.Sync(); err != nil {
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	closed = true
	if err = os.Link(temporaryPath, path); err != nil {
		return err
	}
	if err = os.Remove(temporaryPath); err != nil {
		return err
	}
	return nil
}

func usage(writer io.Writer) {
	fmt.Fprintln(writer, "d-raft semantic counterexample research CLI")
	fmt.Fprintln(writer, "usage: draft <run|replay|inspect|version> [options]")
}

func reportError(writer io.Writer, prefix string, err error) {
	fmt.Fprintf(writer, "%s: %s\n", prefix, safeText(err.Error(), artifact.MaxOutcomeErrorBytes))
}

func safeText(value string, maximum int) string {
	if len(value) > maximum {
		value = value[:maximum] + "..."
	}
	return strconv.QuoteToGraphic(value)
}
