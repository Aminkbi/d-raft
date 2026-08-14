// Command draft-cross executes one portable semantic plan against the d-raft
// reference model and the production go.etcd.io/raft adapter.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/aminkbi/d-raft/adapters/etcdraft"
	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/experiment"
	"github.com/aminkbi/d-raft/internal/strictjson"
	"github.com/aminkbi/d-raft/semanticplan"
)

var version = "devel"

const (
	bundleManifestSchema = "d-raft.cross-bundle/v1"
	maxManifestBytes     = 1 << 20
	maxBundleBytes       = 256 << 20
)

var bundleRoles = []string{
	"reference_capabilities",
	"etcdraft_capabilities",
	"reference_execution",
	"etcdraft_execution",
	"reference_outcome",
	"etcdraft_outcome",
	"comparison",
}

type bundleFile struct {
	Role   string          `json:"role"`
	Name   string          `json:"name"`
	SHA256 string          `json:"sha256"`
	Bytes  artifact.Uint64 `json:"bytes"`
}

type bundleManifest struct {
	Schema          string       `json:"schema"`
	PlanSHA256      string       `json:"plan_sha256"`
	SourceRunSHA256 string       `json:"source_run_sha256"`
	Files           []bundleFile `json:"files"`
}

func main() { os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr)) }

func execute(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	switch args[0] {
	case "derive":
		return deriveCommand(args[1:], stdout, stderr)
	case "run":
		return runCommand(args[1:], stdout, stderr)
	case "verify":
		return verifyCommand(args[1:], stdout, stderr)
	case "recover":
		return recoverCommand(args[1:], stdout, stderr)
	case "version":
		fmt.Fprintf(stdout, "draft-cross %s (reference=%s etcdraft=%s)\n", version, experiment.ReferenceSemanticCapabilities().Adapter.Version, etcdraft.AdapterVersion)
		return 0
	case "help", "-h", "--help":
		usage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "draft-cross: unknown command %q\n", args[0])
		usage(stderr)
		return 2
	}
}

func deriveCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("draft-cross derive", flag.ContinueOnError)
	flags.SetOutput(stderr)
	sourcePath := flags.String("source-run", "", "exact reference run artifact")
	output := flags.String("out", "d-raft-semantic-plan.json", "semantic plan output path")
	fallbackSeed := flags.Uint64("fallback-seed", 1, "seed for additional target choices")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || *sourcePath == "" || !validOutputPrefix(*output) {
		fmt.Fprintln(stderr, "usage: draft-cross derive --source-run RUN [--fallback-seed N] [--out PLAN]")
		return 2
	}
	source, err := readSourceRun(*sourcePath)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: source run: %v\n", err)
		return 2
	}
	if err := verifyReferenceSource(source.run); err != nil {
		fmt.Fprintf(stderr, "draft-cross: source replay: %v\n", err)
		return 1
	}
	directives, err := semanticplan.DirectivesFromTape(source.run.Decisions)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: source decisions: %v\n", err)
		return 2
	}
	workloadEnd := int64(0)
	for _, action := range source.run.Scenario.Actions {
		workloadEnd = max(workloadEnd, action.AtNS)
	}
	plan := semanticplan.Plan{
		Schema: semanticplan.SemanticPlanSchema, Scenario: source.run.Scenario,
		Configuration: source.run.Configuration, Application: apporacle.KVConfig(),
		Convergence: semanticplan.Convergence{
			WorkloadEndNS: workloadEnd, ComparisonBoundaryNS: source.run.Scenario.DurationNS,
		},
		Source: semanticplan.Source{
			Adapter: source.run.Adapter, RunSHA256: source.sha256,
		},
		FallbackSeed: artifact.Uint64(*fallbackSeed), Directives: directives,
	}
	leftCapabilities := experiment.ReferenceSemanticCapabilities()
	rightCapabilities := etcdraft.SemanticCapabilities()
	eligibility, err := semanticplan.Preflight(plan, leftCapabilities, rightCapabilities)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: derive plan: %v\n", err)
		return 2
	}
	if !eligibility.Eligible {
		fmt.Fprintf(stderr, "draft-cross: ineligible source run: %s\n", joinRejections(eligibility.Rejections))
		return 1
	}
	digest, err := semanticplan.DigestPlan(plan)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: plan digest: %v\n", err)
		return 2
	}
	if err := ensureTargetsAbsent([]string{*output}); err != nil {
		fmt.Fprintf(stderr, "draft-cross: output: %v\n", err)
		return 2
	}
	if err := publishResults([]string{*output}, []any{plan}); err != nil {
		fmt.Fprintf(stderr, "draft-cross: publish: %v\n", err)
		return 2
	}
	fmt.Fprintf(stdout, "wrote %s\nplan: %s\ndirectives: %d\nsource: %s\n", *output, digest, len(plan.Directives), source.sha256)
	return 0
}

func runCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("draft-cross run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "portable semantic-plan JSON file")
	sourcePath := flags.String("source-run", "", "exact source run named by the plan")
	outputPrefix := flags.String("out", "d-raft-cross", "output path prefix")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || *planPath == "" || *sourcePath == "" || !validOutputPrefix(*outputPrefix) {
		fmt.Fprintln(stderr, "usage: draft-cross run --plan PLAN --source-run RUN [--out PREFIX]")
		return 2
	}
	planFile, err := os.Open(*planPath)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: open plan: %v\n", err)
		return 2
	}
	plan, decodeErr := semanticplan.DecodePlan(planFile)
	closeErr := planFile.Close()
	if decodeErr != nil {
		fmt.Fprintf(stderr, "draft-cross: decode plan: %v\n", decodeErr)
		return 2
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "draft-cross: close plan: %v\n", closeErr)
		return 2
	}
	source, err := readSourceRun(*sourcePath)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: source run: %v\n", err)
		return 2
	}
	if err := verifyReferenceSource(source.run); err != nil {
		fmt.Fprintf(stderr, "draft-cross: source replay: %v\n", err)
		return 1
	}
	if err := verifySourceRun(plan, source); err != nil {
		fmt.Fprintf(stderr, "draft-cross: source provenance: %v\n", err)
		return 2
	}

	leftCapabilities := experiment.ReferenceSemanticCapabilities()
	rightCapabilities := etcdraft.SemanticCapabilities()
	eligibility, err := semanticplan.Preflight(plan, leftCapabilities, rightCapabilities)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: preflight: %v\n", err)
		return 2
	}
	if !eligibility.Eligible {
		fmt.Fprintf(stderr, "draft-cross: ineligible: %s\n", joinRejections(eligibility.Rejections))
		return 1
	}
	paths := bundlePaths(*outputPrefix)
	if err := ensureTargetsAbsent(paths); err != nil {
		fmt.Fprintf(stderr, "draft-cross: output: %v\n", err)
		return 2
	}

	leftExecution, err := experiment.ExecuteSemanticPlan(plan)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: reference execution: %v\n", err)
		return 2
	}
	rightExecution, err := etcdraft.ExecuteSemanticPlan(plan)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: etcdraft execution: %v\n", err)
		return 2
	}
	leftOutcome, err := semanticplan.NormalizeExecution(plan, leftCapabilities, rightCapabilities, leftExecution)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: normalize reference: %v\n", err)
		return 2
	}
	rightOutcome, err := semanticplan.NormalizeExecution(plan, rightCapabilities, leftCapabilities, rightExecution)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: normalize etcdraft: %v\n", err)
		return 2
	}
	comparison, err := semanticplan.CompareNormalized(leftOutcome, rightOutcome)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: compare: %v\n", err)
		return 2
	}
	values := []any{
		leftCapabilities, rightCapabilities, leftExecution, rightExecution,
		leftOutcome, rightOutcome, comparison,
	}
	if err := publishBundle(*outputPrefix, comparison.PlanSHA256, source.sha256, values); err != nil {
		fmt.Fprintf(stderr, "draft-cross: publish: %v\n", err)
		return 2
	}
	fmt.Fprintf(stdout, "plan: %s\nreference projection: %s\netcdraft projection: %s\nexecution boundary: %s\nsafety: %s\napplication: %s\noutputs: %s.*.json\n",
		comparison.PlanSHA256, leftOutcome.Projection, rightOutcome.Projection,
		comparison.ExecutionBoundary, comparison.Safety, comparison.Application, *outputPrefix)
	if comparison.ExecutionBoundary != semanticplan.ExecutionBothReached || comparison.Safety == semanticplan.AgreementDisagree || comparison.Application != semanticplan.ApplicationAgree {
		return 1
	}
	return 0
}

func verifyCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("draft-cross verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "portable semantic-plan JSON file")
	sourcePath := flags.String("source-run", "", "exact source run named by the plan")
	inputPrefix := flags.String("in", "", "cross-result bundle path prefix")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || *planPath == "" || *sourcePath == "" || !validOutputPrefix(*inputPrefix) {
		fmt.Fprintln(stderr, "usage: draft-cross verify --plan PLAN --source-run RUN --in PREFIX")
		return 2
	}
	plan, err := readPlan(*planPath)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: verify plan: %v\n", err)
		return 2
	}
	source, err := readSourceRun(*sourcePath)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: verify source run: %v\n", err)
		return 2
	}
	comparison, err := verifyBundle(*inputPrefix, plan, source)
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: verify: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "verified %s.manifest.json\nplan: %s\nexecution boundary: %s\nsafety: %s\napplication: %s\n",
		*inputPrefix, comparison.PlanSHA256, comparison.ExecutionBoundary, comparison.Safety, comparison.Application)
	return 0
}

func recoverCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("draft-cross recover", flag.ContinueOnError)
	flags.SetOutput(stderr)
	inputPrefix := flags.String("in", "", "incomplete cross-result bundle path prefix")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || !validOutputPrefix(*inputPrefix) {
		fmt.Fprintln(stderr, "usage: draft-cross recover --in PREFIX")
		return 2
	}
	if _, err := os.Lstat(manifestPath(*inputPrefix)); err == nil {
		fmt.Fprintln(stderr, "draft-cross: recover: refusing to remove a committed bundle")
		return 1
	} else if !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(stderr, "draft-cross: recover: %v\n", err)
		return 2
	}
	targets := make([]string, 0, len(resultPaths(*inputPrefix)))
	for _, path := range resultPaths(*inputPrefix) {
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			fmt.Fprintf(stderr, "draft-cross: recover: %v\n", err)
			return 2
		}
		if !info.Mode().IsRegular() {
			fmt.Fprintf(stderr, "draft-cross: recover: refusing non-regular target %s\n", path)
			return 1
		}
		targets = append(targets, path)
	}
	if len(targets) == 0 {
		fmt.Fprintln(stderr, "draft-cross: recover: no incomplete bundle files found")
		return 1
	}
	removed := 0
	for _, path := range targets {
		if err := os.Remove(path); err != nil {
			fmt.Fprintf(stderr, "draft-cross: recover: %v\n", err)
			return 2
		}
		removed++
	}
	directory, err := os.Open(filepath.Dir(*inputPrefix))
	if err != nil {
		fmt.Fprintf(stderr, "draft-cross: recover: %v\n", err)
		return 2
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		fmt.Fprintf(stderr, "draft-cross: recover: %v\n", errors.Join(syncErr, closeErr))
		return 2
	}
	fmt.Fprintf(stdout, "removed %d incomplete bundle files for %s\n", removed, *inputPrefix)
	return 0
}

type sourceRun struct {
	run    artifact.Run
	sha256 string
}

func readPlan(path string) (semanticplan.Plan, error) {
	file, err := os.Open(path)
	if err != nil {
		return semanticplan.Plan{}, err
	}
	plan, decodeErr := semanticplan.DecodePlan(file)
	closeErr := file.Close()
	if decodeErr != nil {
		return semanticplan.Plan{}, decodeErr
	}
	if closeErr != nil {
		return semanticplan.Plan{}, closeErr
	}
	return plan, nil
}

func readSourceRun(path string) (sourceRun, error) {
	file, err := os.Open(path)
	if err != nil {
		return sourceRun{}, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, int64(artifact.DefaultMaxArtifactBytes)+1))
	closeErr := file.Close()
	if readErr != nil {
		return sourceRun{}, readErr
	}
	if closeErr != nil {
		return sourceRun{}, closeErr
	}
	if len(raw) > artifact.DefaultMaxArtifactBytes {
		return sourceRun{}, artifact.ErrArtifactTooLarge
	}
	run, err := artifact.Decode(bytes.NewReader(raw))
	if err != nil {
		return sourceRun{}, err
	}
	digest := sha256.Sum256(raw)
	return sourceRun{run: run, sha256: hex.EncodeToString(digest[:])}, nil
}

func verifyReferenceSource(run artifact.Run) error {
	wantAdapter := experiment.ReferenceSemanticCapabilities().Adapter
	if run.Schema != artifact.SchemaVersion || run.Adapter != wantAdapter {
		return fmt.Errorf("requires %s from %s@%s", artifact.SchemaVersion, wantAdapter.ID, wantAdapter.Version)
	}
	if name, ok := experiment.CanonicalName(run.Scenario); ok {
		if err := experiment.VerifyCanonical(name, run.Scenario, run.Configuration, uint64(run.Reproducibility.DecisionSeed)); err != nil {
			return err
		}
	}
	replay, err := decision.NewTapeDecider(run.Decisions)
	if err != nil {
		return err
	}
	outcome, err := experiment.Execute(run.Scenario, run.Configuration, replay)
	if err != nil {
		return err
	}
	if err := replay.Finish(); err != nil {
		return err
	}
	if !artifact.OutcomesEqual(run.Outcome, outcome) {
		return errors.New("source outcome does not match exact replay")
	}
	return nil
}

func verifySourceRun(plan semanticplan.Plan, source sourceRun) error {
	if plan.Source.RunSHA256 != source.sha256 {
		return errors.New("source run SHA-256 mismatch")
	}
	if plan.Source.Adapter != source.run.Adapter {
		return errors.New("source adapter mismatch")
	}
	if !reflect.DeepEqual(plan.Scenario, source.run.Scenario) || !reflect.DeepEqual(plan.Configuration, source.run.Configuration) {
		return errors.New("source scenario or configuration mismatch")
	}
	directives, err := semanticplan.DirectivesFromTape(source.run.Decisions)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(plan.Directives, directives) {
		return errors.New("source directives mismatch")
	}
	return nil
}

func resultPaths(prefix string) []string {
	return []string{
		prefix + ".reference.capabilities.json",
		prefix + ".etcdraft.capabilities.json",
		prefix + ".reference.execution.json",
		prefix + ".etcdraft.execution.json",
		prefix + ".reference.outcome.json",
		prefix + ".etcdraft.outcome.json",
		prefix + ".comparison.json",
	}
}

func manifestPath(prefix string) string { return prefix + ".manifest.json" }

func bundlePaths(prefix string) []string {
	return append(resultPaths(prefix), manifestPath(prefix))
}

func validOutputPrefix(prefix string) bool {
	if prefix == "" || strings.IndexByte(prefix, 0) >= 0 {
		return false
	}
	base := filepath.Base(prefix)
	return base != "." && base != string(filepath.Separator)
}

func ensureTargetsAbsent(paths []string) error {
	if len(paths) == 0 {
		return errors.New("empty target set")
	}
	directory := filepath.Dir(paths[0])
	info, err := os.Stat(directory)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("output parent is not a directory: %s", directory)
	}
	for _, path := range paths {
		if filepath.Dir(path) != directory {
			return errors.New("result targets span directories")
		}
		_, err := os.Lstat(path)
		if err == nil {
			return fmt.Errorf("target already exists: %s", path)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func publishResults(paths []string, values []any) (err error) {
	if len(paths) == 0 || len(paths) != len(values) {
		return errors.New("invalid result set")
	}
	documents := make([][]byte, len(values))
	for index, value := range values {
		encoded, encodeErr := encodeDocument(value)
		if encodeErr != nil {
			return encodeErr
		}
		documents[index] = encoded
	}
	return publishDocuments(paths, documents)
}

func publishBundle(prefix, planDigest, sourceDigest string, values []any) error {
	paths := resultPaths(prefix)
	if len(values) != len(paths) || len(paths) != len(bundleRoles) {
		return errors.New("invalid cross-result bundle")
	}
	documents := make([][]byte, len(values), len(values)+1)
	files := make([]bundleFile, len(values))
	for index, value := range values {
		encoded, err := encodeDocument(value)
		if err != nil {
			return err
		}
		documents[index] = encoded
		digest := sha256.Sum256(encoded)
		files[index] = bundleFile{
			Role: bundleRoles[index], Name: filepath.Base(paths[index]),
			SHA256: hex.EncodeToString(digest[:]), Bytes: artifact.Uint64(len(encoded)),
		}
	}
	manifest := bundleManifest{
		Schema: bundleManifestSchema, PlanSHA256: planDigest,
		SourceRunSHA256: sourceDigest, Files: files,
	}
	if err := manifest.validate(prefix); err != nil {
		return err
	}
	manifestDocument, err := encodeDocument(manifest)
	if err != nil {
		return err
	}
	paths = append(paths, manifestPath(prefix))
	documents = append(documents, manifestDocument)
	return publishDocuments(paths, documents)
}

func encodeDocument(value any) ([]byte, error) {
	// Compact encoding preserves json.RawMessage bytes in exact local decision
	// tapes. MarshalIndent would rewrite their insignificant whitespace and thus
	// change the execution digest after a decode/encode round trip.
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

// publishDocuments links the manifest last. Absence of that commit marker
// makes an interrupted bundle unambiguously incomplete, while hard links keep
// every target no-clobber even under a concurrent publisher.
func publishDocuments(paths []string, documents [][]byte) (err error) {
	if len(paths) == 0 || len(paths) != len(documents) {
		return errors.New("invalid document set")
	}
	directory := filepath.Dir(paths[0])
	for _, path := range paths[1:] {
		if filepath.Dir(path) != directory {
			return errors.New("result targets span directories")
		}
	}
	temporary, err := os.MkdirTemp(directory, ".draft-cross-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(temporary) }()
	staged := make([]string, len(paths))
	for index, encoded := range documents {
		staged[index] = filepath.Join(temporary, filepath.Base(paths[index]))
		file, createErr := os.OpenFile(staged[index], os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if createErr != nil {
			return createErr
		}
		if _, writeErr := file.Write(encoded); writeErr != nil {
			_ = file.Close()
			return writeErr
		}
		if syncErr := file.Sync(); syncErr != nil {
			_ = file.Close()
			return syncErr
		}
		if closeErr := file.Close(); closeErr != nil {
			return closeErr
		}
	}
	created := make([]string, 0, len(paths))
	defer func() {
		if err != nil {
			for _, path := range created {
				_ = os.Remove(path)
			}
			_ = syncDirectory(directory)
		}
	}()
	for index, path := range paths {
		// A multi-document publication reserves its final path for the commit
		// marker. Make every data-file directory entry durable before that marker
		// can become visible or durable.
		if len(paths) > 1 && index == len(paths)-1 {
			if err = syncDirectory(directory); err != nil {
				return err
			}
		}
		if err = os.Link(staged[index], path); err != nil {
			return err
		}
		created = append(created, path)
	}
	return syncDirectory(directory)
}

func syncDirectory(directory string) error {
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func (manifest bundleManifest) validate(prefix string) error {
	if manifest.Schema != bundleManifestSchema || !validDigest(manifest.PlanSHA256) || !validDigest(manifest.SourceRunSHA256) {
		return errors.New("invalid bundle manifest header")
	}
	expectedPaths := resultPaths(prefix)
	if manifest.Files == nil || len(manifest.Files) != len(expectedPaths) || len(manifest.Files) != len(bundleRoles) {
		return errors.New("bundle manifest has an incomplete file set")
	}
	var total uint64
	for index, file := range manifest.Files {
		if file.Role != bundleRoles[index] || file.Name != filepath.Base(expectedPaths[index]) || filepath.Base(file.Name) != file.Name || !validDigest(file.SHA256) {
			return fmt.Errorf("invalid bundle manifest file %d", index)
		}
		size := uint64(file.Bytes)
		if size == 0 || size > artifact.DefaultMaxArtifactBytes || total > maxBundleBytes-size {
			return fmt.Errorf("bundle manifest file %d exceeds its resource budget", index)
		}
		total += size
	}
	return nil
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func verifyBundle(prefix string, plan semanticplan.Plan, source sourceRun) (semanticplan.NormalizedComparison, error) {
	manifestRaw, err := readRegularFile(manifestPath(prefix), maxManifestBytes)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return semanticplan.NormalizedComparison{}, errors.New("bundle is incomplete: commit manifest is missing")
		}
		return semanticplan.NormalizedComparison{}, fmt.Errorf("manifest: %w", err)
	}
	var manifest bundleManifest
	if err := decodeStrictDocument(manifestRaw, &manifest); err != nil {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("manifest: %w", err)
	}
	if err := manifest.validate(prefix); err != nil {
		return semanticplan.NormalizedComparison{}, err
	}
	planDigest, err := semanticplan.DigestPlan(plan)
	if err != nil {
		return semanticplan.NormalizedComparison{}, err
	}
	if manifest.PlanSHA256 != planDigest || manifest.SourceRunSHA256 != source.sha256 {
		return semanticplan.NormalizedComparison{}, errors.New("bundle manifest refers to different plan or source bytes")
	}
	if err := verifyReferenceSource(source.run); err != nil {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("source replay: %w", err)
	}
	if err := verifySourceRun(plan, source); err != nil {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("source provenance: %w", err)
	}

	documents := make([][]byte, len(manifest.Files))
	for index, file := range manifest.Files {
		path := filepath.Join(filepath.Dir(prefix), file.Name)
		raw, err := readRegularFile(path, artifact.DefaultMaxArtifactBytes)
		if err != nil {
			return semanticplan.NormalizedComparison{}, fmt.Errorf("bundle file %s: %w", file.Role, err)
		}
		if uint64(len(raw)) != uint64(file.Bytes) {
			return semanticplan.NormalizedComparison{}, fmt.Errorf("bundle file %s size mismatch", file.Role)
		}
		digest := sha256.Sum256(raw)
		if hex.EncodeToString(digest[:]) != file.SHA256 {
			return semanticplan.NormalizedComparison{}, fmt.Errorf("bundle file %s SHA-256 mismatch", file.Role)
		}
		documents[index] = raw
	}

	leftCapabilities, err := semanticplan.DecodeCapabilities(bytes.NewReader(documents[0]))
	if err != nil {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("reference capabilities: %w", err)
	}
	rightCapabilities, err := semanticplan.DecodeCapabilities(bytes.NewReader(documents[1]))
	if err != nil {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("etcdraft capabilities: %w", err)
	}
	wantLeft, wantRight := experiment.ReferenceSemanticCapabilities(), etcdraft.SemanticCapabilities()
	if !reflect.DeepEqual(leftCapabilities, wantLeft) || !reflect.DeepEqual(rightCapabilities, wantRight) {
		return semanticplan.NormalizedComparison{}, errors.New("bundle capabilities do not match this verifier")
	}
	eligibility, err := semanticplan.Preflight(plan, leftCapabilities, rightCapabilities)
	if err != nil {
		return semanticplan.NormalizedComparison{}, err
	}
	if !eligibility.Eligible {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("bundle plan is ineligible: %s", joinRejections(eligibility.Rejections))
	}

	leftExecution, err := semanticplan.DecodeSemanticExecution(bytes.NewReader(documents[2]))
	if err != nil {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("reference execution: %w", err)
	}
	rightExecution, err := semanticplan.DecodeSemanticExecution(bytes.NewReader(documents[3]))
	if err != nil {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("etcdraft execution: %w", err)
	}
	if err := verifyReferenceExecution(plan, leftExecution); err != nil {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("reference exact replay: %w", err)
	}
	if err := verifyEtcdExecution(plan, rightExecution); err != nil {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("etcdraft exact replay: %w", err)
	}

	leftOutcome, err := semanticplan.DecodeNormalizedOutcome(bytes.NewReader(documents[4]))
	if err != nil {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("reference normalized outcome: %w", err)
	}
	rightOutcome, err := semanticplan.DecodeNormalizedOutcome(bytes.NewReader(documents[5]))
	if err != nil {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("etcdraft normalized outcome: %w", err)
	}
	wantLeftOutcome, err := semanticplan.NormalizeExecution(plan, leftCapabilities, rightCapabilities, leftExecution)
	if err != nil {
		return semanticplan.NormalizedComparison{}, err
	}
	wantRightOutcome, err := semanticplan.NormalizeExecution(plan, rightCapabilities, leftCapabilities, rightExecution)
	if err != nil {
		return semanticplan.NormalizedComparison{}, err
	}
	if !reflect.DeepEqual(leftOutcome, wantLeftOutcome) || !reflect.DeepEqual(rightOutcome, wantRightOutcome) {
		return semanticplan.NormalizedComparison{}, errors.New("normalized outcomes do not match the verified executions")
	}
	comparison, err := semanticplan.DecodeNormalizedComparison(bytes.NewReader(documents[6]))
	if err != nil {
		return semanticplan.NormalizedComparison{}, fmt.Errorf("comparison: %w", err)
	}
	wantComparison, err := semanticplan.CompareNormalized(wantLeftOutcome, wantRightOutcome)
	if err != nil {
		return semanticplan.NormalizedComparison{}, err
	}
	if !reflect.DeepEqual(comparison, wantComparison) {
		return semanticplan.NormalizedComparison{}, errors.New("comparison does not match the verified normalized outcomes")
	}
	return comparison, nil
}

func verifyReferenceExecution(plan semanticplan.Plan, execution semanticplan.SemanticExecution) error {
	if err := semanticplan.VerifyExecutionProjection(plan, execution); err != nil {
		return err
	}
	fresh, err := experiment.ExecuteSemanticPlan(plan)
	if err != nil {
		return err
	}
	// Producer build provenance is immutable evidence, not a requirement that
	// the verifier itself was built by the same toolchain or revision. Replace
	// only that field before comparing the regenerated semantic evidence.
	fresh.Reproducibility = execution.Reproducibility
	gotDigest, err := semanticplan.DigestSemanticExecution(execution)
	if err != nil {
		return err
	}
	wantDigest, err := semanticplan.DigestSemanticExecution(fresh)
	if err != nil {
		return err
	}
	if gotDigest != wantDigest {
		return fmt.Errorf("execution evidence differs from deterministic semantic re-execution (%s != %s)", gotDigest, wantDigest)
	}
	return verifyLocalTape(execution, func(decider decision.Decider) (artifact.Outcome, error) {
		return experiment.ExecuteWithApplication(plan.Scenario, plan.Configuration, decider, plan.Application)
	})
}

func verifyEtcdExecution(plan semanticplan.Plan, execution semanticplan.SemanticExecution) error {
	if err := semanticplan.VerifyExecutionProjection(plan, execution); err != nil {
		return err
	}
	fresh, err := etcdraft.ExecuteSemanticPlan(plan)
	if err != nil {
		return err
	}
	fresh.Reproducibility = execution.Reproducibility
	gotDigest, err := semanticplan.DigestSemanticExecution(execution)
	if err != nil {
		return err
	}
	wantDigest, err := semanticplan.DigestSemanticExecution(fresh)
	if err != nil {
		return err
	}
	if gotDigest != wantDigest {
		return fmt.Errorf("execution evidence differs from deterministic semantic re-execution (%s != %s)", gotDigest, wantDigest)
	}
	return verifyLocalTape(execution, func(decider decision.Decider) (artifact.Outcome, error) {
		return etcdraft.ExecuteWithApplication(plan.Scenario, plan.Configuration, decider, plan.Application)
	})
}

func verifyLocalTape(execution semanticplan.SemanticExecution, execute func(decision.Decider) (artifact.Outcome, error)) error {
	if execution.Outcome == nil {
		if execution.Projection.Fidelity != semanticplan.ProjectionFailed {
			return errors.New("successful projection has no replayable outcome")
		}
	}
	replay, err := decision.NewTapeDecider(execution.Decisions)
	if err != nil {
		return err
	}
	outcome, runErr := execute(replay)
	if err := replay.Finish(); err != nil {
		return fmt.Errorf("successful tape prefix was not fully consumed: %w", err)
	}
	if execution.Projection.Fidelity == semanticplan.ProjectionFailed {
		// A failed projector cannot record the rejected choice. Its local tape is
		// therefore an exact successful prefix: replay must consume that prefix and
		// stop only when the next target choice exhausts it. Deterministic fresh
		// semantic re-execution above verifies the original failure and evidence.
		if runErr != nil {
			if !errors.Is(runErr, decision.ErrTapeExhausted) {
				return fmt.Errorf("failed-projection prefix stopped for a different reason: %w", runErr)
			}
			return nil
		}
		if outcome.Status != artifact.OutcomeError || !strings.Contains(outcome.Error, decision.ErrTapeExhausted.Error()) {
			return errors.New("failed-projection prefix did not stop at the next unrecorded choice")
		}
		return nil
	}
	if runErr != nil {
		return runErr
	}
	if execution.Outcome == nil || !artifact.OutcomesEqual(*execution.Outcome, outcome) {
		return errors.New("stored outcome differs from target-local exact replay")
	}
	return nil
}

func readRegularFile(path string, maximum int) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(raw) > maximum {
		return nil, errors.New("file exceeds resource budget")
	}
	return raw, nil
}

func decodeStrictDocument(raw []byte, target any) error {
	if err := strictjson.RejectDuplicateNames(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func joinRejections(rejections []semanticplan.RejectionCode) string {
	values := make([]string, len(rejections))
	for index, rejection := range rejections {
		values[index] = string(rejection)
	}
	return strings.Join(values, ",")
}

func usage(writer io.Writer) {
	fmt.Fprintln(writer, "d-raft portable cross-adapter semantic experiment runner")
	fmt.Fprintln(writer, "usage: draft-cross <derive|run|verify|recover|version> [options]")
}
