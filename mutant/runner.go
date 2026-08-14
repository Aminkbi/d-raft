package mutant

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

const (
	// TestTimeout is both the go test -timeout value and the subprocess bound.
	TestTimeout      = 2 * time.Minute
	maximumPatchSize = 16 << 20
	maximumOutput    = 64 << 10
)

var productionMutationFiles = map[string]struct{}{
	"raft/membership.go": {},
	"raft/node.go":       {},
	"raft/types.go":      {},
}

// Runner executes a validated manifest against RepositoryDir. ManifestDir is
// the directory against which corpus-relative patch paths are resolved.
type Runner struct {
	RepositoryDir string
	ManifestDir   string
	provenance    func() (string, bool, error)
}

type loadedMutant struct {
	activation []byte
	mutation   []byte
}

// Run validates all immutable inputs before creating a detached worktree for
// each mutant. Every created worktree and temporary parent is cleaned up.
func (r Runner) Run(ctx context.Context, manifest Manifest) (Result, error) {
	if err := manifest.Validate(); err != nil {
		return Result{}, err
	}
	repository, err := validateRepository(ctx, r.RepositoryDir, manifest)
	if err != nil {
		return Result{}, err
	}
	loaded := make([]loadedMutant, len(manifest.Mutants))
	for i, entry := range manifest.Mutants {
		activation, err := loadPatch(r.ManifestDir, entry.ActivationPatch)
		if err != nil {
			return Result{}, fmt.Errorf("mutant %q activation patch: %w", entry.ID, err)
		}
		mutation, err := loadPatch(r.ManifestDir, entry.MutantPatch)
		if err != nil {
			return Result{}, fmt.Errorf("mutant %q mutant patch: %w", entry.ID, err)
		}
		loaded[i] = loadedMutant{activation: activation, mutation: mutation}
	}

	provenance := r.provenance
	if provenance == nil {
		provenance = executableProvenance
	}
	baseTree, environment, err := repositoryMetadata(ctx, repository, manifest.BaseCommit, provenance)
	if err != nil {
		return Result{}, err
	}
	manifestSHA256, err := DigestManifest(manifest)
	if err != nil {
		return Result{}, err
	}
	result := Result{
		Schema: ResultSchema, ManifestSchema: ManifestSchema,
		ManifestSHA256: manifestSHA256, Repository: manifest.Repository,
		BaseCommit: manifest.BaseCommit, BaseTree: baseTree, Environment: environment,
		Results: make([]MutantResult, 0, len(manifest.Mutants)),
	}
	for i, entry := range manifest.Mutants {
		result.Results = append(result.Results, r.runOne(ctx, repository, manifest.BaseCommit, entry, loaded[i]))
	}
	if err := result.Validate(); err != nil {
		return Result{}, fmt.Errorf("validate generated result: %w", err)
	}
	return result, nil
}

// DigestManifest returns the SHA-256 digest of the schema-ordered compact JSON
// representation used to bind a result to its decoded manifest.
func DigestManifest(manifest Manifest) (string, error) {
	if err := manifest.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("encode manifest for digest: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func repositoryMetadata(ctx context.Context, repository, baseCommit string, provenance func() (string, bool, error)) (string, Environment, error) {
	baseTreeOutput, err := runGit(ctx, repository, nil, "rev-parse", "--verify", baseCommit+"^{tree}")
	if err != nil {
		return "", Environment{}, fmt.Errorf("resolve base tree: %w", err)
	}
	baseTree := strings.TrimSpace(string(baseTreeOutput))
	if !hex40Pattern.MatchString(baseTree) {
		return "", Environment{}, errors.New("base tree is not a SHA-1 object ID")
	}
	targetOutput, err := runGit(ctx, repository, nil, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", Environment{}, fmt.Errorf("resolve target HEAD: %w", err)
	}
	targetHead := strings.TrimSpace(string(targetOutput))
	if !hex40Pattern.MatchString(targetHead) {
		return "", Environment{}, errors.New("target HEAD is not a SHA-1 object ID")
	}
	status, err := runGit(ctx, repository, nil, "status", "--porcelain=v1", "--untracked-files=normal")
	if err != nil {
		return "", Environment{}, fmt.Errorf("inspect target checkout: %w", err)
	}
	runnerRevision, runnerModified, err := provenance()
	if err != nil {
		return "", Environment{}, err
	}
	return baseTree, Environment{
		RunnerRevision: runnerRevision, RunnerModified: runnerModified,
		TargetHead: targetHead, TargetModified: len(status) != 0,
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
	}, nil
}

func executableProvenance() (string, bool, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false, errors.New("runner executable has no Go build information")
	}
	var revision string
	modified := false
	modifiedKnown := false
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			value, err := strconv.ParseBool(setting.Value)
			if err != nil {
				return "", false, fmt.Errorf("invalid vcs.modified build setting: %w", err)
			}
			modified, modifiedKnown = value, true
		}
	}
	if !hex40Pattern.MatchString(revision) || !modifiedKnown {
		return "", false, errors.New("runner executable lacks exact Git build provenance")
	}
	return revision, modified, nil
}

func (r Runner) runOne(ctx context.Context, repository, base string, entry Mutant, patches loadedMutant) MutantResult {
	result := MutantResult{
		ID: entry.ID, Package: entry.Package, Test: entry.Test, Invariant: entry.Invariant,
		ActivationSHA256: entry.ActivationPatch.SHA256, MutationSHA256: entry.MutantPatch.SHA256,
		Classification: OperationalError,
	}
	if err := validatePatchContent(ctx, repository, entry, patches.activation, true); err != nil {
		result.Detail = "validate activation patch: " + err.Error()
		return result
	}
	if err := validatePatchContent(ctx, repository, entry, patches.mutation, false); err != nil {
		result.Detail = "validate mutant patch: " + err.Error()
		return result
	}
	temporary, err := os.MkdirTemp("", "draft-mutant-")
	if err != nil {
		result.Detail = "create temporary directory: " + err.Error()
		return result
	}
	defer os.RemoveAll(temporary)
	worktree := filepath.Join(temporary, "worktree")
	if output, err := runGit(ctx, repository, nil, "worktree", "add", "--detach", worktree, base); err != nil {
		result.Detail = commandError("create worktree", output, err)
		return result
	}
	defer func() {
		// The path is an exact child created above; removal also deregisters it.
		cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = runGit(cleanupContext, repository, nil, "worktree", "remove", "--force", worktree)
	}()

	if output, err := applyPatch(ctx, worktree, patches.activation); err != nil {
		result.Detail = commandError("apply activation patch", output, err)
		return result
	}
	// Intent-to-add makes the approved new activation file visible to the
	// required post-apply git diff without staging its contents.
	if output, err := runGit(ctx, worktree, nil, "add", "--intent-to-add", "--", activationFile(entry.Package)); err != nil {
		result.Detail = commandError("register activation patch", output, err)
		return result
	}
	if err := inspectDiff(ctx, worktree, base, entry, false); err != nil {
		result.Detail = "inspect activation diff: " + err.Error()
		return result
	}
	baseline, baselineErr := runTest(ctx, worktree, entry.Package, entry.Test)
	result.Baseline = &baseline
	if baselineErr != nil && !isExitError(baselineErr) {
		result.Detail = "baseline test: " + baselineErr.Error()
		return result
	}
	if isOperationalTestFailure(baseline.Output) {
		result.Detail = "baseline test could not execute normally"
		return result
	}
	baselineMarked := hasExactMarker(baseline.Output, ActivationMarker(entry.ID))
	if baseline.ExitCode != 0 {
		if !baselineMarked {
			result.Classification = OperationalError
			result.Detail = "baseline test failed before emitting the activation marker"
			return result
		}
		result.Classification = BaselineFailed
		result.Detail = "activated baseline test failed"
		return result
	}
	if !baselineMarked {
		result.Classification = NotActivated
		result.Detail = "baseline output did not contain the activation marker"
		return result
	}
	if output, err := applyPatch(ctx, worktree, patches.mutation); err != nil {
		result.Detail = commandError("apply mutant patch", output, err)
		return result
	}
	if err := inspectDiff(ctx, worktree, base, entry, true); err != nil {
		result.Detail = "inspect mutant diff: " + err.Error()
		return result
	}
	mutantRun, mutantErr := runTest(ctx, worktree, entry.Package, entry.Test)
	result.Mutant = &mutantRun
	mutantMarked := hasExactMarker(mutantRun.Output, ActivationMarker(entry.ID))
	targetMarked := hasExactMarker(mutantRun.Output, InvariantMarker(entry.Invariant.Name))
	operational := mutantErr != nil && !isExitError(mutantErr) || isOperationalTestFailure(mutantRun.Output) || (mutantRun.ExitCode != 0 && !mutantMarked)
	result.Classification = Classify(ClassificationInput{
		Invariant: entry.Invariant, BaselineRan: true, BaselinePassed: true,
		BaselineMarked: true, MutantRan: !operational, MutantPassed: mutantRun.ExitCode == 0,
		MutantMarked: mutantMarked, TargetMarked: targetMarked, Eligible: true,
		OperationalFail: operational,
	})
	switch result.Classification {
	case SafetyKill, ConformanceKill:
		result.Detail = "test failed with the targeted invariant marker"
	case NonSafetyDetection:
		result.Detail = "test failed without the targeted invariant marker"
	case Survived:
		result.Detail = "targeted test passed after mutation"
	case NotActivated:
		result.Detail = "mutant output did not contain the activation marker"
	case OperationalError:
		if mutantErr != nil {
			result.Detail = "mutant test: " + mutantErr.Error()
		} else {
			result.Detail = "mutant test did not execute normally"
		}
	}
	return result
}

func validatePatchContent(ctx context.Context, repository string, entry Mutant, patch []byte, activation bool) error {
	numstat, err := runGit(ctx, repository, patch, "apply", "--numstat", "--whitespace=error-all", "-")
	if err != nil {
		return fmt.Errorf("parse patch: %s", commandError("git apply --numstat", numstat, err))
	}
	summary, err := runGit(ctx, repository, patch, "apply", "--summary", "--whitespace=error-all", "-")
	if err != nil {
		return fmt.Errorf("summarize patch: %s", commandError("git apply --summary", summary, err))
	}
	lines := nonemptyLines(string(numstat))
	if len(lines) == 0 {
		return errors.New("patch contains no textual changes")
	}
	seen := make(map[string]struct{}, len(lines))
	for _, line := range lines {
		fields := strings.Split(line, "\t")
		if len(fields) != 3 || fields[0] == "-" || fields[1] == "-" {
			return errors.New("binary or malformed patch content is forbidden")
		}
		name := fields[2]
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("patch repeats target %q", name)
		}
		seen[name] = struct{}{}
		if activation {
			if name != activationFile(entry.Package) || fields[1] != "0" {
				return fmt.Errorf("activation patch may only add %q", activationFile(entry.Package))
			}
		} else if _, allowed := productionMutationFiles[name]; !allowed {
			return fmt.Errorf("mutant patch target %q is not an allowlisted production file", name)
		}
	}
	trimmedSummary := strings.TrimSpace(string(summary))
	if activation {
		expected := "create mode 100644 " + activationFile(entry.Package)
		if len(lines) != 1 || trimmedSummary != expected {
			return fmt.Errorf("activation patch must only add a regular 100644 file; summary=%q", trimmedSummary)
		}
	} else if trimmedSummary != "" {
		return fmt.Errorf("mutant patch may not add, delete, rename, copy, or change modes; summary=%q", trimmedSummary)
	}
	return nil
}

func inspectDiff(ctx context.Context, worktree, base string, entry Mutant, mutated bool) error {
	output, err := runGit(ctx, worktree, nil, "diff", "--name-status", "--no-renames", base, "--")
	if err != nil {
		return fmt.Errorf("git diff --name-status: %w", err)
	}
	lines := nonemptyLines(string(output))
	wantActivation := activationFile(entry.Package)
	foundActivation := false
	foundMutation := false
	for _, line := range lines {
		fields := strings.Split(line, "\t")
		if len(fields) != 2 {
			return fmt.Errorf("malformed diff status %q", line)
		}
		if fields[0] == "A" && fields[1] == wantActivation {
			foundActivation = true
			continue
		}
		if mutated && fields[0] == "M" {
			if _, allowed := productionMutationFiles[fields[1]]; allowed {
				foundMutation = true
				continue
			}
		}
		return fmt.Errorf("unexpected worktree change %q", line)
	}
	if !foundActivation || (!mutated && len(lines) != 1) || (mutated && !foundMutation) {
		return fmt.Errorf("expected activation and mutation changes were not present")
	}
	return nil
}

func activationFile(pkg string) string {
	return strings.TrimPrefix(pkg, "./") + "/mutant_activation_test.go"
}

func nonemptyLines(value string) []string {
	var lines []string
	for _, line := range strings.Split(value, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func validateRepository(ctx context.Context, directory string, manifest Manifest) (string, error) {
	if directory == "" {
		return "", errors.New("repository directory is required")
	}
	abs, err := filepath.Abs(directory)
	if err != nil {
		return "", fmt.Errorf("resolve repository: %w", err)
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("resolve repository symlinks: %w", err)
	}
	output, err := runGit(ctx, real, nil, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", fmt.Errorf("repository validation: %s", commandError("git rev-parse", output, err))
	}
	top, err := filepath.EvalSymlinks(strings.TrimSpace(string(output)))
	if err != nil || top != real {
		return "", errors.New("repository directory must be the Git worktree root")
	}
	output, err = runGit(ctx, real, nil, "rev-parse", "--verify", manifest.BaseCommit+"^{commit}")
	if err != nil || strings.TrimSpace(string(output)) != manifest.BaseCommit {
		return "", fmt.Errorf("base_commit %s is not an available commit in the repository", manifest.BaseCommit)
	}
	goMod, err := runGit(ctx, real, nil, "show", manifest.BaseCommit+":go.mod")
	if err != nil {
		return "", fmt.Errorf("read go.mod at base_commit: %w", err)
	}
	module := modulePath(goMod)
	if module != manifest.Repository {
		return "", fmt.Errorf("repository %q does not match module %q at base_commit", manifest.Repository, module)
	}
	return real, nil
}

func modulePath(goMod []byte) string {
	for _, line := range strings.Split(string(goMod), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "module" {
			return fields[1]
		}
	}
	return ""
}

func loadPatch(manifestDir string, patch Patch) ([]byte, error) {
	if manifestDir == "" {
		return nil, errors.New("manifest directory is required")
	}
	root, err := filepath.Abs(manifestDir)
	if err != nil {
		return nil, err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve manifest directory: %w", err)
	}
	joined := filepath.Join(root, filepath.FromSlash(patch.Path))
	if err := rejectSymlinkPath(root, joined); err != nil {
		return nil, err
	}
	candidate, err := filepath.EvalSymlinks(joined)
	if err != nil {
		return nil, fmt.Errorf("resolve patch path: %w", err)
	}
	relative, err := filepath.Rel(root, candidate)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("patch resolves outside the manifest directory")
	}
	file, err := os.Open(candidate)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("patch is not a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximumPatchSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maximumPatchSize {
		return nil, fmt.Errorf("patch exceeds %d bytes", maximumPatchSize)
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != patch.SHA256 {
		return nil, errors.New("sha256 checksum mismatch")
	}
	return data, nil
}

func rejectSymlinkPath(root, candidate string) error {
	relative, err := filepath.Rel(root, candidate)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("patch resolves outside the manifest directory")
	}
	current := root
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("inspect patch path: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("patch path may not contain symlinks")
		}
	}
	return nil
}

func applyPatch(ctx context.Context, worktree string, patch []byte) ([]byte, error) {
	if output, err := runGit(ctx, worktree, patch, "apply", "--check", "--whitespace=error-all", "-"); err != nil {
		return output, err
	}
	return runGit(ctx, worktree, patch, "apply", "--whitespace=error-all", "-")
}

func runTest(parent context.Context, worktree, pkg, test string) (CommandResult, error) {
	ctx, cancel := context.WithTimeout(parent, TestTimeout+15*time.Second)
	defer cancel()
	start := time.Now()
	goCommand := filepath.Join(runtime.GOROOT(), "bin", "go")
	command := exec.CommandContext(ctx, goCommand, "test", "-v", "-count=1", "-timeout="+TestTimeout.String(), "-run=^"+regexp.QuoteMeta(test)+"$", pkg)
	command.Dir = worktree
	environment, envErr := isolatedTestEnvironment(worktree)
	if envErr != nil {
		return CommandResult{ExitCode: -1, DurationMS: time.Since(start).Milliseconds()}, envErr
	}
	command.Env = environment
	buffer := &limitedBuffer{remaining: maximumOutput}
	command.Stdout = buffer
	command.Stderr = buffer
	err := command.Run()
	result := CommandResult{ExitCode: 0, DurationMS: time.Since(start).Milliseconds(), Output: buffer.String(), OutputTruncated: buffer.truncated}
	if err != nil {
		result.ExitCode = -1
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			result.ExitCode = exitError.ExitCode()
		}
		if ctx.Err() != nil {
			return result, fmt.Errorf("command timeout: %w", ctx.Err())
		}
	}
	return result, err
}

func isolatedTestEnvironment(worktree string) ([]string, error) {
	runtimeRoot := filepath.Join(filepath.Dir(worktree), "runtime")
	home := filepath.Join(runtimeRoot, "home")
	temporary := filepath.Join(runtimeRoot, "tmp")
	cache := filepath.Join(runtimeRoot, "gocache")
	for _, directory := range []string{home, temporary, cache} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create isolated test environment: %w", err)
		}
	}
	environment := make([]string, 0, 16)
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if allowedEnvironmentName(name) {
			environment = append(environment, entry)
		}
	}
	environment = append(environment,
		"HOME="+home,
		"TMPDIR="+temporary,
		"GOCACHE="+cache,
		"GOROOT="+runtime.GOROOT(),
		"GOWORK=off",
		"GOTOOLCHAIN=local",
		"GOFLAGS=-mod=readonly",
		"GOPROXY=off",
		"GOSUMDB=off",
	)
	return environment, nil
}

func allowedEnvironmentName(name string) bool {
	upper := strings.ToUpper(name)
	return upper == "PATH" || upper == "LANG" || upper == "LC_ALL" || upper == "LC_CTYPE" || upper == "TZ"
}

func sensitiveEnvironmentName(name string) bool {
	upper := strings.ToUpper(name)
	switch upper {
	case "HOME", "TMPDIR", "GOCACHE", "GOROOT", "GOWORK", "GOTOOLCHAIN", "GOFLAGS", "GOPROXY", "GOSUMDB",
		"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "GIT_ASKPASS", "SSH_ASKPASS", "SSH_AUTH_SOCK", "NETRC", "GOAUTH":
		return true
	}
	return strings.Contains(upper, "TOKEN") || strings.Contains(upper, "SECRET") || strings.Contains(upper, "PASSWORD") || strings.Contains(upper, "CREDENTIAL") || strings.Contains(upper, "PROXY") || strings.Contains(upper, "AUTH") || strings.Contains(upper, "KEY")
}

func hasExactMarker(output, marker string) bool {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == marker || strings.HasSuffix(line, ": "+marker) {
			return true
		}
	}
	return false
}

func isOperationalTestFailure(output string) bool {
	lower := strings.ToLower(output)
	return strings.Contains(lower, "[build failed]") ||
		strings.Contains(lower, "build constraints exclude all go files") ||
		strings.Contains(lower, "panic:") ||
		strings.Contains(lower, "test timed out")
}

func runGit(ctx context.Context, directory string, stdin []byte, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "git", append([]string{"-C", directory}, args...)...)
	if stdin != nil {
		command.Stdin = bytes.NewReader(stdin)
	}
	return command.CombinedOutput()
}

func isExitError(err error) bool {
	var exitError *exec.ExitError
	return errors.As(err, &exitError)
}

func commandError(operation string, output []byte, err error) string {
	message := strings.TrimSpace(string(output))
	if len(message) > 4096 {
		message = message[:4096]
	}
	if message == "" {
		return operation + ": " + err.Error()
	}
	return operation + ": " + err.Error() + ": " + message
}

type limitedBuffer struct {
	buffer    bytes.Buffer
	remaining int
	truncated bool
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	original := len(data)
	if len(data) > b.remaining {
		data = data[:b.remaining]
		b.truncated = true
	}
	_, _ = b.buffer.Write(data)
	b.remaining -= len(data)
	return original, nil
}

func (b *limitedBuffer) String() string { return b.buffer.String() }
