// Command draft-eval runs the bounded d-raft comparative evaluation.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aminkbi/d-raft/evaluation"
)

var version = "devel"

var (
	collectEnvironment  = evaluation.CurrentEnvironment
	runEvaluation       = evaluation.RunWithEnvironment
	syncOutputDirectory = syncDirectory
)

func main() { os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr)) }

func execute(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("draft-eval", flag.ContinueOnError)
	flags.SetOutput(stderr)
	output := flags.String("out", "d-raft-evaluation.json", "no-clobber result path")
	trials := flags.Int("trials", 21, "balanced repeated measured trials per method")
	verify := flags.String("verify", "", "verify a published evaluation result and exit")
	showVersion := flags.Bool("version", false, "print version and exit")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "usage: draft-eval [--trials N] [--out FILE] | draft-eval --verify FILE")
		return 2
	}
	if *showVersion {
		fmt.Fprintf(stdout, "draft-eval %s (%s)\n", version, evaluation.SchemaVersion)
		return 0
	}
	if *verify != "" {
		conflict := false
		flags.Visit(func(current *flag.Flag) {
			if current.Name == "out" || current.Name == "trials" {
				conflict = true
			}
		})
		if conflict {
			fmt.Fprintln(stderr, "draft-eval: --verify cannot be combined with --out or --trials")
			return 2
		}
		file, err := os.Open(*verify)
		if err != nil {
			fmt.Fprintf(stderr, "draft-eval: verify: %v\n", err)
			return 2
		}
		result, decodeErr := evaluation.DecodePublication(file)
		closeErr := file.Close()
		if decodeErr != nil {
			fmt.Fprintf(stderr, "draft-eval: verify: %v\n", decodeErr)
			return 2
		}
		if closeErr != nil {
			fmt.Fprintf(stderr, "draft-eval: verify: close: %v\n", closeErr)
			return 2
		}
		fmt.Fprintf(stdout, "verified %s (%s, %d trials per method)\n", *verify, result.Environment.GitRevision, result.Config.Trials)
		return 0
	}
	config := evaluation.DefaultConfig(*trials)
	if err := evaluation.ValidateConfig(config); err != nil {
		fmt.Fprintf(stderr, "draft-eval: %v\n", err)
		return 2
	}
	environment, err := collectEnvironment()
	if err != nil {
		fmt.Fprintf(stderr, "draft-eval: collect environment: %v\n", err)
		return 2
	}
	if err := evaluation.ValidatePublicationEnvironment(environment); err != nil {
		fmt.Fprintf(stderr, "draft-eval: %v\n", err)
		return 2
	}
	publisher, err := prepareResult(*output)
	if err != nil {
		fmt.Fprintf(stderr, "draft-eval: prepare result: %v\n", err)
		return 2
	}
	defer publisher.abort()
	result, err := runEvaluation(config, environment)
	if err != nil {
		fmt.Fprintf(stderr, "draft-eval: %v\n", err)
		return 2
	}
	if err := publisher.publish(result); err != nil {
		fmt.Fprintf(stderr, "draft-eval: write result: %v\n", err)
		return 2
	}
	fmt.Fprintf(stdout, "wrote %s\n", *output)
	for _, summary := range result.Summaries {
		fmt.Fprintf(stdout, "%s: mean events/s %.0f (95%% t interval %.0f..%.0f), violating runs %d\n", summary.Method, summary.EventsPerSecond95.Mean, summary.EventsPerSecond95.Lower, summary.EventsPerSecond95.Upper, summary.ViolatingRunsTotal)
		if summary.Method == evaluation.MethodCacheOn && summary.CacheHitsTotal == 0 && summary.StatePrunedTotal == 0 {
			fmt.Fprintln(stdout, "note: no repeated exact cache identity was observed; the cache contrast is an overhead/null-cache-hit result, not evidence of pruning efficacy")
		}
	}
	return 0
}

type resultPublisher struct {
	finalPath     string
	parentPath    string
	temporary     *os.File
	temporaryPath string
	committed     bool
}

func prepareResult(path string) (*resultPublisher, error) {
	if path == "" {
		return nil, errors.New("empty output path")
	}
	path = filepath.Clean(path)
	if _, err := os.Lstat(path); err == nil {
		return nil, fmt.Errorf("output already exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	parent := filepath.Dir(path)
	info, err := os.Stat(parent)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("output parent is not a directory: %s", parent)
	}
	temporary, err := os.CreateTemp(parent, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return nil, err
	}
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		_ = os.Remove(temporary.Name())
		return nil, err
	}
	info, err = temporary.Stat()
	if err != nil || info.Mode().Perm() != 0o600 {
		_ = temporary.Close()
		_ = os.Remove(temporary.Name())
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("staging file has unexpected mode %04o", info.Mode().Perm())
	}
	probePath := temporary.Name() + ".link-preflight"
	if err := os.Link(temporary.Name(), probePath); err != nil {
		_ = temporary.Close()
		_ = os.Remove(temporary.Name())
		return nil, fmt.Errorf("output filesystem does not support required hard links: %w", err)
	}
	if err := syncOutputDirectory(parent); err != nil {
		_ = temporary.Close()
		_ = os.Remove(probePath)
		_ = os.Remove(temporary.Name())
		return nil, fmt.Errorf("output filesystem does not support required directory sync: %w", err)
	}
	if err := os.Remove(probePath); err != nil {
		_ = temporary.Close()
		_ = os.Remove(temporary.Name())
		_ = os.Remove(probePath)
		return nil, fmt.Errorf("remove hard-link preflight: %w", err)
	}
	if err := syncOutputDirectory(parent); err != nil {
		_ = temporary.Close()
		_ = os.Remove(temporary.Name())
		return nil, fmt.Errorf("output filesystem does not support required directory sync: %w", err)
	}
	return &resultPublisher{finalPath: path, parentPath: parent, temporary: temporary, temporaryPath: temporary.Name()}, nil
}

func (publisher *resultPublisher) abort() {
	if publisher == nil {
		return
	}
	if publisher.temporary != nil {
		_ = publisher.temporary.Close()
		publisher.temporary = nil
	}
	if !publisher.committed && publisher.temporaryPath != "" {
		_ = os.Remove(publisher.temporaryPath)
		publisher.temporaryPath = ""
	}
}

func (publisher *resultPublisher) publish(result evaluation.Result) error {
	if publisher == nil || publisher.temporary == nil || publisher.temporaryPath == "" || publisher.committed {
		return errors.New("invalid result publisher state")
	}
	if err := evaluation.Encode(publisher.temporary, result); err != nil {
		return err
	}
	if err := publisher.temporary.Sync(); err != nil {
		return err
	}
	if err := publisher.temporary.Close(); err != nil {
		return err
	}
	publisher.temporary = nil
	if err := os.Link(publisher.temporaryPath, publisher.finalPath); err != nil {
		return err
	}
	publisher.committed = true
	if err := syncOutputDirectory(publisher.parentPath); err != nil {
		return fmt.Errorf("result exists at %s but final-link durability was not confirmed: %w", publisher.finalPath, err)
	}
	if err := os.Remove(publisher.temporaryPath); err != nil {
		return fmt.Errorf("result is durable at %s but staging cleanup failed: %w", publisher.finalPath, err)
	}
	publisher.temporaryPath = ""
	if err := syncOutputDirectory(publisher.parentPath); err != nil {
		return fmt.Errorf("result is durable at %s but staging-cleanup durability was not confirmed: %w", publisher.finalPath, err)
	}
	info, err := os.Stat(publisher.finalPath)
	if err != nil {
		return fmt.Errorf("result was linked but cannot be inspected: %w", err)
	}
	if info.Mode().Perm() != 0o600 {
		return fmt.Errorf("result exists with unexpected mode %04o", info.Mode().Perm())
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func writeResult(path string, result evaluation.Result) error {
	publisher, err := prepareResult(path)
	if err != nil {
		return err
	}
	defer publisher.abort()
	return publisher.publish(result)
}
