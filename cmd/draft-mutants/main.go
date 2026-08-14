// Command draft-mutants executes a pinned mutant corpus and writes strict JSON.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aminkbi/d-raft/mutant"
)

func main() {
	os.Exit(execute(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("draft-mutants", flag.ContinueOnError)
	flags.SetOutput(stderr)
	repository := flags.String("repo", ".", "Git repository root")
	manifestFlag := flags.String("manifest", "", "corpus manifest JSON path")
	output := flags.String("out", "-", "result JSON path, or - for stdout")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	manifestPath := *manifestFlag
	if manifestPath == "" && flags.NArg() == 1 {
		manifestPath = flags.Arg(0)
	} else if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "usage: draft-mutants [-repo DIR] [-out FILE] -manifest MANIFEST")
		return 2
	}
	if manifestPath == "" {
		fmt.Fprintln(stderr, "draft-mutants: manifest is required")
		return 2
	}
	file, err := os.Open(manifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "draft-mutants: open manifest: %v\n", err)
		return 2
	}
	manifest, err := mutant.DecodeManifest(file)
	closeErr := file.Close()
	if err != nil {
		fmt.Fprintf(stderr, "draft-mutants: %v\n", err)
		return 2
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "draft-mutants: close manifest: %v\n", closeErr)
		return 2
	}
	absoluteManifest, err := filepath.Abs(manifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "draft-mutants: resolve manifest: %v\n", err)
		return 2
	}
	result, err := (mutant.Runner{RepositoryDir: *repository, ManifestDir: filepath.Dir(absoluteManifest)}).Run(ctx, manifest)
	if err != nil {
		fmt.Fprintf(stderr, "draft-mutants: %v\n", err)
		return 2
	}
	if err := writeResult(*output, stdout, result); err != nil {
		fmt.Fprintf(stderr, "draft-mutants: write result: %v\n", err)
		return 2
	}
	for _, entry := range result.Results {
		switch entry.Classification {
		case mutant.BaselineFailed, mutant.NotActivated, mutant.Ineligible, mutant.OperationalError:
			return 1
		}
	}
	return 0
}

func writeResult(path string, stdout io.Writer, result mutant.Result) error {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}
	if path == "-" {
		_, err := io.Copy(stdout, &encoded)
		return err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	directory := filepath.Dir(absolute)
	temporary, err := os.CreateTemp(directory, ".draft-mutants-result-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := io.Copy(temporary, &encoded); err != nil {
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
	// Link publishes atomically without replacing an existing result.
	if err := os.Link(temporaryName, absolute); err != nil {
		return err
	}
	return nil
}
