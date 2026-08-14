package mutant

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPublishedCorpusV1Result(t *testing.T) {
	t.Parallel()
	root := filepath.Join("..", "corpus", "mutants", "v1")
	manifestFile, err := os.Open(filepath.Join(root, "corpus.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := DecodeManifest(manifestFile)
	closeErr := manifestFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	resultFile, err := os.Open(filepath.Join(root, "results", "linux-amd64.json"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := DecodeResult(resultFile)
	closeErr = resultFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	digest, err := DigestManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if result.ManifestSHA256 != digest || result.Repository != manifest.Repository || result.BaseCommit != manifest.BaseCommit {
		t.Fatalf("result is not bound to manifest: result=%+v manifest=%+v", result, manifest)
	}
	if result.Environment.RunnerModified || result.Environment.TargetModified || result.Environment.GoVersion != "go1.26.6" || result.Environment.GOOS != "linux" || result.Environment.GOARCH != "amd64" {
		t.Fatalf("published result lacks clean pinned environment: %+v", result.Environment)
	}
	if len(result.Results) != len(manifest.Mutants) {
		t.Fatalf("results=%d mutants=%d", len(result.Results), len(manifest.Mutants))
	}
	safetyKills, conformanceKills := 0, 0
	for index, declaration := range manifest.Mutants {
		got := result.Results[index]
		if got.ID != declaration.ID || got.Package != declaration.Package || got.Test != declaration.Test || got.Invariant != declaration.Invariant ||
			got.ActivationSHA256 != declaration.ActivationPatch.SHA256 || got.MutationSHA256 != declaration.MutantPatch.SHA256 {
			t.Fatalf("result[%d] does not match declaration: result=%+v declaration=%+v", index, got, declaration)
		}
		switch got.Classification {
		case SafetyKill:
			safetyKills++
		case ConformanceKill:
			conformanceKills++
		default:
			t.Fatalf("result[%d] has unexpected classification %q", index, got.Classification)
		}
	}
	if safetyKills != 3 || conformanceKills != 3 {
		t.Fatalf("safety_kill=%d conformance_kill=%d", safetyKills, conformanceKills)
	}
}
