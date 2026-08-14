package mutant

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunnerExecutesPinnedWorktreeAndAttributesTarget(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "go.mod"), "module example.com/research/raft\n\ngo 1.26\n")
	writeTestFile(t, filepath.Join(root, "raft", "node.go"), "package raft\n\nfunc Value() int { return 1 }\n")
	runTestCommand(t, root, "git", "init", "-q")
	runTestCommand(t, root, "git", "config", "user.name", "Mutant Test")
	runTestCommand(t, root, "git", "config", "user.email", "mutant@example.invalid")
	runTestCommand(t, root, "git", "add", "go.mod", "raft/node.go")
	runTestCommand(t, root, "git", "commit", "-q", "-m", "base")
	base := strings.TrimSpace(runTestCommand(t, root, "git", "rev-parse", "HEAD"))

	corpus := filepath.Join(root, "corpus")
	activation := "diff --git a/raft/mutant_activation_test.go b/raft/mutant_activation_test.go\nnew file mode 100644\nindex 0000000..7f32745\n--- /dev/null\n+++ b/raft/mutant_activation_test.go\n@@ -0,0 +1,11 @@\n+package raft\n+\n+import \"testing\"\n+\n+func TestSeededMutation(t *testing.T) {\n+\tt.Log(\"DRAFT_MUTANT_ACTIVATED:seeded\")\n+\tif Value() != 1 {\n+\t\tt.Log(\"DRAFT_MUTANT_INVARIANT:election-safety\")\n+\t\tt.Fatal(\"wrong value\")\n+\t}\n+}\n"
	mutation := "diff --git a/raft/node.go b/raft/node.go\nindex 580f32e..c47d5c8 100644\n--- a/raft/node.go\n+++ b/raft/node.go\n@@ -1,3 +1,3 @@\n package raft\n \n-func Value() int { return 1 }\n+func Value() int { return 2 }\n"
	writeTestFile(t, filepath.Join(corpus, "activation.patch"), activation)
	writeTestFile(t, filepath.Join(corpus, "mutation.patch"), mutation)
	manifest := Manifest{
		Schema: ManifestSchema, Repository: "example.com/research/raft", BaseCommit: base,
		Mutants: []Mutant{{ID: "seeded", Package: "./raft", Test: "TestSeededMutation", Invariant: Invariant{Name: "election-safety", Class: InvariantSafety}, ActivationPatch: patchMetadata("activation.patch", []byte(activation)), MutantPatch: patchMetadata("mutation.patch", []byte(mutation))}},
	}
	result, err := (Runner{RepositoryDir: root, ManifestDir: corpus, provenance: func() (string, bool, error) {
		return strings.Repeat("f", 40), false, nil
	}}).Run(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Results) != 1 || result.Results[0].Classification != SafetyKill {
		t.Fatalf("unexpected result: %+v", result)
	}
	wantManifestSHA, err := DigestManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if result.ManifestSHA256 != wantManifestSHA || result.BaseTree == "" || result.Environment.TargetHead != base || result.Environment.RunnerRevision == "" || result.Environment.GoVersion == "" {
		t.Fatalf("missing provenance: %+v", result)
	}
	if result.Results[0].ActivationSHA256 != manifest.Mutants[0].ActivationPatch.SHA256 || result.Results[0].MutationSHA256 != manifest.Mutants[0].MutantPatch.SHA256 {
		t.Fatalf("missing patch provenance: %+v", result.Results[0])
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("result validation: %v", err)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("result validation: %v", err)
	}
	if result.Results[0].Baseline == nil || result.Results[0].Baseline.ExitCode != 0 || result.Results[0].Mutant == nil || result.Results[0].Mutant.ExitCode == 0 {
		t.Fatalf("missing command evidence: %+v", result.Results[0])
	}
	if output := runTestCommand(t, root, "git", "worktree", "list", "--porcelain"); strings.Count(output, "worktree ") != 1 {
		t.Fatalf("temporary worktree was not removed:\n%s", output)
	}
}

func TestRunnerRejectsChecksumBeforeExecution(t *testing.T) {
	manifest := validManifest()
	_, err := (Runner{RepositoryDir: t.TempDir(), ManifestDir: t.TempDir()}).Run(context.Background(), manifest)
	if err == nil {
		t.Fatal("Runner accepted a non-repository")
	}
}

func TestLoadPatchRejectsUnsafeFilesystemObjects(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	contents := []byte("patch\n")
	writeTestFile(t, filepath.Join(root, "valid.patch"), string(contents))
	valid := patchMetadata("valid.patch", contents)
	if _, err := loadPatch(root, valid); err != nil {
		t.Fatalf("valid patch: %v", err)
	}

	wrongHash := valid
	wrongHash.SHA256 = strings.Repeat("0", 64)
	if _, err := loadPatch(root, wrongHash); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("hash mismatch error=%v", err)
	}
	if _, err := loadPatch(root, Patch{Path: "../valid.patch", SHA256: valid.SHA256}); err == nil {
		t.Fatal("traversal path was accepted")
	}
	if err := os.Symlink("valid.patch", filepath.Join(root, "linked.patch")); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPatch(root, Patch{Path: "linked.patch", SHA256: valid.SHA256}); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink error=%v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "directory.patch"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPatch(root, Patch{Path: "directory.patch", SHA256: valid.SHA256}); err == nil || !strings.Contains(err.Error(), "regular") {
		t.Fatalf("directory error=%v", err)
	}
}

func TestPatchContentPolicyRejectsForbiddenTargetsAndOperations(t *testing.T) {
	t.Parallel()
	repository := t.TempDir()
	runTestCommand(t, repository, "git", "init", "-q")
	entry := validManifest().Mutants[0]
	entry.Package = "./raft"
	tests := []struct {
		name       string
		activation bool
		patch      string
	}{
		{"activation filename", true, modificationPatch("raft/not_approved_test.go")},
		{"checker mutation", false, modificationPatch("check/check.go")},
		{"test mutation", false, modificationPatch("raft/node_test.go")},
		{"go mod mutation", false, modificationPatch("go.mod")},
		{"production addition", false, additionPatch("raft/node.go")},
		{"mode change", false, "diff --git a/raft/node.go b/raft/node.go\nold mode 100644\nnew mode 100755\n"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := validatePatchContent(context.Background(), repository, entry, []byte(test.patch), test.activation); err == nil {
				t.Fatal("forbidden patch was accepted")
			}
		})
	}
}

func TestExecutionEvidenceIsExactAndBounded(t *testing.T) {
	t.Parallel()
	marker := ActivationMarker("seeded")
	if !hasExactMarker("    mutant_activation_test.go:6: "+marker+"\n", marker) {
		t.Fatal("exact go test log marker was not recognized")
	}
	for _, output := range []string{marker + "-suffix", "prefix-" + marker, marker + " extra"} {
		if hasExactMarker(output, marker) {
			t.Fatalf("non-exact marker accepted: %q", output)
		}
	}
	for _, output := range []string{"FAIL example [build failed]", "panic: boom", "panic: test timed out after 2m0s"} {
		if !isOperationalTestFailure(output) {
			t.Fatalf("operational failure not recognized: %q", output)
		}
	}
	buffer := &limitedBuffer{remaining: 4}
	if n, err := buffer.Write([]byte("123456")); err != nil || n != 6 || buffer.String() != "1234" || !buffer.truncated {
		t.Fatalf("bounded output n=%d err=%v value=%q truncated=%t", n, err, buffer.String(), buffer.truncated)
	}
}

func TestSensitiveTestEnvironmentNamesAreScrubbed(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"HTTP_PROXY", "https_proxy", "GITHUB_TOKEN", "DB_PASSWORD", "AWS_SECRET_ACCESS_KEY", "GIT_ASKPASS", "SSH_AUTH_SOCK", "GOAUTH"} {
		if !sensitiveEnvironmentName(name) {
			t.Fatalf("%s was not considered sensitive", name)
		}
	}
	for _, name := range []string{"PATH", "LANG", "CGO_ENABLED"} {
		if sensitiveEnvironmentName(name) {
			t.Fatalf("%s was incorrectly scrubbed", name)
		}
	}
	for _, name := range []string{"PATH", "LANG", "LC_ALL", "TZ", "GOROOT"} {
		if !allowedEnvironmentName(name) {
			t.Fatalf("%s should be retained in the isolated environment", name)
		}
	}
	for _, name := range []string{"AWS_REGION", "GITHUB_TOKEN", "HTTP_PROXY", "GOFLAGS"} {
		if allowedEnvironmentName(name) {
			t.Fatalf("%s should not be inherited by the isolated environment", name)
		}
	}
}

func modificationPatch(path string) string {
	return "diff --git a/" + path + " b/" + path + "\n--- a/" + path + "\n+++ b/" + path + "\n@@ -1 +1 @@\n-old\n+new\n"
}

func additionPatch(path string) string {
	return "diff --git a/" + path + " b/" + path + "\nnew file mode 100644\n--- /dev/null\n+++ b/" + path + "\n@@ -0,0 +1 @@\n+new\n"
}

func patchMetadata(path string, contents []byte) Patch {
	digest := sha256.Sum256(contents)
	return Patch{Path: path, SHA256: hex.EncodeToString(digest[:])}
}

func writeTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func runTestCommand(t *testing.T, directory, name string, args ...string) string {
	t.Helper()
	command := exec.Command(name, args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, output)
	}
	return string(output)
}
