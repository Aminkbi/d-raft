package apporacle

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aminkbi/d-raft/raft"
)

func testID(start byte) CommandID {
	var id CommandID
	for index := range id {
		id[index] = start + byte(index)
	}
	return id
}

func put(start byte, key, value []byte) Command {
	return Command{ID: testID(start), Operation: Put, Key: cloneBytes(key), Value: cloneBytes(value)}
}

func remove(start byte, key []byte) Command {
	return Command{ID: testID(start), Operation: Delete, Key: cloneBytes(key)}
}

func mustEncodeCommand(t testing.TB, command Command) []byte {
	t.Helper()
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func mustApply(t testing.TB, machine *Machine, commands ...Command) {
	t.Helper()
	for _, command := range commands {
		if _, err := machine.Apply(command); err != nil {
			t.Fatal(err)
		}
	}
}

func TestKnownAnswerVectors(t *testing.T) {
	machine := New()
	if got, want := machine.Commitment(), (Commitment{
		Schema:      CommitmentSchema,
		Commands:    0,
		ChainDigest: "1b62430e166c36ce68764959cd66890a6d2b2be80f1c1555ce851f7825b795b3",
		StateDigest: "e35da1bb3c855311c69833cbc8a3f5ce35755e92133ca240816f1bb2211921bf",
	}); got != want {
		t.Fatalf("genesis commitment = %+v, want %+v", got, want)
	}
	emptyCheckpoint, err := EncodeCheckpoint(machine.Checkpoint())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(emptyCheckpoint), `{"schema":"d-raft.kv-checkpoint/v1","commands":"0","chain_digest":"1b62430e166c36ce68764959cd66890a6d2b2be80f1c1555ce851f7825b795b3","state_digest":"e35da1bb3c855311c69833cbc8a3f5ce35755e92133ca240816f1bb2211921bf","state":[],"history":[]}`; got != want {
		t.Fatalf("empty checkpoint JSON = %s, want %s", got, want)
	}

	first := put(0x00, []byte("x"), []byte("1"))
	encoded := mustEncodeCommand(t, first)
	if got, want := hex.EncodeToString(encoded), "44524146544b563101000102030405060708090a0b0c0d0e0f00000001000000017831"; got != want {
		t.Fatalf("put encoding = %s, want %s", got, want)
	}
	if got, err := CommandDigest(first); err != nil {
		t.Fatal(err)
	} else if want := "a4af80c9764356340696c115937255fd4157e4900d57859758706e8e79f8d62a"; got != want {
		t.Fatalf("put digest = %s, want %s", got, want)
	}
	block, err := machine.ApplyEncoded(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if block.Ordinal != 1 || block.CommandID != first.ID.String() || block.CommandDigest != "a4af80c9764356340696c115937255fd4157e4900d57859758706e8e79f8d62a" || block.StateDigest != "5afa47ab2fcf92ba11bc6cb680aee8049d589f848614af90bbcf34f9bc1b4c00" || block.Digest != "a7da7c9ae45a7c9560197d4237b1a65d641f128b525dfba6f77c82beb3162ccd" {
		t.Fatalf("put block = %+v", block)
	}
	nonemptyCheckpoint, err := EncodeCheckpoint(machine.Checkpoint())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(nonemptyCheckpoint), `{"schema":"d-raft.kv-checkpoint/v1","commands":"1","chain_digest":"a7da7c9ae45a7c9560197d4237b1a65d641f128b525dfba6f77c82beb3162ccd","state_digest":"5afa47ab2fcf92ba11bc6cb680aee8049d589f848614af90bbcf34f9bc1b4c00","state":[{"key":"eA==","value":"MQ=="}],"history":[{"ordinal":"1","command_id":"000102030405060708090a0b0c0d0e0f","command":"RFJBRlRLVjEBAAECAwQFBgcICQoLDA0ODwAAAAEAAAABeDE=","command_digest":"a4af80c9764356340696c115937255fd4157e4900d57859758706e8e79f8d62a","state_digest":"5afa47ab2fcf92ba11bc6cb680aee8049d589f848614af90bbcf34f9bc1b4c00","digest":"a7da7c9ae45a7c9560197d4237b1a65d641f128b525dfba6f77c82beb3162ccd"}]}`; got != want {
		t.Fatalf("nonempty checkpoint JSON = %s, want %s", got, want)
	}

	second := remove(0x10, []byte("x"))
	encoded = mustEncodeCommand(t, second)
	if got, want := hex.EncodeToString(encoded), "44524146544b563102101112131415161718191a1b1c1d1e1f000000010000000078"; got != want {
		t.Fatalf("delete encoding = %s, want %s", got, want)
	}
	if got, err := CommandDigest(second); err != nil {
		t.Fatal(err)
	} else if want := "89fca1e39d339abdb178a814a02535a2267134e6bb5854dc8bb15e5fc5a03f57"; got != want {
		t.Fatalf("delete digest = %s, want %s", got, want)
	}
	block, err = machine.ApplyEncoded(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if block.Ordinal != 2 || block.StateDigest != "e35da1bb3c855311c69833cbc8a3f5ce35755e92133ca240816f1bb2211921bf" || block.Digest != "99433f07551d21672ceba66edffeaac1c3e6aa4a5ba3a3eef5a2a44f7800a855" {
		t.Fatalf("delete block = %+v", block)
	}
}

func TestCommandIDParsing(t *testing.T) {
	want := testID(0)
	got, err := ParseCommandID("000102030405060708090a0b0c0d0e0f")
	if err != nil || got != want || got.String() != "000102030405060708090a0b0c0d0e0f" {
		t.Fatalf("parsed ID = %s, err=%v", got, err)
	}
	for _, value := range []string{"", "00", "00000000000000000000000000000000", "000102030405060708090A0B0C0D0E0F", "zz0102030405060708090a0b0c0d0e0f"} {
		if _, err := ParseCommandID(value); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("ParseCommandID(%q) error = %v", value, err)
		}
	}
}

func TestCommandBinaryRoundTripAndIsolation(t *testing.T) {
	commands := []Command{
		put(0, []byte{0x00, 0xff, 'k'}, []byte{}),
		put(16, []byte("alpha"), []byte{0x00, 0xfe, 0xff}),
		remove(32, []byte("alpha")),
	}
	for _, original := range commands {
		expected := Command{ID: original.ID, Operation: original.Operation, Key: cloneBytes(original.Key), Value: cloneBytes(original.Value)}
		command := Command{ID: original.ID, Operation: original.Operation, Key: cloneBytes(original.Key), Value: cloneBytes(original.Value)}
		encoded := mustEncodeCommand(t, command)
		decoded, err := DecodeCommand(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(decoded, expected) {
			t.Fatalf("round trip = %#v, want %#v", decoded, expected)
		}

		if len(command.Key) > 0 {
			command.Key[0] ^= 0xff
		}
		if len(command.Value) > 0 {
			command.Value[0] ^= 0xff
		}
		again, err := DecodeCommand(encoded)
		if err != nil || !reflect.DeepEqual(again, expected) {
			t.Fatalf("input mutation changed encoding: %#v, err=%v", again, err)
		}

		encoded[0] ^= 0xff
		if !reflect.DeepEqual(decoded, expected) {
			t.Fatal("encoded-buffer mutation changed decoded command")
		}
	}
}

func TestCommandValidationAndSizeBounds(t *testing.T) {
	invalid := []Command{
		{},
		{ID: testID(0), Operation: 0, Key: []byte("x")},
		{ID: testID(0), Operation: 3, Key: []byte("x")},
		{ID: testID(0), Operation: Put},
		{ID: testID(0), Operation: Delete, Key: []byte("x"), Value: []byte("not-empty")},
		{ID: testID(0), Operation: Put, Key: make([]byte, MaxKeyBytes+1)},
		{ID: testID(0), Operation: Put, Key: []byte("x"), Value: make([]byte, MaxValueBytes+1)},
	}
	for _, command := range invalid {
		if _, err := EncodeCommand(command); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("EncodeCommand(%#v) error = %v", command, err)
		}
	}
	overallTooLarge := Command{ID: testID(0), Operation: Put, Key: make([]byte, MaxKeyBytes), Value: make([]byte, MaxValueBytes)}
	if _, err := EncodeCommand(overallTooLarge); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("combined oversized command error = %v", err)
	}
	if _, err := DecodeCommand(make([]byte, MaxCommandBytes+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized wire command error = %v", err)
	}
}

func TestDecodeCommandRejectsMalformedTruncatedAndTrailingData(t *testing.T) {
	valid := mustEncodeCommand(t, put(0, []byte("key"), []byte("value")))
	for length := 0; length < len(valid); length++ {
		if _, err := DecodeCommand(valid[:length]); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("truncation at %d error = %v", length, err)
		}
	}

	mutations := make([][]byte, 0, 8)
	wrongMagic := cloneBytes(valid)
	wrongMagic[0] ^= 0xff
	mutations = append(mutations, wrongMagic)
	zeroID := cloneBytes(valid)
	clear(zeroID[len(commandMagic)+1 : len(commandMagic)+1+len(CommandID{})])
	mutations = append(mutations, zeroID)
	unknownOperation := cloneBytes(valid)
	unknownOperation[len(commandMagic)] = 0xff
	mutations = append(mutations, unknownOperation)
	deleteWithValue := cloneBytes(valid)
	deleteWithValue[len(commandMagic)] = byte(Delete)
	mutations = append(mutations, deleteWithValue)
	wrongKeyLength := cloneBytes(valid)
	binary.BigEndian.PutUint32(wrongKeyLength[25:29], uint32(len("key")+1))
	mutations = append(mutations, wrongKeyLength)
	wrongValueLength := cloneBytes(valid)
	binary.BigEndian.PutUint32(wrongValueLength[29:33], uint32(len("value")+1))
	mutations = append(mutations, wrongValueLength)
	oversizedKeyLength := cloneBytes(valid)
	binary.BigEndian.PutUint32(oversizedKeyLength[25:29], MaxKeyBytes+1)
	mutations = append(mutations, oversizedKeyLength)
	oversizedValueLength := cloneBytes(valid)
	binary.BigEndian.PutUint32(oversizedValueLength[29:33], MaxValueBytes+1)
	mutations = append(mutations, oversizedValueLength)
	mutations = append(mutations, append(cloneBytes(valid), 0), append([]byte{0}, valid...))

	for index, mutation := range mutations {
		if _, err := DecodeCommand(mutation); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("mutation %d error = %v", index, err)
		}
	}
}

func TestAggregateBoundsFailBeforeMutation(t *testing.T) {
	machine := New()
	value := make([]byte, MaxValueBytes)
	for index := byte(0); ; index++ {
		before := machine.Commitment()
		_, err := machine.Apply(put(index, []byte("same-key"), value))
		if errors.Is(err, ErrTooLarge) {
			if got := machine.Commitment(); got != before {
				t.Fatalf("failed bounded apply mutated commitment: got %+v, want %+v", got, before)
			}
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if index == 100 {
			t.Fatal("history bound was not enforced")
		}
	}

	checkpoint := New().Checkpoint()
	checkpoint.Commands = Uint64(MaxCommands + 1)
	checkpoint.History = make([]Block, MaxCommands+1)
	if _, err := Restore(checkpoint); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized history restore error = %v", err)
	}
}

func TestKVSemanticsAndCanonicalStateOrdering(t *testing.T) {
	machine := New()
	mustApply(t, machine,
		put(0, []byte("b"), []byte("first")),
		put(16, []byte("a"), []byte("one")),
		put(32, []byte("b"), []byte("second")),
		put(48, []byte{0x00, 0xff}, []byte{}),
		remove(64, []byte("a")),
	)
	checkpoint := machine.Checkpoint()
	want := []Pair{
		{Key: []byte{0x00, 0xff}, Value: []byte{}},
		{Key: []byte("b"), Value: []byte("second")},
	}
	if !pairsEqual(checkpoint.State, want) {
		t.Fatalf("state = %#v, want %#v", checkpoint.State, want)
	}
	before := machine.Commitment()
	mustApply(t, machine, remove(80, []byte("missing")))
	after := machine.Commitment()
	if before.StateDigest != after.StateDigest || before.ChainDigest == after.ChainDigest || uint64(after.Commands) != uint64(before.Commands)+1 {
		t.Fatalf("delete-missing transition: before=%+v after=%+v", before, after)
	}
}

func TestDuplicateIDFailsWithoutMutation(t *testing.T) {
	machine := New()
	first := put(0, []byte("x"), []byte("1"))
	mustApply(t, machine, first)
	before := machine.Checkpoint()
	for _, duplicate := range []Command{
		first,
		put(0, []byte("x"), []byte("2")),
		remove(0, []byte("x")),
	} {
		if _, err := machine.Apply(duplicate); !errors.Is(err, ErrDuplicateCommand) {
			t.Fatalf("duplicate error = %v", err)
		}
		if after := machine.Checkpoint(); !reflect.DeepEqual(after, before) {
			t.Fatalf("duplicate mutated machine:\nbefore=%+v\nafter=%+v", before, after)
		}
	}
}

func TestApplicationFaultVariants(t *testing.T) {
	commands := []Command{
		put(0, []byte("a"), []byte("1")),
		put(16, []byte("b"), []byte("2")),
		put(32, []byte("c"), []byte("3")),
	}
	baseline := New()
	mustApply(t, baseline, commands...)
	baselineCommitment := baseline.Commitment()

	dropped := New()
	mustApply(t, dropped, commands[:2]...)
	if dropped.Commitment() == baselineCommitment {
		t.Fatal("dropped application retained the baseline commitment")
	}

	reordered := New()
	mustApply(t, reordered, commands[1], commands[0], commands[2])
	if reordered.Commitment().StateDigest != baselineCommitment.StateDigest {
		t.Fatal("reordering independent puts changed final state")
	}
	if reordered.Commitment().ChainDigest == baselineCommitment.ChainDigest {
		t.Fatal("reordered applications retained the baseline chain")
	}

	duplicate := New()
	mustApply(t, duplicate, commands[0])
	beforeDuplicate := duplicate.Checkpoint()
	if _, err := duplicate.Apply(commands[0]); !errors.Is(err, ErrDuplicateCommand) {
		t.Fatalf("duplicate application error = %v", err)
	}
	if !reflect.DeepEqual(duplicate.Checkpoint(), beforeDuplicate) {
		t.Fatal("duplicate application mutated state")
	}

	corrupt := New()
	wire := mustEncodeCommand(t, commands[0])
	wire[0] ^= 0xff
	beforeCorrupt := corrupt.Checkpoint()
	if _, err := corrupt.ApplyEncoded(wire); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("corrupt application error = %v", err)
	}
	if !reflect.DeepEqual(corrupt.Checkpoint(), beforeCorrupt) {
		t.Fatal("corrupt application mutated state")
	}
}

func TestMachineAndCheckpointViewsAreDeepCopies(t *testing.T) {
	command := put(0, []byte("key"), []byte("value"))
	machine := New()
	block, err := machine.Apply(command)
	if err != nil {
		t.Fatal(err)
	}
	command.Key[0] = 'X'
	command.Value[0] = 'X'
	block.Command[0] ^= 0xff

	blocks := machine.Blocks()
	checkpoint := machine.Checkpoint()
	blocks[0].Command[0] ^= 0xff
	checkpoint.State[0].Key[0] = 'Y'
	checkpoint.State[0].Value[0] = 'Y'
	checkpoint.History[0].Command[0] ^= 0xff

	againBlocks := machine.Blocks()
	againCheckpoint := machine.Checkpoint()
	decoded, err := DecodeCommand(againBlocks[0].Command)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Key, []byte("key")) || !bytes.Equal(decoded.Value, []byte("value")) || !bytes.Equal(againCheckpoint.State[0].Key, []byte("key")) || !bytes.Equal(againCheckpoint.State[0].Value, []byte("value")) {
		t.Fatalf("mutation escaped a deep-copy boundary: command=%+v state=%+v", decoded, againCheckpoint.State)
	}
	if !bytes.Equal(againBlocks[0].Command, againCheckpoint.History[0].Command) {
		t.Fatal("block history views diverged after caller mutation")
	}
}

func TestCheckpointCanonicalEncoding(t *testing.T) {
	machine := New()
	mustApply(t, machine, put(0, []byte("b"), []byte("2")), put(16, []byte("a"), []byte("1")))
	checkpoint := machine.Checkpoint()
	encoded, err := EncodeCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCheckpoint(encoded)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := EncodeCheckpoint(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, reencoded) || !reflect.DeepEqual(decoded, checkpoint) {
		t.Fatal("checkpoint did not have a unique canonical round trip")
	}

	var pretty bytes.Buffer
	if err := json.Indent(&pretty, encoded, "", "  "); err != nil {
		t.Fatal(err)
	}
	unknown := append(cloneBytes(encoded[:len(encoded)-1]), []byte(`,"unknown":true}`)...)
	noncanonical := [][]byte{
		append([]byte(" "), encoded...),
		append(cloneBytes(encoded), '\n'),
		pretty.Bytes(),
		unknown,
		append(cloneBytes(encoded), []byte(`{}`)...),
	}
	for index, data := range noncanonical {
		if _, err := DecodeCheckpoint(data); !errors.Is(err, ErrInvalidCheckpoint) {
			t.Fatalf("noncanonical checkpoint %d error = %v", index, err)
		}
	}

	for length := 1; length < len(encoded); length++ {
		if _, err := DecodeCheckpoint(encoded[:length]); !errors.Is(err, ErrInvalidCheckpoint) {
			t.Fatalf("checkpoint truncation at %d error = %v", length, err)
		}
	}
	if _, err := DecodeCheckpoint(nil); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("empty checkpoint error = %v", err)
	}
	if _, err := DecodeCheckpoint(make([]byte, MaxCheckpointBytes+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized checkpoint error = %v", err)
	}
}

func TestCheckpointRejectsCorruption(t *testing.T) {
	machine := New()
	mustApply(t, machine, put(0, []byte("a"), []byte("1")), put(16, []byte("b"), []byte("2")))
	valid := machine.Checkpoint()
	zeroDigest := strings.Repeat("0", 64)
	mutations := []struct {
		name   string
		mutate func(*Checkpoint)
	}{
		{"schema", func(value *Checkpoint) { value.Schema = "d-raft.kv-checkpoint/v2" }},
		{"commands", func(value *Checkpoint) { value.Commands++ }},
		{"chain digest", func(value *Checkpoint) { value.ChainDigest = zeroDigest }},
		{"state digest", func(value *Checkpoint) { value.StateDigest = zeroDigest }},
		{"nil state", func(value *Checkpoint) { value.State = nil }},
		{"nil history", func(value *Checkpoint) { value.History = nil }},
		{"unsorted state", func(value *Checkpoint) { value.State[0], value.State[1] = value.State[1], value.State[0] }},
		{"duplicate state key", func(value *Checkpoint) { value.State[1].Key = cloneBytes(value.State[0].Key) }},
		{"nil state key", func(value *Checkpoint) { value.State[0].Key = nil }},
		{"nil state value", func(value *Checkpoint) { value.State[0].Value = nil }},
		{"state mismatch", func(value *Checkpoint) { value.State[0].Value[0] ^= 0xff }},
		{"block ordinal", func(value *Checkpoint) { value.History[0].Ordinal = 2 }},
		{"block command ID", func(value *Checkpoint) { value.History[0].CommandID = testID(64).String() }},
		{"nil block command", func(value *Checkpoint) { value.History[0].Command = nil }},
		{"corrupt block command", func(value *Checkpoint) { value.History[0].Command[0] ^= 0xff }},
		{"block command digest", func(value *Checkpoint) { value.History[0].CommandDigest = zeroDigest }},
		{"block state digest", func(value *Checkpoint) { value.History[0].StateDigest = zeroDigest }},
		{"block digest", func(value *Checkpoint) { value.History[0].Digest = zeroDigest }},
		{"duplicate history ID", func(value *Checkpoint) {
			value.History[1].Command = cloneBytes(value.History[0].Command)
			value.History[1].CommandID = value.History[0].CommandID
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			checkpoint := cloneCheckpoint(valid)
			mutation.mutate(&checkpoint)
			if _, err := Restore(checkpoint); !errors.Is(err, ErrInvalidCheckpoint) {
				t.Fatalf("Restore error = %v", err)
			}
			if _, err := EncodeCheckpoint(checkpoint); !errors.Is(err, ErrInvalidCheckpoint) {
				t.Fatalf("EncodeCheckpoint error = %v", err)
			}
			encoded, err := json.Marshal(checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeCheckpoint(encoded); !errors.Is(err, ErrInvalidCheckpoint) {
				t.Fatalf("DecodeCheckpoint error = %v", err)
			}
		})
	}
}

func TestCheckpointCounterRequiresCanonicalDecimalString(t *testing.T) {
	machine := New()
	mustApply(t, machine, put(0, []byte("a"), []byte("1")))
	encoded, err := EncodeCheckpoint(machine.Checkpoint())
	if err != nil {
		t.Fatal(err)
	}
	for _, replacement := range []string{`"commands":1`, `"commands":"01"`, `"commands":"+1"`, `"commands":"18446744073709551616"`} {
		mutation := bytes.Replace(encoded, []byte(`"commands":"1"`), []byte(replacement), 1)
		if bytes.Equal(mutation, encoded) {
			t.Fatalf("replacement %q did not alter encoding", replacement)
		}
		if _, err := DecodeCheckpoint(mutation); !errors.Is(err, ErrInvalidCheckpoint) {
			t.Fatalf("replacement %q error = %v", replacement, err)
		}
	}
}

func TestRestoreAndContinueMatchesUninterruptedExecution(t *testing.T) {
	commands := []Command{
		put(0, []byte("a"), []byte("1")),
		put(16, []byte("b"), []byte("2")),
		remove(32, []byte("a")),
		put(48, []byte("c"), []byte("3")),
	}
	uninterrupted := New()
	mustApply(t, uninterrupted, commands...)

	prefix := New()
	mustApply(t, prefix, commands[:2]...)
	encoded, err := EncodeCheckpoint(prefix.Checkpoint())
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := DecodeCheckpoint(encoded)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := Restore(checkpoint)
	if err != nil {
		t.Fatal(err)
	}

	checkpoint.State[0].Value[0] ^= 0xff
	checkpoint.History[0].Command[0] ^= 0xff
	preserved := restored.Commitment()
	mustApply(t, restored, commands[2:]...)
	if restored.Commitment() != uninterrupted.Commitment() || !reflect.DeepEqual(restored.Blocks(), uninterrupted.Blocks()) {
		t.Fatalf("restored=%+v uninterrupted=%+v preserved=%+v", restored.Commitment(), uninterrupted.Commitment(), preserved)
	}
	if _, err := restored.Apply(commands[0]); !errors.Is(err, ErrDuplicateCommand) {
		t.Fatalf("restored duplicate detection error = %v", err)
	}
}

func TestReplayIgnoresRaftMetadataNoopsAndConfiguration(t *testing.T) {
	first := mustEncodeCommand(t, put(0, []byte("a"), []byte("1")))
	second := mustEncodeCommand(t, put(16, []byte("b"), []byte("2")))
	left := []raft.Entry{
		{Index: 1, Term: 1, Type: raft.EntryNoop},
		{Index: 2, Term: 4, Type: raft.EntryCommand, Data: first},
		{Index: 3, Term: 4, Type: raft.EntryConfigJoint, Data: []byte("ignored")},
		{Index: 4, Term: 7, Type: raft.EntryCommand, Data: second},
	}
	right := []raft.Entry{
		{Index: 91, Term: 20, Type: raft.EntryCommand, Data: first},
		{Index: 92, Term: 21, Type: raft.EntryNoop, Data: []byte("ignored")},
		{Index: 93, Term: 22, Type: raft.EntryConfigFinal, Data: []byte("ignored")},
		{Index: 94, Term: 30, Type: raft.EntryCommand, Data: second},
	}
	leftCommitment, leftBlocks, err := ReplayEntries(nil, left)
	if err != nil {
		t.Fatal(err)
	}
	rightCommitment, rightBlocks, err := ReplayEntries(nil, right)
	if err != nil {
		t.Fatal(err)
	}
	if leftCommitment != rightCommitment || !reflect.DeepEqual(leftBlocks, rightBlocks) {
		t.Fatalf("Raft metadata changed projection:\nleft=%+v\nright=%+v", leftCommitment, rightCommitment)
	}
}

func TestReplayFromCheckpointAndRejectsInvalidInputs(t *testing.T) {
	first := mustEncodeCommand(t, put(0, []byte("a"), []byte("1")))
	second := mustEncodeCommand(t, put(16, []byte("b"), []byte("2")))
	full, _, err := ReplayEntries(nil, []raft.Entry{{Type: raft.EntryCommand, Data: first}, {Type: raft.EntryCommand, Data: second}})
	if err != nil {
		t.Fatal(err)
	}
	prefix := New()
	if _, err := prefix.ApplyEncoded(first); err != nil {
		t.Fatal(err)
	}
	checkpoint := prefix.Checkpoint()
	resumed, blocks, err := ReplayEntries(&checkpoint, []raft.Entry{{Index: 999, Term: 88, Type: raft.EntryCommand, Data: second}})
	if err != nil {
		t.Fatal(err)
	}
	if resumed != full || len(blocks) != 1 || blocks[0].Ordinal != 2 {
		t.Fatalf("resumed=%+v full=%+v blocks=%+v", resumed, full, blocks)
	}

	if _, _, err := ReplayEntries(nil, []raft.Entry{{Type: 0}}); !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("invalid entry error = %v", err)
	}
	if _, _, err := ReplayEntries(nil, []raft.Entry{{Type: raft.EntryCommand, Data: []byte("not-a-command")}}); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("invalid command error = %v", err)
	}
	corrupt := cloneCheckpoint(checkpoint)
	corrupt.ChainDigest = strings.Repeat("0", 64)
	if _, _, err := ReplayEntries(&corrupt, nil); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("invalid base checkpoint error = %v", err)
	}
}

func TestSameStateDifferentHistoryHasDifferentChain(t *testing.T) {
	left, right := New(), New()
	mustApply(t, left, put(0, []byte("a"), []byte("1")), put(16, []byte("b"), []byte("2")))
	mustApply(t, right, put(32, []byte("b"), []byte("2")), put(48, []byte("a"), []byte("1")))
	leftCommitment, rightCommitment := left.Commitment(), right.Commitment()
	if leftCommitment.StateDigest != rightCommitment.StateDigest {
		t.Fatal("equivalent final states had different state digests")
	}
	if leftCommitment.ChainDigest == rightCommitment.ChainDigest {
		t.Fatal("different command histories had equal chain digests")
	}
}

func FuzzDecodeCommand(f *testing.F) {
	valid, err := EncodeCommand(put(0, []byte("key"), []byte("value")))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte("DRAFTKV1"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		command, err := DecodeCommand(data)
		if err != nil {
			return
		}
		encoded, err := EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, encoded) {
			t.Fatal("decoder accepted a non-canonical command")
		}
		if len(data) > 0 {
			before := cloneBytes(command.Key)
			data[0] ^= 0xff
			if !bytes.Equal(command.Key, before) {
				t.Fatal("decoded command aliases its input")
			}
		}
	})
}

func FuzzDecodeCheckpoint(f *testing.F) {
	machine := New()
	mustApply(f, machine, put(0, []byte("key"), []byte("value")))
	valid, err := EncodeCheckpoint(machine.Checkpoint())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte(`{}`))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		checkpoint, err := DecodeCheckpoint(data)
		if err != nil {
			return
		}
		encoded, err := EncodeCheckpoint(checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, encoded) {
			t.Fatal("decoder accepted a non-canonical checkpoint")
		}
		if _, err := Restore(checkpoint); err != nil {
			t.Fatal(err)
		}
	})
}
