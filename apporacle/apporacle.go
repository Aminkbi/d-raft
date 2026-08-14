// Package apporacle implements a versioned, Raft-independent application
// commitment for portable cross-adapter experiments.
package apporacle

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"

	"github.com/aminkbi/d-raft/raft"
)

const (
	CommandSchema    = "d-raft.kv-command/v1"
	StateSchema      = "d-raft.kv-state/v1"
	ChainSchema      = "d-raft.kv-chain/v1"
	CheckpointSchema = "d-raft.kv-checkpoint/v1"
	CommitmentSchema = "d-raft.kv-commitment/v1"

	MaxCommandBytes    = 1 << 20
	MaxCheckpointBytes = 64 << 20
	MaxKeyBytes        = 64 << 10
	MaxValueBytes      = MaxCommandBytes - 64
	MaxCommands        = 50_000
	MaxHistoryBytes    = 12 << 20
	MaxStateBytes      = 8 << 20
)

var (
	ErrInvalidCommand    = errors.New("apporacle: invalid command")
	ErrInvalidCheckpoint = errors.New("apporacle: invalid checkpoint")
	ErrDuplicateCommand  = errors.New("apporacle: duplicate command ID")
	ErrInvalidEntry      = errors.New("apporacle: invalid applied entry type")
	ErrTooLarge          = errors.New("apporacle: encoded value exceeds limit")
)

var commandMagic = [8]byte{'D', 'R', 'A', 'F', 'T', 'K', 'V', '1'}

// Config explicitly opts an adapter into one portable application schema.
// A nil *Config at the adapter boundary preserves opaque legacy proposals.
type Config struct {
	Schema string `json:"schema"`
}

// KVConfig selects the only application profile implemented by this package.
func KVConfig() Config { return Config{Schema: CommandSchema} }

// Validate rejects unknown profiles before an adapter consumes decisions.
func (config Config) Validate() error {
	if config.Schema != CommandSchema {
		return ErrInvalidCommand
	}
	return nil
}

// Uint64 encodes counters as canonical decimal strings so generic JSON tools
// cannot round full-width values through float64.
type Uint64 uint64

func (value Uint64) MarshalJSON() ([]byte, error) {
	return json.Marshal(strconv.FormatUint(uint64(value), 10))
}

func (value *Uint64) UnmarshalJSON(data []byte) error {
	if value == nil {
		return errors.New("apporacle: nil Uint64 receiver")
	}
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return fmt.Errorf("%w: counter must be a decimal string", ErrInvalidCheckpoint)
	}
	parsed, err := strconv.ParseUint(encoded, 10, 64)
	if err != nil || strconv.FormatUint(parsed, 10) != encoded {
		return fmt.Errorf("%w: invalid counter %q", ErrInvalidCheckpoint, encoded)
	}
	*value = Uint64(parsed)
	return nil
}

// CommandID is a stable opaque 128-bit client command identity.
type CommandID [16]byte

func (id CommandID) String() string { return hex.EncodeToString(id[:]) }

// ParseCommandID decodes one canonical lowercase 32-digit hexadecimal ID.
func ParseCommandID(value string) (CommandID, error) {
	var id CommandID
	if len(value) != len(id)*2 {
		return id, ErrInvalidCommand
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || hex.EncodeToString(decoded) != value {
		return id, ErrInvalidCommand
	}
	copy(id[:], decoded)
	if id == (CommandID{}) {
		return CommandID{}, ErrInvalidCommand
	}
	return id, nil
}

// Operation is one deterministic KV state-machine transition.
type Operation uint8

const (
	Put    Operation = 1
	Delete Operation = 2
)

// Command is the adapter-neutral proposal. Empty Put values are valid; Delete
// requires an empty value. Binary keys and values are supported.
type Command struct {
	ID        CommandID
	Operation Operation
	Key       []byte
	Value     []byte
}

// Pair is one canonical binary key/value item in a checkpoint.
type Pair struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// Block commits to one client command and its resulting deterministic state.
// It deliberately excludes Raft term, index, leader, no-op, and membership.
type Block struct {
	Ordinal       Uint64 `json:"ordinal"`
	CommandID     string `json:"command_id"`
	Command       []byte `json:"command"`
	CommandDigest string `json:"command_digest"`
	StateDigest   string `json:"state_digest"`
	Digest        string `json:"digest"`
}

// Commitment is the compact cross-adapter comparison surface.
type Commitment struct {
	Schema      string `json:"schema"`
	Commands    Uint64 `json:"commands"`
	ChainDigest string `json:"chain_digest"`
	StateDigest string `json:"state_digest"`
}

// Checkpoint retains canonical state and ordered blocks. Retaining blocks in
// v1 makes the checkpoint self-verifying: Restore recomputes every transition,
// duplicate-ID decision, state digest, and chain link from genesis.
type Checkpoint struct {
	Schema      string  `json:"schema"`
	Commands    Uint64  `json:"commands"`
	ChainDigest string  `json:"chain_digest"`
	StateDigest string  `json:"state_digest"`
	State       []Pair  `json:"state"`
	History     []Block `json:"history"`
}

// Clone returns an independent copy suitable for durable adapter state.
func (checkpoint Checkpoint) Clone() Checkpoint { return cloneCheckpoint(checkpoint) }

// Machine is a deterministic KV state machine plus portable history chain.
// Duplicate IDs fail closed; v1 does not model idempotent client retries.
type Machine struct {
	state        map[string][]byte
	seen         map[CommandID]string
	history      []Block
	chainDigest  [sha256.Size]byte
	historyBytes int
	stateBytes   int
}

// New returns an empty portable state machine with the versioned genesis head.
func New() *Machine {
	initial := sha256.Sum256(append([]byte(ChainSchema), 0))
	return &Machine{state: make(map[string][]byte), seen: make(map[CommandID]string), history: make([]Block, 0), chainDigest: initial}
}

// Clone returns an independently mutable machine with the same verified state.
func (m *Machine) Clone() (*Machine, error) {
	if m == nil || m.state == nil || m.seen == nil || m.history == nil {
		return nil, ErrInvalidCheckpoint
	}
	clone := &Machine{
		state: make(map[string][]byte, len(m.state)), seen: make(map[CommandID]string, len(m.seen)),
		history: make([]Block, len(m.history)), chainDigest: m.chainDigest,
		historyBytes: m.historyBytes, stateBytes: m.stateBytes,
	}
	for key, value := range m.state {
		clone.state[key] = cloneBytes(value)
	}
	for id, digest := range m.seen {
		clone.seen[id] = digest
	}
	for index, block := range m.history {
		clone.history[index] = cloneBlock(block)
	}
	return clone, nil
}

// EncodeCommand returns the unique binary representation:
//
//	DRAFTKV1 | op:u8 | id:[16]byte | key_len:u32be | value_len:u32be | key | value
func EncodeCommand(command Command) ([]byte, error) {
	if err := validateCommand(command); err != nil {
		return nil, err
	}
	size := len(commandMagic) + 1 + len(command.ID) + 4 + 4 + len(command.Key) + len(command.Value)
	if size > MaxCommandBytes {
		return nil, ErrTooLarge
	}
	encoded := make([]byte, size)
	offset := copy(encoded, commandMagic[:])
	encoded[offset] = byte(command.Operation)
	offset++
	offset += copy(encoded[offset:], command.ID[:])
	binary.BigEndian.PutUint32(encoded[offset:], uint32(len(command.Key)))
	offset += 4
	binary.BigEndian.PutUint32(encoded[offset:], uint32(len(command.Value)))
	offset += 4
	offset += copy(encoded[offset:], command.Key)
	copy(encoded[offset:], command.Value)
	return encoded, nil
}

// DecodeCommand strictly decodes one bounded binary command.
func DecodeCommand(data []byte) (Command, error) {
	const header = 8 + 1 + 16 + 4 + 4
	if len(data) < header {
		return Command{}, ErrInvalidCommand
	}
	if len(data) > MaxCommandBytes {
		return Command{}, ErrTooLarge
	}
	if !bytes.Equal(data[:len(commandMagic)], commandMagic[:]) {
		return Command{}, ErrInvalidCommand
	}
	offset := len(commandMagic)
	command := Command{Operation: Operation(data[offset])}
	offset++
	copy(command.ID[:], data[offset:offset+len(command.ID)])
	offset += len(command.ID)
	keyLength := uint64(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	valueLength := uint64(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if keyLength > MaxKeyBytes || valueLength > MaxValueBytes || keyLength+valueLength != uint64(len(data)-offset) {
		return Command{}, ErrInvalidCommand
	}
	command.Key = cloneBytes(data[offset : offset+int(keyLength)])
	offset += int(keyLength)
	command.Value = cloneBytes(data[offset:])
	if err := validateCommand(command); err != nil {
		return Command{}, err
	}
	return command, nil
}

func validateCommand(command Command) error {
	if command.ID == (CommandID{}) || len(command.Key) == 0 || len(command.Key) > MaxKeyBytes || len(command.Value) > MaxValueBytes {
		return ErrInvalidCommand
	}
	switch command.Operation {
	case Put:
	case Delete:
		if len(command.Value) != 0 {
			return ErrInvalidCommand
		}
	default:
		return ErrInvalidCommand
	}
	return nil
}

// CommandDigest returns SHA-256 over the canonical command bytes.
func CommandDigest(command Command) (string, error) {
	encoded, err := EncodeCommand(command)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// Apply performs one command exactly once and returns its portable block. Any
// returned error leaves the machine unchanged.
func (m *Machine) Apply(command Command) (Block, error) {
	encoded, err := EncodeCommand(command)
	if err != nil {
		return Block{}, err
	}
	return m.applyCanonical(command, encoded)
}

// ApplyEncoded strictly decodes and applies one proposal payload. Any returned
// error leaves the machine unchanged.
func (m *Machine) ApplyEncoded(data []byte) (Block, error) {
	command, err := DecodeCommand(data)
	if err != nil {
		return Block{}, err
	}
	canonical, err := EncodeCommand(command)
	if err != nil {
		return Block{}, err
	}
	return m.applyCanonical(command, canonical)
}

func (m *Machine) applyCanonical(command Command, canonical []byte) (Block, error) {
	if m == nil || m.state == nil || m.seen == nil || m.history == nil {
		return Block{}, ErrInvalidCheckpoint
	}
	commandHash := sha256.Sum256(canonical)
	commandDigest := hex.EncodeToString(commandHash[:])
	if previous, exists := m.seen[command.ID]; exists {
		return Block{}, fmt.Errorf("%w: %s first=%s current=%s", ErrDuplicateCommand, command.ID, previous, commandDigest)
	}
	if len(m.history) >= MaxCommands || len(canonical) > MaxHistoryBytes-m.historyBytes {
		return Block{}, ErrTooLarge
	}
	stateBytes := m.stateBytes
	previousValue, keyExists := m.state[string(command.Key)]
	switch command.Operation {
	case Put:
		if keyExists {
			stateBytes -= len(previousValue)
		} else {
			stateBytes += len(command.Key)
		}
		stateBytes += len(command.Value)
	case Delete:
		if keyExists {
			stateBytes -= len(command.Key) + len(previousValue)
		}
	}
	if stateBytes > MaxStateBytes {
		return Block{}, ErrTooLarge
	}
	switch command.Operation {
	case Put:
		m.state[string(command.Key)] = cloneBytes(command.Value)
	case Delete:
		delete(m.state, string(command.Key))
	}
	ordinal := uint64(len(m.history)) + 1
	stateDigest := digestState(m.state)
	hash := sha256.New()
	hash.Write([]byte(ChainSchema))
	hash.Write([]byte{0})
	hash.Write(m.chainDigest[:])
	writeUint64(hash, ordinal)
	writeUint32(hash, uint32(len(canonical)))
	hash.Write(canonical)
	hash.Write(stateDigest[:])
	copy(m.chainDigest[:], hash.Sum(nil))
	block := Block{
		Ordinal: Uint64(ordinal), CommandID: command.ID.String(), Command: cloneBytes(canonical),
		CommandDigest: commandDigest, StateDigest: hex.EncodeToString(stateDigest[:]), Digest: hex.EncodeToString(m.chainDigest[:]),
	}
	m.seen[command.ID] = commandDigest
	m.history = append(m.history, block)
	m.historyBytes += len(canonical)
	m.stateBytes = stateBytes
	return cloneBlock(block), nil
}

// Blocks returns an independent ordered history.
func (m *Machine) Blocks() []Block {
	result := make([]Block, len(m.history))
	for index, block := range m.history {
		result[index] = cloneBlock(block)
	}
	return result
}

// Commitment returns the compact current comparison value.
func (m *Machine) Commitment() Commitment {
	stateDigest := digestState(m.state)
	return Commitment{
		Schema: CommitmentSchema, Commands: Uint64(len(m.history)),
		ChainDigest: hex.EncodeToString(m.chainDigest[:]), StateDigest: hex.EncodeToString(stateDigest[:]),
	}
}

// Checkpoint returns a complete canonical deep copy.
func (m *Machine) Checkpoint() Checkpoint {
	commitment := m.Commitment()
	checkpoint := Checkpoint{
		Schema: CheckpointSchema, Commands: commitment.Commands,
		ChainDigest: commitment.ChainDigest, StateDigest: commitment.StateDigest,
		State: make([]Pair, 0, len(m.state)), History: m.Blocks(),
	}
	for key, value := range m.state {
		checkpoint.State = append(checkpoint.State, Pair{Key: cloneBytes([]byte(key)), Value: cloneBytes(value)})
	}
	slices.SortFunc(checkpoint.State, func(left, right Pair) int { return bytes.Compare(left.Key, right.Key) })
	return checkpoint
}

// EncodeCheckpoint validates and returns unique canonical JSON snapshot bytes.
func EncodeCheckpoint(checkpoint Checkpoint) ([]byte, error) {
	if _, err := Restore(checkpoint); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxCheckpointBytes {
		return nil, ErrTooLarge
	}
	return encoded, nil
}

// DecodeCheckpoint strictly decodes canonical JSON snapshot bytes.
func DecodeCheckpoint(data []byte) (Checkpoint, error) {
	if len(data) == 0 || len(data) > MaxCheckpointBytes {
		return Checkpoint{}, ErrTooLarge
	}
	var checkpoint Checkpoint
	if err := decodeStrict(data, &checkpoint); err != nil {
		return Checkpoint{}, fmt.Errorf("%w: %v", ErrInvalidCheckpoint, err)
	}
	canonical, err := json.Marshal(checkpoint)
	if err != nil {
		return Checkpoint{}, err
	}
	if !bytes.Equal(data, canonical) {
		return Checkpoint{}, fmt.Errorf("%w: non-canonical encoding", ErrInvalidCheckpoint)
	}
	if _, err := Restore(checkpoint); err != nil {
		return Checkpoint{}, err
	}
	return cloneCheckpoint(checkpoint), nil
}

// Restore replays and validates every retained block before accepting state.
func Restore(checkpoint Checkpoint) (*Machine, error) {
	if checkpoint.Schema != CheckpointSchema || checkpoint.State == nil || checkpoint.History == nil || uint64(checkpoint.Commands) != uint64(len(checkpoint.History)) || !validDigest(checkpoint.ChainDigest) || !validDigest(checkpoint.StateDigest) {
		return nil, ErrInvalidCheckpoint
	}
	if len(checkpoint.History) > MaxCommands {
		return nil, ErrTooLarge
	}
	stateBytes := 0
	for index, pair := range checkpoint.State {
		if len(pair.Key) == 0 || pair.Key == nil || pair.Value == nil || len(pair.Key) > MaxKeyBytes || len(pair.Value) > MaxValueBytes || index > 0 && bytes.Compare(checkpoint.State[index-1].Key, pair.Key) >= 0 {
			return nil, ErrInvalidCheckpoint
		}
		if len(pair.Key)+len(pair.Value) > MaxStateBytes-stateBytes {
			return nil, ErrTooLarge
		}
		stateBytes += len(pair.Key) + len(pair.Value)
	}
	historyBytes := 0
	for _, block := range checkpoint.History {
		if len(block.Command) > MaxHistoryBytes-historyBytes {
			return nil, ErrTooLarge
		}
		historyBytes += len(block.Command)
	}
	machine := New()
	for _, expected := range checkpoint.History {
		if expected.Command == nil || !validDigest(expected.CommandDigest) || !validDigest(expected.StateDigest) || !validDigest(expected.Digest) {
			return nil, ErrInvalidCheckpoint
		}
		actual, err := machine.ApplyEncoded(expected.Command)
		if err != nil || !blocksEqual(actual, expected) {
			return nil, ErrInvalidCheckpoint
		}
	}
	actualCommitment := machine.Commitment()
	if actualCommitment.Commands != checkpoint.Commands || actualCommitment.ChainDigest != checkpoint.ChainDigest || actualCommitment.StateDigest != checkpoint.StateDigest {
		return nil, ErrInvalidCheckpoint
	}
	actualState := machine.Checkpoint().State
	if !pairsEqual(actualState, checkpoint.State) {
		return nil, ErrInvalidCheckpoint
	}
	return machine, nil
}

// ReplayEntries projects applied Raft entries into the portable application
// domain. No-ops and configuration entries are deliberately ignored.
func ReplayEntries(base *Checkpoint, entries []raft.Entry) (Commitment, []Block, error) {
	machine := New()
	var err error
	if base != nil {
		machine, err = Restore(*base)
		if err != nil {
			return Commitment{}, nil, err
		}
	}
	blocks := make([]Block, 0)
	for _, entry := range entries {
		switch entry.Type {
		case raft.EntryNoop, raft.EntryConfigJoint, raft.EntryConfigFinal:
			continue
		case raft.EntryCommand:
			block, applyErr := machine.ApplyEncoded(entry.Data)
			if applyErr != nil {
				return Commitment{}, nil, applyErr
			}
			blocks = append(blocks, block)
		default:
			return Commitment{}, nil, fmt.Errorf("%w: %d", ErrInvalidEntry, entry.Type)
		}
	}
	return machine.Commitment(), blocks, nil
}

func digestState(state map[string][]byte) [sha256.Size]byte {
	keys := make([]string, 0, len(state))
	for key := range state {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(left, right string) int { return bytes.Compare([]byte(left), []byte(right)) })
	hash := sha256.New()
	hash.Write([]byte(StateSchema))
	hash.Write([]byte{0})
	writeUint64(hash, uint64(len(keys)))
	for _, key := range keys {
		writeUint32(hash, uint32(len(key)))
		hash.Write([]byte(key))
		writeUint32(hash, uint32(len(state[key])))
		hash.Write(state[key])
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func cloneCheckpoint(source Checkpoint) Checkpoint {
	source.State = slices.Clone(source.State)
	for index := range source.State {
		source.State[index].Key = cloneBytes(source.State[index].Key)
		source.State[index].Value = cloneBytes(source.State[index].Value)
	}
	source.History = slices.Clone(source.History)
	for index := range source.History {
		source.History[index] = cloneBlock(source.History[index])
	}
	return source
}

func cloneBlock(source Block) Block {
	source.Command = cloneBytes(source.Command)
	return source
}

func cloneBytes(source []byte) []byte {
	result := make([]byte, len(source))
	copy(result, source)
	return result
}

func blocksEqual(left, right Block) bool {
	return left.Ordinal == right.Ordinal && left.CommandID == right.CommandID && bytes.Equal(left.Command, right.Command) && left.CommandDigest == right.CommandDigest && left.StateDigest == right.StateDigest && left.Digest == right.Digest
}

func pairsEqual(left, right []Pair) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal(left[index].Key, right[index].Key) || !bytes.Equal(left[index].Value, right[index].Value) {
			return false
		}
	}
	return true
}

func writeUint64(writer io.Writer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeUint32(writer io.Writer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}
