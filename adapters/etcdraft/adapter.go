// Package etcdraft adapts the production go.etcd.io/raft RawNode core to
// d-raft's deterministic experiment and checker interfaces.
package etcdraft

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	rootraft "github.com/aminkbi/d-raft/raft"
)

const (
	AdapterID            = "go.etcd.io/raft/v3"
	UpstreamVersion      = "v3.7.0"
	AdapterSchemaVersion = "1"
	AdapterVersion       = "3.7.0+d-raft.1"
	MessageCodecVersion  = "go.etcd.io/raft/v3/raftpb/deterministic-protobuf-v1"
)

var (
	ErrUnsupported      = errors.New("etcdraft: unsupported capability")
	ErrInvalidConfig    = errors.New("etcdraft: invalid configuration")
	ErrUnknownNode      = errors.New("etcdraft: unknown node")
	ErrNodeDown         = errors.New("etcdraft: node is down")
	ErrNodeUp           = errors.New("etcdraft: node is already up")
	ErrNoLeader         = errors.New("etcdraft: no unique leader")
	ErrPersistenceOrder = errors.New("etcdraft: invalid persistence order")
	ErrInvalidProposal  = errors.New("etcdraft: proposal payload must not be empty")
)

// Capabilities documents the deliberately narrow first production-adapter
// surface. Fixed all-voter membership is supported; snapshots and membership
// changes fail closed instead of being silently approximated.
type Capabilities struct {
	FixedMembership bool `json:"fixed_membership"`
	Proposals       bool `json:"proposals"`
	Partitions      bool `json:"partitions"`
	CrashRestart    bool `json:"crash_restart"`
	CrashAfterWrite bool `json:"crash_after_write"`
	Snapshots       bool `json:"snapshots"`
	Membership      bool `json:"membership"`
	CanonicalCache  bool `json:"canonical_cache"`
}

func SupportedCapabilities() Capabilities {
	return Capabilities{FixedMembership: true, Proposals: true, Partitions: true, CrashRestart: true, CrashAfterWrite: true}
}

// Config fixes the adapter-specific scheduling policy around RawNode. Protocol
// elections use explicit semantic timers; followers are not Tick'ed, avoiding
// etcd/raft's intentionally non-injectable cryptographic timeout randomizer.
type Config struct {
	Members            []rootraft.NodeID
	Seed               uint64
	Network            NetworkConfig
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	StorageLatency     time.Duration
	MaxSteps           uint64
	StopOnViolation    bool
	Decider            decision.Decider
	Application        *apporacle.Config
}

// NetworkConfig is the stable subset shared with the root simulator.
type NetworkConfig struct {
	MinLatency      time.Duration
	MaxLatency      time.Duration
	LossProbability float64
}

func ConfigurationFrom(source artifact.Configuration, decider decision.Decider) (Config, error) {
	if len(source.Voters) > 0 || len(source.Learners) > 0 {
		voters := slices.Clone(source.Voters)
		slices.Sort(voters)
		members := slices.Clone(source.Members)
		slices.Sort(members)
		if !slices.Equal(voters, members) || len(source.Learners) != 0 {
			return Config{}, fmt.Errorf("%w: initial learners or a voter subset", ErrUnsupported)
		}
	}
	return Config{
		Members: slices.Clone(source.Members), Seed: uint64(source.InfrastructureSeed),
		Network:            NetworkConfig{MinLatency: time.Duration(source.NetworkMinLatencyNS), MaxLatency: time.Duration(source.NetworkMaxLatencyNS), LossProbability: source.NetworkLossProbability},
		ElectionTimeoutMin: time.Duration(source.ElectionTimeoutMinNS), ElectionTimeoutMax: time.Duration(source.ElectionTimeoutMaxNS),
		HeartbeatInterval: time.Duration(source.HeartbeatIntervalNS), StorageLatency: time.Duration(source.StorageLatencyNS),
		MaxSteps: artifact.MaxScenarioSteps, StopOnViolation: source.StopOnViolation, Decider: decider,
	}, nil
}

func (c Config) validate() error {
	if len(c.Members) == 0 || c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax < c.ElectionTimeoutMin || c.HeartbeatInterval <= 0 || c.HeartbeatInterval >= c.ElectionTimeoutMin || c.StorageLatency < 0 {
		return ErrInvalidConfig
	}
	members := slices.Clone(c.Members)
	slices.Sort(members)
	for index, member := range members {
		if member == "" || index > 0 && member == members[index-1] {
			return ErrInvalidConfig
		}
	}
	if c.Network.MinLatency < 0 || c.Network.MaxLatency < c.Network.MinLatency || c.Network.LossProbability < 0 || c.Network.LossProbability > 1 || c.Network.LossProbability != c.Network.LossProbability {
		return ErrInvalidConfig
	}
	if c.Application != nil {
		if err := c.Application.Validate(); err != nil {
			return fmt.Errorf("%w: application profile: %v", ErrInvalidConfig, err)
		}
	}
	return nil
}
