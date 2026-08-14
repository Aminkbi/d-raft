package sim

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

var (
	ErrNilSimulator           = errors.New("sim: router requires a simulator")
	ErrNilRand                = errors.New("sim: router requires a random source")
	ErrEmptyNode              = errors.New("sim: node identifier must not be empty")
	ErrNilHandler             = errors.New("sim: node handler must not be nil")
	ErrDuplicateNode          = errors.New("sim: node is already registered")
	ErrUnknownSource          = errors.New("sim: packet source is not registered")
	ErrUnknownTarget          = errors.New("sim: packet target is not registered")
	ErrInvalidLink            = errors.New("sim: invalid network link configuration")
	ErrInvalidMatrix          = errors.New("sim: invalid partition matrix")
	ErrPacketExhausted        = errors.New("sim: packet identifier space exhausted")
	ErrInvalidNetworkDecision = errors.New("sim: invalid network decision")
)

// NodeID identifies a simulated network endpoint.
type NodeID string

type route struct {
	from NodeID
	to   NodeID
}

// LinkConfig describes a directed network link. Latency is sampled uniformly
// in nanoseconds from the inclusive range [MinLatency, MaxLatency].
type LinkConfig struct {
	MinLatency      time.Duration
	MaxLatency      time.Duration
	LossProbability float64
}

// Validate verifies that a link configuration is usable.
func (c LinkConfig) Validate() error {
	if c.MinLatency < 0 || c.MaxLatency < c.MinLatency {
		return fmt.Errorf("%w: latency range [%s, %s]", ErrInvalidLink, c.MinLatency, c.MaxLatency)
	}
	if c.LossProbability < 0 || c.LossProbability > 1 || c.LossProbability != c.LossProbability {
		return fmt.Errorf("%w: loss probability %v", ErrInvalidLink, c.LossProbability)
	}
	return nil
}

// PartitionMatrix is an immutable directed connectivity matrix. A true cell
// permits traffic; false blocks it. Nodes absent from a matrix are isolated
// while that matrix is active.
type PartitionMatrix struct {
	nodes   []NodeID
	allowed map[route]struct{}
}

// NewPartitionMatrix builds a matrix whose row and column order is nodes.
// allowed must be a square len(nodes)-by-len(nodes) matrix.
func NewPartitionMatrix(nodes []NodeID, allowed [][]bool) (*PartitionMatrix, error) {
	if len(nodes) == 0 || len(allowed) != len(nodes) {
		return nil, fmt.Errorf("%w: dimensions do not match", ErrInvalidMatrix)
	}
	seen := make(map[NodeID]struct{}, len(nodes))
	for _, node := range nodes {
		if node == "" {
			return nil, fmt.Errorf("%w: empty node identifier", ErrInvalidMatrix)
		}
		if _, exists := seen[node]; exists {
			return nil, fmt.Errorf("%w: duplicate node %q", ErrInvalidMatrix, node)
		}
		seen[node] = struct{}{}
	}

	matrix := &PartitionMatrix{
		nodes:   slices.Clone(nodes),
		allowed: make(map[route]struct{}, len(nodes)*len(nodes)),
	}
	for row := range allowed {
		if len(allowed[row]) != len(nodes) {
			return nil, fmt.Errorf("%w: row %d has length %d, want %d", ErrInvalidMatrix, row, len(allowed[row]), len(nodes))
		}
		for column, permits := range allowed[row] {
			if permits {
				matrix.allowed[route{from: nodes[row], to: nodes[column]}] = struct{}{}
			}
		}
	}
	return matrix, nil
}

// NewPartitions constructs a symmetric matrix from disjoint connectivity
// groups. Nodes in the same group can communicate in both directions; nodes
// in different groups cannot. Every node must occur exactly once.
func NewPartitions(groups ...[]NodeID) (*PartitionMatrix, error) {
	if len(groups) == 0 {
		return nil, fmt.Errorf("%w: no groups", ErrInvalidMatrix)
	}
	matrix := &PartitionMatrix{allowed: make(map[route]struct{})}
	seen := make(map[NodeID]struct{})
	for groupIndex, group := range groups {
		if len(group) == 0 {
			return nil, fmt.Errorf("%w: group %d is empty", ErrInvalidMatrix, groupIndex)
		}
		for _, node := range group {
			if node == "" {
				return nil, fmt.Errorf("%w: empty node identifier", ErrInvalidMatrix)
			}
			if _, exists := seen[node]; exists {
				return nil, fmt.Errorf("%w: duplicate node %q", ErrInvalidMatrix, node)
			}
			seen[node] = struct{}{}
			matrix.nodes = append(matrix.nodes, node)
		}
		for _, from := range group {
			for _, to := range group {
				matrix.allowed[route{from: from, to: to}] = struct{}{}
			}
		}
	}
	return matrix, nil
}

// Allows reports whether traffic from source to target is permitted.
func (m *PartitionMatrix) Allows(source, target NodeID) bool {
	if m == nil {
		return true
	}
	_, ok := m.allowed[route{from: source, to: target}]
	return ok
}

// Nodes returns a copy of the matrix's node order.
func (m *PartitionMatrix) Nodes() []NodeID {
	if m == nil {
		return nil
	}
	return slices.Clone(m.nodes)
}

// Packet is a message in transit. Packet values delivered to handlers and
// observers must be treated as read-only.
type Packet[M any] struct {
	ID      uint64
	From    NodeID
	To      NodeID
	Message M
	SentAt  time.Duration
}

// CloneFunc snapshots a message when Send is called. A nil CloneFunc performs
// assignment, which is sufficient for immutable messages and value types.
type CloneFunc[M any] func(M) M

// Handler receives packets synchronously on the simulator's event loop.
type Handler[M any] func(Packet[M])

// NetworkEventKind describes an observable router transition.
type NetworkEventKind uint8

const (
	PacketScheduled NetworkEventKind = iota + 1
	PacketDelivered
	PacketDropped
)

// DropReason explains why a packet was discarded.
type DropReason uint8

const (
	NotDropped DropReason = iota
	DropLoss
	DropPartition
	DropTargetUnavailable
)

// NetworkEvent is emitted synchronously by a Router. DeliveryAt is set for a
// scheduled delivery and for a packet dropped when delivery became due.
// Reason is set for a drop, and WasInFlight distinguishes a delivery-time drop
// from one decided by Send (including when virtual time is zero).
type NetworkEvent[M any] struct {
	Kind        NetworkEventKind
	Packet      Packet[M]
	At          time.Duration
	DeliveryAt  time.Duration
	Reason      DropReason
	WasInFlight bool
}

// Observer sees packet lifecycle transitions. It runs inline and may affect
// scheduling order, so observers used for tracing should avoid side effects.
type Observer[M any] func(NetworkEvent[M])

// SendResult reports the immediate outcome of Send.
type SendResult struct {
	PacketID   uint64
	Scheduled  bool
	DeliveryAt time.Duration
	DropReason DropReason
}

// NetworkDecisionSource replaces Router's random loss and latency draws with
// semantic decisions suitable for recording, replay, and exploration.
type NetworkDecisionSource[M any] interface {
	Drop(Packet[M], LinkConfig) (bool, error)
	Latency(Packet[M], LinkConfig) (time.Duration, error)
}

// Router is a deterministic, directed, in-memory network. It performs all
// work through its Simulator and never starts goroutines.
type Router[M any] struct {
	sim         *Simulator
	rng         *Rand
	defaultLink LinkConfig
	links       map[route]LinkConfig
	handlers    map[NodeID]Handler[M]
	partition   *PartitionMatrix
	clone       CloneFunc[M]
	observer    Observer[M]
	nextPacket  uint64
	exhausted   bool
	trace       TraceSink
	decisions   NetworkDecisionSource[M]
}

// NewRouter constructs a router. Passing nil for clone uses ordinary Go
// assignment; provide a clone function for slices, maps, pointers, or other
// mutable messages that senders might modify after Send returns.
func NewRouter[M any](simulator *Simulator, random *Rand, defaultLink LinkConfig, clone CloneFunc[M]) (*Router[M], error) {
	if simulator == nil {
		return nil, ErrNilSimulator
	}
	if random == nil {
		return nil, ErrNilRand
	}
	if err := defaultLink.Validate(); err != nil {
		return nil, err
	}
	return &Router[M]{
		sim:         simulator,
		rng:         random,
		defaultLink: defaultLink,
		links:       make(map[route]LinkConfig),
		handlers:    make(map[NodeID]Handler[M]),
		clone:       clone,
		nextPacket:  1,
	}, nil
}

// Register adds a node. Duplicate registration returns ErrDuplicateNode.
func (r *Router[M]) Register(node NodeID, handler Handler[M]) error {
	if node == "" {
		return ErrEmptyNode
	}
	if handler == nil {
		return ErrNilHandler
	}
	if _, exists := r.handlers[node]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateNode, node)
	}
	r.handlers[node] = handler
	if r.trace != nil {
		r.recordTrace(TraceEvent{Kind: TraceNodeRegistered, AtNS: traceTime(r.sim.Now()), Node: node})
	}
	return nil
}

// Unregister removes a node and reports whether it existed. Packets already
// in flight to it will be dropped at delivery time.
func (r *Router[M]) Unregister(node NodeID) bool {
	if _, exists := r.handlers[node]; !exists {
		return false
	}
	delete(r.handlers, node)
	if r.trace != nil {
		r.recordTrace(TraceEvent{Kind: TraceNodeUnregistered, AtNS: traceTime(r.sim.Now()), Node: node})
	}
	return true
}

// SetTraceSink sets the synchronous machine-readable trace destination.
// Passing nil disables tracing. Use the same sink for the simulator, random
// source, and router to obtain one globally ordered execution trace.
func (r *Router[M]) SetTraceSink(sink TraceSink) {
	r.trace = sink
}

// SetDecisionSource replaces random network choices. Passing nil restores the
// Router's Rand-based behavior.
func (r *Router[M]) SetDecisionSource(source NetworkDecisionSource[M]) {
	r.decisions = source
}

// SetObserver sets the lifecycle observer. Passing nil disables observation.
func (r *Router[M]) SetObserver(observer Observer[M]) {
	r.observer = observer
}

// SetLink overrides the default configuration for a directed link.
func (r *Router[M]) SetLink(from, to NodeID, config LinkConfig) error {
	if from == "" || to == "" {
		return ErrEmptyNode
	}
	if err := config.Validate(); err != nil {
		return err
	}
	r.links[route{from: from, to: to}] = config
	if r.trace != nil {
		r.recordTrace(TraceEvent{
			Kind: TraceLinkSet,
			AtNS: traceTime(r.sim.Now()),
			From: from,
			To:   to,
			Link: traceLinkConfig(config),
		})
	}
	return nil
}

// ResetLink removes a directed-link override.
func (r *Router[M]) ResetLink(from, to NodeID) {
	delete(r.links, route{from: from, to: to})
	if r.trace != nil {
		r.recordTrace(TraceEvent{Kind: TraceLinkReset, AtNS: traceTime(r.sim.Now()), From: from, To: to})
	}
}

// SetPartition activates a connectivity matrix. Passing nil restores full
// connectivity. The matrix is checked both when a packet is sent and when it
// is due for delivery, so a new partition can cut packets already in flight.
func (r *Router[M]) SetPartition(matrix *PartitionMatrix) {
	r.partition = matrix
	if r.trace == nil {
		return
	}
	event := TraceEvent{
		Kind:            TracePartitionChanged,
		AtNS:            traceTime(r.sim.Now()),
		PartitionActive: traceBool(matrix != nil),
	}
	if matrix != nil {
		event.PartitionNodes = matrix.Nodes()
		for _, from := range matrix.nodes {
			for _, to := range matrix.nodes {
				if matrix.Allows(from, to) {
					event.PartitionAllowed = append(event.PartitionAllowed, TraceRoute{From: from, To: to})
				}
			}
		}
	}
	r.recordTrace(event)
}

// Send submits a packet to the simulated network.
func (r *Router[M]) Send(from, to NodeID, message M) (SendResult, error) {
	if _, exists := r.handlers[from]; !exists {
		return SendResult{}, fmt.Errorf("%w: %q", ErrUnknownSource, from)
	}
	if _, exists := r.handlers[to]; !exists {
		return SendResult{}, fmt.Errorf("%w: %q", ErrUnknownTarget, to)
	}
	if r.exhausted {
		return SendResult{}, ErrPacketExhausted
	}

	packetID := r.nextPacket
	if packetID == ^uint64(0) {
		r.exhausted = true
	} else {
		r.nextPacket++
	}
	if r.clone != nil {
		message = r.clone(message)
	}
	packet := Packet[M]{ID: packetID, From: from, To: to, Message: message, SentAt: r.sim.Now()}

	if !r.partition.Allows(from, to) {
		return r.drop(packet, DropPartition, 0, false), nil
	}

	link, exists := r.links[route{from: from, to: to}]
	if !exists {
		link = r.defaultLink
	}
	dropped := false
	if r.decisions != nil {
		var err error
		dropped, err = r.decisions.Drop(packet, link)
		if err != nil {
			return SendResult{}, err
		}
		if (dropped && link.LossProbability == 0) || (!dropped && link.LossProbability == 1) {
			return SendResult{}, fmt.Errorf("%w: loss decision conflicts with probability %g", ErrInvalidNetworkDecision, link.LossProbability)
		}
	} else {
		dropped = r.rng.Chance(link.LossProbability)
	}
	if dropped {
		return r.drop(packet, DropLoss, 0, false), nil
	}

	var latency time.Duration
	if r.decisions != nil {
		var err error
		latency, err = r.decisions.Latency(packet, link)
		if err != nil {
			return SendResult{}, err
		}
		if latency < link.MinLatency || latency > link.MaxLatency {
			return SendResult{}, fmt.Errorf("%w: latency %s outside [%s, %s]", ErrInvalidNetworkDecision, latency, link.MinLatency, link.MaxLatency)
		}
	} else {
		latency = r.rng.Duration(link.MinLatency, link.MaxLatency)
	}
	var deliveryAt time.Duration
	_, err := r.sim.Schedule(latency, func(_ *Simulator) {
		r.deliver(packet, deliveryAt)
	})
	if err != nil {
		return SendResult{}, err
	}
	deliveryAt = r.sim.Now() + latency // Safe because Schedule checked overflow.
	result := SendResult{PacketID: packetID, Scheduled: true, DeliveryAt: deliveryAt}
	r.observe(NetworkEvent[M]{Kind: PacketScheduled, Packet: packet, At: r.sim.Now(), DeliveryAt: deliveryAt})
	return result, nil
}

func (r *Router[M]) deliver(packet Packet[M], deliveryAt time.Duration) {
	if !r.partition.Allows(packet.From, packet.To) {
		r.drop(packet, DropPartition, deliveryAt, true)
		return
	}
	handler, exists := r.handlers[packet.To]
	if !exists {
		r.drop(packet, DropTargetUnavailable, deliveryAt, true)
		return
	}
	r.observe(NetworkEvent[M]{Kind: PacketDelivered, Packet: packet, At: r.sim.Now(), DeliveryAt: deliveryAt})
	handler(packet)
}

func (r *Router[M]) drop(packet Packet[M], reason DropReason, deliveryAt time.Duration, wasInFlight bool) SendResult {
	r.observe(NetworkEvent[M]{
		Kind:        PacketDropped,
		Packet:      packet,
		At:          r.sim.Now(),
		DeliveryAt:  deliveryAt,
		Reason:      reason,
		WasInFlight: wasInFlight,
	})
	return SendResult{PacketID: packet.ID, DropReason: reason}
}

func (r *Router[M]) observe(event NetworkEvent[M]) {
	if r.trace != nil {
		traceEvent := TraceEvent{
			AtNS:     traceTime(event.At),
			PacketID: event.Packet.ID,
			From:     event.Packet.From,
			To:       event.Packet.To,
			Message:  event.Packet.Message,
		}
		switch event.Kind {
		case PacketScheduled:
			traceEvent.Kind = TracePacketScheduled
			traceEvent.DeliveryAtNS = traceTime(event.DeliveryAt)
		case PacketDelivered:
			traceEvent.Kind = TracePacketDelivered
			traceEvent.DeliveryAtNS = traceTime(event.DeliveryAt)
		case PacketDropped:
			traceEvent.Kind = TracePacketDropped
			traceEvent.DropReason = traceDropReason(event.Reason)
			if event.WasInFlight {
				traceEvent.DeliveryAtNS = traceTime(event.DeliveryAt)
			}
		}
		r.recordTrace(traceEvent)
	}
	if r.observer != nil {
		r.observer(event)
	}
}

func (r *Router[M]) recordTrace(event TraceEvent) {
	if r.trace != nil {
		r.trace.RecordTrace(event)
	}
}

func traceLinkConfig(config LinkConfig) *TraceLinkConfig {
	return &TraceLinkConfig{
		MinLatencyNS:    int64(config.MinLatency),
		MaxLatencyNS:    int64(config.MaxLatency),
		LossProbability: config.LossProbability,
	}
}

func traceDropReason(reason DropReason) string {
	switch reason {
	case DropLoss:
		return "loss"
	case DropPartition:
		return "partition"
	case DropTargetUnavailable:
		return "target_unavailable"
	default:
		return ""
	}
}

// CloneBytes returns an independent copy of a byte slice and is suitable as a
// Router clone function.
func CloneBytes(message []byte) []byte {
	return slices.Clone(message)
}
