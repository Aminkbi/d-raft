package etcdraft

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/decision"
	rootraft "github.com/aminkbi/d-raft/raft"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type envelope struct {
	SenderIncarnation uint64
	SendSequence      uint64
	From              rootraft.NodeID
	To                rootraft.NodeID
	Message           *pb.Message
}

func cloneEnvelope(source envelope) envelope {
	source.Message = proto.Clone(source.Message).(*pb.Message)
	return source
}

type networkDecisions struct{ decider decision.Decider }

type networkContext struct {
	From              rootraft.NodeID `json:"from"`
	To                rootraft.NodeID `json:"to"`
	SenderIncarnation uint64          `json:"sender_incarnation"`
	SendSequence      uint64          `json:"send_sequence"`
	Message           []byte          `json:"message_protobuf"`
	MinLatencyNS      int64           `json:"min_latency_ns"`
	MaxLatencyNS      int64           `json:"max_latency_ns"`
	LossProbability   float64         `json:"loss_probability"`
}

func canonicalNetworkContext(packet sim.Packet[envelope], link sim.LinkConfig) (networkContext, error) {
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(packet.Message.Message)
	if err != nil {
		return networkContext{}, err
	}
	return networkContext{
		From: packet.Message.From, To: packet.Message.To,
		SenderIncarnation: packet.Message.SenderIncarnation, SendSequence: packet.Message.SendSequence,
		Message: wire, MinLatencyNS: int64(link.MinLatency), MaxLatencyNS: int64(link.MaxLatency), LossProbability: link.LossProbability,
	}, nil
}

func (d networkDecisions) Drop(packet sim.Packet[envelope], link sim.LinkConfig) (bool, error) {
	context, err := canonicalNetworkContext(packet, link)
	if err != nil {
		return false, err
	}
	raw, err := json.Marshal(context)
	if err != nil {
		return false, err
	}
	choice := decision.Choice{
		ID:   fmt.Sprintf("network/%s/%d/%d/loss", packet.Message.From, packet.Message.SenderIncarnation, packet.Message.SendSequence),
		Kind: decision.NetworkLoss, Options: lossOptions(link.LossProbability), Context: raw,
	}
	selection, err := d.decider.Choose(choice)
	if err != nil {
		return false, err
	}
	if err := decision.ValidateSelection(choice, selection); err != nil {
		return false, err
	}
	return selection.Option == "drop", nil
}

func (d networkDecisions) Latency(packet sim.Packet[envelope], link sim.LinkConfig) (time.Duration, error) {
	context, err := canonicalNetworkContext(packet, link)
	if err != nil {
		return 0, err
	}
	raw, err := json.Marshal(context)
	if err != nil {
		return 0, err
	}
	minimum, maximum := int64(link.MinLatency), int64(link.MaxLatency)
	choice := decision.Choice{
		ID:   fmt.Sprintf("network/%s/%d/%d/latency", packet.Message.From, packet.Message.SenderIncarnation, packet.Message.SendSequence),
		Kind: decision.NetworkLatency, Min: &minimum, Max: &maximum, Context: raw,
	}
	selection, err := d.decider.Choose(choice)
	if err != nil {
		return 0, err
	}
	if err := decision.ValidateSelection(choice, selection); err != nil {
		return 0, err
	}
	return time.Duration(*selection.Number), nil
}

func lossOptions(probability float64) []decision.Option {
	if probability <= 0 {
		return []decision.Option{{ID: "deliver", Weight: 1}}
	}
	if probability >= 1 {
		return []decision.Option{{ID: "drop", Weight: 1}}
	}
	const scale = uint64(1) << 53
	drop := uint64(math.Ceil(probability * float64(scale)))
	return []decision.Option{{ID: "drop", Weight: drop}, {ID: "deliver", Weight: scale - drop}}
}
