package sim

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestRouterSchedulesAndDeliversInOrder(t *testing.T) {
	t.Parallel()

	simulator := New()
	router := mustRouter[string](t, simulator, NewRand(1), LinkConfig{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 5 * time.Millisecond,
	}, nil)
	register(t, router, "a", func(Packet[string]) {})
	var received []string
	register(t, router, "b", func(packet Packet[string]) {
		received = append(received, fmt.Sprintf("%s@%s", packet.Message, simulator.Now()))
	})

	first, err := router.Send("a", "b", "one")
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	second, err := router.Send("a", "b", "two")
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !first.Scheduled || first.DeliveryAt != 5*time.Millisecond || second.PacketID != first.PacketID+1 {
		t.Fatalf("unexpected results: first=%+v second=%+v", first, second)
	}

	simulator.Run()
	want := []string{"one@5ms", "two@5ms"}
	if !slicesEqual(received, want) {
		t.Fatalf("received = %v, want %v", received, want)
	}
}

func TestRouterClonesMutableMessages(t *testing.T) {
	t.Parallel()

	simulator := New()
	router := mustRouter[[]byte](t, simulator, NewRand(2), LinkConfig{MinLatency: 1, MaxLatency: 1}, CloneBytes)
	register(t, router, "a", func(Packet[[]byte]) {})
	var received string
	register(t, router, "b", func(packet Packet[[]byte]) { received = string(packet.Message) })

	message := []byte("vote")
	if _, err := router.Send("a", "b", message); err != nil {
		t.Fatalf("Send: %v", err)
	}
	message[0] = 'n'
	simulator.Run()
	if received != "vote" {
		t.Fatalf("received %q, want snapshot %q", received, "vote")
	}
}

func TestRouterLossAndObserver(t *testing.T) {
	t.Parallel()

	simulator := New()
	router := mustRouter[int](t, simulator, NewRand(3), LinkConfig{LossProbability: 1}, nil)
	register(t, router, "a", func(Packet[int]) {})
	register(t, router, "b", func(Packet[int]) { t.Fatal("lost packet was delivered") })
	var events []NetworkEvent[int]
	router.SetObserver(func(event NetworkEvent[int]) { events = append(events, event) })

	result, err := router.Send("a", "b", 42)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.Scheduled || result.DropReason != DropLoss || simulator.Len() != 0 {
		t.Fatalf("loss result = %+v, queue len = %d", result, simulator.Len())
	}
	if len(events) != 1 || events[0].Kind != PacketDropped || events[0].Reason != DropLoss {
		t.Fatalf("events = %+v", events)
	}
}

func TestRouterPartitionsAtSendAndDelivery(t *testing.T) {
	t.Parallel()

	simulator := New()
	router := mustRouter[string](t, simulator, NewRand(4), LinkConfig{
		MinLatency: 10 * time.Millisecond,
		MaxLatency: 10 * time.Millisecond,
	}, nil)
	register(t, router, "a", func(Packet[string]) {})
	var received []string
	register(t, router, "b", func(packet Packet[string]) { received = append(received, packet.Message) })
	register(t, router, "c", func(Packet[string]) {})
	partition, err := NewPartitions([]NodeID{"a", "c"}, []NodeID{"b"})
	if err != nil {
		t.Fatalf("NewPartitions: %v", err)
	}

	router.SetPartition(partition)
	blocked, err := router.Send("a", "b", "blocked-at-send")
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if blocked.DropReason != DropPartition {
		t.Fatalf("blocked result = %+v", blocked)
	}

	router.SetPartition(nil)
	if _, err := router.Send("a", "b", "in-flight"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	router.SetPartition(partition)
	simulator.Run()
	if len(received) != 0 {
		t.Fatalf("received across partition: %v", received)
	}

	router.SetPartition(nil)
	if _, err := router.Send("a", "b", "healed"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	simulator.Run()
	if !slicesEqual(received, []string{"healed"}) {
		t.Fatalf("received after heal = %v", received)
	}
}

func TestDirectedPartitionMatrixAndLinkOverride(t *testing.T) {
	t.Parallel()

	matrix, err := NewPartitionMatrix(
		[]NodeID{"a", "b"},
		[][]bool{{true, true}, {false, true}},
	)
	if err != nil {
		t.Fatalf("NewPartitionMatrix: %v", err)
	}
	if !matrix.Allows("a", "b") || matrix.Allows("b", "a") || matrix.Allows("a", "missing") {
		t.Fatal("matrix directionality is wrong")
	}

	simulator := New()
	router := mustRouter[int](t, simulator, NewRand(5), LinkConfig{MinLatency: time.Second, MaxLatency: time.Second}, nil)
	register(t, router, "a", func(Packet[int]) {})
	register(t, router, "b", func(Packet[int]) {})
	router.SetPartition(matrix)
	if err := router.SetLink("a", "b", LinkConfig{MinLatency: 2 * time.Second, MaxLatency: 2 * time.Second}); err != nil {
		t.Fatalf("SetLink: %v", err)
	}
	result, err := router.Send("a", "b", 1)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.DeliveryAt != 2*time.Second {
		t.Fatalf("DeliveryAt = %s, want 2s", result.DeliveryAt)
	}
	blocked, err := router.Send("b", "a", 2)
	if err != nil || blocked.DropReason != DropPartition {
		t.Fatalf("reverse Send = %+v, %v", blocked, err)
	}
}

func TestRouterDropsWhenTargetUnregisters(t *testing.T) {
	t.Parallel()

	simulator := New()
	router := mustRouter[int](t, simulator, NewRand(6), LinkConfig{MinLatency: 1, MaxLatency: 1}, nil)
	register(t, router, "a", func(Packet[int]) {})
	register(t, router, "b", func(Packet[int]) { t.Fatal("unregistered target received packet") })
	var reason DropReason
	router.SetObserver(func(event NetworkEvent[int]) {
		if event.Kind == PacketDropped {
			reason = event.Reason
			if event.DeliveryAt != 1 {
				t.Errorf("drop DeliveryAt = %s, want 1ns", event.DeliveryAt)
			}
		}
	})
	if _, err := router.Send("a", "b", 1); err != nil {
		t.Fatalf("Send: %v", err)
	}
	router.Unregister("b")
	simulator.Run()
	if reason != DropTargetUnavailable {
		t.Fatalf("drop reason = %v, want %v", reason, DropTargetUnavailable)
	}
}

func TestRouterSeedReproducesNetworkTrace(t *testing.T) {
	t.Parallel()

	trace := func(seed uint64) []string {
		simulator := New()
		router := mustRouter[int](t, simulator, NewRand(seed), LinkConfig{
			MinLatency:      time.Millisecond,
			MaxLatency:      10 * time.Millisecond,
			LossProbability: 0.25,
		}, nil)
		register(t, router, "a", func(Packet[int]) {})
		register(t, router, "b", func(Packet[int]) {})
		var result []string
		router.SetObserver(func(event NetworkEvent[int]) {
			result = append(result, fmt.Sprintf("%d/%d/%d/%s", event.Kind, event.Packet.ID, event.Reason, event.DeliveryAt))
		})
		for value := range 100 {
			if _, err := router.Send("a", "b", value); err != nil {
				t.Fatalf("Send: %v", err)
			}
		}
		simulator.Run()
		return result
	}

	left, right := trace(123456), trace(123456)
	if !slicesEqual(left, right) {
		t.Fatal("equal seeds produced different traces")
	}
	if slicesEqual(left, trace(123457)) {
		t.Fatal("distinct seeds produced equal traces")
	}
}

func TestRouterValidation(t *testing.T) {
	t.Parallel()

	if _, err := NewRouter[int](nil, NewRand(0), LinkConfig{}, nil); !errors.Is(err, ErrNilSimulator) {
		t.Fatalf("nil simulator error = %v", err)
	}
	if _, err := NewRouter[int](New(), nil, LinkConfig{}, nil); !errors.Is(err, ErrNilRand) {
		t.Fatalf("nil random error = %v", err)
	}
	if _, err := NewRouter[int](New(), NewRand(0), LinkConfig{LossProbability: 2}, nil); !errors.Is(err, ErrInvalidLink) {
		t.Fatalf("invalid link error = %v", err)
	}
	if _, err := NewPartitionMatrix([]NodeID{"a"}, [][]bool{}); !errors.Is(err, ErrInvalidMatrix) {
		t.Fatalf("invalid matrix error = %v", err)
	}

	router := mustRouter[int](t, New(), NewRand(0), LinkConfig{}, nil)
	if err := router.Register("", func(Packet[int]) {}); !errors.Is(err, ErrEmptyNode) {
		t.Fatalf("empty node error = %v", err)
	}
	register(t, router, "a", func(Packet[int]) {})
	if _, err := router.Send("a", "missing", 1); !errors.Is(err, ErrUnknownTarget) {
		t.Fatalf("unknown target error = %v", err)
	}
}

func TestRouterRejectsVirtualTimeOverflow(t *testing.T) {
	t.Parallel()

	simulator := New()
	simulator.clock.now = time.Duration(^uint64(0) >> 1)
	router := mustRouter[int](t, simulator, NewRand(0), LinkConfig{MinLatency: 1, MaxLatency: 1}, nil)
	register(t, router, "a", func(Packet[int]) {})
	register(t, router, "b", func(Packet[int]) {})
	if _, err := router.Send("a", "b", 1); !errors.Is(err, ErrTimeOverflow) {
		t.Fatalf("Send overflow error = %v", err)
	}
}

func mustRouter[M any](t *testing.T, simulator *Simulator, random *Rand, config LinkConfig, clone CloneFunc[M]) *Router[M] {
	t.Helper()
	router, err := NewRouter(simulator, random, config, clone)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return router
}

func register[M any](t *testing.T, router *Router[M], node NodeID, handler Handler[M]) {
	t.Helper()
	if err := router.Register(node, handler); err != nil {
		t.Fatalf("Register(%q): %v", node, err)
	}
}
