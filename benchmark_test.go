package sim

import (
	"testing"
	"time"
)

func BenchmarkSimulatorScheduleAndRun(b *testing.B) {
	const eventsPerIteration = 1_000
	b.ReportAllocs()
	for b.Loop() {
		simulator := New()
		for index := range eventsPerIteration {
			if _, err := simulator.ScheduleAt(time.Duration(index%100), func(*Simulator) {}); err != nil {
				b.Fatal(err)
			}
		}
		if count := simulator.Run(); count != eventsPerIteration {
			b.Fatalf("Run() = %d", count)
		}
	}
}

func BenchmarkRandUint64(b *testing.B) {
	random := NewRand(42)
	b.ReportAllocs()
	for b.Loop() {
		random.Uint64()
	}
}

func BenchmarkRouterSendAndRun(b *testing.B) {
	const packetsPerIteration = 1_000
	b.ReportAllocs()
	for b.Loop() {
		simulator := New()
		router, err := NewRouter[int](simulator, NewRand(42), LinkConfig{
			MinLatency: time.Millisecond,
			MaxLatency: time.Millisecond,
		}, nil)
		if err != nil {
			b.Fatal(err)
		}
		if err := router.Register("a", func(Packet[int]) {}); err != nil {
			b.Fatal(err)
		}
		received := 0
		if err := router.Register("b", func(Packet[int]) { received++ }); err != nil {
			b.Fatal(err)
		}
		for packet := range packetsPerIteration {
			if _, err := router.Send("a", "b", packet); err != nil {
				b.Fatal(err)
			}
		}
		simulator.Run()
		if received != packetsPerIteration {
			b.Fatalf("received = %d", received)
		}
	}
}
