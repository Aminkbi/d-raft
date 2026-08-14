package sim_test

import (
	"fmt"
	"time"

	sim "github.com/aminkbi/d-raft"
)

func ExampleRouter() {
	simulation := sim.New()
	random := sim.NewRand(20260814)
	router, err := sim.NewRouter(
		simulation,
		random,
		sim.LinkConfig{MinLatency: 10 * time.Millisecond, MaxLatency: 10 * time.Millisecond},
		func(message voteRequest) voteRequest { return message },
	)
	if err != nil {
		panic(err)
	}

	must(router.Register("candidate", func(sim.Packet[voteRequest]) {}))
	must(router.Register("voter", func(packet sim.Packet[voteRequest]) {
		fmt.Printf("term=%d candidate=%s at=%s\n", packet.Message.Term, packet.From, simulation.Now())
	}))

	_, err = router.Send("candidate", "voter", voteRequest{Term: 7})
	must(err)
	simulation.Run()

	// Output:
	// term=7 candidate=candidate at=10ms
}

type voteRequest struct {
	Term uint64
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
