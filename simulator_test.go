package sim

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestSimulatorOrdersByTimeThenInsertion(t *testing.T) {
	t.Parallel()

	simulator := New()
	var got []string
	mustScheduleAt(t, simulator, 10*time.Millisecond, func(*Simulator) { got = append(got, "late") })
	mustScheduleAt(t, simulator, 5*time.Millisecond, func(s *Simulator) {
		got = append(got, "first")
		mustSchedule(t, s, 0, func(*Simulator) { got = append(got, "nested") })
	})
	mustScheduleAt(t, simulator, 5*time.Millisecond, func(*Simulator) { got = append(got, "second") })

	if count := simulator.Run(); count != 4 {
		t.Fatalf("Run() = %d, want 4", count)
	}
	want := []string{"first", "second", "nested", "late"}
	if !slicesEqual(got, want) {
		t.Fatalf("execution order = %q, want %q", got, want)
	}
	if got := simulator.Now(); got != 10*time.Millisecond {
		t.Fatalf("Now() = %s, want 10ms", got)
	}
}

func TestSimulatorCancelMaintainsHeap(t *testing.T) {
	t.Parallel()

	simulator := New()
	var got []int
	ids := make([]EventID, 10)
	for i, delay := range []time.Duration{9, 1, 8, 2, 7, 3, 6, 4, 5, 0} {
		value := i
		ids[i] = mustSchedule(t, simulator, delay, func(*Simulator) { got = append(got, value) })
	}
	for _, index := range []int{0, 3, 6, 9} {
		if !simulator.Cancel(ids[index]) {
			t.Fatalf("Cancel(%d) = false", ids[index])
		}
		if simulator.Cancel(ids[index]) {
			t.Fatalf("second Cancel(%d) = true", ids[index])
		}
	}

	simulator.Run()
	want := []int{1, 5, 7, 8, 4, 2}
	if !slicesEqual(got, want) {
		t.Fatalf("execution order = %v, want %v", got, want)
	}
}

func TestRunUntilAdvancesClockAndIncludesBoundaryWork(t *testing.T) {
	t.Parallel()

	simulator := New()
	var got []int
	mustScheduleAt(t, simulator, 3*time.Second, func(*Simulator) { got = append(got, 3) })
	mustScheduleAt(t, simulator, 5*time.Second, func(s *Simulator) {
		got = append(got, 5)
		mustSchedule(t, s, 0, func(*Simulator) { got = append(got, 50) })
	})
	mustScheduleAt(t, simulator, 6*time.Second, func(*Simulator) { got = append(got, 6) })

	count, err := simulator.RunUntil(5 * time.Second)
	if err != nil {
		t.Fatalf("RunUntil: %v", err)
	}
	if count != 3 || !slicesEqual(got, []int{3, 5, 50}) {
		t.Fatalf("RunUntil ran %d events, values %v", count, got)
	}
	if simulator.Now() != 5*time.Second || simulator.Len() != 1 {
		t.Fatalf("after RunUntil: now=%s len=%d", simulator.Now(), simulator.Len())
	}
}

func TestSimulatorValidationAndZeroValue(t *testing.T) {
	t.Parallel()

	var simulator Simulator
	id := mustSchedule(t, &simulator, 0, func(*Simulator) {})
	if id == 0 {
		t.Fatal("zero-value Simulator issued zero EventID")
	}
	if _, err := simulator.Schedule(-1, func(*Simulator) {}); !errors.Is(err, ErrNegativeTime) {
		t.Fatalf("negative Schedule error = %v", err)
	}
	if _, err := simulator.ScheduleAt(0, nil); err == nil {
		t.Fatal("ScheduleAt accepted nil action")
	}
	simulator.Run()
	if _, err := simulator.RunUntil(-1); !errors.Is(err, ErrNegativeTime) {
		t.Fatalf("negative RunUntil error = %v", err)
	}

	simulator.clock.now = time.Duration(math.MaxInt64)
	if _, err := simulator.Schedule(1, func(*Simulator) {}); !errors.Is(err, ErrTimeOverflow) {
		t.Fatalf("overflow Schedule error = %v", err)
	}
}

func TestRunStepsAndNextEventTime(t *testing.T) {
	t.Parallel()

	simulator := New()
	for range 3 {
		mustSchedule(t, simulator, time.Second, func(*Simulator) {})
	}
	if next, ok := simulator.NextEventTime(); !ok || next != time.Second {
		t.Fatalf("NextEventTime() = (%s, %t)", next, ok)
	}
	if count := simulator.RunSteps(2); count != 2 || simulator.Len() != 1 {
		t.Fatalf("RunSteps result=%d len=%d", count, simulator.Len())
	}
	simulator.Run()
	if _, ok := simulator.NextEventTime(); ok {
		t.Fatal("NextEventTime reported an event for an empty queue")
	}
}

func TestHeapPropertyUnderMixedSchedulingAndCancellation(t *testing.T) {
	t.Parallel()

	simulator := New()
	random := NewRand(0x5eed)
	type expectedEvent struct {
		id       EventID
		at       time.Duration
		canceled bool
	}
	expected := make([]expectedEvent, 5_000)
	var executed []EventID
	for index := range expected {
		at := time.Duration(random.Uint64N(250))
		var id EventID
		id = mustScheduleAt(t, simulator, at, func(*Simulator) { executed = append(executed, id) })
		expected[index] = expectedEvent{id: id, at: at}
	}
	for index := range expected {
		if random.Uint64N(4) == 0 {
			expected[index].canceled = true
			if !simulator.Cancel(expected[index].id) {
				t.Fatalf("Cancel(%d) failed", expected[index].id)
			}
		}
	}

	simulator.Run()
	var want []EventID
	for at := time.Duration(0); at < 250; at++ {
		for _, event := range expected {
			if !event.canceled && event.at == at {
				want = append(want, event.id)
			}
		}
	}
	if !slicesEqual(executed, want) {
		t.Fatal("heap did not preserve time/insertion ordering")
	}
}

func mustSchedule(t *testing.T, simulator *Simulator, delay time.Duration, action Action) EventID {
	t.Helper()
	id, err := simulator.Schedule(delay, action)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	return id
}

func mustScheduleAt(t *testing.T, simulator *Simulator, at time.Duration, action Action) EventID {
	t.Helper()
	id, err := simulator.ScheduleAt(at, action)
	if err != nil {
		t.Fatalf("ScheduleAt: %v", err)
	}
	return id
}

func slicesEqual[S ~[]E, E comparable](left, right S) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
