package sim

import (
	"testing"
	"time"
)

func TestRandGoldenStream(t *testing.T) {
	t.Parallel()

	random := NewRand(0)
	want := []uint64{
		0xe220a8397b1dcdaf,
		0x6e789e6aa1b965f4,
		0x06c45d188009454f,
		0xf88bb8a8724c81ec,
	}
	for index, expected := range want {
		if got := random.Uint64(); got != expected {
			t.Fatalf("value %d = %#016x, want %#016x", index, got, expected)
		}
	}
}

func TestRandReproducibilityAndRanges(t *testing.T) {
	t.Parallel()

	left := NewRand(0xdeadbeef)
	right := NewRand(0xdeadbeef)
	for range 1_000 {
		if got, want := left.Uint64N(17), right.Uint64N(17); got != want || got >= 17 {
			t.Fatalf("Uint64N mismatch or out of range: %d, %d", got, want)
		}
	}

	for range 1_000 {
		value := left.Duration(5*time.Millisecond, 8*time.Millisecond)
		if value < 5*time.Millisecond || value > 8*time.Millisecond {
			t.Fatalf("Duration returned %s", value)
		}
	}
}

func TestRandSplitIsDeterministicAndDistinct(t *testing.T) {
	t.Parallel()

	a := NewRand(42)
	b := NewRand(42)
	a1, a2 := a.Split(), a.Split()
	b1, b2 := b.Split(), b.Split()
	if a1.Uint64() != b1.Uint64() || a2.Uint64() != b2.Uint64() {
		t.Fatal("Split streams are not reproducible")
	}
	if NewRand(42).Split().Uint64() == NewRand(42).Uint64() {
		t.Fatal("child stream unexpectedly equals parent stream")
	}
}

func TestRandChanceBoundariesDoNotConsumeStream(t *testing.T) {
	t.Parallel()

	random := NewRand(7)
	control := NewRand(7)
	if random.Chance(0) || !random.Chance(1) {
		t.Fatal("Chance boundary result is wrong")
	}
	if got, want := random.Uint64(), control.Uint64(); got != want {
		t.Fatalf("boundary Chance consumed random state: got %#x, want %#x", got, want)
	}
}

func TestRandInvalidArgumentsPanic(t *testing.T) {
	t.Parallel()

	assertPanics(t, func() { NewRand(0).Uint64N(0) })
	assertPanics(t, func() { NewRand(0).IntN(0) })
	assertPanics(t, func() { NewRand(0).Chance(-0.1) })
	assertPanics(t, func() { NewRand(0).Duration(-1, 0) })
}

func assertPanics(t *testing.T, function func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("function did not panic")
		}
	}()
	function()
}
