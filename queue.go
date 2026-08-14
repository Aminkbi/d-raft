package sim

import "time"

type scheduledEvent struct {
	id     EventID
	when   time.Duration
	order  uint64
	action Action
	index  int
}

// eventHeap is a min-heap ordered by virtual time and then insertion order.
// The explicit implementation avoids interface conversions in the hot path.
type eventHeap []*scheduledEvent

func (h eventHeap) less(i, j int) bool {
	if h[i].when != h[j].when {
		return h[i].when < h[j].when
	}
	return h[i].order < h[j].order
}

func (h eventHeap) swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *eventHeap) push(event *scheduledEvent) {
	event.index = len(*h)
	*h = append(*h, event)
	h.up(event.index)
}

func (h *eventHeap) pop() *scheduledEvent {
	return h.remove(0)
}

func (h *eventHeap) remove(index int) *scheduledEvent {
	last := len(*h) - 1
	h.swap(index, last)
	event := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	event.index = -1
	if index != last {
		if !h.down(index) {
			h.up(index)
		}
	}
	return event
}

func (h *eventHeap) up(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if !h.less(index, parent) {
			return
		}
		h.swap(index, parent)
		index = parent
	}
}

func (h *eventHeap) down(index int) bool {
	original := index
	for {
		left := 2*index + 1
		if left >= len(*h) {
			break
		}
		smallest := left
		right := left + 1
		if right < len(*h) && h.less(right, left) {
			smallest = right
		}
		if !h.less(smallest, index) {
			break
		}
		h.swap(index, smallest)
		index = smallest
	}
	return index != original
}
