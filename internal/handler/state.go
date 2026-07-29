// Bounded data structures for the stateful routes: a sliding window, a dedup
// set and an alert throttle.
//
// Two rules run through all of it.
//
// Bounded, like the buffers are. Capping the channels while leaving the
// aggregation maps unbounded would simply move the exhaustion target: unlimited
// unique subject keys is a straightforward memory-exhaustion vector, and the
// subject key comes off a public firehose. Every structure here has a size cap
// and a TTL.
//
// Event time, not wall clock. After a reconnect the service replays events that
// are already minutes old. A wall-clock window would read that catch-up as a
// burst of simultaneous activity and fire a false alert on exactly the path
// that recovery exercises most.
//
// Nothing here takes a lock, because nothing here is shared: each instance
// belongs to exactly one shard, and shardSet holds the mutex that serialises
// access to it (D3). Keeping the locking one level up means these three types
// stay plain data structures that a test can drive directly.
package handler

import "container/list"

// Window counts occurrences of a key over a sliding window of event time.
type Window struct {
	windowUS int64
	maxKeys  int

	entries map[string]*list.Element
	lru     *list.List // front = most recently touched

	onEvict func(reason string)
}

type entry struct {
	key    string
	stamps []int64 // event-time microseconds, ascending
}

// NewWindow builds a window of windowUS microseconds holding at most maxKeys
// distinct keys.
func NewWindow(windowUS int64, maxKeys int, onEvict func(reason string)) *Window {
	if maxKeys <= 0 {
		maxKeys = 10000
	}
	if onEvict == nil {
		onEvict = func(string) {}
	}
	return &Window{
		windowUS: windowUS,
		maxKeys:  maxKeys,
		entries:  make(map[string]*list.Element, maxKeys/4+1),
		lru:      list.New(),
		onEvict:  onEvict,
	}
}

// Add records one occurrence of key at event time nowUS and returns the number
// of occurrences within the window, including this one.
func (w *Window) Add(key string, nowUS int64) int {
	el, ok := w.entries[key]
	if !ok {
		w.evictIfFull()
		e := &entry{key: key}
		el = w.lru.PushFront(e)
		w.entries[key] = el
	} else {
		w.lru.MoveToFront(el)
	}

	e := el.Value.(*entry)
	e.stamps = append(e.stamps, nowUS)

	// Trim anything that has fallen out of the window. Stamps are appended in
	// arrival order which is near-sorted; a linear trim from the front is right
	// for the small slices this holds.
	cutoff := nowUS - w.windowUS
	i := 0
	for i < len(e.stamps) && e.stamps[i] < cutoff {
		i++
	}
	if i > 0 {
		e.stamps = append(e.stamps[:0], e.stamps[i:]...)
	}
	return len(e.stamps)
}

// Sweep drops keys with no occurrences left inside the window. Called on a
// timer so a key that stops appearing is released rather than waiting for LRU
// pressure.
func (w *Window) Sweep(nowUS int64) {
	cutoff := nowUS - w.windowUS
	for key, el := range w.entries {
		e := el.Value.(*entry)
		if len(e.stamps) == 0 || e.stamps[len(e.stamps)-1] < cutoff {
			w.lru.Remove(el)
			delete(w.entries, key)
			w.onEvict("ttl")
		}
	}
}

func (w *Window) evictIfFull() {
	for len(w.entries) >= w.maxKeys {
		back := w.lru.Back()
		if back == nil {
			return
		}
		w.lru.Remove(back)
		delete(w.entries, back.Value.(*entry).key)
		w.onEvict("size")
	}
}

// Len is the number of live keys.
func (w *Window) Len() int { return len(w.entries) }

// Dedup suppresses events already seen, over a bounded window (D7).
//
// D2 plus the reconnect rewind guarantees duplicates even in single-process
// operation, before any scaling is involved. Beyond the window a duplicate is
// accepted and a counter is marginally wrong — a deliberate trade of exactness
// for bounded memory. The production answer is the same external store that
// holds the counters.
type Dedup struct {
	windowUS int64
	maxKeys  int
	seen     map[string]int64
	order    *list.List
	elems    map[string]*list.Element
	onEvict  func(reason string)
}

func NewDedup(windowUS int64, maxKeys int, onEvict func(reason string)) *Dedup {
	if maxKeys <= 0 {
		maxKeys = 50000
	}
	if onEvict == nil {
		onEvict = func(string) {}
	}
	return &Dedup{
		windowUS: windowUS,
		maxKeys:  maxKeys,
		seen:     make(map[string]int64, maxKeys/4+1),
		order:    list.New(),
		elems:    make(map[string]*list.Element, maxKeys/4+1),
		onEvict:  onEvict,
	}
}

// Seen records key and reports whether it had already been recorded inside the
// window.
func (d *Dedup) Seen(key string, nowUS int64) bool {
	if at, ok := d.seen[key]; ok {
		if nowUS-at <= d.windowUS {
			return true
		}
		// Stale: treat as new and refresh.
		d.remove(key)
	}

	for len(d.seen) >= d.maxKeys {
		back := d.order.Back()
		if back == nil {
			break
		}
		d.remove(back.Value.(string))
		d.onEvict("size")
	}

	d.seen[key] = nowUS
	d.elems[key] = d.order.PushFront(key)
	return false
}

func (d *Dedup) remove(key string) {
	if el, ok := d.elems[key]; ok {
		d.order.Remove(el)
		delete(d.elems, key)
	}
	delete(d.seen, key)
}

// Len is the number of remembered keys.
func (d *Dedup) Len() int { return len(d.seen) }

// Throttle allows one event per key per interval of event time.
//
// Used on alert paths: a threshold can be crossed deliberately, so an
// unthrottled alert path is a remotely triggerable flood against whatever sits
// downstream of it. It also stops a single popular subject producing one alert
// per event for as long as it stays above the threshold.
type Throttle struct {
	intervalUS int64
	last       map[string]int64
	maxKeys    int
	onEvict    func(reason string)
}

func NewThrottle(intervalUS int64, maxKeys int, onEvict func(reason string)) *Throttle {
	if maxKeys <= 0 {
		maxKeys = 10000
	}
	if onEvict == nil {
		onEvict = func(string) {}
	}
	return &Throttle{intervalUS: intervalUS, last: make(map[string]int64), maxKeys: maxKeys, onEvict: onEvict}
}

// Allow reports whether key may fire at event time nowUS.
func (t *Throttle) Allow(key string, nowUS int64) bool {
	if at, ok := t.last[key]; ok && nowUS-at < t.intervalUS {
		return false
	}
	if len(t.last) >= t.maxKeys {
		// Cheap bound: drop the whole table rather than carry a second index.
		// Over-alerting briefly after a reset is preferable to unbounded growth,
		// and this only triggers under a deliberate cardinality attack.
		t.last = make(map[string]int64, t.maxKeys/4+1)
		t.onEvict("size")
	}
	t.last[key] = nowUS
	return true
}
