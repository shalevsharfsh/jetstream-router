package handler

import "testing"

// The stateful logic, where off-by-one errors hide.
func TestWindowCountsWithinTheWindow(t *testing.T) {
	w := NewWindow(60*sec, 100, nil)

	base := int64(1_000_000_000)
	for i := 1; i <= 5; i++ {
		if got := w.Add("post-a", base+int64(i)*sec); got != i {
			t.Fatalf("Add #%d returned %d, want %d", i, got, i)
		}
	}
}

// A burst an hour ago is not a burst now.
func TestWindowForgetsOldEntries(t *testing.T) {
	w := NewWindow(60*sec, 100, nil)
	base := int64(1_000_000_000)

	for i := 0; i < 10; i++ {
		w.Add("post-a", base+int64(i)*sec)
	}
	// Ten minutes later, every earlier stamp is outside the window.
	if got := w.Add("post-a", base+600*sec); got != 1 {
		t.Errorf("after the window elapsed, count = %d, want 1", got)
	}
}

// Windows are evaluated on EVENT time, not wall clock. This is what stops a
// post-reconnect replay of minutes-old events reading as a simultaneous burst
// and firing a false alert on the exact path recovery exercises most.
func TestWindowUsesEventTimeNotWallClock(t *testing.T) {
	w := NewWindow(60*sec, 100, nil)

	// Events from an hour ago, replayed now, spread across ten minutes of event
	// time. On wall clock they all arrive at once; on event time they are far
	// apart and must not accumulate.
	oldBase := int64(1_000_000_000)
	last := 0
	for i := 0; i < 10; i++ {
		last = w.Add("post-a", oldBase+int64(i)*120*sec) // 2 minutes apart
	}
	if last != 1 {
		t.Errorf("replayed events 2 minutes apart accumulated to %d in a 60s window", last)
	}
}

func TestWindowIsBoundedBySize(t *testing.T) {
	evictions := 0
	w := NewWindow(60*sec, 10, func(string) { evictions++ })

	base := int64(1_000_000_000)
	for i := 0; i < 100; i++ {
		w.Add(string(rune('a'+i%26))+string(rune('a'+i/26)), base+int64(i))
	}
	if w.Len() > 10 {
		t.Errorf("window holds %d keys with a cap of 10", w.Len())
	}
	if evictions == 0 {
		t.Error("size cap never evicted; an unbounded map is a memory-exhaustion vector")
	}
}

func TestWindowSweepReleasesStaleKeys(t *testing.T) {
	w := NewWindow(60*sec, 100, nil)
	base := int64(1_000_000_000)
	w.Add("old", base)
	w.Add("new", base+600*sec)

	w.Sweep(base + 600*sec)
	if w.Len() != 1 {
		t.Errorf("after sweep %d keys remain, want 1", w.Len())
	}
}

// D7. Duplicates are guaranteed by the cursor rewind, so this is a correctness
// requirement rather than an optimisation.
func TestDedupSuppressesWithinItsWindow(t *testing.T) {
	d := NewDedup(120*sec, 1000, nil)
	base := int64(1_000_000_000)

	if d.Seen("did|coll|rkey|create", base) {
		t.Error("first sighting reported as a duplicate")
	}
	if !d.Seen("did|coll|rkey|create", base+sec) {
		t.Error("replayed event was not suppressed")
	}
	// A different operation on the same record is a different event.
	if d.Seen("did|coll|rkey|delete", base+sec) {
		t.Error("delete of the same record was treated as a duplicate of its create")
	}
}

func TestDedupForgetsBeyondItsWindow(t *testing.T) {
	d := NewDedup(10*sec, 1000, nil)
	base := int64(1_000_000_000)
	d.Seen("k", base)
	// Beyond the window a duplicate is accepted and the counter is marginally
	// wrong. That is the deliberate trade of exactness for bounded memory.
	if d.Seen("k", base+11*sec) {
		t.Error("dedup remembered a key beyond its window; memory would be unbounded")
	}
}

func TestDedupIsBoundedBySize(t *testing.T) {
	evictions := 0
	d := NewDedup(3600*sec, 50, func(string) { evictions++ })
	base := int64(1_000_000_000)
	for i := 0; i < 500; i++ {
		d.Seen(string(rune(i))+"-key", base+int64(i))
	}
	if d.Len() > 50 {
		t.Errorf("dedup holds %d keys with a cap of 50", d.Len())
	}
	if evictions == 0 {
		t.Error("size cap never evicted")
	}
}

// Thresholds can be crossed deliberately, so an unthrottled alert path is a
// remotely triggerable flood against whatever sits downstream.
func TestThrottleAllowsOncePerInterval(t *testing.T) {
	th := NewThrottle(60*sec, 1000, nil)
	base := int64(1_000_000_000)

	if !th.Allow("subject", base) {
		t.Fatal("first alert was throttled")
	}
	for i := 1; i < 50; i++ {
		if th.Allow("subject", base+int64(i)*sec) {
			t.Fatalf("alert %d fired inside the cooldown", i)
		}
	}
	if !th.Allow("subject", base+61*sec) {
		t.Error("alert did not fire again after the cooldown elapsed")
	}
	// A different subject is unaffected.
	if !th.Allow("other", base+sec) {
		t.Error("throttle on one subject suppressed another")
	}
}

// Regression: the default route once throttled on the raw collection while
// logging a bounded bucket, so every novel lexicon produced its own log line.
// The throttle key and the logged value must be the same bounded thing.
func TestThrottleKeyMustBeTheBoundedValue(t *testing.T) {
	th := NewThrottle(60*sec, 1000, nil)
	base := int64(1_000_000_000)

	allowed := 0
	for i := 0; i < 500; i++ {
		// 500 distinct unknown collections, all mapping to one bucket.
		if th.Allow("other", base+int64(i)) {
			allowed++
		}
	}
	if allowed != 1 {
		t.Errorf("bucketed throttle allowed %d lines in one window, want 1", allowed)
	}
}
