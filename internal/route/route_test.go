package route

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func ev(did string) event.Event {
	return event.Event{
		Key:    event.Key{Kind: "commit", Collection: "app.bsky.feed.like", Operation: "create"},
		DID:    did,
		TimeUS: time.Now().UnixMicro(),
	}
}

// The test that actually proves the architecture.
//
// Saturate one route and assert its drop counter rises while a second route
// keeps draining normally. If this passes, the isolation claim is not just a
// diagram.
func TestIsolationUnderCongestion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A route whose handler is wedged: it never returns until released.
	release := make(chan struct{})
	var stuckHandled atomic.Int64
	stuck := New(Config{Name: "stuck", Buffer: 4, Workers: 1, Policy: PolicyDrop},
		HandlerFunc(func(ctx context.Context, _ int, _ event.Event) error {
			<-release
			stuckHandled.Add(1)
			return nil
		}), quietLog())

	// A healthy route sharing nothing with it. Buffered generously on purpose:
	// the claim under test is that the WEDGED neighbour cannot affect it, so the
	// healthy route must not be shedding for reasons of its own.
	var healthyHandled atomic.Int64
	healthy := New(Config{Name: "healthy", Buffer: 1024, Workers: 2, Policy: PolicyDrop},
		HandlerFunc(func(ctx context.Context, _ int, _ event.Event) error {
			healthyHandled.Add(1)
			return nil
		}), quietLog())

	stuck.Start(ctx)
	healthy.Start(ctx)

	// Offer far more than the wedged route can hold.
	const n = 500
	accepted, shed := 0, 0
	for i := 0; i < n; i++ {
		if stuck.Offer(ctx, ev("did:plc:a")) {
			accepted++
		} else {
			shed++
		}
		// Every offer to the congested route is interleaved with one to the
		// healthy route, exactly as the single ingest goroutine would.
		if !healthy.Offer(ctx, ev("did:plc:b")) {
			t.Fatal("healthy route shed; it is too small for this test to mean anything")
		}
	}

	if shed == 0 {
		t.Fatal("congested route never shed; the buffer bound is not doing anything")
	}
	if accepted > 8 {
		t.Errorf("congested route accepted %d events with a buffer of 4", accepted)
	}

	// The healthy route must have drained everything offered to it, entirely
	// unaffected by its neighbour being wedged.
	deadline := time.Now().Add(3 * time.Second)
	for healthyHandled.Load() < int64(n) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := healthyHandled.Load(); got != int64(n) {
		t.Errorf("healthy route handled %d/%d while its neighbour was congested", got, n)
	}

	close(release)
}

// The other half of the isolation claim.
//
// In Go an unrecovered panic in any goroutine terminates the process, which
// would take down every other route. If this test does not crash the test
// binary, the recover boundary is doing its job.
func TestPanicIsContainedToOneEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handled atomic.Int64
	r := New(Config{Name: "panicky", Buffer: 16, Workers: 1, Policy: PolicyDrop, MaxAttempts: 1},
		HandlerFunc(func(_ context.Context, _ int, e event.Event) error {
			if e.DID == "did:plc:boom" {
				var m map[string]string
				m["nil map write"] = "panics" // deliberate
			}
			handled.Add(1)
			return nil
		}), quietLog())
	r.Start(ctx)

	r.Offer(ctx, ev("did:plc:ok"))
	r.Offer(ctx, ev("did:plc:boom"))
	r.Offer(ctx, ev("did:plc:ok"))

	deadline := time.Now().Add(2 * time.Second)
	for handled.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := handled.Load(); got != 2 {
		t.Errorf("handled %d good events either side of a panic, want 2", got)
	}
}

// Retries occupy a worker, never the caller.
func TestTransientErrorsRetryAndPermanentOnesDoNot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int64
	r := New(Config{Name: "flaky", Buffer: 8, Workers: 1, Policy: PolicyDrop,
		MaxAttempts: 3, RetryBase: time.Millisecond},
		HandlerFunc(func(_ context.Context, _ int, e event.Event) error {
			attempts.Add(1)
			if e.DID == "did:plc:permanent" {
				return Permanent(errors.New("cannot parse"))
			}
			return errors.New("webhook timeout")
		}), quietLog())
	r.Start(ctx)

	r.Offer(ctx, ev("did:plc:transient"))
	time.Sleep(150 * time.Millisecond)
	if got := attempts.Load(); got != 3 {
		t.Errorf("transient error attempted %d times, want MaxAttempts=3", got)
	}

	attempts.Store(0)
	r.Offer(ctx, ev("did:plc:permanent"))
	time.Sleep(100 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Errorf("permanent error attempted %d times, want 1", got)
	}
}

// The block policy applies real backpressure — and is bounded, so one route can
// never hold the stream hostage indefinitely.
func TestBlockPolicyWaitsThenGivesUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	defer close(release)

	r := New(Config{Name: "blocking", Buffer: 1, Workers: 1, Policy: PolicyBlock,
		BlockTimeout: 80 * time.Millisecond},
		HandlerFunc(func(context.Context, int, event.Event) error {
			<-release
			return nil
		}), quietLog())
	r.Start(ctx)

	// One into the worker, one into the buffer, then the third must wait.
	r.Offer(ctx, ev("did:plc:a"))
	r.Offer(ctx, ev("did:plc:b"))

	start := time.Now()
	ok := r.Offer(ctx, ev("did:plc:c"))
	elapsed := time.Since(start)

	if ok {
		t.Error("third offer succeeded; the buffer bound was not applied")
	}
	if elapsed < 60*time.Millisecond {
		t.Errorf("block policy returned after %v; it did not actually wait", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("block policy waited %v; the timeout is not bounding it", elapsed)
	}
}

// I2, pinned by test after a review found it violated.
//
// Offer() runs on the ingest goroutine. If anything on that path touches
// ev.Record, the heaviest route's JSON parse lands on the one goroutine whose
// stall halts the entire stream — and parses attacker-controlled, arbitrarily
// nested content there too. An earlier design selected a per-worker queue by
// hashing a key from inside the record, which did exactly that.
//
// The record is handed through untouched and is decoded only by the worker.
func TestRecordIsDecodedOnlyByTheWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	body := []byte(`{"subject":{"uri":"at://did:plc:x/app.bsky.feed.post/a"},"pad":"` +
		strings.Repeat("x", 4096) + `"}`)

	seen := make(chan []byte, 1)
	r := New(Config{Name: "i2", Buffer: 8, Workers: 4, Policy: PolicyDrop},
		HandlerFunc(func(_ context.Context, _ int, e event.Event) error {
			seen <- e.Record
			return nil
		}), quietLog())
	r.Start(ctx)

	e := ev("did:plc:a")
	e.Record = body
	if !r.Offer(ctx, e) {
		t.Fatal("offer rejected")
	}

	select {
	case got := <-seen:
		if string(got) != string(body) {
			t.Error("record was altered before reaching the worker")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker never received the event")
	}

	// The structural half of the guarantee: Route holds no handler-supplied key
	// extractor, so there is nothing on the Offer path that could decode. If a
	// future change reintroduces one, this comment is the place it will be
	// argued about — and I2 says it needs arguing.
}

// Drain must finish in-flight work rather than dropping it, so a rolling deploy
// is invisible downstream.
func TestDrainFinishesBufferedWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handled atomic.Int64
	r := New(Config{Name: "draining", Buffer: 64, Workers: 2, Policy: PolicyDrop},
		HandlerFunc(func(context.Context, int, event.Event) error {
			time.Sleep(2 * time.Millisecond)
			handled.Add(1)
			return nil
		}), quietLog())
	r.Start(ctx)

	const n = 40
	for i := 0; i < n; i++ {
		r.Offer(ctx, ev("did:plc:a"))
	}
	r.Drain(3 * time.Second)

	if got := handled.Load(); got != n {
		t.Errorf("drain finished with %d/%d events handled", got, n)
	}
}
