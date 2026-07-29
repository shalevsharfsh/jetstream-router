// Package route is the isolation domain.
//
// One route is a bounded channel plus a dedicated worker pool, with its own
// buffer size, concurrency, full-buffer policy, retry budget and metrics.
// Nothing crosses the boundary between two routes: they share no channel, no
// state and no worker. That is what makes "a stalled route fills its own buffer
// and nothing else's" true rather than aspirational.
//
// The honest limit of that claim, stated here because it is easy to overclaim:
// isolation is BETWEEN routes, not WITHIN one. All routes live in one process,
// so they share a scheduler, a heap and a garbage collector. D6's per-event
// recover() is what stops that shared fate from being fatal — without it a
// single panic in any handler terminates the process and takes every other
// route with it, which would make the isolation claim false in the code even
// though it is true in the diagram.
package route

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
	"github.com/shalevsharfsh/jetstream-router/internal/obs"
)

// Policy decides what happens when a route's buffer is full.
type Policy string

const (
	// PolicyDrop sheds the event and counts it. The default, and the reason the
	// ingest goroutine never blocks (D1).
	PolicyDrop Policy = "drop"
	// PolicyBlock applies real backpressure by making the ingest goroutine wait.
	//
	// Available deliberately, but understand what it costs: there is exactly one
	// reader, so blocking one route stalls EVERY route. It is only safe on a
	// low-volume route where congestion is implausible. If a route is both
	// critical and busy, the honest answer is a durable broker for that route,
	// not this flag.
	PolicyBlock Policy = "block"
)

// ErrPermanent marks an error that will never succeed on retry. Returning it
// sends the event straight to the dead-letter path instead of burning the
// retry budget on something structurally broken.
var ErrPermanent = errors.New("permanent")

// Permanent wraps err so the runner treats it as non-retryable.
func Permanent(err error) error { return fmt.Errorf("%w: %w", ErrPermanent, err) }

// Handler is the per-type work. It knows nothing about channels, retries or
// any other route — which is what keeps the interesting logic unit-testable
// with no infrastructure at all.
type Handler interface {
	// Handle processes one event. Returning an error retries it unless the
	// error wraps ErrPermanent. Shard is the index of the worker within the
	// pool, so a stateful handler can own a slice of the keyspace exclusively.
	Handle(ctx context.Context, shard int, ev event.Event) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, shard int, ev event.Event) error

func (f HandlerFunc) Handle(ctx context.Context, shard int, ev event.Event) error {
	return f(ctx, shard, ev)
}

// Config is one route's tuning. Everything an operator might want to change
// during an incident lives here.
type Config struct {
	Name        string        `json:"name"`
	Buffer      int           `json:"buffer"`
	Workers     int           `json:"workers"`
	Policy      Policy        `json:"policy"`
	MaxAttempts int           `json:"max_attempts"`
	RetryBase   time.Duration `json:"-"`
	// BlockTimeout caps how long the ingest goroutine will wait on a block-policy
	// route. Without it, "block" is an unbounded stall of the entire stream.
	BlockTimeout time.Duration `json:"-"`
}

// UnmarshalJSON accepts durations as strings ("2s"), because this struct is
// written by a human in a ConfigMap and 2000000000 is not a number anyone should
// have to type or review.
func (c *Config) UnmarshalJSON(b []byte) error {
	type alias Config // avoid recursing into this method
	wire := struct {
		*alias
		RetryBase    string `json:"retry_base"`
		BlockTimeout string `json:"block_timeout"`
	}{alias: (*alias)(c)}

	if err := json.Unmarshal(b, &wire); err != nil {
		return err
	}
	for _, f := range []struct {
		raw  string
		name string
		dst  *time.Duration
	}{
		{wire.RetryBase, "retry_base", &c.RetryBase},
		{wire.BlockTimeout, "block_timeout", &c.BlockTimeout},
	} {
		if f.raw == "" {
			continue
		}
		d, err := time.ParseDuration(f.raw)
		if err != nil {
			return fmt.Errorf("route %s: %s: %w", c.Name, f.name, err)
		}
		*f.dst = d
	}
	return nil
}

func (c Config) withDefaults() Config {
	if c.Buffer <= 0 {
		c.Buffer = 1024
	}
	if c.Workers <= 0 {
		c.Workers = 1
	}
	if c.Policy == "" {
		c.Policy = PolicyDrop
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.RetryBase <= 0 {
		c.RetryBase = 100 * time.Millisecond
	}
	if c.BlockTimeout <= 0 {
		c.BlockTimeout = 2 * time.Second
	}
	return c
}

// Route owns a buffer and the pool that drains it.
type Route struct {
	cfg     Config
	handler Handler
	log     *slog.Logger

	// One bounded channel for the route, drained by every worker in its pool.
	//
	// Sharding deliberately does NOT happen here. Selecting a per-worker queue
	// would require the shard key, and for the stateful routes that key lives
	// inside commit.record — so computing it here would decode the record body
	// on the single ingest goroutine, which is exactly what I2 forbids and what
	// D1 opens by objecting to. Handlers shard their own state instead, after
	// decoding on their own goroutine. See handler.shardSet.
	queue chan event.Event
	wg    sync.WaitGroup
}

// New builds a route. It does not start the workers; call Start.
func New(cfg Config, h Handler, log *slog.Logger) *Route {
	cfg = cfg.withDefaults()

	r := &Route{
		cfg:     cfg,
		handler: h,
		log:     log.With("route", cfg.Name),
		queue:   make(chan event.Event, cfg.Buffer),
	}
	obs.QueueCapacity.WithLabelValues(cfg.Name).Set(float64(cfg.Buffer))

	// Materialise the per-route counters at zero so they exist before anything
	// happens to them. A Prometheus labelled counter is absent until first
	// incremented, which makes "no drops" and "no such route" look identical on
	// a dashboard and in the README's own grep — and absent is the reading you
	// least want to get wrong about a drop counter.
	obs.EventsRouted.WithLabelValues(cfg.Name).Add(0)
	obs.EventsDropped.WithLabelValues(cfg.Name).Add(0)
	obs.HandlerPanics.WithLabelValues(cfg.Name).Add(0)
	obs.QueueDepth.WithLabelValues(cfg.Name).Set(0)

	return r
}

func (r *Route) Name() string { return r.cfg.Name }

// Depth is the number of events currently buffered.
func (r *Route) Depth() int { return len(r.queue) }

// Offer hands an event to the route and reports whether it was accepted.
//
// This is D1. On the drop policy the send is non-blocking: if the buffer is
// full the event is shed and counted, and the ingest goroutine carries on
// immediately. Blocking here would stall the single reader and therefore every
// other route — head-of-line blocking reintroduced through the back door, with
// the slowest consumer setting the throughput of the whole system.
//
// The cost is that shedding is irreversible: the cursor advances on enqueue
// (D2), so a dropped event is never replayed and the counter is the only record
// that it existed.
func (r *Route) Offer(ctx context.Context, ev event.Event) bool {
	q := r.queue

	if r.cfg.Policy == PolicyBlock {
		start := time.Now()
		timer := time.NewTimer(r.cfg.BlockTimeout)
		defer timer.Stop()
		select {
		case q <- ev:
			obs.EventsBlocked.WithLabelValues(r.cfg.Name).Add(time.Since(start).Seconds())
			obs.EventsRouted.WithLabelValues(r.cfg.Name).Inc()
			obs.QueueDepth.WithLabelValues(r.cfg.Name).Set(float64(r.Depth()))
			return true
		case <-timer.C:
			// Even "block" is bounded. An unbounded wait would let one route
			// hold the entire stream hostage indefinitely.
			obs.EventsBlocked.WithLabelValues(r.cfg.Name).Add(time.Since(start).Seconds())
			obs.EventsDropped.WithLabelValues(r.cfg.Name).Inc()
			return false
		case <-ctx.Done():
			return false
		}
	}

	select {
	case q <- ev:
		obs.EventsRouted.WithLabelValues(r.cfg.Name).Inc()
		obs.QueueDepth.WithLabelValues(r.cfg.Name).Set(float64(r.Depth()))
		return true
	default:
		obs.EventsDropped.WithLabelValues(r.cfg.Name).Inc()
		return false
	}
}

// Start launches the worker pool.
func (r *Route) Start(ctx context.Context) {
	for i := 0; i < r.cfg.Workers; i++ {
		r.wg.Add(1)
		go r.worker(ctx, i, r.queue)
	}
	r.log.Info("route started",
		"workers", r.cfg.Workers,
		"buffer", r.cfg.Buffer,
		"policy", string(r.cfg.Policy))
}

func (r *Route) worker(ctx context.Context, shard int, q chan event.Event) {
	defer r.wg.Done()
	for ev := range q {
		r.process(ctx, shard, ev)
		obs.QueueDepth.WithLabelValues(r.cfg.Name).Set(float64(r.Depth()))
	}
}

// process runs one event through the handler with retries, and contains any
// panic to this event (D6).
func (r *Route) process(ctx context.Context, shard int, ev event.Event) {
	for attempt := 1; ; attempt++ {
		start := time.Now()
		err := r.callHandler(ctx, shard, ev)
		obs.HandlerSeconds.WithLabelValues(r.cfg.Name).Observe(time.Since(start).Seconds())

		switch {
		case err == nil:
			obs.Handled.WithLabelValues(r.cfg.Name, "ok").Inc()
			return

		case errors.Is(err, ErrPermanent):
			// Will never succeed. Do not spend the retry budget on it.
			obs.Handled.WithLabelValues(r.cfg.Name, "permanent").Inc()
			obs.DeadLettered.WithLabelValues(r.cfg.Name, "permanent").Inc()
			r.log.Warn("permanent failure; dead-lettered",
				"kind", ev.Key.Kind, "operation", ev.Key.Operation, "error", err.Error())
			return

		case attempt >= r.cfg.MaxAttempts:
			obs.Handled.WithLabelValues(r.cfg.Name, "exhausted").Inc()
			obs.DeadLettered.WithLabelValues(r.cfg.Name, "retries_exhausted").Inc()
			r.log.Warn("retries exhausted; dead-lettered",
				"attempts", attempt, "error", err.Error())
			return

		default:
			// Transient. Retrying occupies a worker, never the ingest goroutine —
			// the route's own buffer absorbs the slack, and if it fills, this
			// route starts shedding while every other route keeps running.
			obs.Handled.WithLabelValues(r.cfg.Name, "retry").Inc()
			backoff := r.cfg.RetryBase * time.Duration(1<<uint(attempt-1))
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
		}
	}
}

// callHandler is the recover boundary.
//
// In Go an unrecovered panic in ANY goroutine terminates the whole process. One
// malformed record reaching one handler would therefore take down every other
// route. Recovering per event, counting it, and moving to the next event is what
// makes the isolation claim true in the code and not only in the diagram.
func (r *Route) callHandler(ctx context.Context, shard int, ev event.Event) (err error) {
	defer func() {
		if p := recover(); p != nil {
			obs.HandlerPanics.WithLabelValues(r.cfg.Name).Inc()
			// The panic value can contain attacker-influenced data, so it is
			// logged as a value and never formatted into the message.
			r.log.Error("handler panicked; contained",
				"panic", fmt.Sprint(p),
				"collection", ev.Key.Collection,
				"operation", ev.Key.Operation)
			err = Permanent(errors.New("handler panicked"))
		}
	}()
	return r.handler.Handle(ctx, shard, ev)
}

// Drain closes the queues and waits for in-flight work to finish.
//
// Called after the ingestor has stopped reading, so nothing new arrives. The
// ordering matters on SIGTERM: stop reading, drain, then commit the cursor —
// which leaves a bounded replay overlap rather than a gap.
func (r *Route) Drain(timeout time.Duration) {
	close(r.queue)
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()

	select {
	case <-done:
		r.log.Info("route drained")
	case <-time.After(timeout):
		r.log.Warn("drain deadline exceeded; abandoning buffered events",
			"remaining", r.Depth())
	}
}
