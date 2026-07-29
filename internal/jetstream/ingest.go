// Package jetstream owns the single WebSocket, the connection state machine and
// the cursor.
//
// This is the one component that cannot be replicated. Jetstream is one socket
// with no consumer group, so exactly one goroutine reads it, and anything that
// goroutine does inline is charged against the throughput of the entire stream.
// That single fact generates most of the design: the partial decode in
// classify, the non-blocking send in route.Offer, and the cursor semantics here.
//
// Scaling the front is sharding, not replicas — N connections partitioned by
// wantedCollections or wantedDids, each a strict singleton. Two readers over the
// same range process everything twice.
package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
	"github.com/shalevsharfsh/jetstream-router/internal/obs"
)

// healthyFor is how long a connection must hold before the reconnect backoff
// is considered to have served its purpose and resets.
const healthyFor = time.Minute

// defaultIdleTimeout applies when the caller leaves IdleTimeout unset.
const defaultIdleTimeout = 30 * time.Second

// State is the connection lifecycle. Keeping cursor handling, backoff and
// replay detection in one explicit state machine is considerably clearer than
// scattering booleans through the read loop (D5).
type State string

const (
	StateDisconnected State = "disconnected"
	StateConnecting   State = "connecting"
	StateReplaying    State = "replaying" // catching up from a stored cursor
	StateLive         State = "live"      // caught up to the tip
	StateReconnecting State = "reconnecting"
)

var allStates = []State{
	StateDisconnected, StateConnecting, StateReplaying, StateLive, StateReconnecting,
}

// Dispatcher receives classified events. Returning false means the event was
// shed; the ingestor counts it and moves on without blocking.
type Dispatcher interface {
	Dispatch(ctx context.Context, ev event.Event) bool
}

// Config tunes the ingestor.
type Config struct {
	URL string
	// WantedCollections asks the server to send only these. Off by default:
	// filtering at the source is legitimate load shedding, but discarding the
	// type mix removes the problem this service exists to solve.
	WantedCollections []string
	// ReplayRewind is how far back from the stored cursor to resume. We
	// deliberately reprocess a small overlap rather than risk a gap, because a
	// duplicate is detectable (D7) and a gap is not — Jetstream has no sequence
	// numbers, so nothing tells you that you missed anything.
	ReplayRewind time.Duration
	// LiveThreshold is the lag below which we consider ourselves caught up.
	LiveThreshold time.Duration
	// MaxLag is the lag above which readiness fails.
	MaxLag time.Duration
	// ReplayWindow is the server's approximate retention. A stored cursor older
	// than this cannot be honoured, and the resulting gap is unrecoverable and
	// of unknown size — so it is measured and alerted rather than swallowed.
	ReplayWindow time.Duration
	// MaxFrameBytes rejects oversized frames at the edge. Every field on this
	// stream is attacker-controlled, so the parser gets a limit.
	MaxFrameBytes int64
	// IdleTimeout bounds a single Read. Without it a half-open connection —
	// one the kernel still believes is established because no FIN ever
	// arrived — blocks the read loop forever. The firehose delivers hundreds
	// of events a second, so any silence beyond a few seconds means the
	// connection is gone whatever the socket claims.
	IdleTimeout time.Duration
	BackoffMax  time.Duration
	CommitEvery time.Duration
}

// Ingestor reads the stream and hands events to a dispatcher.
type Ingestor struct {
	cfg    Config
	cursor *Cursor
	disp   Dispatcher
	health *obs.Health
	log    *slog.Logger

	state State
	// connected is read from the maintenance goroutine, so it cannot be the
	// State field, which the read loop owns.
	connected atomic.Bool
}

func New(cfg Config, cur *Cursor, d Dispatcher, h *obs.Health, log *slog.Logger) *Ingestor {
	// A zero IdleTimeout would make every Read expire immediately rather than
	// never — the opposite of what an unset field should mean. Normalise it
	// here so a caller that has not thought about it gets the safe reading.
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}
	return &Ingestor{
		cfg: cfg, cursor: cur, disp: d, health: h,
		log: log.With("component", "ingest"), state: StateDisconnected,
	}
}

func (in *Ingestor) setState(s State) {
	if in.state == s {
		return
	}
	in.state = s
	for _, st := range allStates {
		v := 0.0
		if st == s {
			v = 1.0
		}
		obs.ConnState.WithLabelValues(string(st)).Set(v)
	}
	in.log.Info("connection state", "state", string(s))
}

// subscribeURL builds the resume URL, applying the rewind and detecting a
// lapsed replay window.
func (in *Ingestor) subscribeURL() (string, error) {
	u, err := url.Parse(in.cfg.URL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	for _, c := range in.cfg.WantedCollections {
		q.Add("wantedCollections", c)
	}

	if pos := in.cursor.Latest(); pos > 0 {
		resume := time.UnixMicro(pos).Add(-in.cfg.ReplayRewind)
		age := time.Since(resume)

		if in.cfg.ReplayWindow > 0 && age > in.cfg.ReplayWindow {
			// The stored position is older than anything the server still holds.
			// We cannot get those events and — with no sequence numbers — we
			// cannot even say how many there were. Record it, alert, resume from
			// the live tip rather than silently pretending the gap did not happen.
			obs.ReplayGap.Inc()
			in.log.Error("replay window lapsed; unrecoverable gap",
				"cursor_age_seconds", int64(age.Seconds()),
				"replay_window_seconds", int64(in.cfg.ReplayWindow.Seconds()))
			u.RawQuery = q.Encode()
			return u.String(), nil
		}
		q.Set("cursor", strconv.FormatInt(resume.UnixMicro(), 10))
	}

	u.RawQuery = q.Encode()
	return u.String(), nil
}

// Run reads until ctx is cancelled, reconnecting forever.
func (in *Ingestor) Run(ctx context.Context) {
	go in.maintenanceLoop(ctx)

	attempt := 0
	for ctx.Err() == nil {
		if attempt == 0 {
			in.setState(StateConnecting)
		} else {
			in.setState(StateReconnecting)
		}

		connectedAt := time.Now()
		err := in.connectAndRead(ctx)
		if ctx.Err() != nil {
			break
		}

		// A connection that held for a meaningful stretch is evidence the
		// upstream is healthy, so the next failure starts backing off from
		// scratch. Without this the counter only ever climbs: six brief blips
		// over a week leave the process pinned at the maximum delay, and a
		// seventh disconnect after eight flawless hours waits the full
		// backoff before even trying.
		if time.Since(connectedAt) > healthyFor {
			attempt = 0
		}

		attempt++
		obs.Reconnects.Inc()
		in.setState(StateDisconnected)
		in.health.SetReady(false, "disconnected")

		// Exponential backoff with full jitter. Jitter matters: without it a
		// fleet reconnects in lockstep and hammers the upstream at exactly the
		// moment it is struggling.
		d := time.Duration(1<<min(attempt, 6)) * time.Second
		if d > in.cfg.BackoffMax {
			d = in.cfg.BackoffMax
		}
		d = time.Duration(rand.Int63n(int64(d) + 1))
		in.log.Warn("disconnected; backing off",
			"attempt", attempt, "delay_seconds", d.Seconds(), "error", errString(err))

		select {
		case <-time.After(d):
		case <-ctx.Done():
			return
		}
	}
	in.setState(StateDisconnected)
}

func (in *Ingestor) connectAndRead(ctx context.Context) error {
	target, err := in.subscribeURL()
	if err != nil {
		return err
	}

	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	conn, _, err := websocket.Dial(dialCtx, target, nil)
	cancel()
	if err != nil {
		return err
	}
	defer conn.CloseNow() //nolint:errcheck // best effort on the way out

	if in.cfg.MaxFrameBytes > 0 {
		conn.SetReadLimit(in.cfg.MaxFrameBytes)
	}

	// If we resumed from a cursor we are behind by construction, so we start in
	// Replaying and only claim Live once lag drops under the threshold. That
	// distinction is not cosmetic: replayed events are minutes old, and a
	// handler that windowed on wall clock would read the catch-up as a burst of
	// simultaneous activity and fire a false alert on the exact path that
	// recovery exercises most. Handlers window on event time for this reason.
	if in.cursor.Latest() > 0 {
		in.setState(StateReplaying)
	} else {
		in.setState(StateLive)
	}
	in.connected.Store(true)
	defer in.connected.Store(false)
	in.health.SetReady(true, "connected")

	for {
		// Bounded read. A half-open connection produces no bytes and no error,
		// so without a deadline this loop is where the process goes to die
		// quietly — still "connected", still reporting ready, processing
		// nothing. The timeout turns that into an ordinary read error, which
		// the state machine already knows how to handle.
		readCtx, cancelRead := context.WithTimeout(ctx, in.cfg.IdleTimeout)
		_, data, err := conn.Read(readCtx)
		cancelRead()
		if err != nil {
			if ctx.Err() == nil && readCtx.Err() != nil {
				in.log.Warn("no frames within the idle timeout; treating the connection as dead",
					"idle_timeout_seconds", in.cfg.IdleTimeout.Seconds())
			}
			return err
		}
		obs.EventsReceived.Inc()

		ev, cerr := event.Decode(data)
		if cerr != nil {
			// A malformed frame is counted and discarded, never fatal. It is
			// also not the same thing as an unroutable one, and the two have
			// separate counters so schema drift is visible.
			obs.EventsMalformed.Inc()
			continue
		}

		in.disp.Dispatch(ctx, ev)

		// Advance regardless of whether the dispatch was accepted. See Cursor.
		in.cursor.Advance(ev.TimeUS)

		// Only the Replaying -> Live transition stays here, because it is the
		// one thing that genuinely requires a read to have happened. The lag
		// signal and readiness are refreshed on a timer instead; see
		// maintenanceLoop.
		if in.state == StateReplaying && in.cursor.Lag() <= in.cfg.LiveThreshold {
			in.setState(StateLive)
		}
	}
}

// refreshLag publishes the lag signal and gates readiness on it.
//
// Called from the maintenance ticker rather than the read loop, and that
// separation is the whole point. Readiness fails on cursor lag (D5) — but if
// lag were only recomputed after a successful read, a wedged read loop would
// freeze the metric at its last healthy value and the pod would report ready
// forever while processing nothing. A health signal must not be computed by the
// code path it exists to check.
//
// Safe to call from another goroutine: Health is atomic, Prometheus gauges are
// concurrency-safe, and the connection state is read through an atomic rather
// than the State field the read loop owns.
func (in *Ingestor) refreshLag() {
	if in.cursor.Latest() == 0 {
		return // nothing seen yet; lag is meaningless
	}
	lag := in.cursor.Lag()
	obs.CursorLag.Set(lag.Seconds())

	if !in.connected.Load() {
		return // Run() owns the readiness message while disconnected
	}
	if lag > in.cfg.MaxLag {
		in.health.SetReady(false, fmt.Sprintf("lagging %.0fs", lag.Seconds()))
		return
	}
	in.health.SetReady(true, fmt.Sprintf("lag %.1fs", lag.Seconds()))
}

// maintenanceLoop persists the cursor and refreshes the lag signal on a fixed
// interval, independent of whether any frames are arriving.
func (in *Ingestor) maintenanceLoop(ctx context.Context) {
	t := time.NewTicker(in.cfg.CommitEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := in.cursor.Commit(); err != nil {
				in.log.Error("cursor commit failed", "error", err.Error())
			}
			in.refreshLag()
		case <-ctx.Done():
			return
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		return "websocket close " + strconv.Itoa(int(ce.Code))
	}
	// Keep upstream error text out of the log verbatim where it may echo
	// server-supplied content.
	s := err.Error()
	if len(s) > 200 {
		s = s[:200]
	}
	return strings.ToValidUTF8(s, "")
}
