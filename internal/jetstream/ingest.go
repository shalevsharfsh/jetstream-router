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
	"time"

	"github.com/coder/websocket"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
	"github.com/shalevsharfsh/jetstream-router/internal/obs"
)

// healthyFor is how long a connection must hold before the reconnect backoff
// is considered to have served its purpose and resets.
const healthyFor = time.Minute

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
	BackoffMax    time.Duration
	CommitEvery   time.Duration
}

// Ingestor reads the stream and hands events to a dispatcher.
type Ingestor struct {
	cfg    Config
	cursor *Cursor
	disp   Dispatcher
	health *obs.Health
	log    *slog.Logger

	state State
}

func New(cfg Config, cur *Cursor, d Dispatcher, h *obs.Health, log *slog.Logger) *Ingestor {
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
	go in.commitLoop(ctx)

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
	in.health.SetReady(true, "connected")

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
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
		in.updateLag()
	}
}

func (in *Ingestor) updateLag() {
	lag := in.cursor.Lag()
	obs.CursorLag.Set(lag.Seconds())

	if in.state == StateReplaying && lag <= in.cfg.LiveThreshold {
		in.setState(StateLive)
	}

	switch {
	case lag > in.cfg.MaxLag:
		in.health.SetReady(false, fmt.Sprintf("lagging %.0fs", lag.Seconds()))
	default:
		in.health.SetReady(true, fmt.Sprintf("lag %.1fs", lag.Seconds()))
	}
}

func (in *Ingestor) commitLoop(ctx context.Context) {
	t := time.NewTicker(in.cfg.CommitEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := in.cursor.Commit(); err != nil {
				in.log.Error("cursor commit failed", "error", err.Error())
			}
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
