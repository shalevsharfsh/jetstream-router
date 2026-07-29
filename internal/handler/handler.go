// Package handler holds the per-route work, one file per route.
//
// Every handler implements route.Handler and knows nothing about channels,
// retries, the cursor or any other route. That is what keeps the interesting
// logic testable with no infrastructure at all.
//
// Handlers that keep state also implement route.Sharded, so events are
// dispatched by hash(key) % workers and each key has exactly one owning
// goroutine — no mutexes, no races. The trade-off is hot keys: a viral post
// pins one worker, and because the buffer is per-route rather than per-shard,
// a sufficiently hot key can push that whole route into shedding. The
// production answer is an external store with atomic increments, which removes
// the need to shard at all.
package handler

import (
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
	"github.com/shalevsharfsh/jetstream-router/internal/obs"
)

// Limits bound the aggregation state (I7).
type Limits struct {
	MaxKeysPerShard int
	DedupUS         int64
}

// Sink is where downstream work goes. One interface, so there is exactly one
// place to change when it becomes a webhook or a broker publish.
type Sink interface {
	Alert(level, route, msg string, attrs ...any)
}

// LogSink emits structured log lines.
//
// Record text is never passed through here. Post bodies are attacker-controlled,
// so writing them out would hand an adversary partial control of whatever
// aggregator or SIEM ingests these logs — forged entries, broken parsers,
// injected fields. Callers pass structural and derived values only: lengths,
// language codes, whether a match occurred.
type LogSink struct {
	Log *slog.Logger
}

// Alert records the alert and logs it. level is "info" or "warn".
func (s LogSink) Alert(level, route, msg string, attrs ...any) {
	obs.Alerts.WithLabelValues(route, msg).Inc()
	args := append([]any{"route", route}, attrs...)
	if level == "warn" {
		s.Log.Warn(msg, args...)
		return
	}
	s.Log.Info(msg, args...)
}

// --- record shapes -----------------------------------------------------------
//
// These differ between routes, which is why each route decodes its own rather
// than sharing one clever extractor.

// postRecord is the part of a post the content route matches on.
type postRecord struct {
	Text  string   `json:"text"`
	Langs []string `json:"langs"`
}

// engagementRecord covers like and repost: subject is a strong ref object.
type engagementRecord struct {
	Subject struct {
		URI string `json:"uri"`
	} `json:"subject"`
}

// followRecord: subject is a bare DID string, not an object.
type followRecord struct {
	Subject string `json:"subject"`
}

func decode(raw json.RawMessage, into any) bool {
	if len(raw) == 0 {
		return false
	}
	return json.Unmarshal(raw, into) == nil
}

func lower(ss []string) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		out = append(out, strings.ToLower(s))
	}
	return out
}

func evictCounter(route string) func(string) {
	return func(reason string) { obs.StateEvicted.WithLabelValues(route, reason).Inc() }
}

// dedupGate is the shared idempotency check for the stateful routes (I8).
//
// The cursor advancing on enqueue plus the reconnect rewind guarantees
// duplicates even in single-process operation, before any scaling is involved.
type dedupGate struct {
	route string
	seen  []*Dedup // one per shard, owned by that shard's goroutine
}

func newDedupGate(route string, shards int, l Limits) *dedupGate {
	if shards < 1 {
		shards = 1
	}
	g := &dedupGate{route: route, seen: make([]*Dedup, shards)}
	for i := range g.seen {
		g.seen[i] = NewDedup(l.DedupUS, l.MaxKeysPerShard, evictCounter(route))
	}
	return g
}

func (g *dedupGate) duplicate(shard int, ev event.Event) bool {
	if shard >= len(g.seen) || shard < 0 {
		shard = 0
	}
	if g.seen[shard].Seen(ev.DedupKey(), ev.TimeUS) {
		obs.Duplicates.WithLabelValues(g.route).Inc()
		return true
	}
	return false
}
