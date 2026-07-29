package handler

import (
	"context"
	"strings"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
)

// SocialGraph detects a burst of follows for one account.
//
// Keyed on the account being followed, not the follower. A spike of distinct
// accounts following one target inside a short window is the shape of both
// organic virality and coordinated inauthentic behaviour; telling those apart is
// a modelling problem well outside this service, so it raises the signal and
// says so rather than pretending to judge.
type SocialGraph struct {
	sink      Sink
	threshold int
	windowUS  int64
	state     *shardSet
}

// NewSocialGraph builds the route's handler with one state shard per worker.
func NewSocialGraph(sink Sink, shards, threshold int, windowUS int64, l Limits) *SocialGraph {
	return &SocialGraph{
		sink:      sink,
		threshold: threshold,
		windowUS:  windowUS,
		state:     newShardSet("social-graph", shards, windowUS, l),
	}
}

// Handle counts one follow.
func (g *SocialGraph) Handle(_ context.Context, _ int, ev event.Event) error {
	var rec followRecord
	if !decode(ev.Record, &rec) || !strings.HasPrefix(rec.Subject, "did:") {
		return nil
	}

	count, dup, alert := g.state.observe(rec.Subject, ev.DedupKey(), ev.TimeUS, g.threshold)
	if dup || !alert {
		return nil
	}

	// Counted follow events, not distinct followers: deduplicating actors would
	// need a per-target set. Named rather than silently conflated.
	g.sink.Alert("info", "social-graph", "burst detected",
		"target", rec.Subject, "count", count,
		"window_s", g.windowUS/1_000_000, "metric", "follow_events")
	return nil
}
