package handler

import (
	"context"
	"strings"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
	"github.com/shalevsharfsh/jetstream-router/internal/obs"
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
	windows   []*Window
	throttles []*Throttle
	dedup     *dedupGate
}

// NewSocialGraph builds the route's handler with one state shard per worker.
func NewSocialGraph(sink Sink, shards, threshold int, windowUS int64, l Limits) *SocialGraph {
	if shards < 1 {
		shards = 1
	}
	g := &SocialGraph{sink: sink, threshold: threshold, windowUS: windowUS,
		dedup: newDedupGate("social-graph", shards, l)}
	for i := 0; i < shards; i++ {
		g.windows = append(g.windows, NewWindow(windowUS, l.MaxKeysPerShard, evictCounter("social-graph")))
		g.throttles = append(g.throttles, NewThrottle(windowUS, l.MaxKeysPerShard, evictCounter("social-graph")))
	}
	return g
}

// ShardKey partitions by the followee.
func (g *SocialGraph) ShardKey(ev event.Event) string {
	var rec followRecord
	if !decode(ev.Record, &rec) {
		return ""
	}
	return rec.Subject
}

// Handle counts one follow.
func (g *SocialGraph) Handle(_ context.Context, shard int, ev event.Event) error {
	if shard >= len(g.windows) || shard < 0 {
		shard = 0
	}
	var rec followRecord
	if !decode(ev.Record, &rec) || !strings.HasPrefix(rec.Subject, "did:") {
		return nil
	}
	if g.dedup.duplicate(shard, ev) {
		return nil
	}

	n := g.windows[shard].Add(rec.Subject, ev.TimeUS)
	obs.StateEntries.WithLabelValues("social-graph").Set(float64(g.windows[shard].Len()))

	if n < g.threshold {
		return nil
	}
	if !g.throttles[shard].Allow(rec.Subject, ev.TimeUS) {
		obs.AlertsThrottled.WithLabelValues("social-graph").Inc()
		return nil
	}

	// Counted follow events, not distinct followers: deduplicating actors would
	// need a per-target set. Named rather than silently conflated.
	g.sink.Alert("info", "social-graph", "burst detected",
		"target", rec.Subject, "count", n,
		"window_s", g.windowUS/1_000_000, "metric", "follow_events")
	return nil
}
