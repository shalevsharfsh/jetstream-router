package handler

import (
	"context"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
	"github.com/shalevsharfsh/jetstream-router/internal/obs"
)

// Engagement keeps rolling per-subject counts for likes and reposts and raises
// an alert when one crosses a threshold.
//
// The highest-volume route by a wide margin. Counting is per *target* post, not
// per actor: the question is whether a post is getting unusual traction, so
// likes and reposts of the same URI accumulate together.
type Engagement struct {
	sink      Sink
	threshold int
	windows   []*Window
	throttles []*Throttle
	dedup     *dedupGate
}

// NewEngagement builds the route's handler with one state shard per worker.
func NewEngagement(sink Sink, shards, threshold int, windowUS int64, l Limits) *Engagement {
	if shards < 1 {
		shards = 1
	}
	e := &Engagement{sink: sink, threshold: threshold, dedup: newDedupGate("engagement", shards, l)}
	for i := 0; i < shards; i++ {
		e.windows = append(e.windows, NewWindow(windowUS, l.MaxKeysPerShard, evictCounter("engagement")))
		e.throttles = append(e.throttles, NewThrottle(windowUS, l.MaxKeysPerShard, evictCounter("engagement")))
	}
	return e
}

// ShardKey partitions by the post being engaged with, so every count for one
// subject is owned by one goroutine.
func (e *Engagement) ShardKey(ev event.Event) string {
	var rec engagementRecord
	if !decode(ev.Record, &rec) {
		return ""
	}
	return rec.Subject.URI
}

// Handle counts one like or repost.
func (e *Engagement) Handle(_ context.Context, shard int, ev event.Event) error {
	if shard >= len(e.windows) || shard < 0 {
		shard = 0
	}
	var rec engagementRecord
	if !decode(ev.Record, &rec) || rec.Subject.URI == "" {
		// Not countable, but not worth a retry or a dead letter either.
		return nil
	}
	if e.dedup.duplicate(shard, ev) {
		return nil
	}

	n := e.windows[shard].Add(rec.Subject.URI, ev.TimeUS)
	obs.StateEntries.WithLabelValues("engagement").Set(float64(e.windows[shard].Len()))

	if n < e.threshold {
		return nil
	}
	// Thresholds can be crossed deliberately, so an unthrottled alert path is a
	// remotely triggerable flood against whatever sits downstream of it.
	if !e.throttles[shard].Allow(rec.Subject.URI, ev.TimeUS) {
		obs.AlertsThrottled.WithLabelValues("engagement").Inc()
		return nil
	}

	e.sink.Alert("warn", "engagement", "threshold crossed",
		"subject", rec.Subject.URI, "count", n,
		"threshold", e.threshold, "collection", ev.Key.Collection)
	return nil
}
