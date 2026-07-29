package handler

import (
	"context"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
)

// Engagement keeps rolling per-subject counts for likes and reposts and raises
// an alert when one crosses a threshold.
//
// The highest-volume route by a wide margin — roughly three quarters of the
// firehose. Counting is per *target* post, not per actor: the question is
// whether a post is getting unusual traction, so likes and reposts of the same
// URI accumulate together.
//
// The subject URI lives inside commit.record, so it is decoded here, on this
// worker's goroutine, and never on the shared reader (I2). See shardSet.
type Engagement struct {
	sink      Sink
	threshold int
	state     *shardSet
}

// NewEngagement builds the route's handler with one state shard per worker.
func NewEngagement(sink Sink, shards, threshold int, windowUS int64, l Limits) *Engagement {
	return &Engagement{
		sink:      sink,
		threshold: threshold,
		state:     newShardSet("engagement", shards, windowUS, l),
	}
}

// Handle counts one like or repost.
func (e *Engagement) Handle(_ context.Context, _ int, ev event.Event) error {
	var rec engagementRecord
	if !decode(ev.Record, &rec) || rec.Subject.URI == "" {
		// Not countable, but not worth a retry or a dead letter either.
		return nil
	}

	count, dup, alert := e.state.observe(rec.Subject.URI, ev.DedupKey(), ev.TimeUS, e.threshold)
	if dup || !alert {
		return nil
	}

	e.sink.Alert("warn", "engagement", "threshold crossed",
		"subject", rec.Subject.URI, "count", count,
		"threshold", e.threshold, "collection", ev.Key.Collection)
	return nil
}
