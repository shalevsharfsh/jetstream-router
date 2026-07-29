package handler

import (
	"context"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
	"github.com/shalevsharfsh/jetstream-router/internal/obs"
)

// Retraction handles every delete, whatever its collection.
//
// Deletions are a different kind of work from creates: cleanup and compliance
// rather than analysis. Keeping them on their own route means a backlog of
// deletions cannot delay notifications, and a bug in content matching cannot
// stop retractions being processed.
//
// A real constraint, found by reading the wire rather than the docs: a delete
// commit carries no record. So when a like is retracted we know who retracted it
// and which record of theirs it was, but not which post it pointed at — exactly
// what a counter decrement needs. Referential cleanup is impossible without an
// index the create path writes ((did, collection, rkey) -> subject), which is
// roughly one extra write per engagement event. Whether that is worth paying is
// a decision about whether counts must be exact or merely indicative; for a
// traction signal it is not.
//
// So this records the retraction — "was this deleted?" is answerable, which is
// what a compliance path needs — and exposes the unresolvable ones as a metric
// rather than pretending the cleanup happened.
type Retraction struct {
	sink Sink
}

// NewRetraction builds the route's handler.
func NewRetraction(sink Sink) *Retraction { return &Retraction{sink: sink} }

// countedCollections would need a counter decrement if the index existed.
var countedCollections = map[string]bool{
	"app.bsky.feed.like":    true,
	"app.bsky.feed.repost":  true,
	"app.bsky.graph.follow": true,
}

// knownCollections bounds the metric label. Collections are user-extensible in
// the AT Protocol, so the raw value would let a stranger mint unbounded time
// series from the public internet.
var knownCollections = map[string]bool{
	"app.bsky.feed.like":    true,
	"app.bsky.feed.repost":  true,
	"app.bsky.graph.follow": true,
	"app.bsky.feed.post":    true,
}

// Handle records one deletion.
func (r *Retraction) Handle(_ context.Context, _ int, ev event.Event) error {
	label := ev.Key.Collection
	if !knownCollections[label] {
		label = "other"
	}
	resolution := "not-applicable"
	if countedCollections[ev.Key.Collection] {
		resolution = "no-index"
	}
	obs.Retractions.WithLabelValues(label, resolution).Inc()
	return nil
}
