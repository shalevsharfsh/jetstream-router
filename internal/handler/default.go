package handler

import (
	"context"
	"log/slog"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
	"github.com/shalevsharfsh/jetstream-router/internal/obs"
)

// Default absorbs identity and account frames and any collection with no route.
//
// It would have been less code to drop these at the ingestor. That would also
// mean the day the network ships a new lexicon, or the day someone fat-fingers
// the routing ConfigMap, the events vanish with no trace and the first symptom is
// a business question nobody can answer. Counting them makes "we are receiving
// something we do not understand" a graph instead of an outage, and a rising rate
// here is the evidence that justifies building a real route.
type Default struct {
	log *slog.Logger
	// seen bounds how often an unknown collection is logged. Collections are
	// attacker-supplied, so logging each new one unthrottled is a log-flood
	// primitive; the counter carries the rate.
	seen *Throttle
}

// NewDefault builds the fallback route's handler.
func NewDefault(log *slog.Logger) *Default {
	return &Default{log: log, seen: NewThrottle(60*1_000_000, 512, func(string) {})}
}

// Handle counts one unrouted event.
func (d *Default) Handle(_ context.Context, _ int, ev event.Event) error {
	reason := "unmapped-collection"
	if ev.Key.Kind != event.KindCommit {
		// identity and account frames have no collection and can never match the
		// table. Expected forever, and a different signal from the one below.
		reason = "non-commit-kind"
	}
	obs.Unrouted.WithLabelValues(reason).Inc()

	if reason == "unmapped-collection" {
		// Throttle on the BUCKET, not the raw collection. Keying the throttle on
		// an attacker-supplied value while logging the bucket means every novel
		// lexicon earns its own log line — which is the log-flood vector this is
		// supposed to prevent. Found by watching it run: ~300 unknown-collection
		// events a minute produced a line each.
		b := bucket(ev.Key.Collection)
		if d.seen.Allow(b, ev.TimeUS) {
			d.log.Info("unknown collection", "route", "default", "bucket", b)
		}
	}
	return nil
}

// bucket maps a collection to its lexicon namespace, or "other".
func bucket(collection string) string {
	if knownCollections[collection] {
		return collection
	}
	for _, prefix := range []string{"app.bsky.feed.", "app.bsky.graph.", "app.bsky.actor.", "chat.bsky."} {
		if len(collection) > len(prefix) && collection[:len(prefix)] == prefix {
			return prefix + "*"
		}
	}
	return "other"
}
