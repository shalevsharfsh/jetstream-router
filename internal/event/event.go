// Package event holds the envelope types and the lazy record decoding.
//
// I2: the reader decodes only what routing needs. Jetstream is one socket with
// no consumer group, so exactly one goroutine reads it, and anything that
// goroutine does inline is charged against the throughput of the entire stream.
// Routing needs three fields; the record body is left as json.RawMessage and
// parsed by the worker that owns it, on that worker's goroutine.
//
// Fully decoding every record on the reader would spend the stream's throughput
// budget materialising bodies for events a full buffer is about to shed anyway.
package event

import (
	"encoding/json"
	"errors"
)

// Jetstream's vocabulary, spelled out so a typo is a compile error.
const (
	KindCommit   = "commit"
	KindIdentity = "identity"
	KindAccount  = "account"

	OpCreate = "create"
	OpUpdate = "update"
	OpDelete = "delete"
)

// ErrMalformed means a frame could not be understood well enough to route.
//
// Counted separately from "reached the default route". A malformed frame is an
// upstream-schema signal; an unknown collection is an ordinary and expected
// occurrence. Conflating them hides schema drift inside a metric that is meant
// to be non-zero.
var ErrMalformed = errors.New("malformed event")

// Key is the routing tuple.
//
// An event's type is not a single field, and Operation cross-cuts Collection: a
// delete may be of a post, a like or a follow. The routing table is therefore
// two-dimensional rather than a flat switch.
type Key struct {
	Kind       string
	Collection string // empty for identity/account
	Operation  string // empty for identity/account
}

// Event is what crosses the boundary from the ingestor to a route.
type Event struct {
	Key    Key
	DID    string
	TimeUS int64
	RKey   string

	// Record is the undecoded record body. Handlers unmarshal it themselves.
	Record json.RawMessage
}

// DedupKey identifies a logical record for idempotency (I8).
//
// rkey is a TID unique within a repo *and collection*, so did+rkey alone can
// collide across collections. Operation is included so a create and a later
// delete of the same record remain distinct events.
func (e Event) DedupKey() string {
	return e.DID + "|" + e.Key.Collection + "|" + e.RKey + "|" + e.Key.Operation
}

// envelope mirrors only the fields routing needs. Record is RawMessage so the
// decoder skips the body rather than materialising it.
type envelope struct {
	DID    string `json:"did"`
	TimeUS int64  `json:"time_us"`
	Kind   string `json:"kind"`
	Commit *struct {
		Operation  string          `json:"operation"`
		Collection string          `json:"collection"`
		RKey       string          `json:"rkey"`
		Record     json.RawMessage `json:"record"`
	} `json:"commit"`
}

// Decode parses one frame's envelope.
//
// Returns ErrMalformed rather than a zero value, so a caller cannot accidentally
// route a frame it failed to understand.
func Decode(frame []byte) (Event, error) {
	var env envelope
	if err := json.Unmarshal(frame, &env); err != nil {
		return Event{}, ErrMalformed
	}
	if env.Kind == "" {
		return Event{}, ErrMalformed
	}

	ev := Event{
		Key:    Key{Kind: env.Kind},
		DID:    env.DID,
		TimeUS: env.TimeUS,
	}

	// identity and account frames carry no commit block at all. They are
	// perfectly valid events that simply have no collection, which is why they
	// can never match the collection map and always reach the default route.
	if env.Kind != KindCommit {
		return ev, nil
	}

	if env.Commit == nil || env.Commit.Collection == "" || env.Commit.Operation == "" {
		return Event{}, ErrMalformed
	}

	ev.Key.Collection = env.Commit.Collection
	ev.Key.Operation = env.Commit.Operation
	ev.RKey = env.Commit.RKey
	ev.Record = env.Commit.Record
	return ev, nil
}
