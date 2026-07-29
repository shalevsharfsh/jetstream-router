package event

import (
	"encoding/json"
	"testing"
)

const (
	postCreate = `{"did":"did:plc:abc","time_us":1785266126687031,"kind":"commit",
	  "commit":{"rev":"3mrpyre","operation":"create","collection":"app.bsky.feed.post",
	  "rkey":"3mrpyre7fdk25","record":{"$type":"app.bsky.feed.post","text":"hello","langs":["en"]}}}`

	postDelete = `{"did":"did:plc:abc","time_us":1785266126765876,"kind":"commit",
	  "commit":{"rev":"222223vjggf22","operation":"delete","collection":"app.bsky.feed.post",
	  "rkey":"3mqmrv6uselv2"}}`

	identityEvt = `{"did":"did:plc:xyz","time_us":1785266204952625,"kind":"identity",
	  "identity":{"did":"did:plc:xyz","handle":"someone.bsky.social"}}`
)

// I2: the reader must not pay to materialise record bodies.
func TestRecordIsLeftUndecoded(t *testing.T) {
	ev, err := Decode([]byte(postCreate))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(ev.Record) == 0 {
		t.Fatal("record should be captured for the owning worker")
	}
	// Still raw at this point; the handler decodes it on its own goroutine.
	var body map[string]any
	if err := json.Unmarshal(ev.Record, &body); err != nil {
		t.Fatalf("raw record is not valid JSON: %v", err)
	}
	if body["text"] != "hello" {
		t.Errorf("record body = %v", body)
	}
	if ev.Key.Collection != "app.bsky.feed.post" || ev.Key.Operation != "create" {
		t.Errorf("routing tuple = %+v", ev.Key)
	}
}

// Guards the assumption the retraction handler is built on.
func TestDeleteCarriesNoRecord(t *testing.T) {
	ev, err := Decode([]byte(postDelete))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(ev.Record) != 0 {
		t.Errorf("delete frame carried a record: %s", ev.Record)
	}
	if ev.RKey == "" {
		t.Error("delete frame should still carry an rkey")
	}
}

// An identity frame has no commit block; decoding it must not nil-deref.
func TestNonCommitKindsDecodeWithoutACommitBlock(t *testing.T) {
	ev, err := Decode([]byte(identityEvt))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if ev.Key.Kind != KindIdentity {
		t.Errorf("kind = %q", ev.Key.Kind)
	}
	if ev.Key.Collection != "" || ev.Key.Operation != "" {
		t.Errorf("identity frame produced a collection/operation: %+v", ev.Key)
	}
}

func TestMalformedIsDistinctFromUnroutable(t *testing.T) {
	for _, tc := range []struct{ name, frame string }{
		{"not json", `{ nope`},
		{"empty object", `{}`},
		{"no kind", `{"did":"did:plc:a"}`},
		{"commit without body", `{"kind":"commit"}`},
		{"commit without collection", `{"kind":"commit","commit":{"operation":"create"}}`},
		{"commit without operation", `{"kind":"commit","commit":{"collection":"app.bsky.feed.post"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Decode([]byte(tc.frame)); err != ErrMalformed {
				t.Errorf("Decode error = %v, want ErrMalformed", err)
			}
		})
	}

	// An unknown collection is NOT malformed — it is a normal event we have no
	// route for, and the two must not share a metric.
	unknown := `{"kind":"commit","time_us":1,"commit":{"operation":"create",
	  "collection":"com.example.brand.new","rkey":"r"}}`
	if _, err := Decode([]byte(unknown)); err != nil {
		t.Errorf("unknown collection should decode cleanly, got %v", err)
	}
}

// I8: rkey is unique only within a collection.
func TestDedupKeyIncludesCollectionAndOperation(t *testing.T) {
	base := Event{Key: Key{Kind: "commit", Collection: "app.bsky.feed.like", Operation: "create"},
		DID: "did:plc:a", RKey: "rk"}

	otherCollection := base
	otherCollection.Key.Collection = "app.bsky.feed.repost"
	if base.DedupKey() == otherCollection.DedupKey() {
		t.Error("dedup key collides across collections; did+rkey alone is not unique")
	}

	deleted := base
	deleted.Key.Operation = OpDelete
	if base.DedupKey() == deleted.DedupKey() {
		t.Error("create and delete of one record must be distinct events")
	}
}
