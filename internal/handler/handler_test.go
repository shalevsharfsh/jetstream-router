package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
)

const sec = int64(1_000_000)

type capture struct {
	alerts []map[string]any
}

func (c *capture) Alert(level, route, msg string, attrs ...any) {
	m := map[string]any{"level": level, "route": route, "alert": msg}
	for i := 0; i+1 < len(attrs); i += 2 {
		if k, ok := attrs[i].(string); ok {
			m[k] = attrs[i+1]
		}
	}
	c.alerts = append(c.alerts, m)
}

func (c *capture) count(kind string) int {
	n := 0
	for _, a := range c.alerts {
		if a["alert"] == kind {
			n++
		}
	}
	return n
}

func post(text string, langs ...string) event.Event {
	rec := map[string]any{"text": text}
	if len(langs) > 0 {
		rec["langs"] = langs
	}
	return mkEvent("app.bsky.feed.post", "create", "did:plc:author", "rk1", rec, 1_000_000_000)
}

// --------------------------------------------------------------- content --

func TestContentMatchesWholeWordsNotSubstrings(t *testing.T) {
	sink := &capture{}
	h := NewContent(sink, []string{"ai"}, nil)

	// Regression: plain substring matching fired on all of these, which on a
	// notification path is worse than not matching at all.
	for _, decoy := range []string{
		"she said hello", "here we go again", "check your email",
		"sat on a chair", "in Dubai today", "maintain the service",
	} {
		if err := h.Handle(context.Background(), 0, post(decoy)); err != nil {
			t.Fatal(err)
		}
	}
	if got := sink.count("keyword matched"); got != 0 {
		t.Errorf("matched a substring %d times: %v", got, sink.alerts)
	}

	// The real thing still matches, punctuation and casing included.
	for _, real := range []string{"thoughts on AI, generally?", "ai is fine", "about ai."} {
		sink.alerts = nil
		if err := h.Handle(context.Background(), 0, post(real)); err != nil {
			t.Fatal(err)
		}
		if sink.count("keyword matched") != 1 {
			t.Errorf("did not match %q", real)
		}
	}
}

// \b cannot match next to punctuation, which is why the boundary is expressed
// as a lookaround pair instead.
func TestContentMatchesKeywordsContainingPunctuation(t *testing.T) {
	sink := &capture{}
	h := NewContent(sink, []string{"c++", ".net"}, nil)

	if err := h.Handle(context.Background(), 0, post("rewriting it in c++ today")); err != nil {
		t.Fatal(err)
	}
	if sink.count("keyword matched") != 1 {
		t.Error(`"c++" did not match; \b would have made this impossible`)
	}
}

func TestContentLanguageFilter(t *testing.T) {
	sink := &capture{}
	h := NewContent(sink, []string{"hello"}, []string{"en"})

	if err := h.Handle(context.Background(), 0, post("hello world", "ja")); err != nil {
		t.Fatal(err)
	}
	if sink.count("keyword matched") != 0 {
		t.Error("matched a post outside the configured languages")
	}
	if err := h.Handle(context.Background(), 0, post("hello world", "en")); err != nil {
		t.Fatal(err)
	}
	if sink.count("keyword matched") != 1 {
		t.Error("did not match a post in a configured language")
	}
}

// Post text is attacker-controlled, so it must never reach the sink.
func TestContentNeverEmitsRecordText(t *testing.T) {
	sink := &capture{}
	h := NewContent(sink, []string{"secret"}, nil)
	body := "secret plans: <script>alert(1)</script> and a newline\ninjected=field"

	if err := h.Handle(context.Background(), 0, post(body)); err != nil {
		t.Fatal(err)
	}
	if len(sink.alerts) != 1 {
		t.Fatal("expected one alert")
	}
	for k, v := range sink.alerts[0] {
		if s, ok := v.(string); ok && s == body {
			t.Errorf("field %q leaked the post text into the alert", k)
		}
	}
	if _, ok := sink.alerts[0]["text_len"]; !ok {
		t.Error("expected the length rather than the text")
	}
}

// ------------------------------------------------------------ engagement --

func TestEngagementAlertsOnceAtTheThreshold(t *testing.T) {
	sink := &capture{}
	h := NewEngagement(sink, 4, 5, 60*sec, Limits{MaxKeysPerShard: 1000, DedupUS: 120 * sec})
	uri := "at://did:plc:x/app.bsky.feed.post/hot"

	base := int64(1_000_000_000)
	for i := 0; i < 4; i++ {
		mustHandle(t, h, like(uri, fmt.Sprintf("did:plc:actor%d", i), base+int64(i)))
	}
	if sink.count("threshold crossed") != 0 {
		t.Fatal("alerted below the threshold")
	}

	mustHandle(t, h, like(uri, "did:plc:actor4", base+4))
	if sink.count("threshold crossed") != 1 {
		t.Fatalf("threshold crossing produced %d alerts, want 1", sink.count("threshold crossed"))
	}

	// Twenty more while still above threshold must not produce twenty alerts.
	for i := 5; i < 25; i++ {
		mustHandle(t, h, like(uri, fmt.Sprintf("did:plc:actor%d", i), base+int64(i)))
	}
	if got := sink.count("threshold crossed"); got != 1 {
		t.Errorf("a popular post produced %d alerts; the throttle is not working", got)
	}
}

func TestEngagementCountsPerTargetNotGlobally(t *testing.T) {
	sink := &capture{}
	h := NewEngagement(sink, 4, 3, 60*sec, Limits{MaxKeysPerShard: 1000, DedupUS: 120 * sec})
	base := int64(1_000_000_000)

	for i := 0; i < 6; i++ {
		uri := fmt.Sprintf("at://did:plc:x/app.bsky.feed.post/%d", i)
		mustHandle(t, h, like(uri, "did:plc:actor", base+int64(i)))
	}
	if got := sink.count("threshold crossed"); got != 0 {
		t.Errorf("six likes across six posts produced %d alerts; counts are not per-target", got)
	}
}

// D7 in practice: the reconnect overlap must not double-count.
func TestEngagementSuppressesReplayedDuplicates(t *testing.T) {
	sink := &capture{}
	h := NewEngagement(sink, 4, 3, 60*sec, Limits{MaxKeysPerShard: 1000, DedupUS: 120 * sec})
	uri := "at://did:plc:x/app.bsky.feed.post/a"
	base := int64(1_000_000_000)

	e1 := like(uri, "did:plc:one", base)
	e2 := like(uri, "did:plc:two", base+1)
	mustHandle(t, h, e1)
	mustHandle(t, h, e2)

	// The same two events again, as a reconnect rewind would deliver them.
	mustHandle(t, h, e1)
	mustHandle(t, h, e2)

	// Only a genuinely third distinct like should reach the threshold of 3.
	if sink.count("threshold crossed") != 0 {
		t.Fatal("replayed duplicates pushed the counter over the threshold")
	}
	mustHandle(t, h, like(uri, "did:plc:three", base+2))
	if sink.count("threshold crossed") != 1 {
		t.Error("the third distinct like did not cross the threshold")
	}
}

// ----------------------------------------------------------- socialgraph --

func TestFollowBurstKeysOnTheFolloweeNotTheFollower(t *testing.T) {
	sink := &capture{}
	h := NewSocialGraph(sink, 4, 3, 60*sec, Limits{MaxKeysPerShard: 1000, DedupUS: 120 * sec})
	base := int64(1_000_000_000)

	for i := 0; i < 3; i++ {
		mustHandle(t, h, follow("did:plc:target", fmt.Sprintf("did:plc:follower%d", i), base+int64(i)))
	}
	if sink.count("burst detected") != 1 {
		t.Fatal("three accounts following one target is a burst")
	}

	sink.alerts = nil
	for i := 0; i < 3; i++ {
		mustHandle(t, h, follow(fmt.Sprintf("did:plc:other%d", i), "did:plc:busy", base+10+int64(i)))
	}
	if sink.count("burst detected") != 0 {
		t.Error("one account following three targets is not a burst")
	}
}

// ------------------------------------------------------------ retraction --

func TestRetractionAcceptsEveryCollection(t *testing.T) {
	h := NewRetraction(&capture{})
	for _, c := range []string{
		"app.bsky.feed.post", "app.bsky.feed.like",
		"app.bsky.graph.follow", "com.example.unknown.lexicon",
	} {
		ev := mkEvent(c, "delete", "did:plc:a", "rk", nil, 1_000_000_000)
		if err := h.Handle(context.Background(), 0, ev); err != nil {
			t.Errorf("delete of %s returned %v", c, err)
		}
	}
}

// ---------------------------------------------------------------- helpers --

func mustHandle(t *testing.T, h interface {
	Handle(context.Context, int, event.Event) error
}, ev event.Event) {
	t.Helper()
	if err := h.Handle(context.Background(), 0, ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func like(uri, did string, timeUS int64) event.Event {
	return mkEvent("app.bsky.feed.like", "create", did, "rk-"+did,
		map[string]any{"subject": map[string]any{"uri": uri, "cid": "bafy"}}, timeUS)
}

func follow(target, did string, timeUS int64) event.Event {
	return mkEvent("app.bsky.graph.follow", "create", did, "rk-"+did+target,
		map[string]any{"subject": target}, timeUS)
}

func mkEvent(collection, op, did, rkey string, record map[string]any, timeUS int64) event.Event {
	var raw json.RawMessage
	if record != nil {
		b, _ := json.Marshal(record)
		raw = b
	}
	return event.Event{
		Key:    event.Key{Kind: "commit", Collection: collection, Operation: op},
		DID:    did,
		RKey:   rkey,
		TimeUS: timeUS,
		Record: raw,
	}
}

// The state is sharded and mutated under a lock rather than owned by a single
// goroutine (see shardSet). That is only safe if concurrent workers touching
// the same key still produce one consistent count — which is what D3 actually
// promises. Run under -race.
func TestConcurrentWorkersOnOneKeyStayConsistent(t *testing.T) {
	sink := &capture{}
	h := NewEngagement(sink, 8, 50, 60*sec, Limits{MaxKeysPerShard: 5000, DedupUS: 120 * sec})
	uri := "at://did:plc:x/app.bsky.feed.post/hot"
	base := int64(1_000_000_000)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				// Distinct actors so the dedup gate does not suppress them.
				ev := like(uri, fmt.Sprintf("did:plc:a%d-%d", w, i), base+int64(w*100+i))
				_ = h.Handle(context.Background(), w, ev)
			}
		}(w)
	}
	wg.Wait()

	// 800 distinct likes of one post, threshold 50, throttled to one alert per
	// window: exactly one alert, no double-counting, no lost updates.
	if got := sink.count("threshold crossed"); got != 1 {
		t.Errorf("alerts = %d, want exactly 1 across 8 concurrent workers", got)
	}
}

// I7 promises a size cap AND a TTL. A review found the TTL sweep was defined
// but never called outside tests, so only the size cap was live.
func TestTTLSweepRunsWithoutExternalTimer(t *testing.T) {
	h := NewEngagement(&capture{}, 1, 1_000_000, 60*sec,
		Limits{MaxKeysPerShard: 1_000_000, DedupUS: 1 * sec})
	base := int64(1_000_000_000)

	// Well past sweepEvery distinct one-shot keys, each far outside the window
	// of the last. With no sweep these all persist until LRU pressure, which
	// with a million-key cap never arrives.
	for i := 0; i < sweepEvery*2; i++ {
		uri := fmt.Sprintf("at://did:plc:x/app.bsky.feed.post/%d", i)
		_ = h.Handle(context.Background(), 0, like(uri, "did:plc:a", base+int64(i)*120*sec))
	}

	live := h.state.shards[0].window.Len()
	if live >= sweepEvery {
		t.Errorf("window holds %d keys after %d one-shot events; TTL sweep is not running",
			live, sweepEvery*2)
	}
}
