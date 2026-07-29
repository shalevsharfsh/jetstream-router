package routing

import (
	"testing"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
)

// Fixtures are trimmed from frames captured off the live firehose, not
// invented — including the fact that a delete commit carries no record.
const (
	postCreate = `{"did":"did:plc:abc","time_us":1785266126687031,"kind":"commit",
	  "commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"3mrpyre7fdk25",
	  "record":{"text":"hello","langs":["en"]}}}`

	likeCreate = `{"did":"did:plc:abc","time_us":1785266126673819,"kind":"commit",
	  "commit":{"operation":"create","collection":"app.bsky.feed.like","rkey":"3mrpyre2sga2y",
	  "record":{"subject":{"uri":"at://did:plc:x/app.bsky.feed.post/abc","cid":"bafy"}}}}`

	repostCreate = `{"did":"did:plc:abc","time_us":1785266126694113,"kind":"commit",
	  "commit":{"operation":"create","collection":"app.bsky.feed.repost","rkey":"r1",
	  "record":{"subject":{"uri":"at://did:plc:x/app.bsky.feed.post/abc","cid":"bafy"}}}}`

	followCreate = `{"did":"did:plc:abc","time_us":1785266126680550,"kind":"commit",
	  "commit":{"operation":"create","collection":"app.bsky.graph.follow","rkey":"3mrpyref6452f",
	  "record":{"subject":"did:plc:target"}}}`

	// No "record" key at all. This is the real shape.
	postDelete = `{"did":"did:plc:abc","time_us":1785266126765876,"kind":"commit",
	  "commit":{"operation":"delete","collection":"app.bsky.feed.post","rkey":"3mqmrv6uselv2"}}`

	// An identity event with no commit block — the case that must not nil-deref.
	identityEvt = `{"did":"did:plc:xyz","time_us":1785266204952625,"kind":"identity",
	  "identity":{"did":"did:plc:xyz","handle":"someone.bsky.social"}}`

	accountEvt = `{"did":"did:plc:xyz","time_us":1785266204952624,"kind":"account",
	  "account":{"active":false,"status":"deleted"}}`

	// A collection with no rule anywhere in the table.
	blockCreate = `{"did":"did:plc:abc","time_us":1785266126690000,"kind":"commit",
	  "commit":{"operation":"create","collection":"app.bsky.graph.block","rkey":"3mq2",
	  "record":{"subject":"did:plc:blocked"}}}`
)

func testTable() Table {
	return Table{
		Rules: []Rule{
			// Deliberately listed LAST, to prove that operation-first precedence
			// is a property of the router and not of ConfigMap ordering.
			{Match: Match{Kind: "commit", Collection: "app.bsky.feed.post"}, Route: "content"},
			{Match: Match{Kind: "commit", Collection: "app.bsky.feed.like"}, Route: "engagement"},
			{Match: Match{Kind: "commit", Collection: "app.bsky.feed.repost"}, Route: "engagement"},
			{Match: Match{Kind: "commit", Collection: "app.bsky.graph.follow"}, Route: "social-graph"},
			{Match: Match{Kind: "commit", Operation: "delete"}, Route: "retraction"},
		},
		Fallback: "default",
	}
}

func mustRouter(t *testing.T) *Router {
	t.Helper()
	r, err := New(testTable())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

func route(t *testing.T, r *Router, frame string) string {
	t.Helper()
	ev, err := event.Decode([]byte(frame))
	if err != nil {
		t.Fatalf("Decode(%s): %v", frame, err)
	}
	return r.Route(ev.Key)
}

// Test 1 in CLAUDE.md: the router table. The core of the exercise.
func TestRouterTable(t *testing.T) {
	r := mustRouter(t)

	cases := []struct {
		name  string
		frame string
		want  string
	}{
		{"post create", postCreate, "content"},
		{"like create", likeCreate, "engagement"},
		{"repost create", repostCreate, "engagement"},
		{"follow create", followCreate, "social-graph"},
		{"delete beats collection", postDelete, "retraction"},
		{"identity event has no commit block", identityEvt, "default"},
		{"account event has no commit block", accountEvt, "default"},
		{"unknown collection reaches default", blockCreate, "default"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := route(t, r, tc.frame); got != tc.want {
				t.Errorf("route = %q, want %q", got, tc.want)
			}
		})
	}
}

// I3, the rule most likely to be reintroduced as a bug later. Parametrised
// across the whole table rather than spot-checked for exactly that reason.
func TestDeleteIsMatchedBeforeTheCollectionMap(t *testing.T) {
	r := mustRouter(t)

	for _, collection := range []string{
		"app.bsky.feed.post",
		"app.bsky.feed.like",
		"app.bsky.feed.repost",
		"app.bsky.graph.follow",
		"app.bsky.graph.block",    // not in the table at all
		"com.example.new.lexicon", // does not exist yet
	} {
		t.Run(collection, func(t *testing.T) {
			k := event.Key{Kind: "commit", Collection: collection, Operation: "delete"}
			if got := r.Route(k); got != "retraction" {
				t.Errorf("delete of %s routed to %q, want retraction", collection, got)
			}
		})
	}
}

// A glob lets a new sibling lexicon join the route its siblings already use
// without a code change.
func TestCollectionGlobs(t *testing.T) {
	r, err := New(Table{
		Rules: []Rule{
			{Match: Match{Kind: "commit", Operation: "delete"}, Route: "retraction"},
			{Match: Match{Kind: "commit", Collection: "app.bsky.feed.*"}, Route: "feed"},
		},
		Fallback: "default",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.threadgate"} {
		if got := r.Route(event.Key{Kind: "commit", Collection: c, Operation: "create"}); got != "feed" {
			t.Errorf("%s routed to %q, want feed", c, got)
		}
	}
	if got := r.Route(event.Key{Kind: "commit", Collection: "app.bsky.graph.follow", Operation: "create"}); got != "default" {
		t.Errorf("graph.follow matched the feed glob: %q", got)
	}
	// A glob must not weaken delete precedence.
	if got := r.Route(event.Key{Kind: "commit", Collection: "app.bsky.feed.post", Operation: "delete"}); got != "retraction" {
		t.Errorf("delete under a glob routed to %q, want retraction", got)
	}
}

// I9: unknown types are routed, never silently dropped.
func TestFallbackIsRequired(t *testing.T) {
	if _, err := New(Table{Rules: testTable().Rules}); err == nil {
		t.Error("a table with no fallback was accepted; unknown types would vanish")
	}
}

func TestBadGlobIsRejectedAtStartup(t *testing.T) {
	_, err := New(Table{
		Rules:    []Rule{{Match: Match{Collection: "app.bsky.[feed"}, Route: "x"}},
		Fallback: "default",
	})
	if err == nil {
		t.Error("malformed glob accepted; it would silently match nothing")
	}
}

func TestRoutesEnumeratesEveryReachableRoute(t *testing.T) {
	got := mustRouter(t).Routes()
	want := map[string]bool{
		"retraction": true, "content": true, "engagement": true,
		"social-graph": true, "default": true,
	}
	if len(got) != len(want) {
		t.Fatalf("Routes() = %v, want %d entries", got, len(want))
	}
	for _, r := range got {
		if !want[r] {
			t.Errorf("unexpected route %q", r)
		}
	}
}

// The server-side filter is derived from the table so the two cannot drift.
func TestCollectionsSkipsGlobsAndOperationRules(t *testing.T) {
	got := mustRouter(t).Collections()
	if len(got) != 4 {
		t.Errorf("Collections() = %v, want the 4 concrete collections", got)
	}
	for _, c := range got {
		if c == "" {
			t.Error("empty collection in the derived filter")
		}
	}
}
