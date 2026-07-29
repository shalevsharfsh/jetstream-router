package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
	"github.com/shalevsharfsh/jetstream-router/internal/obs"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeJetstream is a stand-in for the real endpoint.
//
// Running a real WebSocket server in-process is what makes the genuinely hard
// behaviour testable: reconnection, cursor resume and the replay-window case.
// You cannot ask the live firehose to drop your connection on cue, and a test
// that depends on the network is a network test, not a logic test.
type fakeJetstream struct {
	mu      sync.Mutex
	queries []url.Values

	frames []string
	// dropAfterFirst closes the first connection abruptly once released.
	dropOn chan struct{}
	srv    *httptest.Server
}

func newFakeJetstream(frames []string, dropOn chan struct{}) *fakeJetstream {
	f := &fakeJetstream{frames: frames, dropOn: dropOn}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	return f
}

func (f *fakeJetstream) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	first := len(f.queries) == 0
	f.queries = append(f.queries, r.URL.Query())
	f.mu.Unlock()

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	ctx := r.Context()

	for _, frame := range f.frames {
		if err := c.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
			return
		}
	}

	if f.dropOn != nil && first {
		select {
		case <-f.dropOn:
		case <-ctx.Done():
			return
		}
		// Abrupt close, no handshake — how a real firehose drops you.
		c.CloseNow() //nolint:errcheck
		return
	}
	<-ctx.Done()
}

func (f *fakeJetstream) url() string {
	return "ws" + f.srv.URL[len("http"):]
}

func (f *fakeJetstream) connections() []url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]url.Values, len(f.queries))
	copy(out, f.queries)
	return out
}

func (f *fakeJetstream) Close() { f.srv.Close() }

// recorder captures dispatched events.
type recorder struct {
	mu     sync.Mutex
	events []event.Event
	accept atomic.Bool
}

func newRecorder() *recorder {
	r := &recorder{}
	r.accept.Store(true)
	return r
}

func (r *recorder) Dispatch(_ context.Context, ev event.Event) bool {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
	return r.accept.Load()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

func postFrame(timeUS int64) string {
	return fmt.Sprintf(`{"did":"did:plc:a","time_us":%d,"kind":"commit","commit":`+
		`{"operation":"create","collection":"app.bsky.feed.post","rkey":"r%d",`+
		`"record":{"text":"hello","langs":["en"]}}}`, timeUS, timeUS)
}

func testConfig(u string) Config {
	return Config{
		URL:           u,
		ReplayRewind:  5 * time.Second,
		ReplayWindow:  10 * time.Minute,
		LiveThreshold: 3 * time.Second,
		MaxLag:        60 * time.Second,
		MaxFrameBytes: 1 << 20,
		IdleTimeout:   30 * time.Second,
		BackoffMax:    50 * time.Millisecond,
		CommitEvery:   10 * time.Millisecond,
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

// End-to-end wiring against a fake server: frames in, classified events out.
func TestIngestClassifiesAndDispatches(t *testing.T) {
	now := time.Now().UnixMicro()
	f := newFakeJetstream([]string{
		postFrame(now),
		`{"did":"did:plc:b","time_us":` + strconv.FormatInt(now+1, 10) + `,"kind":"account","account":{"status":"deleted"}}`,
		`{ this is not json`, // must not kill the reader
		postFrame(now + 2),
	}, nil)
	defer f.Close()

	rec := newRecorder()
	cur := NewCursor(filepath.Join(t.TempDir(), "cursor.json"))
	in := New(testConfig(f.url()), cur, rec, obs.NewHealth(), quietLog())

	ctx, cancel := context.WithCancel(context.Background())
	go in.Run(ctx)
	waitFor(t, func() bool { return rec.count() >= 3 }, "three well-formed frames")
	cancel()

	if got := rec.count(); got != 3 {
		t.Errorf("dispatched %d events, want 3 (the malformed frame must be counted, not routed)", got)
	}
}

// The behaviour public implementations most often miss: reconnect, and resume
// from where we left off rather than silently restarting at the live tip.
func TestReconnectResumesFromCursor(t *testing.T) {
	base := time.Now().Add(-time.Second).UnixMicro()
	drop := make(chan struct{})
	f := newFakeJetstream([]string{postFrame(base), postFrame(base + 1000)}, drop)
	defer f.Close()

	rec := newRecorder()
	cur := NewCursor(filepath.Join(t.TempDir(), "cursor.json"))
	in := New(testConfig(f.url()), cur, rec, obs.NewHealth(), quietLog())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go in.Run(ctx)

	waitFor(t, func() bool { return cur.Latest() >= base+1000 }, "cursor to advance")
	close(drop) // kill the connection
	waitFor(t, func() bool { return len(f.connections()) >= 2 }, "a reconnect")

	conns := f.connections()
	if conns[0].Get("cursor") != "" {
		t.Error("first connect should be cold, with no cursor")
	}
	resumed := conns[1].Get("cursor")
	if resumed == "" {
		t.Fatal("reconnect did not resume from a cursor; that would silently open a gap")
	}

	got, err := strconv.ParseInt(resumed, 10, 64)
	if err != nil {
		t.Fatalf("cursor %q is not an integer: %v", resumed, err)
	}
	want := base + 1000 - int64(5*time.Second/time.Microsecond)
	if got != want {
		t.Errorf("resumed at %d, want %d (last position rewound by ReplayRewind)", got, want)
	}
	if got >= base+1000 {
		t.Error("resume point was not rewound; a gap is undetectable so we must overlap")
	}
}

// A cold start must pick up where the previous process left off.
func TestColdStartUsesPersistedCursor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.json")
	stored := time.Now().Add(-2 * time.Second).UnixMicro()

	writer := NewCursor(path)
	writer.Advance(stored)
	if err := writer.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	f := newFakeJetstream(nil, nil)
	defer f.Close()

	cur := NewCursor(path)
	if got := cur.Load(); got != stored {
		t.Fatalf("Load() = %d, want %d", got, stored)
	}

	in := New(testConfig(f.url()), cur, newRecorder(), obs.NewHealth(), quietLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go in.Run(ctx)

	waitFor(t, func() bool { return len(f.connections()) >= 1 }, "a connection")
	resumed, _ := strconv.ParseInt(f.connections()[0].Get("cursor"), 10, 64)
	if resumed != stored-int64(5*time.Second/time.Microsecond) {
		t.Errorf("resumed at %d, want the stored position rewound by ReplayRewind", resumed)
	}
}

// A cursor older than the server's retention cannot be honoured. The gap is
// unrecoverable AND of unknown size — Jetstream has no sequence numbers — so it
// must be recorded and alerted, not silently swallowed by resuming at the tip.
func TestLapsedReplayWindowIsDetectedNotSwallowed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.json")
	ancient := time.Now().Add(-2 * time.Hour).UnixMicro()

	w := NewCursor(path)
	w.Advance(ancient)
	if err := w.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	f := newFakeJetstream(nil, nil)
	defer f.Close()

	cur := NewCursor(path)
	cur.Load()
	cfg := testConfig(f.url())
	cfg.ReplayWindow = 10 * time.Minute

	in := New(cfg, cur, newRecorder(), obs.NewHealth(), quietLog())
	target, err := in.subscribeURL()
	if err != nil {
		t.Fatalf("subscribeURL: %v", err)
	}
	u, _ := url.Parse(target)
	if u.Query().Get("cursor") != "" {
		t.Error("a lapsed cursor was sent to the server anyway")
	}
}

// The server-side filter is derived from the routing table, so the two cannot
// drift apart — but it is opt-in, because discarding the type mix removes the
// problem the service exists to solve.
func TestServerFilterIsOptInAndDerived(t *testing.T) {
	f := newFakeJetstream(nil, nil)
	defer f.Close()

	cfg := testConfig(f.url())
	in := New(cfg, NewCursor(filepath.Join(t.TempDir(), "c.json")), newRecorder(), obs.NewHealth(), quietLog())
	target, _ := in.subscribeURL()
	u, _ := url.Parse(target)
	if len(u.Query()["wantedCollections"]) != 0 {
		t.Error("server filter applied without being enabled")
	}

	cfg.WantedCollections = []string{"app.bsky.feed.post", "app.bsky.feed.like"}
	in2 := New(cfg, NewCursor(filepath.Join(t.TempDir(), "c2.json")), newRecorder(), obs.NewHealth(), quietLog())
	target2, _ := in2.subscribeURL()
	u2, _ := url.Parse(target2)
	if len(u2.Query()["wantedCollections"]) != 2 {
		t.Errorf("wantedCollections = %v, want 2 entries", u2.Query()["wantedCollections"])
	}
}

// D2: the cursor advances past events that were shed, which is what makes
// shedding irreversible rather than deferred.
func TestCursorAdvancesPastDroppedEvents(t *testing.T) {
	now := time.Now().UnixMicro()
	f := newFakeJetstream([]string{postFrame(now), postFrame(now + 1000)}, nil)
	defer f.Close()

	rec := newRecorder()
	rec.accept.Store(false) // every dispatch is shed

	cur := NewCursor(filepath.Join(t.TempDir(), "cursor.json"))
	in := New(testConfig(f.url()), cur, rec, obs.NewHealth(), quietLog())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go in.Run(ctx)

	waitFor(t, func() bool { return cur.Latest() >= now+1000 }, "cursor to advance past shed events")

	if cur.Latest() < now+1000 {
		t.Error("cursor did not advance past dropped events")
	}
}

func TestCursorCommitIsAtomicAndReloadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "cursor.json")
	c := NewCursor(path)
	c.Advance(12345)
	if err := c.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// A truncated cursor file silently becomes "start from live", so the write
	// must be atomic. Reloading it must give back exactly what was written.
	again := NewCursor(path)
	if got := again.Load(); got != 12345 {
		t.Errorf("reloaded cursor = %d, want 12345", got)
	}

	// Monotonic: an older position never rewinds the cursor.
	c.Advance(99)
	if got := c.Latest(); got != 12345 {
		t.Errorf("cursor moved backwards to %d", got)
	}
}

var _ = json.Marshal

// A silent connection must be detected and recycled.
//
// The failure this guards against is not a crash — it is the opposite. A
// half-open connection produces no bytes and no error, so an unbounded Read
// blocks forever while the socket still reports established. The process stays
// up, stays "connected", and processes nothing.
func TestSilentConnectionIsTimedOutAndReconnected(t *testing.T) {
	// A server that completes the handshake, sends one frame, then says nothing.
	f := newFakeJetstream([]string{postFrame(time.Now().UnixMicro())}, nil)
	defer f.Close()

	cfg := testConfig(f.url())
	cfg.IdleTimeout = 250 * time.Millisecond
	cfg.BackoffMax = 20 * time.Millisecond

	rec := newRecorder()
	cur := NewCursor(filepath.Join(t.TempDir(), "cursor.json"))
	in := New(cfg, cur, rec, obs.NewHealth(), quietLog())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go in.Run(ctx)

	// Without a read deadline the ingestor sits on the first connection forever.
	waitFor(t, func() bool { return len(f.connections()) >= 2 },
		"the idle timeout to recycle a silent connection")
}

// Readiness must not be computed by the loop it is meant to be checking.
//
// Lag gates readiness (D5). If lag were only recalculated after a successful
// read, a wedged read loop would freeze it at its last healthy value and the pod
// would report ready forever while processing nothing — a health check that
// cannot observe its own failure mode.
func TestLagIsRefreshedWithoutReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.json")
	cur := NewCursor(path)

	// A position two hours old, as a stalled ingestor would leave behind.
	cur.Advance(time.Now().Add(-2 * time.Hour).UnixMicro())

	f := newFakeJetstream(nil, nil) // accepts, then silence
	defer f.Close()

	cfg := testConfig(f.url())
	cfg.MaxLag = time.Minute
	cfg.CommitEvery = 20 * time.Millisecond
	cfg.IdleTimeout = time.Hour // deliberately useless, to isolate the refresh path

	health := obs.NewHealth()
	in := New(cfg, cur, newRecorder(), health, quietLog())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go in.Run(ctx)

	// No frame ever arrives, so nothing in the read loop runs. Readiness has to
	// fail anyway, driven by the maintenance ticker.
	waitFor(t, func() bool {
		return strings.HasPrefix(health.Detail(), "lagging")
	}, "readiness to fail on lag with no reads at all")
}
