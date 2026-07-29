package jetstream

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/shalevsharfsh/jetstream-router/internal/obs"
)

// Cursor tracks stream position.
//
// D2: it advances on ENQUEUE, not on handler completion. Once an event is in
// its route's buffer it is that route's problem. Tracking completion would mean
// maintaining N independent positions and committing the minimum — real
// complexity in a pipeline that is at-least-once regardless.
//
// Two consequences, both deliberate and both stated rather than discovered:
//
//   - A crash loses whatever is buffered in memory, across every route. The
//     production fix is to commit on broker acknowledgement instead, which is
//     why that is the single most valuable upgrade in the production path.
//
//   - A DROPPED event advances the cursor too. It was never enqueued, but the
//     events around it were, and the cursor is a single monotonic position — so
//     shedding is irreversible, not deferred. The alternative would be holding
//     the cursor back on the route that is already overloaded, which stalls the
//     stream in order to protect the very events it cannot keep up with.
//     events_dropped_total is the only record that a dropped event existed.
type Cursor struct {
	path     string
	latest   atomic.Int64 // newest time_us handed to a route (or shed past)
	commited atomic.Int64
}

type cursorFile struct {
	TimeUS int64 `json:"time_us"`
}

func NewCursor(path string) *Cursor {
	return &Cursor{path: path}
}

// Load reads the persisted position. A missing or unreadable file means "start
// from live", which is the correct behaviour for a first run.
func (c *Cursor) Load() int64 {
	b, err := os.ReadFile(c.path)
	if err != nil {
		return 0
	}
	var f cursorFile
	if json.Unmarshal(b, &f) != nil {
		return 0
	}
	c.latest.Store(f.TimeUS)
	c.commited.Store(f.TimeUS)
	obs.CursorUS.Set(float64(f.TimeUS))
	return f.TimeUS
}

// Advance records that the stream has moved past timeUS.
//
// Called for every frame the ingestor handles — routed, dropped or malformed —
// because all of them mean "we have consumed this position".
func (c *Cursor) Advance(timeUS int64) {
	if timeUS <= 0 {
		return
	}
	for {
		cur := c.latest.Load()
		if timeUS <= cur {
			return
		}
		if c.latest.CompareAndSwap(cur, timeUS) {
			return
		}
	}
}

// Latest is the newest position seen, whether or not it has been persisted.
// Preferred over the file on a mid-flight reconnect: it is newer, so resuming
// from it replays less.
func (c *Cursor) Latest() int64 { return c.latest.Load() }

// Commit persists the current position. Called on an interval (~1/s) rather
// than per event: a write per event would put a syscall on the hot path for a
// value that is only ever used after a restart.
func (c *Cursor) Commit() error {
	v := c.latest.Load()
	if v == 0 || v == c.commited.Load() {
		return nil
	}
	b, err := json.Marshal(cursorFile{TimeUS: v})
	if err != nil {
		return err
	}
	// Write-then-rename: a crash mid-write must not leave a truncated cursor
	// file, because an unparseable cursor silently becomes "start from live".
	tmp := c.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(c.path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, c.path); err != nil {
		return err
	}
	c.commited.Store(v)
	obs.CursorUS.Set(float64(v))
	return nil
}

// Lag reports how far behind wall clock the newest handled event is.
//
// This compares two machines' clocks — time_us is Jetstream's own, and it is
// instance-local — so it is meaningful only to within their skew. That is fine
// for a "tens of seconds behind" signal and it is not used as anything more
// precise. It gates readiness: an ingestor holding a healthy socket while two
// minutes behind is not working, and a readiness probe that only checked the
// socket would report it as fine.
func (c *Cursor) Lag() time.Duration {
	v := c.latest.Load()
	if v == 0 {
		return 0
	}
	d := time.Since(time.UnixMicro(v))
	if d < 0 {
		return 0
	}
	return d
}
