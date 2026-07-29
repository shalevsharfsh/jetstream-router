package handler

import (
	"hash/fnv"
	"sync"

	"github.com/shalevsharfsh/jetstream-router/internal/obs"
)

// sweepEvery is how many events a shard processes between TTL sweeps.
//
// The size cap (LRU) applies on every insert, but the TTL cap only takes effect
// when something walks the map. Doing it on a fixed event count rather than a
// timer keeps the work on the goroutine that already holds the shard lock — a
// separate ticker goroutine would need the same lock and buy nothing.
const sweepEvery = 2048

// shardSet is the per-route aggregation state, partitioned by key.
//
// D3's goal is that a given key is only ever mutated by one thing at a time.
// The original implementation achieved that by giving each worker its own
// queue and hashing events to a worker before enqueue — but the hash key for
// these routes lives inside commit.record, so computing it required decoding
// the record body on the single ingest goroutine. That violated I2 (the reader
// decodes only what routing needs), put the heaviest route's full JSON parse on
// the one goroutine whose stall stops everything, and decoded each record
// twice: once to pick a shard, once in the handler.
//
// So the partition moved here. The worker decodes on its own goroutine, derives
// the key, and takes that shard's lock. Same guarantee — one mutator per key at
// a time — with contention spread across N shards, and nothing decoded on the
// reader.
//
// The cost is honest: D3 no longer gets its "no mutexes at all" property. That
// was a means, not the end; the end was per-key consistency, and a sharded
// mutex delivers it. An uncontended lock around a map write is a few
// nanoseconds, whereas a full json.Unmarshal on the shared reader is charged
// against the throughput of the entire stream.
type shardSet struct {
	route  string
	shards []*shard
}

type shard struct {
	mu        sync.Mutex
	window    *Window
	throttle  *Throttle
	dedup     *Dedup
	sinceSwep int
}

func newShardSet(route string, n int, windowUS int64, l Limits) *shardSet {
	if n < 1 {
		n = 1
	}
	s := &shardSet{route: route, shards: make([]*shard, n)}
	evict := evictCounter(route)
	for i := range s.shards {
		s.shards[i] = &shard{
			window:   NewWindow(windowUS, l.MaxKeysPerShard, evict),
			throttle: NewThrottle(windowUS, l.MaxKeysPerShard, evict),
			dedup:    NewDedup(l.DedupUS, l.MaxKeysPerShard, evict),
		}
	}
	return s
}

func (s *shardSet) for_(key string) *shard {
	if len(s.shards) == 1 {
		return s.shards[0]
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return s.shards[int(h.Sum32())%len(s.shards)]
}

// observe records one occurrence of key and reports the count over the window,
// whether the event was a duplicate, and whether an alert may fire.
//
// One call, one lock acquisition: dedup, count and throttle for a key all live
// on the same shard, so splitting them would mean taking the same lock three
// times for no benefit.
func (s *shardSet) observe(key, dedupKey string, nowUS int64, threshold int) (count int, dup, alert bool) {
	sh := s.for_(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if sh.dedup.Seen(dedupKey, nowUS) {
		obs.Duplicates.WithLabelValues(s.route).Inc()
		return 0, true, false
	}

	count = sh.window.Add(key, nowUS)

	// TTL enforcement. Without this the size cap is the only live bound, and a
	// key that appears once and never again holds its entry until LRU pressure
	// evicts it — which is not what I7 promises.
	sh.sinceSwep++
	if sh.sinceSwep >= sweepEvery {
		sh.sinceSwep = 0
		sh.window.Sweep(nowUS)
	}
	obs.StateEntries.WithLabelValues(s.route).Set(float64(sh.window.Len()))

	if count < threshold {
		return count, false, false
	}
	// Thresholds can be crossed deliberately, so an unthrottled alert path is a
	// remotely triggerable flood against whatever sits downstream.
	if !sh.throttle.Allow(key, nowUS) {
		obs.AlertsThrottled.WithLabelValues(s.route).Inc()
		return count, false, false
	}
	return count, false, true
}
