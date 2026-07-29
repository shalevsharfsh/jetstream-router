// Package config is the config struct, loaded from the ConfigMap.
//
// Everything an operator might plausibly want to change during an incident —
// buffer sizes, drop policies, thresholds, the routing table itself — is
// configuration. Nothing here is a framework: the surface is small enough that
// a struct, a JSON file and one flag beat a dependency.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/shalevsharfsh/jetstream-router/internal/route"
	"github.com/shalevsharfsh/jetstream-router/internal/routing"
)

// Config is the whole runtime configuration.
type Config struct {
	Jetstream  Jetstream               `json:"jetstream"`
	Admin      Admin                   `json:"admin"`
	Routing    routing.Table           `json:"routing"`
	Routes     map[string]route.Config `json:"routes"`
	Keywords   []string                `json:"keywords"`
	Languages  []string                `json:"languages"`
	Thresholds Thresholds              `json:"thresholds"`
	State      StateLimits             `json:"state"`
	Shutdown   Duration                `json:"shutdown_timeout"`
}

type Jetstream struct {
	URL string `json:"url"`
	// WantedCollections is the server-side filter. Empty by default and
	// deliberately so: filtering at the source is legitimate load shedding, but
	// discarding the type mix removes the problem this service exists to solve,
	// and removes any ability to observe what was dropped.
	//
	// The literal "derive" asks the router for the concrete collections its
	// table names, so the filter cannot drift out of sync with what we can route.
	WantedCollections []string `json:"wantedCollections"`

	CursorPath    string   `json:"cursor_path"`
	ReplayRewind  Duration `json:"replay_rewind"`
	ReplayWindow  Duration `json:"replay_window"`
	LiveThreshold Duration `json:"live_threshold"`
	MaxLag        Duration `json:"max_lag"`
	MaxFrameBytes int64    `json:"max_frame_bytes"`
	IdleTimeout   Duration `json:"idle_timeout"`
	BackoffMax    Duration `json:"backoff_max"`
	CommitEvery   Duration `json:"commit_every"`
}

type Admin struct {
	Addr string `json:"addr"`
}

// Thresholds are the alerting rules for the stateful routes.
type Thresholds struct {
	Engagement       int      `json:"engagement"`
	EngagementWindow Duration `json:"engagement_window"`
	FollowBurst      int      `json:"followBurst"`
	FollowWindow     Duration `json:"followBurst_window"`
}

// StateLimits bounds the aggregation state (I7).
//
// Capping the channels while leaving these unbounded would only move the
// exhaustion target: unlimited unique subject keys is a straightforward
// memory-exhaustion vector, and subject keys come off a public firehose.
type StateLimits struct {
	MaxKeysPerShard int      `json:"max_keys_per_shard"`
	DedupWindow     Duration `json:"dedup_window"`
}

// Duration is a JSON-friendly time.Duration ("30s", "5m").
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// D returns the underlying duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Default is what the service runs with when nothing is supplied, so the binary
// is runnable with no setup.
//
// The per-route numbers are deliberately NOT uniform, and the buffer sizes are
// derived rather than chosen: a buffer is time, not space, and each is sized for
// roughly 60 seconds of tolerance at twice its measured arrival rate. See
// deploy/configmap.yaml for the table and DESIGN.md D1 for the reasoning.
func Default() Config {
	return Config{
		Jetstream: Jetstream{
			URL:           "wss://jetstream2.us-east.bsky.network/subscribe",
			CursorPath:    "/var/lib/jetstream-router/cursor.json",
			ReplayRewind:  Duration(5 * time.Second),
			ReplayWindow:  Duration(10 * time.Minute),
			LiveThreshold: Duration(3 * time.Second),
			MaxLag:        Duration(60 * time.Second),
			MaxFrameBytes: 1 << 20, // every field on this stream is attacker-controlled
			// The firehose never goes quiet for this long; silence means the
			// connection is half-open whatever the socket reports.
			IdleTimeout: Duration(30 * time.Second),
			BackoffMax:  Duration(30 * time.Second),
			CommitEvery: Duration(time.Second),
		},
		Admin: Admin{Addr: ":8080"},
		Routing: routing.Table{
			Rules: []routing.Rule{
				// Operation-first: this rule is evaluated before any collection
				// rule regardless of where it appears here (see routing.Router).
				{Match: routing.Match{Kind: "commit", Operation: "delete"}, Route: "retraction"},
				{Match: routing.Match{Kind: "commit", Collection: "app.bsky.feed.post"}, Route: "content"},
				{Match: routing.Match{Kind: "commit", Collection: "app.bsky.feed.like"}, Route: "engagement"},
				{Match: routing.Match{Kind: "commit", Collection: "app.bsky.feed.repost"}, Route: "engagement"},
				{Match: routing.Match{Kind: "commit", Collection: "app.bsky.graph.follow"}, Route: "social-graph"},
			},
			Fallback: "default",
		},
		Routes: map[string]route.Config{
			"content":      {Buffer: 4096, Workers: 4, Policy: route.PolicyDrop},
			"engagement":   {Buffer: 32768, Workers: 8, Policy: route.PolicyDrop},
			"social-graph": {Buffer: 2048, Workers: 2, Policy: route.PolicyDrop},
			// Deletions are cleanup and compliance work: losing one is worse than
			// delaying one, and the volume is low enough that congestion is
			// implausible. This is the route the block policy exists for.
			"retraction": {Buffer: 2048, Workers: 2, Policy: route.PolicyBlock,
				BlockTimeout: 2 * time.Second},
			"default": {Buffer: 2048, Workers: 1, Policy: route.PolicyDrop},
		},
		Keywords:  []string{"kubernetes", "security", "agent", "bluesky"},
		Languages: []string{"en"},
		Thresholds: Thresholds{
			Engagement:       100,
			EngagementWindow: Duration(5 * time.Minute),
			FollowBurst:      25,
			FollowWindow:     Duration(5 * time.Minute),
		},
		State: StateLimits{
			MaxKeysPerShard: 20000,
			DedupWindow:     Duration(2 * time.Minute),
		},
		Shutdown: Duration(25 * time.Second),
	}
}

// Load reads a JSON config file over the defaults.
func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, cfg.Validate()
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, cfg.Validate()
		}
		return cfg, err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, fmt.Errorf("parse %s: %w", path, err)
	}
	return cfg, cfg.Validate()
}

// Validate fails at startup rather than at 3am.
func (c Config) Validate() error {
	r, err := routing.New(c.Routing)
	if err != nil {
		return err
	}
	for _, name := range r.Routes() {
		if _, ok := c.Routes[name]; !ok {
			// A route the table can produce but which has no config would
			// silently blackhole every event that reaches it.
			return fmt.Errorf("routing table produces route %q with no entry in routes", name)
		}
	}
	if c.Thresholds.Engagement <= 0 || c.Thresholds.FollowBurst <= 0 {
		return fmt.Errorf("thresholds must be positive")
	}
	if c.State.MaxKeysPerShard <= 0 {
		return fmt.Errorf("state.max_keys_per_shard must be positive: every map is bounded")
	}
	return nil
}
