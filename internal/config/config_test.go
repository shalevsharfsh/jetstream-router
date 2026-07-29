package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The ConfigMap is written and reviewed by humans, so durations are strings.
// This broke on first deploy, which is what config validation at startup is for.
func TestDurationsLoadFromStrings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	body := `{
	  "routes": {
	    "content":      {"buffer": 8, "workers": 1, "policy": "drop"},
	    "engagement":   {"buffer": 8, "workers": 1, "policy": "drop"},
	    "social-graph": {"buffer": 8, "workers": 1, "policy": "drop"},
	    "retraction":   {"buffer": 8, "workers": 1, "policy": "block", "block_timeout": "2s"},
	    "default":      {"buffer": 8, "workers": 1, "policy": "drop"}
	  },
	  "thresholds": {"engagement": 10, "engagement_window": "5m",
	                 "followBurst": 5, "followBurst_window": "90s"},
	  "state": {"max_keys_per_shard": 100, "dedup_window": "2m"}
	}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Routes["retraction"].BlockTimeout; got != 2*time.Second {
		t.Errorf("block_timeout = %v, want 2s", got)
	}
	if got := cfg.Thresholds.FollowWindow.D(); got != 90*time.Second {
		t.Errorf("followBurst_window = %v, want 90s", got)
	}
	if got := cfg.State.DedupWindow.D(); got != 2*time.Minute {
		t.Errorf("dedup_window = %v, want 2m", got)
	}
}

// A route the table can produce but which has no config would silently
// blackhole every event that reaches it. Fail at startup, loudly.
func TestValidateRejectsARouteWithNoConfig(t *testing.T) {
	cfg := Default()
	delete(cfg.Routes, "retraction")
	if err := cfg.Validate(); err == nil {
		t.Error("a routing table naming an unconfigured route was accepted")
	}
}

func TestDefaultsAreValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Errorf("Default() is not valid: %v", err)
	}
}

// I7: every map is bounded.
func TestValidateRejectsUnboundedState(t *testing.T) {
	cfg := Default()
	cfg.State.MaxKeysPerShard = 0
	if err := cfg.Validate(); err == nil {
		t.Error("unbounded state was accepted")
	}
}
