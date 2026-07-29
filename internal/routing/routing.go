// Package routing maps an event to a route name.
//
// Everything here is a pure function: an event in, a route name out. No I/O, no
// state, no logging. That is what makes the routing table — the core of the
// exercise — exhaustively testable without standing up a socket, a channel or a
// worker.
//
// The table is configuration rather than code (a ConfigMap), so adding an event
// type to an existing route is a config change. Adding a genuinely new route is
// not, and should not be: a new route implies a buffer, a pool, a drop policy
// and its own metrics.
package routing

import (
	"fmt"
	"path"
	"strings"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
)

// Match is one rule's predicate. An empty field matches anything.
//
// Collection accepts a glob ("app.bsky.feed.*"), because lexicon namespaces are
// hierarchical and a new sibling type usually belongs on the route its siblings
// already use.
type Match struct {
	Kind       string `json:"kind"`
	Collection string `json:"collection"`
	Operation  string `json:"operation"`
}

// Rule binds a predicate to a route.
type Rule struct {
	Match Match  `json:"match"`
	Route string `json:"route"`
}

// Table is the configured routing table.
type Table struct {
	Rules []Rule `json:"rules"`
	// Fallback receives anything unmatched. Required: unknown types must go
	// somewhere (I9).
	Fallback string `json:"fallback"`
}

// Router resolves events to route names.
type Router struct {
	// Rules are split into two passes rather than one ordered list, because
	// operation-first precedence is an invariant of the design (I3) and should
	// not depend on someone keeping the ConfigMap in the right order.
	opRules   []Rule // rules that pin an operation
	collRules []Rule // rules that do not
	fallback  string
}

// New builds a Router from a table.
func New(t Table) (*Router, error) {
	if t.Fallback == "" {
		return nil, fmt.Errorf("routing.fallback must be set: unknown types must go somewhere")
	}
	r := &Router{fallback: t.Fallback}
	for i, rule := range t.Rules {
		if rule.Route == "" {
			return nil, fmt.Errorf("routing.rules[%d] has no route", i)
		}
		if rule.Match.Collection != "" {
			if _, err := path.Match(rule.Match.Collection, "probe"); err != nil {
				return nil, fmt.Errorf("routing.rules[%d]: bad collection glob %q: %w",
					i, rule.Match.Collection, err)
			}
		}
		if rule.Match.Operation != "" {
			r.opRules = append(r.opRules, rule)
		} else {
			r.collRules = append(r.collRules, rule)
		}
	}
	return r, nil
}

// Route resolves an event's key to a route name.
//
// Precedence is operation-first, and that is load-bearing (I3):
//
//  1. Rules that pin an operation are evaluated first. `operation == delete`
//     therefore beats the collection map — a deleted post goes to retraction,
//     not content. This is not stylistic: a delete commit carries no record at
//     all, so a create-path handler would have nothing to match against even if
//     it received one. Getting this backwards is the easiest way to break the
//     routing, which is why the test for it is parametrised across the whole
//     table rather than spot-checked.
//
//  2. Then collection rules, in configured order.
//
//  3. Then the fallback. Nothing is ever silently discarded: new lexicons appear
//     on the network continuously, and discovering one should be an observation,
//     not an outage.
func (r *Router) Route(k event.Key) string {
	for _, rule := range r.opRules {
		if matches(rule.Match, k) {
			return rule.Route
		}
	}
	for _, rule := range r.collRules {
		if matches(rule.Match, k) {
			return rule.Route
		}
	}
	return r.fallback
}

func matches(m Match, k event.Key) bool {
	if m.Kind != "" && m.Kind != k.Kind {
		return false
	}
	if m.Operation != "" && m.Operation != k.Operation {
		return false
	}
	if m.Collection != "" {
		if k.Collection == "" {
			return false
		}
		// path.Match treats '.' as an ordinary character and '*' as "any run of
		// non-separator characters", which is the behaviour wanted for NSIDs.
		ok, err := path.Match(m.Collection, k.Collection)
		if err != nil || !ok {
			return false
		}
	}
	return true
}

// Routes lists every route the table can produce, so the runtime builds exactly
// the routes that are reachable and no others.
func (r *Router) Routes() []string {
	seen := map[string]bool{}
	var out []string
	add := func(name string) {
		if name != "" && !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	for _, rule := range r.opRules {
		add(rule.Route)
	}
	for _, rule := range r.collRules {
		add(rule.Route)
	}
	add(r.fallback)
	return out
}

// Collections lists the concrete (non-glob) collections the table names, for
// Jetstream's server-side filter.
//
// Derived from the table rather than configured beside it, so the filter cannot
// drift out of sync with what we actually know how to route. It is off by
// default: filtering at the source is legitimate load shedding, but discarding
// the type mix removes the problem this service exists to solve, and removes
// any ability to observe what was dropped.
func (r *Router) Collections() []string {
	var out []string
	for _, rule := range r.collRules {
		c := rule.Match.Collection
		if c != "" && !strings.ContainsAny(c, "*?[") {
			out = append(out, c)
		}
	}
	return out
}
