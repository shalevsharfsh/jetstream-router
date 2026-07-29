package handler

import (
	"context"
	"regexp"
	"strings"

	"github.com/shalevsharfsh/jetstream-router/internal/event"
)

// Content matches posts against a configured keyword and language set.
//
// Stateless, so it needs no sharding. Latency-sensitive by nature: a
// notification that arrives ten minutes late is worth much less than one that
// arrives now.
type Content struct {
	sink     Sink
	patterns []keyword
	langs    map[string]bool
}

type keyword struct {
	word string
	re   *regexp.Regexp
}

// NewContent compiles the keyword set once at startup.
//
// The boundary is a lookaround pair, not \b. \b is a *transition* between a word
// and a non-word character, so `\bc\+\+\b` can never match — the character after
// '+' would have to be a word character. The lookaround form asks the question
// actually meant, "not glued to a word", and handles c++, .NET and ai alike.
//
// Plain substring matching was the obvious first implementation and it is wrong
// in a way that only shows up against real data: with "ai" in the list it fires
// on said, again, email and Dubai. On a notification path that is worse than not
// matching at all, because it trains whoever receives the alerts to ignore them.
//
// Known limitation: word characters are undefined for scripts that do not
// delimit words with spaces. The language filter is the honest mitigation.
func NewContent(sink Sink, keywords, langs []string) *Content {
	c := &Content{sink: sink, langs: map[string]bool{}}
	for _, l := range lower(langs) {
		c.langs[l] = true
	}
	for _, w := range keywords {
		w = strings.TrimSpace(w)
		if w == "" {
			continue
		}
		re := regexp.MustCompile(`(?i)(?:^|[^\p{L}\p{N}_])` +
			regexp.QuoteMeta(w) + `(?:[^\p{L}\p{N}_]|$)`)
		c.patterns = append(c.patterns, keyword{word: strings.ToLower(w), re: re})
	}
	return c
}

// Handle matches one post.
func (c *Content) Handle(_ context.Context, _ int, ev event.Event) error {
	// Deletes route to retraction, so one arriving here means the routing table
	// and this handler disagree.
	if ev.Key.Operation == event.OpDelete {
		return nil
	}

	var rec postRecord
	if !decode(ev.Record, &rec) || rec.Text == "" {
		return nil
	}

	if len(c.langs) > 0 {
		ok := false
		for _, l := range lower(rec.Langs) {
			if c.langs[l] {
				ok = true
				break
			}
		}
		if !ok {
			return nil
		}
	}

	var hit []string
	for _, k := range c.patterns {
		if k.re.MatchString(rec.Text) {
			hit = append(hit, k.word)
		}
	}
	if len(hit) == 0 {
		return nil
	}

	// The text itself is deliberately absent: its length, not its content.
	c.sink.Alert("info", "content", "keyword matched",
		"did", ev.DID, "rkey", ev.RKey,
		"matched", hit, "langs", rec.Langs, "text_len", len(rec.Text))
	return nil
}
