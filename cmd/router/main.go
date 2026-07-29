// Command router runs the Jetstream event router.
//
// One process: an ingestor that owns the socket, a pure classifier and router,
// and one bounded queue plus dedicated worker pool per event type.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shalevsharfsh/jetstream-router/internal/config"
	"github.com/shalevsharfsh/jetstream-router/internal/event"
	"github.com/shalevsharfsh/jetstream-router/internal/handler"
	"github.com/shalevsharfsh/jetstream-router/internal/jetstream"
	"github.com/shalevsharfsh/jetstream-router/internal/obs"
	"github.com/shalevsharfsh/jetstream-router/internal/route"
	"github.com/shalevsharfsh/jetstream-router/internal/routing"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", os.Getenv("CONFIG_PATH"), "path to config JSON")
	flag.Parse()

	log := obs.Logger()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Error("invalid configuration", "error", err.Error())
		os.Exit(2)
	}

	if err := run(cfg, log); err != nil {
		log.Error("exited with error", "error", err.Error())
		os.Exit(1)
	}
}

func run(cfg config.Config, log *slog.Logger) error {
	router, err := routing.New(cfg.Routing)
	if err != nil {
		return err
	}

	health := obs.NewHealth()
	admin := obs.Serve(cfg.Admin.Addr, health, log)

	// SIGTERM arrives on every rolling deploy. The ordered drain below is what
	// makes those invisible downstream.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	routes, err := buildRoutes(ctx, cfg, router, log)
	if err != nil {
		return err
	}

	inCfg := ingestConfig(cfg, router)
	cursor := jetstream.NewCursor(cfg.Jetstream.CursorPath)
	in := jetstream.New(inCfg, cursor, &dispatcher{router: router, routes: routes, log: log}, health, log)

	log.Info("starting",
		"routes", router.Routes(),
		"server_filter", len(inCfg.WantedCollections) > 0,
		"admin_addr", cfg.Admin.Addr)

	done := make(chan struct{})
	go func() { in.Run(ctx); close(done) }()

	<-ctx.Done()
	log.Info("shutdown signal received; draining")

	// Order matters (I5). The ingestor stops reading first, so nothing new
	// arrives; then routes drain against a deadline; then the cursor is
	// committed last, so a restart replays a bounded overlap rather than
	// opening a gap.
	<-done
	for _, r := range routes {
		r.Drain(cfg.Shutdown.D())
	}
	if err := cursor.Commit(); err != nil {
		log.Error("final cursor commit failed", "error", err.Error())
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = admin.Shutdown(shutdownCtx)

	log.Info("stopped")
	return nil
}

// buildRoutes constructs exactly the routes the table can reach, and no others.
func buildRoutes(ctx context.Context, cfg config.Config, router *routing.Router,
	log *slog.Logger) (map[string]*route.Route, error) {

	sink := handler.LogSink{Log: log}
	limits := handler.Limits{
		MaxKeysPerShard: cfg.State.MaxKeysPerShard,
		DedupUS:         cfg.State.DedupWindow.D().Microseconds(),
	}

	handlers := map[string]route.Handler{
		"content": handler.NewContent(sink, cfg.Keywords, cfg.Languages),
		"engagement": handler.NewEngagement(sink, cfg.Routes["engagement"].Workers,
			cfg.Thresholds.Engagement, cfg.Thresholds.EngagementWindow.D().Microseconds(), limits),
		"social-graph": handler.NewSocialGraph(sink, cfg.Routes["social-graph"].Workers,
			cfg.Thresholds.FollowBurst, cfg.Thresholds.FollowWindow.D().Microseconds(), limits),
		"retraction": handler.NewRetraction(sink),
		"default":    handler.NewDefault(log),
	}

	routes := map[string]*route.Route{}
	for _, name := range router.Routes() {
		h, ok := handlers[name]
		if !ok {
			return nil, errNoHandler(name)
		}
		rc := cfg.Routes[name]
		rc.Name = name
		r := route.New(rc, h, log)
		r.Start(ctx)
		routes[name] = r
	}
	return routes, nil
}

type errNoHandler string

func (e errNoHandler) Error() string {
	return "routing table names route " + string(e) + " but no handler is registered for it"
}

func ingestConfig(cfg config.Config, router *routing.Router) jetstream.Config {
	wanted := cfg.Jetstream.WantedCollections
	// "derive" asks the router for the collections its own table names, so the
	// server-side filter can never drift out of sync with the routing table.
	if len(wanted) == 1 && wanted[0] == "derive" {
		wanted = router.Collections()
	}
	return jetstream.Config{
		URL:               cfg.Jetstream.URL,
		WantedCollections: wanted,
		ReplayRewind:      cfg.Jetstream.ReplayRewind.D(),
		ReplayWindow:      cfg.Jetstream.ReplayWindow.D(),
		LiveThreshold:     cfg.Jetstream.LiveThreshold.D(),
		MaxLag:            cfg.Jetstream.MaxLag.D(),
		MaxFrameBytes:     cfg.Jetstream.MaxFrameBytes,
		BackoffMax:        cfg.Jetstream.BackoffMax.D(),
		CommitEvery:       cfg.Jetstream.CommitEvery.D(),
	}
}

// dispatcher resolves a classified event to its route and offers it.
type dispatcher struct {
	router *routing.Router
	routes map[string]*route.Route
	log    *slog.Logger
}

func (d *dispatcher) Dispatch(ctx context.Context, ev event.Event) bool {
	name := d.router.Route(ev.Key)
	r, ok := d.routes[name]
	if !ok {
		// Validate() makes this unreachable. If it ever happens, count it rather
		// than panicking on the ingest goroutine.
		obs.EventsDropped.WithLabelValues("unknown").Inc()
		return false
	}

	// Debug rather than Info: at firehose rates this is several hundred lines a
	// second, and a log line per event is a denial-of-service on your own log
	// pipeline. The per-route counters carry the same information at a rate a
	// human can actually read.
	d.log.Debug("routed", "route", name,
		"collection", ev.Key.Collection, "op", ev.Key.Operation)

	return r.Offer(ctx, ev)
}
