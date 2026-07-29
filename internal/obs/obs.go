// Package obs holds logging, metrics and the health endpoints.
//
// Metric labels are a fixed, enumerable set. Route names come from
// configuration and are therefore bounded; collection names come off a public
// firehose and are NOT — anyone can publish records under a lexicon they
// invented, so using a collection as a label would let a stranger grow the
// metric space without limit and exhaust the metrics backend during exactly the
// incident you needed it for. Unknown collections are counted under one bucket.
package obs

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	EventsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "events_received_total",
		Help: "Frames read off the Jetstream WebSocket.",
	})
	EventsMalformed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "events_malformed_total",
		Help: "Frames that could not be classified. Distinct from events routed to " +
			"the default route: this one means upstream schema drift.",
	})
	EventsRouted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "events_routed_total",
		Help: "Events enqueued onto a route.",
	}, []string{"route"})
	EventsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "events_dropped_total",
		Help: "Events shed because a route's buffer was full. Per D2 the cursor has " +
			"already advanced past these, so they are gone for good — this counter is " +
			"the only record that they existed.",
	}, []string{"route"})
	EventsBlocked = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_blocked_seconds_total",
		Help: "Seconds the ingest goroutine spent blocked on a route configured with " +
			"the block policy. Non-zero means one route is dictating stream throughput.",
	}, []string{"route"})

	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "route_queue_depth",
		Help: "Events currently buffered on a route.",
	}, []string{"route"})
	QueueCapacity = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "route_queue_capacity",
		Help: "Configured buffer size of a route.",
	}, []string{"route"})

	Handled = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "events_handled_total",
		Help: "Events processed by a route's workers, by outcome.",
	}, []string{"route", "outcome"})
	HandlerSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "handler_seconds",
		Help:    "Per-event handler latency.",
		Buckets: []float64{.0005, .001, .005, .01, .05, .1, .5, 1, 5},
	}, []string{"route"})
	HandlerPanics = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "handler_panics_total",
		Help: "Panics recovered inside a worker. Without recovery each of these would " +
			"have terminated the process and taken every other route with it.",
	}, []string{"route"})
	DeadLettered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dead_lettered_total",
		Help: "Events abandoned after a permanent error or an exhausted retry budget.",
	}, []string{"route", "reason"})
	Duplicates = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duplicates_suppressed_total",
		Help: "Events suppressed by the dedup window (D7). Expected to be non-zero " +
			"after every reconnect.",
	}, []string{"route"})

	Alerts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "alerts_total",
		Help: "Downstream work actually triggered — the point of the service.",
	}, []string{"route", "alert"})
	AlertsThrottled = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "alerts_throttled_total",
		Help: "Alerts suppressed by the per-route rate limit. Thresholds can be crossed " +
			"deliberately, so an unthrottled alert path is a remotely triggerable flood.",
	}, []string{"route"})

	StateEntries = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "state_entries",
		Help: "Live entries in a route's aggregation state. Bounded like the buffers are.",
	}, []string{"route"})
	StateEvicted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "state_evicted_total",
		Help: "State entries evicted by the size or TTL cap.",
	}, []string{"route", "reason"})

	ConnState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "connection_state",
		Help: "1 for the ingestor's current state, 0 otherwise.",
	}, []string{"state"})
	Reconnects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "reconnects_total",
		Help: "WebSocket reconnect attempts.",
	})
	CursorUS = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cursor_time_us",
		Help: "Last committed cursor position.",
	})
	CursorLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cursor_lag_seconds",
		Help: "Wall clock minus the last enqueued event's timestamp. The most useful " +
			"single number here: 'are we keeping up?' asked directly. Gates readiness.",
	})
	ReplayGap = promauto.NewCounter(prometheus.CounterOpts{
		Name: "replay_window_lapsed_total",
		Help: "Times the stored cursor was older than the server's replay window. Each " +
			"one is an unrecoverable gap of unknown size.",
	})
)

// Logger is the process logger. JSON to stdout: the collector owns shipping.
//
// Record text never reaches a log line. Post bodies are attacker-controlled, so
// writing them out hands an adversary partial control of whatever aggregator or
// SIEM ingests these logs — forged entries, broken parsers, injected fields.
// Only structural fields and derived values are logged: lengths, languages,
// whether a match occurred.
func Logger() *slog.Logger {
	level := slog.LevelInfo
	if v := os.Getenv("LOG_LEVEL"); v == "debug" || v == "DEBUG" {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

// Health tracks liveness and readiness, which mean different things.
//
// Liveness answers "is this process wedged" and a failure gets the pod killed.
// Readiness answers "should work be reaching me right now". An ingestor that has
// lost its upstream is un-ready but perfectly alive: failing liveness there
// would have Kubernetes restart the pod at precisely the moment the reconnect
// backoff is doing its job (D5).
type Health struct {
	live    atomic.Bool
	ready   atomic.Bool
	detail  atomic.Value // string
	started time.Time
}

func NewHealth() *Health {
	h := &Health{started: time.Now()}
	h.live.Store(true)
	h.detail.Store("starting")
	return h
}

func (h *Health) SetReady(ready bool, detail string) {
	h.ready.Store(ready)
	h.detail.Store(detail)
}

func (h *Health) Detail() string {
	s, _ := h.detail.Load().(string)
	return s
}

// Serve exposes /healthz, /readyz and /metrics on one port.
func Serve(addr string, h *Health, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if h.live.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if h.ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(h.Detail() + "\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(h.Detail() + "\n"))
	})

	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("admin server stopped", "error", err)
		}
	}()
	return srv
}

var (
	// Retractions counts deletions by collection and whether their aggregate
	// impact could be resolved. A high "no-index" rate is the visible cost of
	// not maintaining a record->subject index; see handler.Retraction.
	Retractions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "retractions_total",
		Help: "Deletions processed, by collection and resolvability.",
	}, []string{"collection", "resolution"})

	// Unrouted separates the two reasons an event reaches the default route.
	// They imply different responses: non-commit-kind is expected forever,
	// while unmapped-collection is the signal that a new lexicon may deserve
	// its own route.
	Unrouted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "unrouted_total",
		Help: "Events that reached the default route, by reason.",
	}, []string{"reason"})
)
