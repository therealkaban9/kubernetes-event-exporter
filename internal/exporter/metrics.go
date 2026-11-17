package exporter

import (
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry                                                                    *prometheus.Registry
	Processed, Failures, QueueDrops, Retries, Duplicates, Filtered, WatchErrors prometheus.Counter
	Duration                                                                    prometheus.Histogram
	QueueDepth                                                                  prometheus.Gauge
}

func NewMetrics() *Metrics {
	m := &Metrics{registry: prometheus.NewRegistry()}
	for name, p := range map[string]*prometheus.Counter{"processed_total": &m.Processed, "processing_failures_total": &m.Failures, "queue_drops_total": &m.QueueDrops, "retries_total": &m.Retries, "duplicates_total": &m.Duplicates, "filtered_total": &m.Filtered, "kubernetes_watch_errors_total": &m.WatchErrors} {
		*p = prometheus.NewCounter(prometheus.CounterOpts{Name: "kubernetes_event_exporter_" + name, Help: "Number of " + name + "."})
		m.registry.MustRegister(*p)
	}
	m.Duration = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "kubernetes_event_exporter_processing_duration_seconds", Help: "Event export duration including retries.", Buckets: prometheus.DefBuckets})
	m.QueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{Name: "kubernetes_event_exporter_queue_depth", Help: "Events waiting for export."})
	m.registry.MustRegister(m.Duration, m.QueueDepth)
	return m
}

type State struct{ synced, operational, stopping atomic.Bool }

func (s *State) Ready() bool { return s.synced.Load() && s.operational.Load() && !s.stopping.Load() }

func Handler(m *Metrics, s *State) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !s.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	return mux
}
