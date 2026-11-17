package exporter

import (
	"container/list"
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

type deduplicator struct {
	mu       sync.Mutex
	capacity int
	done     map[string]*list.Element
	pending  map[string]bool
	order    *list.List
}

func newDeduplicator(capacity int) *deduplicator {
	return &deduplicator{capacity: capacity, done: map[string]*list.Element{}, pending: map[string]bool{}, order: list.New()}
}
func (d *deduplicator) begin(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el := d.done[id]; el != nil {
		d.order.MoveToBack(el)
		return false
	}
	if d.pending[id] {
		return false
	}
	d.pending[id] = true
	return true
}
func (d *deduplicator) finish(id string, success bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.pending, id)
	if !success {
		return
	}
	d.done[id] = d.order.PushBack(id)
	if d.order.Len() > d.capacity {
		el := d.order.Front()
		delete(d.done, el.Value.(string))
		d.order.Remove(el)
	}
}

type Processor struct {
	cfg     Config
	sink    Sink
	metrics *Metrics
	state   *State
	log     *slog.Logger
	dedup   *deduplicator
	wait    func(context.Context, time.Duration) error
	jitter  func() float64
}

func NewProcessor(c Config, s Sink, m *Metrics, state *State, log *slog.Logger) *Processor {
	return &Processor{cfg: c, sink: s, metrics: m, state: state, log: log, dedup: newDeduplicator(c.DedupCapacity), wait: waitContext, jitter: rand.Float64}
}

func waitContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryable(err error) bool {
	var status *HTTPStatusError
	if errors.As(err, &status) {
		return status.Status == 429 || status.Status >= 500
	}
	return !errors.Is(err, context.Canceled)
}

func (p *Processor) Process(ctx context.Context, r Record) {
	id := r["event.id"].(string)
	if !p.dedup.begin(id) {
		p.metrics.Duplicates.Inc()
		return
	}
	success := false
	defer func() { p.dedup.finish(id, success) }()
	start := time.Now()
	defer func() { p.metrics.Duration.Observe(time.Since(start).Seconds()) }()
	var err error
	for attempt := 0; attempt <= p.cfg.MaxRetries; attempt++ {
		requestCtx, cancel := context.WithTimeout(ctx, p.cfg.ExportTimeout)
		err = p.sink.Send(requestCtx, r)
		cancel()
		if err == nil {
			success = true
			p.metrics.Processed.Inc()
			p.state.operational.Store(true)
			return
		}
		p.state.operational.Store(false)
		if !retryable(err) || attempt == p.cfg.MaxRetries || ctx.Err() != nil {
			break
		}
		p.metrics.Retries.Inc()
		if err = p.wait(ctx, Backoff(p.cfg.RetryInitialDelay, p.cfg.RetryMaxDelay, attempt, p.jitter())); err != nil {
			break
		}
	}
	p.metrics.Failures.Inc()
	p.log.Error("event export failed", "namespace", r["kubernetes.namespace_name"], "event_reason", r["event.reason"], "error", err)
}
