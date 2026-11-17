package exporter

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// EventProcessor consumes immutable event snapshots and honors cancellation.
type EventProcessor interface{ Process(context.Context, Record) }

// EventController owns event producers and their graceful shutdown.
type EventController interface{ Run(context.Context) error }

type controllerFunc func(context.Context) error

func (f controllerFunc) Run(ctx context.Context) error { return f(ctx) }

type Application struct {
	cfg        Config
	client     kubernetes.Interface
	sink       Sink
	log        *slog.Logger
	Metrics    *Metrics
	State      *State
	queue      chan Record
	processor  EventProcessor
	controller EventController
}

func NewApplication(c Config, client kubernetes.Interface, sink Sink, log *slog.Logger) *Application {
	m, s := NewMetrics(), &State{}
	s.operational.Store(true)
	a := &Application{cfg: c, client: client, sink: sink, log: log, Metrics: m, State: s, queue: make(chan Record, c.QueueCapacity)}
	a.processor = NewProcessor(c, sink, m, s, log)
	a.controller = controllerFunc(a.runController)
	return a
}

// enqueue runs on the informer handler and never blocks on the storage sink.
func (a *Application) enqueue(obj any) {
	e, ok := obj.(*corev1.Event)
	if !ok {
		return
	}
	if a.cfg.MaxEventAge > 0 {
		stamp := eventTimestamp(e)
		if !stamp.IsZero() && time.Since(stamp) > a.cfg.MaxEventAge {
			a.Metrics.Filtered.Inc()
			return
		}
	}
	r := Normalize(e, a.cfg, time.Now())
	a.Metrics.QueueDepth.Inc()
	select {
	case a.queue <- r:
	default:
		a.Metrics.QueueDepth.Dec()
		a.Metrics.QueueDrops.Inc()
		a.log.Warn("event queue full; snapshot dropped", "namespace", e.Namespace, "event_reason", e.Reason)
	}
}

func (a *Application) runController(ctx context.Context) error {
	stop := make(chan struct{})
	namespaces := a.cfg.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{corev1.NamespaceAll}
	}
	factories := make([]informers.SharedInformerFactory, 0, len(namespaces))
	synced := make([]cache.InformerSynced, 0, len(namespaces))
	for _, ns := range namespaces {
		factory := informers.NewSharedInformerFactoryWithOptions(a.client, 0, informers.WithNamespace(ns))
		informer := factory.Core().V1().Events().Informer()
		_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: a.enqueue, UpdateFunc: func(old, new any) {
			before, bok := old.(*corev1.Event)
			after, aok := new.(*corev1.Event)
			if bok && aok && before.ResourceVersion == after.ResourceVersion {
				return
			}
			a.enqueue(new)
		}})
		if err != nil {
			return fmt.Errorf("register event handler: %w", err)
		}
		if err = informer.SetWatchErrorHandlerWithContext(func(_ context.Context, _ *cache.Reflector, err error) {
			a.Metrics.WatchErrors.Inc()
			a.log.Warn("Kubernetes event watch failed", "namespace", ns, "error", err)
		}); err != nil {
			return fmt.Errorf("register watch error handler: %w", err)
		}
		factories = append(factories, factory)
		synced = append(synced, informer.HasSynced)
	}
	processing, cancel := context.WithCancel(context.Background())
	defer cancel()
	var workers sync.WaitGroup
	for i := 0; i < a.cfg.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for r := range a.queue {
				a.Metrics.QueueDepth.Dec()
				if processing.Err() != nil {
					a.Metrics.ShutdownDrops.Inc()
					continue
				}
				a.processor.Process(processing, r)
			}
		}()
	}
	for _, f := range factories {
		f.Start(stop)
	}
	if cache.WaitForCacheSync(ctx.Done(), synced...) {
		a.State.synced.Store(true)
		a.log.Info("Kubernetes event caches synchronized", "namespace", a.cfg.Namespaces)
	}
	<-ctx.Done()
	a.State.stopping.Store(true)
	close(stop)
	for _, f := range factories {
		f.Shutdown()
	}
	close(a.queue)
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	timer := time.NewTimer(a.cfg.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		cancel()
		// Sink's contract requires honoring context cancellation. Join workers
		// before releasing sink resources; queued snapshots are discarded locally.
		<-done
		return fmt.Errorf("event drain exceeded shutdown timeout")
	}
}

// Run serves probes/metrics while observing Events, then drains on cancellation.
// A single Application instance runs once; listener ownership transfers to Run.
func (a *Application) Run(ctx context.Context, listener net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	server := &http.Server{Handler: Handler(a.Metrics, a.State), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.Serve(listener) }()
	controllerDone := make(chan error, 1)
	go func() { controllerDone <- a.controller.Run(ctx) }()
	var err error
	controllerStopped := false
	select {
	case <-ctx.Done():
	case err = <-serverErrors:
		cancel()
	case err = <-controllerDone:
		controllerStopped = true
		cancel()
	}
	a.State.stopping.Store(true)
	shutdownCtx, stop := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer stop()
	if !controllerStopped {
		if controllerErr := <-controllerDone; controllerErr != nil {
			err = controllerErr
		}
	}
	if e := server.Shutdown(shutdownCtx); e != nil {
		_ = server.Close()
		if err == nil {
			err = fmt.Errorf("shutdown HTTP server: %w", e)
		}
	}
	if closer, ok := a.sink.(interface{ Close() }); ok {
		closer.Close()
	}
	return err
}
