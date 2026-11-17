package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestHTTPSink(t *testing.T) {
	for _, status := range []int{200, 204, 400, 401, 429, 500, 302} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.Header.Get("Content-Type") != "application/stream+json" {
					t.Error("request headers")
				}
				u, p, ok := r.BasicAuth()
				if !ok || u != "user" || p != "password" {
					t.Error("basic auth")
				}
				body, _ := io.ReadAll(r.Body)
				if !strings.HasSuffix(string(body), "\n") {
					t.Error("missing newline")
				}
				var record Record
				if e := json.Unmarshal(body, &record); e != nil {
					t.Error(e)
				}
				w.Header().Set("Location", "/redirect")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(strings.Repeat("x", 5000)))
			}))
			defer server.Close()
			c := testConfig(t)
			c.SinkURL = server.URL
			c.SinkUsername = "user"
			c.SinkPassword = "password"
			sink := NewHTTPSink(c)
			defer sink.Close()
			err := sink.Send(context.Background(), Record{"event.id": "abc"})
			if status >= 200 && status < 300 {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				var e *HTTPStatusError
				if !errors.As(err, &e) || e.Status != status || !strings.Contains(err.Error(), "HTTP status") {
					t.Fatal(err)
				}
			}
		})
	}
	c := testConfig(t)
	sink := NewHTTPSink(c)
	defer sink.Close()
	if e := sink.Send(context.Background(), Record{"bad": make(chan int)}); e == nil {
		t.Fatal("marshal error")
	}
	sink.endpoint = ":bad"
	if e := sink.Send(context.Background(), Record{}); e == nil {
		t.Fatal("URL error")
	}
	sink.endpoint = "http://localhost:1/?password=secret"
	e := sink.Send(context.Background(), Record{})
	if e == nil || strings.Contains(e.Error(), "secret") {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e = sink.Send(ctx, Record{}); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

type sinkFunc func(context.Context, Record) error

func (f sinkFunc) Send(ctx context.Context, r Record) error { return f(ctx, r) }
func quietLog() *slog.Logger                                { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func TestProcessorRetries(t *testing.T) {
	for _, tc := range []struct {
		name                                string
		errs                                []error
		wantCalls, wantSuccess, wantRetries int
		cancelWait                          bool
	}{
		{"success", nil, 1, 1, 0, false}, {"transient", []error{&HTTPStatusError{429}, &HTTPStatusError{503}}, 3, 1, 2, false},
		{"permanent", []error{&HTTPStatusError{401}}, 1, 0, 0, false}, {"network", []error{errors.New("network")}, 2, 1, 1, false},
		{"exhausted", []error{errors.New("network"), errors.New("network"), errors.New("network")}, 3, 0, 2, false},
		{"cancelled", []error{context.Canceled}, 1, 0, 0, false}, {"wait_cancelled", []error{errors.New("network")}, 1, 0, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig(t)
			c.MaxRetries = 2
			m, s := NewMetrics(), &State{}
			calls := 0
			p := NewProcessor(c, sinkFunc(func(ctx context.Context, r Record) error {
				if _, ok := ctx.Deadline(); !ok {
					t.Error("missing deadline")
				}
				calls++
				if calls <= len(tc.errs) {
					return tc.errs[calls-1]
				}
				return nil
			}), m, s, quietLog())
			p.wait = func(context.Context, time.Duration) error {
				if tc.cancelWait {
					return context.Canceled
				}
				return nil
			}
			p.jitter = func() float64 { return 1 }
			r := Record{"event.id": "id"}
			p.Process(context.Background(), r)
			if calls != tc.wantCalls || int(testutil.ToFloat64(m.Processed)) != tc.wantSuccess || int(testutil.ToFloat64(m.Retries)) != tc.wantRetries {
				t.Fatalf("calls=%d success=%v retries=%v", calls, testutil.ToFloat64(m.Processed), testutil.ToFloat64(m.Retries))
			}
			if tc.wantSuccess == 1 {
				p.Process(context.Background(), r)
				if calls != tc.wantCalls || testutil.ToFloat64(m.Duplicates) != 1 {
					t.Fatal("dedup")
				}
			} else {
				if testutil.ToFloat64(m.Failures) != 1 {
					t.Fatal("failure metric")
				}
				if !p.dedup.begin("id") {
					t.Fatal("failed export cached")
				}
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := testConfig(t)
	m := NewMetrics()
	p := NewProcessor(c, sinkFunc(func(context.Context, Record) error { return errors.New("failure") }), m, &State{}, quietLog())
	p.Process(ctx, Record{"event.id": "id"})
	if testutil.ToFloat64(m.Retries) != 0 {
		t.Fatal("retry on cancelled context")
	}
}

func TestBackoffAndWait(t *testing.T) {
	for _, tc := range []struct {
		initial, max time.Duration
		attempt      int
		jitter       float64
		want         time.Duration
	}{{10, 100, 0, 1, 10}, {10, 100, 1, 1, 20}, {10, 100, 9, 1, 100}, {200, 100, 0, 1, 100}, {10, 100, 0, 0, 5}} {
		if got := Backoff(tc.initial, tc.max, tc.attempt, tc.jitter); got != tc.want {
			t.Fatal(got, tc.want)
		}
	}
	if e := waitContext(context.Background(), time.Nanosecond); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := waitContext(ctx, time.Hour); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestDedupEvictionAndConcurrentProcessing(t *testing.T) {
	d := newDeduplicator(2)
	for _, id := range []string{"a", "b"} {
		if !d.begin(id) {
			t.Fatal(id)
		}
		d.finish(id, true)
	}
	if d.begin("a") {
		t.Fatal("duplicate")
	}
	d.begin("c")
	d.finish("c", true)
	if !d.begin("b") {
		t.Fatal("LRU did not evict")
	}
	d.finish("b", false)
	c := testConfig(t)
	m := NewMetrics()
	started, release := make(chan struct{}), make(chan struct{})
	p := NewProcessor(c, sinkFunc(func(context.Context, Record) error { close(started); <-release; return nil }), m, &State{}, quietLog())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); p.Process(context.Background(), Record{"event.id": "same"}) }()
	<-started
	p.Process(context.Background(), Record{"event.id": "same"})
	close(release)
	wg.Wait()
	if testutil.ToFloat64(m.Duplicates) != 1 || testutil.ToFloat64(m.Processed) != 1 {
		t.Fatal("inflight dedup")
	}
}

func TestProbesAndMetrics(t *testing.T) {
	m, s := NewMetrics(), &State{}
	h := Handler(m, s)
	check := func(path string, status int) {
		t.Helper()
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest("GET", path, nil))
		if r.Code != status {
			t.Fatal(path, r.Code)
		}
	}
	check("/healthz", 200)
	check("/readyz", 503)
	s.synced.Store(true)
	check("/readyz", 503)
	s.operational.Store(true)
	check("/readyz", 200)
	s.stopping.Store(true)
	check("/readyz", 503)
	check("/metrics", 200)
	check("/unknown", 404)
}
