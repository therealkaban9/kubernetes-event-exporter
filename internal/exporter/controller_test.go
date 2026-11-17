package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
)

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func listen(t *testing.T) net.Listener {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	return l
}
func exampleEvent() *corev1.Event {
	return &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "team", UID: types.UID("uid-1"), ResourceVersion: "1", CreationTimestamp: metav1.Now()}, Type: "Warning", Reason: "BackOff", Message: "restart backoff", Count: 1, InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "checkout-abc", UID: "pod-1", Namespace: "team"}}
}

// TestE2EFakeKubernetesToHTTP exercises the real informer, normalization, HTTP
// retry/sink, metrics, readiness and graceful drain without a live cluster.
func TestE2EFakeKubernetesToHTTP(t *testing.T) {
	records := make(chan Record, 10)
	var attempts atomic.Int32
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(w, "temporary", 503)
			return
		}
		var record Record
		if e := json.NewDecoder(r.Body).Decode(&record); e != nil {
			t.Error(e)
		}
		records <- record
		w.WriteHeader(204)
	}))
	defer storage.Close()
	c := testConfig(t)
	c.SinkURL = storage.URL
	c.Namespaces = []string{"team"}
	c.RetryInitialDelay = time.Millisecond
	c.RetryMaxDelay = time.Millisecond
	c.BusinessMappings = map[string]string{"team": "b2c"}
	c.ServiceMappings = map[string]string{"team/Pod/checkout-abc": "checkout"}
	e := exampleEvent()
	outside := e.DeepCopy()
	outside.Namespace = "outside"
	client := fake.NewSimpleClientset(e, outside)
	a := NewApplication(c, client, NewHTTPSink(c), quietLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := listen(t)
	base := "http://" + l.Addr().String()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx, l) }()
	eventually(t, func() bool { return a.State.Ready() && len(records) == 1 })
	first := <-records
	if first["business"] != "b2c" || first["service.name"] != "checkout" || first["event.count"] != float64(1) || first["kubernetes.namespace_name"] != "team" {
		t.Fatal(first)
	}
	resp, err := http.Get(base + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	e.ResourceVersion = "2"
	e.Count = 3
	e.Series = &corev1.EventSeries{Count: 4, LastObservedTime: metav1.NewMicroTime(time.Now())}
	if _, err = client.CoreV1().Events("team").Update(ctx, e, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return len(records) == 1 })
	second := <-records
	if second["event.count"] != float64(4) || first["event.id"] == second["event.id"] {
		t.Fatal(second)
	}
	if _, err = client.CoreV1().Events("team").Update(ctx, e, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = client.CoreV1().Events("team").Delete(ctx, e.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if a.State.Ready() || testutil.ToFloat64(a.Metrics.Processed) != 2 || testutil.ToFloat64(a.Metrics.Retries) != 1 || testutil.ToFloat64(a.Metrics.QueueDepth) != 0 {
		t.Fatal("lifecycle/metrics mismatch")
	}
	if len(records) != 0 {
		t.Fatal("duplicate or forbidden namespace exported")
	}
}

func TestQueueFiltering(t *testing.T) {
	c := testConfig(t)
	c.QueueCapacity = 1
	c.MaxEventAge = time.Minute
	a := NewApplication(c, fake.NewSimpleClientset(), sinkFunc(func(context.Context, Record) error { return nil }), quietLog())
	a.enqueue("not an event")
	old := exampleEvent()
	old.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	a.enqueue(old)
	e := exampleEvent()
	a.enqueue(e)
	a.enqueue(e)
	if len(a.queue) != 1 || testutil.ToFloat64(a.Metrics.QueueDrops) != 1 || testutil.ToFloat64(a.Metrics.Filtered) != 1 {
		t.Fatal("bounded queue/filter")
	}
}

func TestWatchErrorAndReconnect(t *testing.T) {
	c := testConfig(t)
	client := fake.NewSimpleClientset()
	var lists atomic.Int32
	client.PrependReactor("list", "events", func(k8stesting.Action) (bool, runtime.Object, error) {
		if lists.Add(1) == 1 {
			return true, nil, errors.New("transient list error")
		}
		return false, nil, nil
	})
	watches := make(chan *watch.RaceFreeFakeWatcher, 10)
	client.PrependWatchReactor("events", func(k8stesting.Action) (bool, watch.Interface, error) {
		w := watch.NewRaceFreeFake()
		watches <- w
		return true, w, nil
	})
	var processed atomic.Int32
	a := NewApplication(c, client, sinkFunc(func(context.Context, Record) error { processed.Add(1); return nil }), quietLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx, listen(t)) }()
	eventually(t, func() bool { return len(watches) > 0 && a.State.Ready() })
	w := <-watches
	e := exampleEvent()
	w.Add(e)
	eventually(t, func() bool { return processed.Load() == 1 })
	w.Stop()
	eventually(t, func() bool { return len(watches) > 0 })
	next := <-watches
	e = e.DeepCopy()
	e.ResourceVersion = "2"
	e.Count = 2
	next.Modify(e)
	eventually(t, func() bool { return processed.Load() == 2 })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if testutil.ToFloat64(a.Metrics.WatchErrors) < 1 {
		t.Fatal("watch/list error not counted")
	}
}

func TestDrainTimeoutAndUnready(t *testing.T) {
	c := testConfig(t)
	c.ShutdownTimeout = 10 * time.Millisecond
	c.MaxRetries = 0
	c.Workers = 1
	client := fake.NewSimpleClientset(exampleEvent())
	started := make(chan struct{})
	finished := make(chan struct{})
	a := NewApplication(c, client, sinkFunc(func(ctx context.Context, _ Record) error {
		close(started)
		<-ctx.Done()
		close(finished)
		return ctx.Err()
	}), quietLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := listen(t)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx, l) }()
	<-started
	eventually(t, a.State.Ready)
	for _, rv := range []string{"2", "3"} {
		e := exampleEvent()
		e.ResourceVersion = rv
		a.enqueue(e)
	}
	cancel()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "drain") {
		t.Fatal(err)
	}
	<-finished
	if a.State.Ready() {
		t.Fatal("ready during shutdown")
	}
	if testutil.ToFloat64(a.Metrics.ShutdownDrops) != 2 || testutil.ToFloat64(a.Metrics.QueueDepth) != 0 {
		t.Fatal("pending events were not discarded without new sink requests")
	}
}

func TestHTTPShutdownTimeout(t *testing.T) {
	c := testConfig(t)
	c.ShutdownTimeout = 10 * time.Millisecond
	a := NewApplication(c, fake.NewSimpleClientset(), sinkFunc(func(context.Context, Record) error { return nil }), quietLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := listen(t)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx, l) }()
	eventually(t, a.State.Ready)
	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("GET /metrics HTTP/1.1\r\n")); err != nil {
		t.Fatal(err)
	}
	// An incomplete request keeps an accepted HTTP connection in StateNew.
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err = <-done; err == nil || !strings.Contains(err.Error(), "shutdown HTTP") {
		t.Fatal(err)
	}
}

func TestEarlyControllerFailureStopsHTTP(t *testing.T) {
	a := NewApplication(testConfig(t), fake.NewSimpleClientset(), sinkFunc(func(context.Context, Record) error { return nil }), quietLog())
	want := errors.New("cannot register watch handler")
	a.controller = controllerFunc(func(context.Context) error { return want })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	l := listen(t)
	if err := a.Run(ctx, l); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if a.State.Ready() {
		t.Fatal("ready after controller failure")
	}
}

func TestCancelledBeforeSyncAndServerError(t *testing.T) {
	for _, serverError := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancelled", true: "server_error"}[serverError], func(t *testing.T) {
			c := testConfig(t)
			a := NewApplication(c, fake.NewSimpleClientset(), sinkFunc(func(context.Context, Record) error { return nil }), quietLog())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			l := listen(t)
			if serverError {
				_ = l.Close()
			} else {
				cancel()
			}
			err := a.Run(ctx, l)
			if (err != nil) != serverError {
				t.Fatal(err)
			}
			if a.State.Ready() {
				t.Fatal("ready")
			}
		})
	}
}

func TestKubernetesConfig(t *testing.T) {
	inside := &rest.Config{Host: "https://inside"}
	c, e := kubernetesConfig("", func() (*rest.Config, error) { return inside, nil })
	if e != nil || c != inside {
		t.Fatal(c, e)
	}
	_, e = kubernetesConfig("", func() (*rest.Config, error) { return nil, errors.New("bad credentials") })
	if e == nil {
		t.Fatal("incluster error")
	}
	p := filepath.Join(t.TempDir(), "config")
	if e = os.WriteFile(p, []byte("apiVersion: v1\nkind: Config\nclusters:\n- name: local\n  cluster:\n    server: https://127.0.0.1:6443\ncontexts:\n- name: local\n  context:\n    cluster: local\n    user: local\ncurrent-context: local\nusers:\n- name: local\n  user: {}\n"), 0600); e != nil {
		t.Fatal(e)
	}
	c, e = KubernetesConfig(p)
	if e != nil || c.Host != "https://127.0.0.1:6443" {
		t.Fatal(c, e)
	}
	t.Setenv("KUBECONFIG", p)
	c, e = kubernetesConfig("", func() (*rest.Config, error) { return nil, rest.ErrNotInCluster })
	if e != nil || c.Host != "https://127.0.0.1:6443" {
		t.Fatal(c, e)
	}
	_, e = KubernetesConfig(p + "-missing")
	if e == nil {
		t.Fatal("missing kubeconfig")
	}
}
