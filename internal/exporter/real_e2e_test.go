//go:build e2e

package exporter

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// TestRealClusterE2E is opt-in: it creates only its own temporary namespace,
// observes actual Kubernetes List/Watch and queries actual VictoriaLogs storage.
func TestRealClusterE2E(t *testing.T) {
	kubePath, sinkURL, queryURL := os.Getenv("EVENT_EXPORTER_E2E_KUBECONFIG"), os.Getenv("EVENT_EXPORTER_E2E_SINK_URL"), os.Getenv("EVENT_EXPORTER_E2E_QUERY_URL")
	if kubePath == "" || sinkURL == "" || queryURL == "" {
		t.Skip("requires explicit disposable Kubernetes kubeconfig and VictoriaLogs ingest/query URLs")
	}
	kube, err := KubernetesConfig(kubePath)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(kube)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ns, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "event-exporter-e2e-"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if e := client.CoreV1().Namespaces().Delete(cleanup, ns.Name, metav1.DeleteOptions{}); e != nil {
			t.Errorf("delete temporary namespace: %v", e)
		}
	})
	c := testConfig(t)
	c.SinkURL = sinkURL
	c.Namespaces = []string{ns.Name}
	c.SinkUsername = os.Getenv("EVENT_EXPORTER_SINK_USERNAME")
	c.SinkPassword = os.Getenv("EVENT_EXPORTER_SINK_PASSWORD")
	a := NewApplication(c, client, NewHTTPSink(c), quietLog())
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	l := listen(t)
	done := make(chan error, 1)
	go func() { done <- a.Run(runCtx, l) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Errorf("shutdown exporter: %v", err)
		}
	})
	eventually(t, a.State.Ready)
	e := exampleEvent()
	e.Namespace = ns.Name
	e.InvolvedObject.Namespace = ns.Name
	e.ResourceVersion = ""
	e.UID = ""
	e.CreationTimestamp = metav1.Time{}
	e, err = client.CoreV1().Events(ns.Name).Create(ctx, e, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	query := func(count int) bool {
		body := url.Values{"query": {fmt.Sprintf(`record_type:kubernetes_event event.uid:%q event.count:%q`, string(e.UID), fmt.Sprint(count))}, "limit": {"10"}}
		req, err := http.NewRequestWithContext(ctx, "POST", queryURL, strings.NewReader(body.Encode()))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if c.SinkUsername != "" {
			req.SetBasicAuth(c.SinkUsername, c.SinkPassword)
		}
		response, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		return response.StatusCode == 200 && strings.Contains(string(data), string(e.UID))
	}
	eventually(t, func() bool { return query(1) })
	e.Count = 2
	e.LastTimestamp = metav1.Now()
	if _, err = client.CoreV1().Events(ns.Name).Update(ctx, e, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return query(2) })
}
