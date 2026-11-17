package exporter

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testConfig(t testing.TB) Config {
	t.Helper()
	c, e := LoadConfig(func(k string) string {
		return map[string]string{"EVENT_EXPORTER_CLUSTER": "test", "EVENT_EXPORTER_ENVIRONMENT": "dev", "EVENT_EXPORTER_INGEST": "k8s-test", "EVENT_EXPORTER_SINK_URL": "http://localhost/insert/jsonline"}[k]
	})
	if e != nil {
		t.Fatal(e)
	}
	return c
}

func TestConfig(t *testing.T) {
	base := map[string]string{"CLUSTER": "test", "ENVIRONMENT": "dev", "INGEST": "k8s-test", "SINK_URL": "http://localhost/insert/jsonline"}
	cases := []struct {
		key, value string
		bad        bool
	}{
		{"QUEUE_CAPACITY", "2", false}, {"QUEUE_CAPACITY", "x", true}, {"QUEUE_CAPACITY", "0", true}, {"WORKERS", "129", true},
		{"MAX_RETRIES", "-1", true}, {"MAX_RETRIES", "21", true}, {"DEDUP_CAPACITY", "0", true}, {"KUBE_BURST", "0", true},
		{"KUBE_QPS", "1.5", false}, {"KUBE_QPS", "x", true}, {"KUBE_QPS", "NaN", true}, {"KUBE_QPS", "0", true},
		{"RETRY_INITIAL_DELAY", "1ms", false}, {"RETRY_INITIAL_DELAY", "x", true}, {"EXPORT_TIMEOUT", "0s", true},
		{"SHUTDOWN_TIMEOUT", "-1s", true}, {"MAX_EVENT_AGE", "-1s", true}, {"RETRY_MAX_DELAY", "1ms", true},
		{"LOG_LEVEL", "debug", false}, {"LOG_LEVEL", "garbage", true}, {"SINK_URL", "file:///tmp/x", true},
		{"SINK_URL", "http://user:pw@localhost", true}, {"SINK_URL", "http://localhost/#x", true}, {"SINK_URL", "http://[", true},
		{"CLUSTER", "", true}, {"ENVIRONMENT", "", true}, {"INGEST", "", true}, {"HTTP_ADDRESS", ":0", false},
		{"NAMESPACES", " one, two,one,, ", false}, {"BUSINESS_MAPPINGS", `{"one":"b2c"}`, false},
		{"BUSINESS_MAPPINGS", `{"":"b2c"}`, true}, {"BUSINESS_MAPPINGS", `{"one":""}`, true},
		{"SERVICE_MAPPINGS", "no", true}, {"BUSINESS_PREFIX_MAPPINGS", `{"b2cdev01-":"b2c"}`, false},
		{"SINK_PASSWORD", " literal space ", false},
	}
	for _, tc := range cases {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			c, e := LoadConfig(func(k string) string {
				k = strings.TrimPrefix(k, "EVENT_EXPORTER_")
				if k == tc.key {
					return tc.value
				}
				return base[k]
			})
			if (e != nil) != tc.bad {
				t.Fatalf("error=%v bad=%v", e, tc.bad)
			}
			if tc.key == "NAMESPACES" && len(c.Namespaces) != 2 {
				t.Fatal(c.Namespaces)
			}
			if tc.key == "SINK_PASSWORD" && c.SinkPassword != tc.value {
				t.Fatal("password trimmed")
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	for _, kind := range []string{"Pod", "Service", "Node", "Deployment"} {
		for _, mode := range []string{"none", "creation", "event", "legacy", "series"} {
			t.Run(kind+mode, func(t *testing.T) {
				c := testConfig(t)
				c.BusinessPrefixMappings = map[string]string{"team": "wrong", "team-a": "right"}
				c.ServiceMappings = map[string]string{"team-a/Deployment/demo": "demo-service"}
				e := &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "ev", Namespace: "team-a"}, Message: "hello", InvolvedObject: corev1.ObjectReference{Kind: kind, Name: "demo"}, Source: corev1.EventSource{Host: "node1"}}
				want := now
				switch mode {
				case "creation":
					e.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
					want = e.CreationTimestamp.Time
				case "event":
					e.EventTime = metav1.NewMicroTime(now.Add(-time.Minute))
					want = e.EventTime.Time
				case "legacy":
					e.FirstTimestamp = metav1.NewTime(now.Add(-time.Hour))
					e.LastTimestamp = metav1.NewTime(now.Add(-time.Second))
					want = e.LastTimestamp.Time
					e.Count = 2
				case "series":
					e.Series = &corev1.EventSeries{Count: 5, LastObservedTime: metav1.NewMicroTime(now.Add(-time.Second))}
					want = e.Series.LastObservedTime.Time
				}
				r := Normalize(e, c, now)
				if r["_time"] != want.Format(time.RFC3339Nano) || r["business"] != "right" {
					t.Fatal(r)
				}
				if len(r["event.id"].(string)) != 64 {
					t.Fatal(r)
				}
				if mode == "series" && r["event.count"] != int32(5) {
					t.Fatal(r)
				}
				if kind == "Deployment" && r["service.name"] != "demo-service" {
					t.Fatal(r)
				}
				if kind == "Service" && r["service.source"] != "object_reference" {
					t.Fatal(r)
				}
				if kind == "Pod" && r["kubernetes.pod_name"] != "demo" {
					t.Fatal(r)
				}
				if kind == "Node" && r["kubernetes.node_name"] != "demo" {
					t.Fatal(r)
				}
				c.BusinessMappings = map[string]string{"team-a": "exact"}
				if Normalize(e, c, now)["business"] != "exact" {
					t.Fatal("exact must win")
				}
			})
		}
	}
}

func TestEventTimestamp(t *testing.T) {
	n := time.Now().UTC()
	e := &corev1.Event{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(n)}}
	if !eventTimestamp(e).Equal(n) {
		t.Fatal("creation")
	}
	e.EventTime = metav1.NewMicroTime(n.Add(time.Second))
	if !eventTimestamp(e).Equal(e.EventTime.Time) {
		t.Fatal("event")
	}
	e.LastTimestamp = metav1.NewTime(n.Add(2 * time.Second))
	if !eventTimestamp(e).Equal(e.LastTimestamp.Time) {
		t.Fatal("last")
	}
	e.Series = &corev1.EventSeries{LastObservedTime: metav1.NewMicroTime(n.Add(3 * time.Second))}
	if !eventTimestamp(e).Equal(e.Series.LastObservedTime.Time) {
		t.Fatal("series")
	}
}

func FuzzNormalize(f *testing.F) {
	f.Add("team-a", "Pod", "demo", "hello", int32(1))
	f.Fuzz(func(t *testing.T, ns, kind, name, msg string, count int32) {
		e := &corev1.Event{ObjectMeta: metav1.ObjectMeta{Namespace: ns}, Message: msg, Count: count, InvolvedObject: corev1.ObjectReference{Kind: kind, Name: name}}
		r := Normalize(e, testConfig(t), time.Unix(0, 0))
		if r["kubernetes.namespace_name"] != ns || r["event.count"].(int32) < 1 {
			t.Fatal(r)
		}
	})
}
