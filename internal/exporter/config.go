package exporter

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config contains explicit operator settings. Credentials must never be logged.
type Config struct {
	Cluster, Environment, Ingest                                                  string
	Namespaces                                                                    []string
	BusinessMappings, BusinessPrefixMappings, ServiceMappings                     map[string]string
	SinkURL, SinkUsername, SinkPassword, HTTPAddress                              string
	QueueCapacity, Workers, MaxRetries, DedupCapacity, KubeBurst                  int
	RetryInitialDelay, RetryMaxDelay, ExportTimeout, ShutdownTimeout, MaxEventAge time.Duration
	KubeQPS                                                                       float32
	LogLevel                                                                      slog.Level
}

// LoadConfig reads environment variables through an injectable lookup.
func LoadConfig(get func(string) string) (Config, error) {
	c := Config{HTTPAddress: ":2112", QueueCapacity: 1024, Workers: 2, MaxRetries: 5,
		DedupCapacity: 16384, KubeBurst: 20, KubeQPS: 10, RetryInitialDelay: 200 * time.Millisecond,
		RetryMaxDelay: 10 * time.Second, ExportTimeout: 10 * time.Second, ShutdownTimeout: 30 * time.Second}
	read := func(k string) string { return strings.TrimSpace(get("EVENT_EXPORTER_" + k)) }
	c.Cluster, c.Environment, c.Ingest = read("CLUSTER"), read("ENVIRONMENT"), read("INGEST")
	c.SinkURL, c.SinkUsername = read("SINK_URL"), get("EVENT_EXPORTER_SINK_USERNAME")
	c.SinkPassword = get("EVENT_EXPORTER_SINK_PASSWORD")
	if v := read("HTTP_ADDRESS"); v != "" {
		c.HTTPAddress = v
	}
	seen := map[string]bool{}
	for _, n := range strings.Split(read("NAMESPACES"), ",") {
		n = strings.TrimSpace(n)
		if n != "" && !seen[n] {
			c.Namespaces = append(c.Namespaces, n)
			seen[n] = true
		}
	}
	for k, p := range map[string]*int{"QUEUE_CAPACITY": &c.QueueCapacity, "WORKERS": &c.Workers, "MAX_RETRIES": &c.MaxRetries, "DEDUP_CAPACITY": &c.DedupCapacity, "KUBE_BURST": &c.KubeBurst} {
		if v := read(k); v != "" {
			n, e := strconv.Atoi(v)
			if e != nil {
				return c, fmt.Errorf("%s: %w", k, e)
			}
			*p = n
		}
	}
	for k, p := range map[string]*time.Duration{"RETRY_INITIAL_DELAY": &c.RetryInitialDelay, "RETRY_MAX_DELAY": &c.RetryMaxDelay, "EXPORT_TIMEOUT": &c.ExportTimeout, "SHUTDOWN_TIMEOUT": &c.ShutdownTimeout, "MAX_EVENT_AGE": &c.MaxEventAge} {
		if v := read(k); v != "" {
			n, e := time.ParseDuration(v)
			if e != nil {
				return c, fmt.Errorf("%s: %w", k, e)
			}
			*p = n
		}
	}
	if v := read("KUBE_QPS"); v != "" {
		n, e := strconv.ParseFloat(v, 32)
		if e != nil {
			return c, fmt.Errorf("KUBE_QPS: %w", e)
		}
		c.KubeQPS = float32(n)
	}
	if v := read("LOG_LEVEL"); v != "" {
		if e := c.LogLevel.UnmarshalText([]byte(v)); e != nil {
			return c, fmt.Errorf("LOG_LEVEL: %w", e)
		}
	}
	for k, p := range map[string]*map[string]string{"BUSINESS_MAPPINGS": &c.BusinessMappings, "BUSINESS_PREFIX_MAPPINGS": &c.BusinessPrefixMappings, "SERVICE_MAPPINGS": &c.ServiceMappings} {
		if v := read(k); v != "" {
			if e := json.Unmarshal([]byte(v), p); e != nil {
				return c, fmt.Errorf("%s: %w", k, e)
			}
			for key, value := range *p {
				if key == "" || value == "" {
					return c, fmt.Errorf("%s: empty mapping", k)
				}
			}
		}
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	u, e := url.Parse(c.SinkURL)
	if e != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("SINK_URL must be an absolute HTTP(S) URL without credentials or fragment")
	}
	if c.Cluster == "" || c.Environment == "" || c.Ingest == "" {
		return fmt.Errorf("CLUSTER, ENVIRONMENT and INGEST are required")
	}
	if c.QueueCapacity < 1 || c.Workers < 1 || c.Workers > 128 || c.MaxRetries < 0 || c.MaxRetries > 20 || c.DedupCapacity < 1 || c.KubeBurst < 1 || c.KubeQPS <= 0 || c.KubeQPS > 10000 || math.IsNaN(float64(c.KubeQPS)) {
		return fmt.Errorf("invalid queue, workers, retries, dedup or Kubernetes throttling limit")
	}
	if c.RetryInitialDelay <= 0 || c.RetryMaxDelay < c.RetryInitialDelay || c.ExportTimeout <= 0 || c.ShutdownTimeout <= 0 || c.MaxEventAge < 0 {
		return fmt.Errorf("invalid timeout, backoff or maximum event age")
	}
	return nil
}
