package exporter

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Record is a flat JSON object; dotted fields match existing logs-ui ACL fields.
type Record map[string]any

func Normalize(e *corev1.Event, c Config, now time.Time) Record {
	first := e.FirstTimestamp.Time
	if first.IsZero() {
		first = e.EventTime.Time
	}
	if first.IsZero() {
		first = e.CreationTimestamp.Time
	}
	if first.IsZero() {
		first = now
	}
	last := e.LastTimestamp.Time
	count := e.Count
	if e.Series != nil {
		if !e.Series.LastObservedTime.IsZero() {
			last = e.Series.LastObservedTime.Time
		}
		if e.Series.Count > count {
			count = e.Series.Count
		}
	}
	if last.IsZero() {
		last = first
	}
	if count < 1 {
		count = 1
	}
	ns := e.Namespace
	r := Record{"_time": last.UTC().Format(time.RFC3339Nano), "_msg": e.Message, "record_type": "kubernetes_event",
		"cluster": c.Cluster, "environment": c.Environment, "ingest": c.Ingest, "kubernetes.namespace_name": ns,
		"event.uid": string(e.UID), "event.resource_version": e.ResourceVersion, "event.type": e.Type, "event.reason": e.Reason,
		"event.action": e.Action, "event.count": count, "event.first_timestamp": first.UTC().Format(time.RFC3339Nano),
		"event.last_timestamp": last.UTC().Format(time.RFC3339Nano), "event.object.kind": e.InvolvedObject.Kind,
		"event.object.name": e.InvolvedObject.Name, "event.object.uid": string(e.InvolvedObject.UID),
		"event.object.namespace": e.InvolvedObject.Namespace, "event.reporting_controller": e.ReportingController,
		"event.reporting_instance": e.ReportingInstance, "event.source.component": e.Source.Component}
	// Identity remains stable across retries, but each Kubernetes revision is a snapshot.
	identity := c.Cluster + "\x00" + ns + "\x00" + string(e.UID) + "\x00" + e.ResourceVersion
	if e.UID == "" {
		identity += "\x00" + e.Name
	}
	h := sha256.Sum256([]byte(identity))
	r["event.id"] = hex.EncodeToString(h[:])
	if e.InvolvedObject.Kind == "Pod" {
		r["kubernetes.pod_name"] = e.InvolvedObject.Name
	}
	if e.InvolvedObject.Kind == "Node" {
		r["kubernetes.node_name"] = e.InvolvedObject.Name
	}
	if e.Source.Host != "" {
		r["event.source.host"] = e.Source.Host
	}
	business := c.BusinessMappings[ns]
	if business == "" {
		longest := 0
		for prefix, value := range c.BusinessPrefixMappings {
			if len(prefix) > longest && strings.HasPrefix(ns, prefix) {
				business = value
				longest = len(prefix)
			}
		}
	}
	if business != "" {
		r["business"] = business
	}
	service := c.ServiceMappings[ns+"/"+e.InvolvedObject.Kind+"/"+e.InvolvedObject.Name]
	if service != "" {
		r["service.name"] = service
		r["service.source"] = "configured_mapping"
	} else if e.InvolvedObject.Kind == "Service" {
		r["service.name"] = e.InvolvedObject.Name
		r["service.source"] = "object_reference"
	}
	return r
}

func eventTimestamp(e *corev1.Event) time.Time {
	if e.Series != nil && !e.Series.LastObservedTime.IsZero() {
		return e.Series.LastObservedTime.Time
	}
	if !e.LastTimestamp.IsZero() {
		return e.LastTimestamp.Time
	}
	if !e.EventTime.IsZero() {
		return e.EventTime.Time
	}
	return e.CreationTimestamp.Time
}
