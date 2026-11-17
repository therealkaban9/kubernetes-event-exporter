#!/usr/bin/env bash
set -euo pipefail

# Explicit invocation provisions and removes an isolated local cluster/container.
# No production kubeconfig or current-context is used.
: "${EVENT_EXPORTER_E2E_VICTORIALOGS_IMAGE:?Set a pinned VictoriaLogs Docker image}"
command -v kind >/dev/null
command -v docker >/dev/null
docker info >/dev/null
task_dir=$(mktemp -d)
task_name="event-exporter-e2e-$$"
cleanup() {
  docker rm -f "$task_name" >/dev/null 2>&1 || true
  kind delete cluster --name "$task_name" >/dev/null 2>&1 || true
  rm -rf "$task_dir"
}
trap cleanup EXIT
kind create cluster --name "$task_name" --kubeconfig "$task_dir/kubeconfig" --image "${EVENT_EXPORTER_E2E_KIND_IMAGE:-kindest/node:v1.34.0}" --wait 90s
docker run -d --name "$task_name" -p 127.0.0.1::9428 "$EVENT_EXPORTER_E2E_VICTORIALOGS_IMAGE" >/dev/null
task_port=$(docker port "$task_name" 9428/tcp)
export EVENT_EXPORTER_E2E_KUBECONFIG="$task_dir/kubeconfig"
export EVENT_EXPORTER_E2E_SINK_URL="http://$task_port/insert/jsonline?_stream_fields=record_type,cluster,kubernetes.namespace_name"
export EVENT_EXPORTER_E2E_QUERY_URL="http://$task_port/select/logsql/query"
go test -tags=e2e ./internal/exporter -run '^TestRealClusterE2E$' -v -count=1
