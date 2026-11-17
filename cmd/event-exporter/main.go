package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/therealkaban9/kubernetes-event-exporter/internal/exporter"
	"k8s.io/client-go/kubernetes"
)

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewJSONHandler(os.Stdout, nil)).Error("exporter stopped", "service", "kubernetes-event-exporter", "error", err)
		os.Exit(1)
	}
}
func run() error {
	c, err := exporter.LoadConfig(os.Getenv)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: c.LogLevel})).With("service", "kubernetes-event-exporter")
	kube, err := exporter.KubernetesConfig(os.Getenv("KUBECONFIG"))
	if err != nil {
		return err
	}
	kube.QPS, kube.Burst, kube.UserAgent = c.KubeQPS, c.KubeBurst, "kubernetes-event-exporter/0.1.0"
	client, err := kubernetes.NewForConfig(kube)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	listener, err := net.Listen("tcp", c.HTTPAddress)
	if err != nil {
		return fmt.Errorf("listen for probes: %w", err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	return exporter.NewApplication(c, client, exporter.NewHTTPSink(c), log).Run(ctx, listener)
}
