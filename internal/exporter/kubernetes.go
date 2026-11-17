package exporter

import (
	"errors"
	"fmt"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func KubernetesConfig(path string) (*rest.Config, error) {
	return kubernetesConfig(path, rest.InClusterConfig)
}
func kubernetesConfig(path string, inCluster func() (*rest.Config, error)) (*rest.Config, error) {
	if path == "" {
		config, err := inCluster()
		if err == nil {
			return config, nil
		}
		if !errors.Is(err, rest.ErrNotInCluster) {
			return nil, fmt.Errorf("in-cluster configuration: %w", err)
		}
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = path
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	return config, nil
}
