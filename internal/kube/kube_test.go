package kube

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func TestTargetsReturnsActualNodesAndLinkedInterfaces(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	dynamicClient := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		nodeGVR: "NodeList", linkGVR: "LinkList",
	},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "c9s.run/v1alpha1", "kind": "Node",
			"metadata": map[string]any{"name": "r1", "namespace": "lab-a"},
		}},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "c9s.run/v1alpha1", "kind": "Node",
			"metadata": map[string]any{"name": "s1", "namespace": "lab-a"},
		}},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "c9s.run/v1alpha1", "kind": "Link",
			"metadata": map[string]any{"name": "r1-s1", "namespace": "lab-a"},
			"spec": map[string]any{
				"endpointA": map[string]any{"nodeName": "r1", "interfaceName": "eth1"},
				"endpointB": map[string]any{"nodeName": "s1", "interfaceName": "eth2"},
			},
		}},
	)
	backend, err := NewBackend("lab-a", kubernetesfake.NewClientset(), dynamicClient, &rest.Config{Host: "https://example.test"})
	if err != nil {
		t.Fatal(err)
	}
	targets, err := backend.Targets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[0].Node != "r1" || targets[0].Interfaces[0] != "eth1" ||
		targets[1].Node != "s1" || targets[1].Interfaces[0] != "eth2" {
		t.Fatalf("targets = %#v", targets)
	}
}
