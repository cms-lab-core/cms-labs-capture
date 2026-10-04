package controller

import (
	"context"
	"io"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"

	"github.com/maintainer64/cms-labs-capture/internal/config"
)

func TestReconcileCreatesCaptureRuntime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: "lab-a", Labels: map[string]string{DefaultManagedNamespaceLabel: DefaultManagedNamespaceValue},
		}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: DefaultSourceName, Namespace: "lab-a", UID: types.UID("source-uid"),
			Labels: map[string]string{DefaultSourceLabel: "true"},
		}, Data: map[string]string{"config.yaml": "server:\n  address: 127.0.0.1:9999\n  identityHeader: Unsafe-Header\nlimits:\n  maxConcurrent: 1\n"}},
	)
	operator, err := New(client, Options{
		Image: "example.test/capture:1", ProxyNamespace: "cms-labs-system",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err = operator.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	deployment, err := client.AppsV1().Deployments("lab-a").Get(ctx, RuntimeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	container := deployment.Spec.Template.Spec.Containers[0]
	if container.Image != "example.test/capture:1" || container.Args[0] != "serve" ||
		deployment.Spec.Template.Spec.ServiceAccountName != RuntimeName {
		t.Fatalf("deployment = %#v", deployment.Spec.Template.Spec)
	}
	service, err := client.CoreV1().Services("lab-a").Get(ctx, RuntimeName, metav1.GetOptions{})
	if err != nil || service.Annotations["cms-labs.io/tool"] != "capture" || service.Spec.Ports[0].Port != 8080 {
		t.Fatalf("service = %#v, %v", service, err)
	}
	runtimeConfig, err := client.CoreV1().ConfigMaps("lab-a").Get(ctx, RuntimeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Decode([]byte(runtimeConfig.Data["config.yaml"]))
	if err != nil {
		t.Fatal(err)
	}
	if err = configuration.Validate(); err != nil || configuration.Limits.MaxConcurrent != 1 ||
		configuration.Server.Address != config.DefaultAddress || configuration.Server.IdentityHeader != "X-CMS-Identity" {
		t.Fatalf("runtime config = %#v, %v", configuration, err)
	}
	role, err := client.RbacV1().Roles("lab-a").Get(ctx, RuntimeName, metav1.GetOptions{})
	if err != nil || len(role.Rules) != 3 {
		t.Fatalf("role = %#v, %v", role, err)
	}
	if _, err = client.NetworkingV1().NetworkPolicies("lab-a").Get(ctx, RuntimeName, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	policy, err := client.NetworkingV1().NetworkPolicies("lab-a").Get(ctx, RuntimeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	workspaceLabels := policy.Spec.Ingress[0].From[0].PodSelector.MatchLabels
	if workspaceLabels["labs.cmslabs.ru/component"] != "workspace" {
		t.Fatalf("workspace peer labels = %#v", workspaceLabels)
	}
	writes := mutationCount(client.Actions())
	if err = operator.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if current := mutationCount(client.Actions()); current != writes {
		t.Fatalf("unchanged reconcile performed %d writes", current-writes)
	}
}

func TestReconcileRejectsForeignNamespace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "foreign"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: DefaultSourceName, Namespace: "foreign", UID: types.UID("source-uid"),
			Labels: map[string]string{DefaultSourceLabel: "true"},
		}, Data: map[string]string{"config.yaml": "{}\n"}},
	)
	operator, err := New(client, Options{Image: "example.test/capture:1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = operator.ReconcileAll(ctx); err == nil {
		t.Fatal("foreign namespace was accepted")
	}
}

func mutationCount(actions []kubetesting.Action) int {
	count := 0
	for _, action := range actions {
		switch action.GetVerb() {
		case "create", "update", "patch", "delete":
			count++
		}
	}
	return count
}
