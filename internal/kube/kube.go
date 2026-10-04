package kube

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"
)

const (
	topologyNodeLabel     = "c9s.run/topologyNode"
	directWorkloadLabel   = "c9s.run/direct-workload"
	connectivityContainer = "clabwire"
)

var (
	nodeGVR = schema.GroupVersionResource{Group: "c9s.run", Version: "v1alpha1", Resource: "nodes"}
	linkGVR = schema.GroupVersionResource{Group: "c9s.run", Version: "v1alpha1", Resource: "links"}
)

type Target struct {
	Node       string   `json:"node"`
	Interfaces []string `json:"interfaces"`
}

type CaptureSpec struct {
	Node        string
	Interface   string
	Duration    time.Duration
	PacketLimit int
	SnapLength  int
}

type Backend struct {
	namespace string
	client    kubernetes.Interface
	dynamic   dynamic.Interface
	config    *rest.Config
}

func NewBackend(namespace string, client kubernetes.Interface, dynamicClient dynamic.Interface, configuration *rest.Config) (*Backend, error) {
	if namespace == "" || client == nil || dynamicClient == nil || configuration == nil {
		return nil, errors.New("namespace, clients and REST configuration are required")
	}
	return &Backend{namespace: namespace, client: client, dynamic: dynamicClient, config: configuration}, nil
}

func (b *Backend) Targets(ctx context.Context) ([]Target, error) {
	nodes, err := b.dynamic.Resource(nodeGVR).Namespace(b.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing Clabernetes Nodes: %w", err)
	}
	interfaces := make(map[string]map[string]struct{}, len(nodes.Items))
	for _, node := range nodes.Items {
		interfaces[node.GetName()] = map[string]struct{}{}
	}
	links, err := b.dynamic.Resource(linkGVR).Namespace(b.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing Clabernetes Links: %w", err)
	}
	for _, link := range links.Items {
		for _, endpoint := range []string{"endpointA", "endpointB"} {
			node, _, _ := nestedString(link.Object, "spec", endpoint, "nodeName")
			name, _, _ := nestedString(link.Object, "spec", endpoint, "interfaceName")
			if _, exists := interfaces[node]; exists && name != "" {
				interfaces[node][name] = struct{}{}
			}
		}
	}
	result := make([]Target, 0, len(interfaces))
	for node, values := range interfaces {
		names := make([]string, 0, len(values))
		for name := range values {
			names = append(names, name)
		}
		sort.Strings(names)
		result = append(result, Target{Node: node, Interfaces: names})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Node < result[right].Node })
	return result, nil
}

func (b *Backend) Capture(ctx context.Context, spec CaptureSpec, output io.Writer) error {
	if output == nil {
		return errors.New("capture output is required")
	}
	node, err := b.dynamic.Resource(nodeGVR).Namespace(b.namespace).Get(ctx, spec.Node, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("reading Clabernetes Node %q: %w", spec.Node, err)
	}
	interfaceExists, err := b.interfaceExists(ctx, spec.Node, spec.Interface)
	if err != nil {
		return err
	}
	if !interfaceExists {
		return fmt.Errorf("interface %q is not linked to Node %q", spec.Interface, spec.Node)
	}
	pod, err := b.resolvePod(ctx, spec.Node)
	if err != nil {
		return err
	}
	command := []string{
		"/clabernetes/manager", "node-runtime", "packet-capture",
		"--plan", "/var/run/clabernetes/plan/plan.json",
		"--input", "/var/run/clabernetes/input/input.json",
		"--connectivityRevision", "/var/run/clabernetes/connectivity-revision/revision.json",
		"--nodeID", string(node.GetUID()), "--interface", spec.Interface,
		"--snapLength", strconv.Itoa(spec.SnapLength),
		"--packetLimit", strconv.Itoa(spec.PacketLimit),
		"--duration", spec.Duration.String(),
	}
	request := b.client.CoreV1().RESTClient().Post().Resource("pods").Namespace(b.namespace).Name(pod.Name).
		SubResource("exec").VersionedParams(&corev1.PodExecOptions{
		Container: connectivityContainer, Command: command, Stdout: true, Stderr: true,
	}, scheme.ParameterCodec)
	spdyExecutor, err := remotecommand.NewSPDYExecutor(b.config, "POST", request.URL())
	if err != nil {
		return fmt.Errorf("preparing capture exec: %w", err)
	}
	websocketExecutor, err := remotecommand.NewWebSocketExecutor(b.config, "GET", request.URL().String())
	if err != nil {
		return fmt.Errorf("preparing capture websocket exec: %w", err)
	}
	executor, err := remotecommand.NewFallbackExecutor(websocketExecutor, spdyExecutor, func(streamErr error) bool {
		return httpstream.IsUpgradeFailure(streamErr) || httpstream.IsHTTPSProxyError(streamErr)
	})
	if err != nil {
		return fmt.Errorf("preparing capture transport fallback: %w", err)
	}
	var stderr bytes.Buffer
	if err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: output, Stderr: &stderr}); err != nil {
		message := stderr.String()
		if len(message) > 1024 {
			message = message[len(message)-1024:]
		}
		return fmt.Errorf("capturing %s:%s: %w: %s", spec.Node, spec.Interface, err, message)
	}
	return nil
}

func (b *Backend) interfaceExists(ctx context.Context, node, interfaceName string) (bool, error) {
	links, err := b.dynamic.Resource(linkGVR).Namespace(b.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("listing Clabernetes Links: %w", err)
	}
	for _, link := range links.Items {
		for _, endpoint := range []string{"endpointA", "endpointB"} {
			endpointNode, _, _ := nestedString(link.Object, "spec", endpoint, "nodeName")
			endpointInterface, _, _ := nestedString(link.Object, "spec", endpoint, "interfaceName")
			if endpointNode == node && endpointInterface == interfaceName {
				return true, nil
			}
		}
	}
	return false, nil
}

func (b *Backend) resolvePod(ctx context.Context, node string) (*corev1.Pod, error) {
	selectors := []string{
		labels.Set{topologyNodeLabel: node}.String(),
		labels.Set{directWorkloadLabel: node}.String(),
	}
	for _, selector := range selectors {
		pods, err := b.client.CoreV1().Pods(b.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err == nil {
			if pod := readyPod(pods.Items); pod != nil {
				return pod, nil
			}
		}
	}
	services, err := b.client.CoreV1().Services(b.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{topologyNodeLabel: node}.String(),
	})
	if err == nil {
		for _, service := range services.Items {
			if len(service.Spec.Selector) == 0 {
				continue
			}
			pods, listErr := b.client.CoreV1().Pods(b.namespace).List(ctx, metav1.ListOptions{
				LabelSelector: labels.Set(service.Spec.Selector).String(),
			})
			if listErr == nil {
				if pod := readyPod(pods.Items); pod != nil {
					return pod, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("no running Clabernetes Pod found for Node %q", node)
}

func readyPod(pods []corev1.Pod) *corev1.Pod {
	for index := range pods {
		if pods[index].DeletionTimestamp == nil && pods[index].Status.Phase == corev1.PodRunning {
			return &pods[index]
		}
	}
	return nil
}

func nestedString(object map[string]any, fields ...string) (string, bool, error) {
	current := any(object)
	for _, field := range fields {
		mapping, ok := current.(map[string]any)
		if !ok {
			return "", false, nil
		}
		current, ok = mapping[field]
		if !ok {
			return "", false, nil
		}
	}
	value, ok := current.(string)
	return value, ok, nil
}
