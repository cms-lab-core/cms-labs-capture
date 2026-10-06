package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"

	"github.com/cms-lab-core/cms-labs-capture/internal/config"
)

const (
	DefaultSourceName            = "cms-labs-capture-config"
	DefaultSourceLabel           = "cms-labs.io/capture-config"
	DefaultManagedNamespaceLabel = "app.kubernetes.io/managed-by"
	DefaultManagedNamespaceValue = "clabgate"
	DefaultProxyNamespace        = "cms-labs-system"
	RuntimeName                  = "cms-labs-capture"
	managedByValue               = "cms-labs-capture-controller"
)

type Options struct {
	Image                 string
	ImagePullPolicy       corev1.PullPolicy
	ReconcileInterval     time.Duration
	SourceName            string
	SourceLabel           string
	ManagedNamespaceLabel string
	ManagedNamespaceValue string
	ProxyNamespace        string
	StorageSize           resource.Quantity
}

type Controller struct {
	client  kubernetes.Interface
	options Options
	logger  *slog.Logger
}

func New(client kubernetes.Interface, options Options, logger *slog.Logger) (*Controller, error) {
	if client == nil || options.Image == "" {
		return nil, errors.New("kubernetes client and runtime image are required")
	}
	if options.ImagePullPolicy == "" {
		options.ImagePullPolicy = corev1.PullIfNotPresent
	}
	if options.ReconcileInterval <= 0 {
		options.ReconcileInterval = 15 * time.Second
	}
	if options.SourceName == "" {
		options.SourceName = DefaultSourceName
	}
	if options.SourceLabel == "" {
		options.SourceLabel = DefaultSourceLabel
	}
	if options.ManagedNamespaceLabel == "" {
		options.ManagedNamespaceLabel = DefaultManagedNamespaceLabel
	}
	if options.ManagedNamespaceValue == "" {
		options.ManagedNamespaceValue = DefaultManagedNamespaceValue
	}
	if options.ProxyNamespace == "" {
		options.ProxyNamespace = DefaultProxyNamespace
	}
	if options.StorageSize.IsZero() {
		options.StorageSize = resource.MustParse("128Mi")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Controller{client: client, options: options, logger: logger}, nil
}

func (c *Controller) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.options.ReconcileInterval)
	defer ticker.Stop()
	for {
		if err := c.ReconcileAll(ctx); err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Error("capture controller reconcile failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (c *Controller) ReconcileAll(ctx context.Context) error {
	selector := labels.Set{c.options.SourceLabel: "true"}.String()
	sources, err := c.client.CoreV1().ConfigMaps("").List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return err
	}
	var result error
	for index := range sources.Items {
		source := &sources.Items[index]
		if source.Name != c.options.SourceName {
			continue
		}
		if err = c.Reconcile(ctx, source); err != nil {
			result = errors.Join(result, fmt.Errorf("%s/%s: %w", source.Namespace, source.Name, err))
		}
	}
	return result
}

func (c *Controller) Reconcile(ctx context.Context, source *corev1.ConfigMap) error {
	if source == nil || source.Namespace == "" || source.Name != c.options.SourceName {
		return errors.New("invalid capture declaration identity")
	}
	namespace, err := c.client.CoreV1().Namespaces().Get(ctx, source.Namespace, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if namespace.Labels[c.options.ManagedNamespaceLabel] != c.options.ManagedNamespaceValue {
		return fmt.Errorf("namespace is not managed by %s=%s", c.options.ManagedNamespaceLabel, c.options.ManagedNamespaceValue)
	}
	raw := source.Data["config.yaml"]
	if raw == "" {
		raw = "{}\n"
	}
	configuration, err := config.Decode([]byte(raw))
	if err != nil {
		return fmt.Errorf("decoding config.yaml: %w", err)
	}
	configuration.Server = config.Server{Address: config.DefaultAddress, IdentityHeader: "X-CMS-Identity"}
	configuration.Storage.Path = config.DefaultStoragePath
	if err = configuration.Validate(); err != nil {
		return err
	}
	encoded, err := yaml.Marshal(configuration)
	if err != nil {
		return err
	}
	owner := metav1.OwnerReference{
		APIVersion: "v1", Kind: "ConfigMap", Name: source.Name, UID: source.UID,
		Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true),
	}
	metadata := metav1.ObjectMeta{
		Name: RuntimeName, Namespace: source.Namespace, Labels: runtimeLabels(source.Name),
		OwnerReferences: []metav1.OwnerReference{owner},
	}
	if err = c.applyConfigMap(ctx, &corev1.ConfigMap{ObjectMeta: metadata, Data: map[string]string{"config.yaml": string(encoded)}}); err != nil {
		return err
	}
	if err = c.applyServiceAccount(ctx, &corev1.ServiceAccount{ObjectMeta: metadata, AutomountServiceAccountToken: ptr.To(true)}); err != nil {
		return err
	}
	role := &rbacv1.Role{ObjectMeta: metadata, Rules: []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"pods", "services"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{""}, Resources: []string{"pods/exec"}, Verbs: []string{"get", "create"}},
		{APIGroups: []string{"c9s.run"}, Resources: []string{"nodes", "links"}, Verbs: []string{"get", "list", "watch"}},
	}}
	if err = c.applyRole(ctx, role); err != nil {
		return err
	}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metadata,
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: RuntimeName, Namespace: source.Namespace}},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: RuntimeName},
	}
	if err = c.applyRoleBinding(ctx, binding); err != nil {
		return err
	}
	if err = c.applyDeployment(ctx, runtimeDeployment(metadata, c.options)); err != nil {
		return err
	}
	serviceMetadata := metadata
	serviceMetadata.Annotations = map[string]string{"cms-labs.io/tool": "capture"}
	service := &corev1.Service{
		ObjectMeta: serviceMetadata,
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app.kubernetes.io/name": RuntimeName},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 8080, TargetPort: intstr.FromString("http")}},
		},
	}
	if err = c.applyService(ctx, service); err != nil {
		return err
	}
	if err = c.applyNetworkPolicy(ctx, runtimeNetworkPolicy(metadata, c.options.ProxyNamespace)); err != nil {
		return err
	}
	c.logger.Debug("capture runtime reconciled", "namespace", source.Namespace)
	return nil
}

func runtimeLabels(source string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name": RuntimeName, "app.kubernetes.io/managed-by": managedByValue,
		"cms-labs.io/source-config": source,
	}
}

func runtimeDeployment(metadata metav1.ObjectMeta, options Options) *appsv1.Deployment {
	selector := map[string]string{"app.kubernetes.io/name": RuntimeName}
	return &appsv1.Deployment{
		ObjectMeta: metadata,
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(1)), Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector},
				Spec: corev1.PodSpec{
					ServiceAccountName: RuntimeName, TerminationGracePeriodSeconds: ptr.To(int64(15)),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To(int64(65532)), RunAsGroup: ptr.To(int64(65532)),
						FSGroup:        ptr.To(int64(65532)),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name: "capture", Image: options.Image, ImagePullPolicy: options.ImagePullPolicy, Args: []string{"serve"},
						Env: []corev1.EnvVar{
							{Name: "CMS_LABS_CAPTURE_CONFIG", Value: "/etc/cms-labs-capture/config.yaml"},
							{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
						},
						Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "config", MountPath: "/etc/cms-labs-capture", ReadOnly: true},
							{Name: "captures", MountPath: config.DefaultStoragePath},
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
							Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")}}, InitialDelaySeconds: 1, PeriodSeconds: 5},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("25m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
							Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: RuntimeName}}}},
						{Name: "captures", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &options.StorageSize}}},
					},
				},
			},
		},
	}
}

func runtimeNetworkPolicy(metadata metav1.ObjectMeta, proxyNamespace string) *networkingv1.NetworkPolicy {
	// Clabgate labels its Jupyter Deployment as a workspace component. Matching the
	// stable component contract keeps this policy independent of the Service name.
	peers := []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"labs.cmslabs.ru/component": "workspace"}}}}
	if proxyNamespace != "" {
		peers = append(peers, networkingv1.NetworkPolicyPeer{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": proxyNamespace}}})
	}
	protocol := corev1.ProtocolTCP
	port := intstr.FromInt(8080)
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metadata,
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": RuntimeName}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress:     []networkingv1.NetworkPolicyIngressRule{{From: peers, Ports: []networkingv1.NetworkPolicyPort{{Protocol: &protocol, Port: &port}}}},
		},
	}
}

func (c *Controller) applyConfigMap(ctx context.Context, desired *corev1.ConfigMap) error {
	api := c.client.CoreV1().ConfigMaps(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) applyServiceAccount(ctx context.Context, desired *corev1.ServiceAccount) error {
	api := c.client.CoreV1().ServiceAccounts(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		desired.Secrets = existing.Secrets
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) applyRole(ctx context.Context, desired *rbacv1.Role) error {
	api := c.client.RbacV1().Roles(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) applyRoleBinding(ctx context.Context, desired *rbacv1.RoleBinding) error {
	api := c.client.RbacV1().RoleBindings(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) applyDeployment(ctx context.Context, desired *appsv1.Deployment) error {
	api := c.client.AppsV1().Deployments(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) applyService(ctx context.Context, desired *corev1.Service) error {
	api := c.client.CoreV1().Services(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		desired.Spec.ClusterIP, desired.Spec.ClusterIPs = existing.Spec.ClusterIP, existing.Spec.ClusterIPs
		desired.Spec.IPFamilies, desired.Spec.IPFamilyPolicy = existing.Spec.IPFamilies, existing.Spec.IPFamilyPolicy
		desired.Spec.HealthCheckNodePort = existing.Spec.HealthCheckNodePort
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) applyNetworkPolicy(ctx context.Context, desired *networkingv1.NetworkPolicy) error {
	api := c.client.NetworkingV1().NetworkPolicies(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}
