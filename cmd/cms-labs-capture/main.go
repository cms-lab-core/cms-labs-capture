package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/maintainer64/cms-labs-capture/internal/capture"
	"github.com/maintainer64/cms-labs-capture/internal/config"
	capturecontroller "github.com/maintainer64/cms-labs-capture/internal/controller"
	"github.com/maintainer64/cms-labs-capture/internal/httpapi"
	"github.com/maintainer64/cms-labs-capture/internal/kube"
)

const serviceAccountToken = "/var/run/secrets/kubernetes.io/serviceaccount/token" //nolint:gosec // path, not credential.

var version = "dev"

func main() {
	mode := "serve"
	arguments := os.Args[1:]
	if len(arguments) > 0 && arguments[0] != "" && arguments[0][0] != '-' {
		mode, arguments = arguments[0], arguments[1:]
	}
	var err error
	switch mode {
	case "serve":
		err = serve(arguments)
	case "controller":
		err = runController(arguments)
	case "version", "--version", "-version":
		fmt.Printf("cms-labs-capture %s\n", version)
		return
	default:
		err = fmt.Errorf("unknown mode %q (use serve, controller or version)", mode)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "cms-labs-capture: %s\n", err)
		os.Exit(1)
	}
}

func serve(arguments []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := flags.String("config", envOr("CMS_LABS_CAPTURE_CONFIG", "/etc/cms-labs-capture/config.yaml"), "configuration file")
	namespace := flags.String("namespace", os.Getenv("POD_NAMESPACE"), "lab namespace")
	verbose := flags.Bool("verbose", false, "debug logging")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *namespace == "" {
		return errors.New("--namespace or POD_NAMESPACE is required")
	}
	configuration, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	logger := newLogger(*verbose)
	restConfiguration, err := restConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(restConfiguration)
	if err != nil {
		return err
	}
	dynamicClient, err := dynamic.NewForConfig(restConfiguration)
	if err != nil {
		return err
	}
	backend, err := kube.NewBackend(*namespace, client, dynamicClient, restConfiguration)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	manager, err := capture.NewManager(ctx, backend, configuration)
	if err != nil {
		return err
	}
	defer manager.Close()
	go manager.RunReaper(ctx)
	api, err := httpapi.New(manager, configuration.Server.IdentityHeader, logger)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr: configuration.Server.Address, Handler: api.Handler(),
		ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second,
	}
	errorsChannel := make(chan error, 1)
	go func() { errorsChannel <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
		return httpapi.Shutdown(server)
	case err = <-errorsChannel:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func runController(arguments []string) error {
	flags := flag.NewFlagSet("controller", flag.ContinueOnError)
	image := flags.String("runtime-image", "", "capture runtime image")
	pullPolicy := flags.String("runtime-image-pull-policy", string(corev1.PullIfNotPresent), "runtime image pull policy")
	interval := flags.Duration("reconcile-interval", 15*time.Second, "full reconciliation interval")
	sourceName := flags.String("source-name", capturecontroller.DefaultSourceName, "per-lab configuration ConfigMap name")
	sourceLabel := flags.String("source-label", capturecontroller.DefaultSourceLabel, "per-lab configuration label")
	managedLabel := flags.String("managed-namespace-label", capturecontroller.DefaultManagedNamespaceLabel, "required lab namespace label")
	managedValue := flags.String("managed-namespace-value", capturecontroller.DefaultManagedNamespaceValue, "required lab namespace label value")
	proxyNamespace := flags.String("proxy-namespace", capturecontroller.DefaultProxyNamespace, "trusted frontend proxy namespace")
	storageSize := flags.String("storage-size", "128Mi", "per-lab temporary capture storage")
	verbose := flags.Bool("verbose", false, "debug logging")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *image == "" {
		return errors.New("--runtime-image is required")
	}
	quantity, err := resource.ParseQuantity(*storageSize)
	if err != nil {
		return fmt.Errorf("invalid --storage-size: %w", err)
	}
	restConfiguration, err := restConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(restConfiguration)
	if err != nil {
		return err
	}
	operator, err := capturecontroller.New(client, capturecontroller.Options{
		Image: *image, ImagePullPolicy: corev1.PullPolicy(*pullPolicy), ReconcileInterval: *interval,
		SourceName: *sourceName, SourceLabel: *sourceLabel,
		ManagedNamespaceLabel: *managedLabel, ManagedNamespaceValue: *managedValue,
		ProxyNamespace: *proxyNamespace, StorageSize: quantity,
	}, newLogger(*verbose))
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return operator.Run(ctx)
}

func restConfig() (*rest.Config, error) {
	if _, err := os.Stat(serviceAccountToken); err == nil {
		return rest.InClusterConfig()
	}
	path := os.Getenv("KUBECONFIG")
	if path == "" {
		return nil, errors.New("no in-cluster credentials and KUBECONFIG is unset")
	}
	return clientcmd.BuildConfigFromFlags("", path)
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func newLogger(verbose bool) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}
