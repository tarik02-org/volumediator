package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	volumediatorv1alpha1 "github.com/tarik02-org/volumediator/api/v1alpha1"
	volumediatorcontroller "github.com/tarik02-org/volumediator/internal/controller"
	"github.com/tarik02-org/volumediator/internal/node"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var version = "dev"

func main() {
	root := &cobra.Command{
		Use:           "volumediator",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(controllerCommand(), nodeCommand())
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func controllerCommand() *cobra.Command {
	var metricsAddress string
	var healthAddress string
	var webhookPort int
	var certificateDirectory string
	var webhookNamespace string
	var webhookService string
	var leaderElection bool
	var forceDeleteAfter time.Duration
	var unstageTimeout time.Duration
	var restageTimeout time.Duration

	command := &cobra.Command{
		Use: "controller",
		RunE: func(_ *cobra.Command, _ []string) error {
			if webhookNamespace == "" {
				return errors.New("--webhook-namespace is required")
			}
			scheme := runtime.NewScheme()
			if err := clientgoscheme.AddToScheme(scheme); err != nil {
				return err
			}
			if err := admissionregistrationv1.AddToScheme(scheme); err != nil {
				return err
			}
			if err := volumediatorv1alpha1.AddToScheme(scheme); err != nil {
				return err
			}

			server := webhook.NewServer(webhook.Options{Port: webhookPort, CertDir: certificateDirectory})
			manager, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
				Scheme:                 scheme,
				Metrics:                metricsserver.Options{BindAddress: metricsAddress},
				HealthProbeBindAddress: healthAddress,
				WebhookServer:          server,
				LeaderElection:         leaderElection,
				LeaderElectionID:       "volumediator-controller.volumediator.tarik02.me",
			})
			if err != nil {
				return err
			}

			reconciler := &volumediatorcontroller.Reconciler{
				Client:           manager.GetClient(),
				WebhookNamespace: webhookNamespace,
				WebhookService:   webhookService,
				ForceDeleteAfter: forceDeleteAfter,
				UnstageTimeout:   unstageTimeout,
				RestageTimeout:   restageTimeout,
			}
			if err := reconciler.SetupWithManager(manager); err != nil {
				return err
			}
			webhookServer := manager.GetWebhookServer()
			webhookServer.Register("/mutate-v1-pod", &admission.Webhook{Handler: &volumediatorcontroller.PodMutator{
				Client: manager.GetClient(), Decoder: admission.NewDecoder(scheme),
			}})
			if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
				return err
			}
			if err := manager.AddReadyzCheck("readyz", webhookServer.StartedChecker()); err != nil {
				return err
			}
			return manager.Start(ctrl.SetupSignalHandler())
		},
	}
	command.Flags().StringVar(&metricsAddress, "metrics-bind-address", ":8080", "metrics listener address")
	command.Flags().StringVar(&healthAddress, "health-probe-bind-address", ":8081", "health probe listener address")
	command.Flags().IntVar(&webhookPort, "webhook-port", 9443, "admission webhook listener port")
	command.Flags().StringVar(&certificateDirectory, "webhook-cert-dir", "/tmp/k8s-webhook-server/serving-certs", "TLS certificate directory")
	command.Flags().StringVar(&webhookNamespace, "webhook-namespace", os.Getenv("POD_NAMESPACE"), "namespace containing the webhook Service")
	command.Flags().StringVar(&webhookService, "webhook-service", "volumediator-webhook", "webhook Service name")
	command.Flags().BoolVar(&leaderElection, "leader-elect", true, "use Kubernetes leader election")
	command.Flags().DurationVar(&forceDeleteAfter, "force-delete-after", 2*time.Minute, "time before force-deleting a consumer blocked during eviction")
	command.Flags().DurationVar(&unstageTimeout, "unstage-timeout", 5*time.Minute, "maximum time to wait for complete CSI unstage")
	command.Flags().DurationVar(&restageTimeout, "restage-timeout", 10*time.Minute, "maximum time to wait for a clean restage observation")
	return command
}

func nodeCommand() *cobra.Command {
	var nodeName string
	var hostRoot string
	var scanInterval time.Duration
	var tune2fsPath string
	var umountPath string
	var allowedStorageClasses []string
	var blockedStorageClasses []string
	var ext4ErrorStates []string

	command := &cobra.Command{
		Use: "node",
		RunE: func(_ *cobra.Command, _ []string) error {
			if nodeName == "" {
				return errors.New("--node-name is required")
			}
			if len(ext4ErrorStates) == 0 {
				return errors.New("at least one --ext4-error-state must explicitly enable the detector")
			}
			scheme := runtime.NewScheme()
			if err := clientgoscheme.AddToScheme(scheme); err != nil {
				return err
			}
			if err := volumediatorv1alpha1.AddToScheme(scheme); err != nil {
				return err
			}
			clusterClient, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
			if err != nil {
				return err
			}
			runner := &node.Runner{
				Client: clusterClient, Log: ctrl.Log.WithName("node"), NodeName: nodeName,
				HostRoot: hostRoot, ScanInterval: scanInterval, Tune2fsPath: tune2fsPath, UmountPath: umountPath,
				AllowedStorageClasses: stringSet(allowedStorageClasses),
				BlockedStorageClasses: stringSet(blockedStorageClasses),
				ErrorStates:           stringSet(ext4ErrorStates),
			}
			return runner.Run(ctrl.SetupSignalHandler())
		},
	}
	command.Flags().StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"), "Kubernetes node name")
	command.Flags().StringVar(&hostRoot, "host-root", "/host", "host filesystem mount")
	command.Flags().DurationVar(&scanInterval, "scan-interval", 10*time.Second, "filesystem scan interval")
	command.Flags().StringVar(&tune2fsPath, "tune2fs", "tune2fs", "tune2fs executable")
	command.Flags().StringVar(&umountPath, "umount", "umount", "umount executable")
	command.Flags().StringSliceVar(&allowedStorageClasses, "allow-storage-class", nil, "StorageClass eligible for remediation")
	command.Flags().StringSliceVar(&blockedStorageClasses, "block-storage-class", nil, "StorageClass excluded from remediation")
	command.Flags().StringSliceVar(&ext4ErrorStates, "ext4-error-state", nil, "ext4 filesystem state that triggers the detector")
	return command
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func init() {
	zapOptions := zap.Options{Development: false}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOptions)))
}
