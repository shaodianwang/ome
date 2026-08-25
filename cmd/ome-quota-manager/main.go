// Command ome-quota-manager is the OME fleet-quota control plane (OEP-0024): it
// assembles the AcceleratorQuota tree, reports each node's position and
// condition, and — in a later increment — renders the tree into Kueue or
// projects per-cluster shares onto members.
//
// It is a separate binary from ome-manager on purpose. Rendering a tree needs
// cluster-wide write on Kueue's ClusterQueue and Cohort, and that grant should
// not ride the ServiceAccount that also runs the InferenceService reconcilers
// and the fail-closed pod mutating webhook. A separate Deployment keeps the
// blast radius of the quota plane to the quota plane, and lets an operator run,
// scale, and roll it independently of serving.
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/acceleratorquota"
	kueuebackend "github.com/ome-projects/ome/pkg/quota/backend/kueue"
	quotacert "github.com/ome-projects/ome/pkg/quota/cert"
	"github.com/ome-projects/ome/pkg/quota/tree"
	quotawebhook "github.com/ome-projects/ome/pkg/webhook/admission/acceleratorquota"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	// Unconditional, unlike ome-manager's probe table: Kueue is optional for
	// serving, but this whole binary is opt-in, so registering the types costs
	// nothing when they are unused. Reads go through the uncached reader, so a
	// cluster without the CRDs installed gets a no-match error the caller
	// handles rather than an informer that never syncs.
	utilruntime.Must(kueuev1beta2.AddToScheme(scheme))
}

// options are all supplied by the chart. Every knob is a flag rather than a
// ConfigMap: this binary has a handful of deploy-time decisions and no hot-tunable
// ones, so a flag avoids a ConfigMap read, the RBAC to do it, and a parse layer
// that could disagree with what the chart rendered.
type options struct {
	mode                 string
	metricsAddr          string
	probeAddr            string
	enableLeaderElection bool
	leaderElectionID     string
	// maxTreeDepth bounds a node's distance from the root, in edges. Zero
	// disables the check rather than assuming a bound.
	maxTreeDepth int
	// resyncInterval re-runs the tree checks on an idle plane, so a violation
	// only a clock can clear is noticed without an edit. Zero leaves reconciles
	// purely event-driven.
	resyncInterval time.Duration
	// acceleratorResources are the resource-name suffixes that count as
	// accelerator capacity. Empty disables capacity derivation, and with it the
	// Node and ResourceFlavor reads it needs.
	acceleratorResources string
	// capacityHysteresisPercent damps the derived high-water mark. Zero means no
	// damping rather than a default band.
	capacityHysteresisPercent int
	coverResources            string
	fieldManager              string
	// enableWebhook serves the admission validator. It is separable from the
	// controller because the two fail differently: a webhook outage with
	// failurePolicy=Fail blocks writes, so an operator debugging a wedged
	// cluster needs to be able to run the controller without it.
	enableWebhook  bool
	webhookPort    int
	webhookCertDir string
	// internalCertManagement generates and rotates the webhook serving cert in
	// process, and injects the caBundle into this component's own
	// ValidatingWebhookConfiguration. On by default so the admission path works
	// with no cert-manager in the cluster; turn it off to have cert-manager (or
	// anything else) supply the cert instead.
	internalCertManagement bool
	certNamespace          string
	certSecretName         string
	webhookServiceName     string
	webhookConfigName      string
	caName                 string
	caOrganization         string
	zapOpts                zap.Options
}

func main() {
	opts := options{
		metricsAddr:            ":8080",
		probeAddr:              ":8081",
		enableLeaderElection:   true,
		leaderElectionID:       "ome-quota-manager-leader-lock",
		enableWebhook:          true,
		webhookPort:            9443,
		webhookCertDir:         "/tmp/k8s-webhook-server/serving-certs",
		internalCertManagement: true,
		zapOpts:                zap.Options{TimeEncoder: zapcore.ISO8601TimeEncoder},
	}

	flag.StringVar(&opts.mode, "mode", opts.mode,
		"Which half of the quota plane to run. \"workload\" renders the local AcceleratorQuota "+
			"tree into Kueue on this serving cluster; \"management\" holds the authored fleet tree "+
			"and projects per-cluster shares onto members. Required — a manager runs exactly one "+
			"mode, because the two write different halves of the reserved root's status and "+
			"combining them is a last-writer-wins race.")
	flag.StringVar(&opts.metricsAddr, "metrics-bind-address", opts.metricsAddr,
		"The address the metrics endpoint binds to.")
	flag.StringVar(&opts.probeAddr, "health-probe-addr", opts.probeAddr,
		"The address the health/readiness probe endpoint binds to.")
	flag.BoolVar(&opts.enableLeaderElection, "leader-elect", opts.enableLeaderElection,
		"Elect a leader so exactly one replica reconciles. Keep this on whenever replicas > 1: "+
			"two active instances would race each other's status writes on every node.")
	flag.StringVar(&opts.leaderElectionID, "leader-election-id", opts.leaderElectionID,
		"Name of the leader-election lock.")
	flag.IntVar(&opts.maxTreeDepth, "max-tree-depth", opts.maxTreeDepth,
		"Greatest permitted distance from the root, in edges (the root is 0, a top-tier grouping "+
			"is 1). 0 disables the depth check rather than assuming a bound.")
	flag.StringVar(&opts.acceleratorResources, "accelerator-resources", opts.acceleratorResources,
		"Comma-separated extended resource names that count as accelerator capacity, written in full, "+
			"e.g. \"google.com/tpu,nvidia.com/gpu\". Exact names rather than a pattern, because this list "+
			"decides what every budget is measured against. A vendor left off contributes no capacity, "+
			"which surfaces as CapacityExceeded on any budget written against it rather than passing "+
			"silently. Empty disables capacity derivation entirely: without it nothing distinguishes a "+
			"node's accelerators from its cpu, and there is no safe guess. Workload mode only — a "+
			"management-mode manager has no local silicon to measure.")
	flag.IntVar(&opts.capacityHysteresisPercent, "capacity-hysteresis-percent", opts.capacityHysteresisPercent,
		"How far observed capacity must fall below the recorded high-water mark before the mark follows "+
			"it down. Budget checks read the mark, not the instantaneous value, because capacity dips for "+
			"reasons unrelated to entitlement — a drain, a reboot, a device plugin restarting — and "+
			"following those down would mark budgets Degraded every time a rack was patched. Growth is "+
			"always believed immediately. 0 disables damping.")
	flag.StringVar(&opts.coverResources, "cover-resources", opts.coverResources,
		"Comma-separated resource=quantity pairs every rendered ClusterQueue funds alongside its "+
			"accelerator budget, e.g. \"cpu=1k,memory=1Ti\". Kueue refuses to admit a workload "+
			"requesting a resource its queue does not cover, and every serving pod requests cpu and "+
			"memory, so a queue budgeted only for accelerators admits nothing and reports the reason "+
			"nowhere. These are a ceiling high enough not to be a budget, which is why they are "+
			"configured rather than authored on the CR. Empty disables materialization.")
	flag.StringVar(&opts.fieldManager, "field-manager", opts.fieldManager,
		"Field manager that owns the Kueue objects this process applies, and the value of the "+
			"managed-by label it selects them by. Two managers pointed at one cluster must not share "+
			"it, or each will treat the other's objects as its own. Empty disables materialization.")
	flag.DurationVar(&opts.resyncInterval, "resync-interval", opts.resyncInterval,
		"Re-run the tree checks on an otherwise-idle plane, so a violation only a clock can clear "+
			"is noticed without an edit. 0 leaves reconciles purely event-driven.")
	flag.BoolVar(&opts.enableWebhook, "webhook", opts.enableWebhook,
		"Serve the AcceleratorQuota validating webhook. On by default: it is what turns a "+
			"tree-breaking write into a rejected kubectl apply instead of a Degraded condition "+
			"minutes later. Disable only to run the controller while the webhook's certs or "+
			"Service are broken, since the ValidatingWebhookConfiguration is failurePolicy=Fail.")
	flag.IntVar(&opts.webhookPort, "webhook-port", opts.webhookPort,
		"Port the webhook server binds to.")
	flag.StringVar(&opts.webhookCertDir, "webhook-cert-dir", opts.webhookCertDir,
		"Directory holding tls.crt and tls.key for the webhook server. With internal cert "+
			"management this must be where the cert Secret is mounted: the process writes the "+
			"Secret, and the kubelet projects it back down to this path.")
	flag.BoolVar(&opts.internalCertManagement, "internal-cert-management", opts.internalCertManagement,
		"Generate and rotate the webhook serving cert in process, and inject the caBundle into "+
			"this component's own ValidatingWebhookConfiguration. On by default so admission works "+
			"out of the box with no cert-manager in the cluster. Set false to have cert-manager "+
			"supply the cert, in which case the chart must mount it and annotate the webhook config.")
	// The object names below are derived from the Helm release name, so there is
	// no sensible compiled-in default: one would silently disagree with any
	// release not called what the binary guessed.
	flag.StringVar(&opts.certNamespace, "cert-namespace", opts.certNamespace,
		"Namespace holding the webhook Secret and Service. Required with internal cert management; "+
			"the chart supplies it from the downward API.")
	flag.StringVar(&opts.certSecretName, "cert-secret-name", opts.certSecretName,
		"Secret the generated CA and serving cert are stored in. The chart must create it empty — "+
			"the rotator updates it and never creates it.")
	flag.StringVar(&opts.webhookServiceName, "webhook-service-name", opts.webhookServiceName,
		"Webhook Service name, used to derive the certificate's DNS name.")
	flag.StringVar(&opts.webhookConfigName, "webhook-config-name", opts.webhookConfigName,
		"ValidatingWebhookConfiguration to inject the generated caBundle into.")
	flag.StringVar(&opts.caName, "ca-name", opts.caName,
		"Common name of the generated CA.")
	flag.StringVar(&opts.caOrganization, "ca-organization", opts.caOrganization,
		"Organization of the generated CA.")
	opts.zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts.zapOpts)))

	mode := acceleratorquota.Mode(opts.mode)
	switch mode {
	case acceleratorquota.ModeWorkload, acceleratorquota.ModeManagement:
	case "":
		setupLog.Error(fmt.Errorf("--mode is required (want %q or %q)",
			acceleratorquota.ModeWorkload, acceleratorquota.ModeManagement), "bad flag")
		os.Exit(1)
	default:
		setupLog.Error(fmt.Errorf("invalid --mode %q (want %q or %q)",
			opts.mode, acceleratorquota.ModeWorkload, acceleratorquota.ModeManagement), "bad flag")
		os.Exit(1)
	}

	mgrOpts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: opts.metricsAddr},
		HealthProbeBindAddress: opts.probeAddr,
		LeaderElection:         opts.enableLeaderElection,
		LeaderElectionID:       opts.leaderElectionID,
	}
	if opts.enableWebhook {
		mgrOpts.WebhookServer = webhook.NewServer(webhook.Options{
			Port:    opts.webhookPort,
			CertDir: opts.webhookCertDir,
		})
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOpts)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	setupLog.Info("Setting up AcceleratorQuota controller",
		"mode", mode, "rootName", v1beta1.AcceleratorQuotaRootName,
		"maxTreeDepth", opts.maxTreeDepth, "resyncInterval", opts.resyncInterval)

	r := &acceleratorquota.Reconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Log:       ctrl.Log.WithName("controllers").WithName("AcceleratorQuota"),
		APIReader: mgr.GetAPIReader(),
		Options: tree.Options{
			// Fixed, not configured: the name is part of the CR contract, so a
			// tree authored against one plane reads the same on every other.
			RootName: v1beta1.AcceleratorQuotaRootName,
			MaxDepth: opts.maxTreeDepth,
		},
		ResyncInterval: opts.resyncInterval,
	}
	// Workload mode only. A management-mode manager holds the authored fleet
	// tree; the nodes it would measure belong to members it reaches over a
	// remote client, and their capacity arrives as status read back from each
	// member's own root rather than as something this process samples.
	if mode == acceleratorquota.ModeWorkload {
		r.Capacity = acceleratorquota.CapacityOptions{
			Resources:         splitAndTrim(opts.acceleratorResources),
			HysteresisPercent: int32(opts.capacityHysteresisPercent),
		}
		if r.Capacity.Enabled() {
			setupLog.Info("Deriving cluster capacity onto the reserved root",
				"resources", r.Capacity.Resources,
				"hysteresisPercent", r.Capacity.HysteresisPercent)
		}

		cover, err := parseCoverResources(opts.coverResources)
		if err != nil {
			setupLog.Error(err, "invalid --cover-resources")
			os.Exit(1)
		}
		backendOpts := kueuebackend.Options{FieldManager: opts.fieldManager, CoverResources: cover}
		if err := backendOpts.Validate(); err != nil {
			setupLog.Error(err, "invalid materialization configuration")
			os.Exit(1)
		}
		if backendOpts.Enabled() {
			r.Materialize = acceleratorquota.MaterializeOptions{
				Backend: &kueuebackend.Backend{
					Writer:  mgr.GetClient(),
					Reader:  mgr.GetAPIReader(),
					Options: backendOpts,
				},
			}
			setupLog.Info("Materializing the quota tree into Kueue",
				"fieldManager", backendOpts.FieldManager,
				"coverResources", opts.coverResources)
		}
	}
	if err := r.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create the AcceleratorQuota controller")
		os.Exit(1)
	}

	// certsReady is closed once the serving cert is on disk and the caBundle is
	// injected. Without internal cert management the cert is already mounted, so
	// there is nothing to wait for.
	certsReady := make(chan struct{})
	if opts.enableWebhook && opts.internalCertManagement {
		setupLog.Info("Setting up internal cert management",
			"secret", opts.certSecretName, "service", opts.webhookServiceName,
			"webhookConfig", opts.webhookConfigName)
		if err := quotacert.Manage(mgr, quotacert.Options{
			Namespace:         opts.certNamespace,
			SecretName:        opts.certSecretName,
			ServiceName:       opts.webhookServiceName,
			WebhookConfigName: opts.webhookConfigName,
			CertDir:           opts.webhookCertDir,
			CAName:            opts.caName,
			CAOrganization:    opts.caOrganization,
		}, certsReady); err != nil {
			setupLog.Error(err, "unable to set up internal cert management")
			os.Exit(1)
		}
	} else {
		close(certsReady)
	}

	if opts.enableWebhook {
		// Registered from a goroutine that waits on the cert, so manager startup
		// is not blocked: the rotator itself is a manager Runnable, so waiting
		// inline here would deadlock — nothing would generate the cert we are
		// waiting for.
		go func() {
			quotacert.WaitForReady(setupLog, certsReady)
			setupLog.Info("Registering AcceleratorQuota validating webhook")
			// The same Options the controller uses: admission and reconcile must
			// agree on what a valid tree is, or a write the webhook admits goes
			// straight to Degraded.
			mgr.GetWebhookServer().Register("/validate-ome-io-v1beta1-acceleratorquota", &webhook.Admission{
				Handler: &quotawebhook.Validator{
					Client:  mgr.GetAPIReader(),
					Decoder: admission.NewDecoder(mgr.GetScheme()),
					Options: r.Options,
				},
			})
		}()
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up the health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("certs", func(*http.Request) error {
		select {
		case <-certsReady:
			return nil
		default:
			return fmt.Errorf("webhook certificate is not ready")
		}
	}); err != nil {
		setupLog.Error(err, "unable to set up the certificate readiness check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up the readiness check")
		os.Exit(1)
	}

	setupLog.Info("Starting ome-quota-manager", "mode", mode)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// parseCoverResources reads "cpu=1k,memory=1Ti" into quantities. An empty list
// is not an error: it is how an operator says materialization is off.
func parseCoverResources(csv string) (map[corev1.ResourceName]resource.Quantity, error) {
	pairs := splitAndTrim(csv)
	if len(pairs) == 0 {
		return nil, nil
	}
	out := make(map[corev1.ResourceName]resource.Quantity, len(pairs))
	for _, pair := range pairs {
		name, value, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("cover resource %q is not name=quantity", pair)
		}
		name = strings.TrimSpace(name)
		qty, err := resource.ParseQuantity(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("cover resource %q: %w", name, err)
		}
		out[corev1.ResourceName(name)] = qty
	}
	return out, nil
}

// splitAndTrim turns a comma-separated flag into a slice, dropping blanks so a
// trailing comma or a quoted empty string disables derivation rather than
// configuring a suffix that matches every resource name.
func splitAndTrim(csv string) []string {
	var out []string
	for _, part := range strings.Split(csv, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
