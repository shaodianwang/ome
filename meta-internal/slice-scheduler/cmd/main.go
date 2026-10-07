/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"time"

	uberzap "go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	v1 "tpu.google.com/slice-scheduler/apis/partition/v1beta1"
	slicev1beta1 "tpu.google.com/slice-scheduler/apis/slice/v1beta1"
	"tpu.google.com/slice-scheduler/internal/generators"
	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/plugins"
	"tpu.google.com/slice-scheduler/internal/pod_reconciler"
	"tpu.google.com/slice-scheduler/internal/policy"
	"tpu.google.com/slice-scheduler/internal/scheduler"
	"tpu.google.com/slice-scheduler/internal/schedulerlib"
	"tpu.google.com/slice-scheduler/internal/watcher"

	// +kubebuilder:scaffold:imports

	_ "tpu.google.com/slice-scheduler/internal/metrics"
)

var (
	scheme = runtime.NewScheme()

	// Version information
	buildTimestamp = "unknown"
	buildCommit    = "unknown"
	semver         = "unknown"
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(slicev1beta1.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func trimPod(obj interface{}) (interface{}, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return obj, nil
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              pod.Name,
			Namespace:         pod.Namespace,
			Labels:            pod.Labels,
			Annotations:       pod.Annotations,
			DeletionTimestamp: pod.DeletionTimestamp,
		},
		Spec: corev1.PodSpec{
			SchedulerName: pod.Spec.SchedulerName,
			NodeName:      pod.Spec.NodeName,
		},
	}, nil
}

func main() {
	var tpuType string
	var metricsAddr string
	var pprofAddr string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var enableNodeReadiness bool
	var excludeCordonedNodes bool
	var enablePodReconciler bool
	var waitForSliceReady bool
	var podReconcilerWorkers int
	var rescheduleHealthyDelay time.Duration
	var rescheduleUnhealthyDelay time.Duration
	var rescheduleTimeout time.Duration
	flag.StringVar(&tpuType, "tpu-type", "v6e", "Scheduler for a particular tpuType")
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metric endpoint binds to. "+
		"Use the port :8080. If not set, it will be '0 in order to disable the metrics server")
	flag.StringVar(&pprofAddr, "pprof-bind-address", "",
		"The address the pprof endpoint binds to. "+
			"If not set or set to '0', pprof is disabled.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set the metrics endpoint is served securely")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.BoolVar(&enableNodeReadiness, "enable-node-readiness", false,
		"If set, the scheduler takes the k8s node presence and readiness into account.")
	flag.BoolVar(&excludeCordonedNodes, "exclude-cordoned-nodes", false,
		"If set, new slices are not placed on partitions whose k8s nodes are cordoned. "+
			"Slices already on a cordoned node are left in place.")
	flag.BoolVar(&enablePodReconciler, "enable-pod-reconciler", false, "Enable the pod reconciler for pod-to-node binding")
	flag.BoolVar(&waitForSliceReady, "wait-for-slice-ready", true,
		"If set, the pod-reconciler will wait for the associated slice to be in READY state before binding.")
	flag.IntVar(&podReconcilerWorkers, "pod-reconciler-workers", 4, "Number of concurrent pod reconciler workers")
	flag.DurationVar(&rescheduleHealthyDelay, "reschedule-healthy-delay", 1*time.Minute,
		"Delay for rescheduling slices when partitions are healthy.")
	flag.DurationVar(&rescheduleUnhealthyDelay, "reschedule-unhealthy-delay", 5*time.Second,
		"Delay for rescheduling slices when partitions are unhealthy.")
	flag.DurationVar(&rescheduleTimeout, "reschedule-timeout", 10*time.Minute,
		"Timeout for healthy-partition backoff before forcing reschedule. Set to 0 to disable backoff.")
	opts := zap.Options{

		ZapOpts: []uberzap.Option{uberzap.AddCaller()},
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")
	klog.SetLogger(setupLog)

	setupLog.Info("Starting manager", "version", semver, "buildCommit", buildCommit, "buildTimestamp", buildTimestamp)

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	tlsOpts := []func(*tls.Config){}
	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: tlsOpts,
	})

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Pod{}: {
					Field:     fields.SelectorFromSet(fields.Set{"spec.schedulerName": interfaces.SliceSchedulerName}),
					Transform: trimPod,
				},
			},
		},
		Metrics: metricsserver.Options{
			BindAddress:   metricsAddr,
			SecureServing: secureMetrics,
			TLSOpts:       tlsOpts,
		},
		Logger:                 setupLog.WithName("manager"),
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		PprofBindAddress:       pprofAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "1342b0bb.tpu.google.com",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Index Pods by node name for efficient lookup in the informer cache.
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&corev1.Pod{},
		"spec.nodeName",
		func(rawObj client.Object) []string {
			pod := rawObj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		},
	); err != nil {
		setupLog.Error(err, "unable to set up field indexer for pod nodeName")
		os.Exit(1)
	}

	// Initialize modular scheduler components
	gens := []interfaces.PlacementGenerator{
		generators.NewGroupGenerator(generators.GroupGeneratorConfig{BeamWidth: 3}),
	}

	filters := []interfaces.FilterPlugin{
		&plugins.ExcludeDegradedFilter{},
		&plugins.HealthyFilter{},
		&plugins.ManagedFilter{},
		&plugins.PriorityFilter{},
	}
	if excludeCordonedNodes {
		filters = append(filters, &plugins.ExcludeCordonedFilter{})
	}

	scorers := []interfaces.ScorePlugin{
		&plugins.PreemptionScorer{},
		&plugins.DegradationSinkScorer{},
		&plugins.MinimizeSwappedCubesScorer{},
		&plugins.TightestFitScorer{},
	}

	schedulerLib := schedulerlib.NewDefaultScheduler(gens, filters, scorers)

	partitionCache := watcher.NewPartitionCache(enableNodeReadiness)

	slicePolicy := policy.NewCloudPolicy()

	reconciler, err := scheduler.NewSliceReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		ctrl.Log.WithName(interfaces.SliceSchedulerName),
		tpuType,
		mgr.GetEventRecorderFor(interfaces.SliceSchedulerName),
		enableNodeReadiness,
		schedulerLib,
		partitionCache,
		rescheduleHealthyDelay,
		rescheduleUnhealthyDelay,
		slicePolicy,
		rescheduleTimeout,
	)
	if err == nil {
		reconciler.EnableAsyncBinding(100, 4)
	}
	if err != nil {
		setupLog.Error(err, "unable to Initialize Slice Reconciler")
		os.Exit(1)
	}

	if err = reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Slice")
		os.Exit(1)
	}
	if err := mgr.Add(reconciler); err != nil {
		setupLog.Error(err, "unable to add reconciler as a runnable", "controller", "Slice")
		os.Exit(1)
	}

	if enablePodReconciler {
		podReconciler := pod_reconciler.NewPodReconciler(
			mgr.GetClient(),
			mgr.GetScheme(),
			ctrl.Log.WithName("pod-reconciler"),
			partitionCache,
			mgr.GetEventRecorderFor("pod-reconciler"),
			podReconcilerWorkers,
			waitForSliceReady,
			slicePolicy,
		)

		if err := podReconciler.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "PodReconciler")
			os.Exit(1)
		}
		setupLog.Info("PodReconciler enabled", "workers", podReconcilerWorkers, "waitForSliceReady", waitForSliceReady)
	}

	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
