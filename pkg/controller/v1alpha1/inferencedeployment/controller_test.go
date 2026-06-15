package inferencedeployment

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	knapis "knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	omev1alpha1 "github.com/ome-projects/ome/pkg/apis/ome/v1alpha1"
	omev1beta1 "github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestReconcileAppliesInferenceService(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	deployment := &omev1alpha1.InferenceDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "llama",
			Namespace:  "control",
			Generation: 7,
		},
		Spec: testInferenceDeploymentSpec(),
	}

	local := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(deployment).
		WithStatusSubresource(&omev1alpha1.InferenceDeployment{}).
		Build()
	existingISVC := &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "serving", Generation: 4},
		Status: inferenceServiceStatus(
			corev1.ConditionTrue,
			"Ready",
			"ready",
			4,
			isvcCondition(omev1beta1.EngineReady, corev1.ConditionTrue, "Ready", "engine ready"),
		),
	}
	remote := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingISVC).
		WithStatusSubresource(&omev1beta1.InferenceService{}).
		Build()

	reconciler := &InferenceDeploymentReconciler{
		Client: local,
		Log:    logr.Discard(),
		Scheme: scheme,
		NewRemoteClient: func(context.Context, string, *runtime.Scheme) (client.Client, error) {
			return remote, nil
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "control", Name: "llama"}})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("Reconcile() requeueAfter = %s, want 0", result.RequeueAfter)
	}

	isvc := &omev1beta1.InferenceService{}
	if err := remote.Get(ctx, client.ObjectKey{Namespace: "serving", Name: "llama"}, isvc); err != nil {
		t.Fatalf("remote InferenceService not applied: %v", err)
	}
	if isvc.Spec.Model != nil {
		t.Fatalf("InferenceService model = %#v, want nil", isvc.Spec.Model)
	}
	if isvc.Spec.Runtime == nil || isvc.Spec.Runtime.Name != defaultRuntimeName {
		t.Fatalf("InferenceService runtime = %#v, want %s", isvc.Spec.Runtime, defaultRuntimeName)
	}
	if isvc.Spec.Engine == nil || isvc.Spec.Engine.MinReplicas == nil || *isvc.Spec.Engine.MinReplicas != 2 {
		t.Fatalf("engine minReplicas = %#v, want 2", isvc.Spec.Engine)
	}
	if isvc.Spec.Engine.MaxReplicas != 4 {
		t.Fatalf("engine maxReplicas = %d, want 4", isvc.Spec.Engine.MaxReplicas)
	}
	if isvc.Spec.Engine.Runner == nil || isvc.Spec.Engine.Runner.Image != "sglang:test" {
		t.Fatalf("engine runner = %#v, want image sglang:test", isvc.Spec.Engine.Runner)
	}
	if !startsWithStrings(isvc.Spec.Engine.Runner.Command, sglangLaunchCommand()) {
		t.Fatalf("engine command = %#v, want prefix %#v", isvc.Spec.Engine.Runner.Command, sglangLaunchCommand())
	}
	if got := envValue(isvc.Spec.Engine.Runner.Env, checkpointEnvName); got != "manifold://model" {
		t.Fatalf("%s env = %q, want checkpoint", checkpointEnvName, got)
	}
	if got := envValue(isvc.Spec.Engine.Runner.Env, "MODEL_PATH"); got != "/opt/ml/model" {
		t.Fatalf("MODEL_PATH env = %q, want default model path", got)
	}
	if got := envValue(isvc.Spec.Engine.Runner.Env, "PARALLELISM_SIZE"); got != "8" {
		t.Fatalf("PARALLELISM_SIZE env = %q, want 8", got)
	}
	if got := envValue(isvc.Spec.Engine.Runner.Env, "CLUSTER_CACHE_VERIFY_TENSOR_HASH_AGAINST"); got != "@tp8ep4" {
		t.Fatalf("CLUSTER_CACHE_VERIFY_TENSOR_HASH_AGAINST env = %q, want @tp8ep4", got)
	}
	if got := envValue(isvc.Spec.Engine.Runner.Env, "CLUSTER_CACHE_HEADLESS_SERVICE"); got != "cluster-cache-headless.clustercache.svc.cluster.local:10090" {
		t.Fatalf("CLUSTER_CACHE_HEADLESS_SERVICE env = %q, want hidden default", got)
	}
	if len(isvc.Spec.Engine.Runner.Args) != 0 {
		t.Fatalf("engine args = %#v, want empty because SGLang flags belong in command", isvc.Spec.Engine.Runner.Args)
	}
	if !containsArgPair(isvc.Spec.Engine.Runner.Command, "--tp-size", "8") {
		t.Fatalf("engine command = %#v, want --tp-size 8", isvc.Spec.Engine.Runner.Command)
	}
	if !containsArgPair(isvc.Spec.Engine.Runner.Command, "--dtype", "bfloat16") {
		t.Fatalf("engine command = %#v, want --dtype bfloat16", isvc.Spec.Engine.Runner.Command)
	}
	if !containsArgPair(isvc.Spec.Engine.Runner.Command, "--mem-fraction-static", "0.82") {
		t.Fatalf("engine command = %#v, want common --mem-fraction-static 0.82", isvc.Spec.Engine.Runner.Command)
	}
	if !containsArg(isvc.Spec.Engine.Runner.Command, "--enable-sp") {
		t.Fatalf("engine command = %#v, want prefill-only --enable-sp", isvc.Spec.Engine.Runner.Command)
	}
	if !containsArgPair(isvc.Spec.Engine.Runner.Command, "--disaggregation-mode", "prefill") {
		t.Fatalf("engine command = %#v, want prefill disaggregation mode", isvc.Spec.Engine.Runner.Command)
	}
	if isvc.Spec.Decoder == nil || isvc.Spec.Decoder.MinReplicas == nil || *isvc.Spec.Decoder.MinReplicas != 1 {
		t.Fatalf("decoder minReplicas = %#v, want 1", isvc.Spec.Decoder)
	}
	if !startsWithStrings(isvc.Spec.Decoder.Runner.Command, sglangLaunchCommand()) {
		t.Fatalf("decoder command = %#v, want prefix %#v", isvc.Spec.Decoder.Runner.Command, sglangLaunchCommand())
	}
	if len(isvc.Spec.Decoder.Runner.Args) != 0 {
		t.Fatalf("decoder args = %#v, want empty because SGLang flags belong in command", isvc.Spec.Decoder.Runner.Args)
	}
	if !containsArgPair(isvc.Spec.Decoder.Runner.Command, "--disaggregation-mode", "decode") {
		t.Fatalf("decoder command = %#v, want decode disaggregation mode", isvc.Spec.Decoder.Runner.Command)
	}
	if !containsArgPair(isvc.Spec.Decoder.Runner.Command, "--mem-fraction-static", "0.70") {
		t.Fatalf("decoder command = %#v, want decode override --mem-fraction-static 0.70", isvc.Spec.Decoder.Runner.Command)
	}
	if got := isvc.Labels[labelDeployment]; got != "llama" {
		t.Fatalf("deployment label = %q, want llama", got)
	}

	updated := &omev1alpha1.InferenceDeployment{}
	if err := local.Get(ctx, client.ObjectKey{Namespace: "control", Name: "llama"}, updated); err != nil {
		t.Fatalf("get updated InferenceDeployment: %v", err)
	}
	ready := findCondition(updated.Status.Conditions, omev1alpha1.InferenceDeploymentReady)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %#v, want True", ready)
	}
	if ready.Reason != "InferenceServicesReady" {
		t.Fatalf("Ready reason = %q, want InferenceServicesReady", ready.Reason)
	}
	if len(updated.Status.Clusters) != 1 || updated.Status.Clusters[0].DesiredReplicas != 2 {
		t.Fatalf("status clusters = %#v, want one cluster with desired replicas 2", updated.Status.Clusters)
	}
	clusterReady := findCondition(updated.Status.Clusters[0].Conditions, omev1alpha1.InferenceDeploymentReady)
	if clusterReady == nil || clusterReady.Status != metav1.ConditionTrue {
		t.Fatalf("cluster Ready condition = %#v, want True", clusterReady)
	}
}

func TestReconcileRecordsObservedServingReplicas(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	deployment := &omev1alpha1.InferenceDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "control", Generation: 3},
		Spec:       testInferenceDeploymentSpec(),
	}
	existingISVC := &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "serving"},
		Spec: omev1beta1.InferenceServiceSpec{
			Engine: &omev1beta1.EngineSpec{},
		},
		Status: omev1beta1.InferenceServiceStatus{
			Status: duckv1.Status{
				ObservedGeneration: 11,
				Conditions: duckv1.Conditions{{
					Type:    knapis.ConditionReady,
					Status:  corev1.ConditionTrue,
					Reason:  "Ready",
					Message: "ready",
				}, {
					Type:    omev1beta1.EngineReady,
					Status:  corev1.ConditionTrue,
					Reason:  "Ready",
					Message: "engine ready",
				}},
			},
			Components: map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
				omev1beta1.EngineComponent: {
					Lifecycle: &omev1beta1.LifecycleStatus{
						Replicas:        5,
						ReadyReplicas:   4,
						ServingReplicas: 3,
					},
				},
			},
		},
	}

	local := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(deployment).
		WithStatusSubresource(&omev1alpha1.InferenceDeployment{}).
		Build()
	remote := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingISVC).
		WithStatusSubresource(&omev1beta1.InferenceService{}).
		Build()

	reconciler := &InferenceDeploymentReconciler{
		Client: local,
		Log:    logr.Discard(),
		Scheme: scheme,
		NewRemoteClient: func(context.Context, string, *runtime.Scheme) (client.Client, error) {
			return remote, nil
		},
	}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "control", Name: "llama"}}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	updated := &omev1alpha1.InferenceDeployment{}
	if err := local.Get(ctx, client.ObjectKey{Namespace: "control", Name: "llama"}, updated); err != nil {
		t.Fatalf("get updated InferenceDeployment: %v", err)
	}
	status := updated.Status.Clusters[0]
	if status.CurrentReplicas != 5 || status.ReadyReplicas != 4 || status.ServingReplicas != 3 {
		t.Fatalf("observed replicas = current %d ready %d serving %d, want 5/4/3", status.CurrentReplicas, status.ReadyReplicas, status.ServingReplicas)
	}
	if status.InferenceServiceObservedGeneration != 11 {
		t.Fatalf("inferenceServiceObservedGeneration = %d, want 11", status.InferenceServiceObservedGeneration)
	}
}

func TestReconcileRequiresRouterReadyButIgnoresRouterReplicas(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	deployment := &omev1alpha1.InferenceDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "control", Generation: 3},
		Spec:       testInferenceDeploymentSpecWithRouter(),
	}
	existingISVC := &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "serving"},
		Spec: omev1beta1.InferenceServiceSpec{
			Engine: &omev1beta1.EngineSpec{},
			Router: &omev1beta1.RouterSpec{},
		},
		Status: omev1beta1.InferenceServiceStatus{
			Status: duckv1.Status{
				ObservedGeneration: 11,
				Conditions: duckv1.Conditions{
					isvcCondition(knapis.ConditionReady, corev1.ConditionTrue, "Ready", "ready"),
					isvcCondition(omev1beta1.EngineReady, corev1.ConditionTrue, "Ready", "engine ready"),
					isvcCondition(omev1beta1.RouterReady, corev1.ConditionFalse, "RouterNotReady", "router not ready"),
				},
			},
			Components: map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
				omev1beta1.RouterComponent: {
					Lifecycle: &omev1beta1.LifecycleStatus{
						Replicas:        9,
						ReadyReplicas:   8,
						ServingReplicas: 7,
					},
				},
				omev1beta1.EngineComponent: {
					Lifecycle: &omev1beta1.LifecycleStatus{
						Replicas:        5,
						ReadyReplicas:   4,
						ServingReplicas: 3,
					},
				},
			},
		},
	}

	local := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(deployment).
		WithStatusSubresource(&omev1alpha1.InferenceDeployment{}).
		Build()
	remote := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingISVC).
		WithStatusSubresource(&omev1beta1.InferenceService{}).
		Build()

	reconciler := &InferenceDeploymentReconciler{
		Client: local,
		Log:    logr.Discard(),
		Scheme: scheme,
		NewRemoteClient: func(context.Context, string, *runtime.Scheme) (client.Client, error) {
			return remote, nil
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "control", Name: "llama"}})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != inferenceServiceStatusPollInterval {
		t.Fatalf("Reconcile() requeueAfter = %s, want %s", result.RequeueAfter, inferenceServiceStatusPollInterval)
	}

	updated := &omev1alpha1.InferenceDeployment{}
	if err := local.Get(ctx, client.ObjectKey{Namespace: "control", Name: "llama"}, updated); err != nil {
		t.Fatalf("get updated InferenceDeployment: %v", err)
	}
	status := updated.Status.Clusters[0]
	if status.CurrentReplicas != 5 || status.ReadyReplicas != 4 || status.ServingReplicas != 3 {
		t.Fatalf("replicas = current %d ready %d serving %d, want engine 5/4/3", status.CurrentReplicas, status.ReadyReplicas, status.ServingReplicas)
	}
	ready := findCondition(updated.Status.Conditions, omev1alpha1.InferenceDeploymentReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "InferenceServicesNotReady" {
		t.Fatalf("Ready condition = %#v, want False/InferenceServicesNotReady", ready)
	}
	clusterReady := findCondition(status.Conditions, omev1alpha1.InferenceDeploymentReady)
	if clusterReady == nil || clusterReady.Status != metav1.ConditionFalse || clusterReady.Reason != "RouterNotReady" {
		t.Fatalf("cluster Ready condition = %#v, want False/RouterNotReady", clusterReady)
	}
	routerReady := findCondition(status.Conditions, string(omev1beta1.RouterReady))
	if routerReady == nil || routerReady.Status != metav1.ConditionFalse {
		t.Fatalf("router Ready condition = %#v, want False", routerReady)
	}
}

func TestReconcileWaitsForInferenceServiceReady(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	deployment := &omev1alpha1.InferenceDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "control", Generation: 3},
		Spec:       testInferenceDeploymentSpec(),
	}
	existingISVC := &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "serving", Generation: 4},
		Spec: omev1beta1.InferenceServiceSpec{
			Engine: &omev1beta1.EngineSpec{},
		},
		Status: inferenceServiceStatus(corev1.ConditionFalse, "IngressNotReady", "waiting for route", 3),
	}

	local := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(deployment).
		WithStatusSubresource(&omev1alpha1.InferenceDeployment{}).
		Build()
	remote := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingISVC).
		WithStatusSubresource(&omev1beta1.InferenceService{}).
		Build()

	reconciler := &InferenceDeploymentReconciler{
		Client: local,
		Log:    logr.Discard(),
		Scheme: scheme,
		NewRemoteClient: func(context.Context, string, *runtime.Scheme) (client.Client, error) {
			return remote, nil
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "control", Name: "llama"}})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != inferenceServiceStatusPollInterval {
		t.Fatalf("Reconcile() requeueAfter = %s, want %s", result.RequeueAfter, inferenceServiceStatusPollInterval)
	}

	updated := &omev1alpha1.InferenceDeployment{}
	if err := local.Get(ctx, client.ObjectKey{Namespace: "control", Name: "llama"}, updated); err != nil {
		t.Fatalf("get updated InferenceDeployment: %v", err)
	}
	ready := findCondition(updated.Status.Conditions, omev1alpha1.InferenceDeploymentReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "InferenceServicesNotReady" {
		t.Fatalf("Ready condition = %#v, want False/InferenceServicesNotReady", ready)
	}
	clusterReady := findCondition(updated.Status.Clusters[0].Conditions, omev1alpha1.InferenceDeploymentReady)
	if clusterReady == nil || clusterReady.Status != metav1.ConditionFalse || clusterReady.Reason != "IngressNotReady" {
		t.Fatalf("cluster Ready condition = %#v, want False/IngressNotReady", clusterReady)
	}
}

func TestReconcileRecordsTargetFailureWhenTargetClusterIsMissing(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	deployment := &omev1alpha1.InferenceDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "control", Generation: 3},
		Spec:       testInferenceDeploymentSpec(),
	}

	local := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(deployment).
		WithStatusSubresource(&omev1alpha1.InferenceDeployment{}).
		Build()
	reconciler := &InferenceDeploymentReconciler{
		Client: local,
		Log:    logr.Discard(),
		Scheme: scheme,
	}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "control", Name: "broken"}}); err == nil {
		t.Fatalf("Reconcile() error = nil, want missing remote client factory error")
	}

	updated := &omev1alpha1.InferenceDeployment{}
	if err := local.Get(ctx, client.ObjectKey{Namespace: "control", Name: "broken"}, updated); err != nil {
		t.Fatalf("get updated InferenceDeployment: %v", err)
	}
	ready := findCondition(updated.Status.Conditions, omev1alpha1.InferenceDeploymentReady)
	if ready == nil || ready.Status != metav1.ConditionFalse {
		t.Fatalf("Ready condition = %#v, want False", ready)
	}
	clusterReady := findCondition(updated.Status.Clusters[0].Conditions, omev1alpha1.InferenceDeploymentReady)
	if clusterReady == nil || clusterReady.Reason != "RemoteClientFailed" {
		t.Fatalf("cluster Ready condition = %#v, want RemoteClientFailed", clusterReady)
	}
}

func TestReconcileMarksInvalidSpecWithoutRemoteApply(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	spec := testInferenceDeploymentSpec()
	spec.Clusters[0].Prefill.MinReplicas = 5
	spec.Clusters[0].Prefill.MaxReplicas = 2
	deployment := &omev1alpha1.InferenceDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "control", Generation: 3},
		Spec:       spec,
	}

	local := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(deployment).
		WithStatusSubresource(&omev1alpha1.InferenceDeployment{}).
		Build()
	reconciler := &InferenceDeploymentReconciler{
		Client: local,
		Log:    logr.Discard(),
		Scheme: scheme,
		NewRemoteClient: func(context.Context, string, *runtime.Scheme) (client.Client, error) {
			t.Fatalf("remote client should not be created for invalid spec")
			return nil, nil
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "control", Name: "broken"}})
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want nil", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("Reconcile() requeueAfter = %s, want 0", result.RequeueAfter)
	}

	updated := &omev1alpha1.InferenceDeployment{}
	if err := local.Get(ctx, client.ObjectKey{Namespace: "control", Name: "broken"}, updated); err != nil {
		t.Fatalf("get updated InferenceDeployment: %v", err)
	}
	ready := findCondition(updated.Status.Conditions, omev1alpha1.InferenceDeploymentReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "InvalidSpec" {
		t.Fatalf("Ready condition = %#v, want False/InvalidSpec", ready)
	}
	clusterReady := findCondition(updated.Status.Clusters[0].Conditions, omev1alpha1.InferenceDeploymentReady)
	if clusterReady == nil || clusterReady.Reason != "InvalidSpec" {
		t.Fatalf("cluster Ready condition = %#v, want InvalidSpec", clusterReady)
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := omev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1alpha1 scheme: %v", err)
	}
	if err := omev1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1 scheme: %v", err)
	}
	return scheme
}

func testInferenceDeploymentSpec() omev1alpha1.InferenceDeploymentSpec {
	return omev1alpha1.InferenceDeploymentSpec{
		Namespace:         "serving",
		PriorityClassName: "prod",
		Checkpoint:        "manifold://model",
		Clusters: []omev1alpha1.WorkloadCluster{{
			Name: "cluster-a",
			Prefill: omev1alpha1.ComponentScale{
				MinReplicas: 2,
				MaxReplicas: 4,
			},
			Decode: &omev1alpha1.ComponentScale{
				MinReplicas: 1,
				MaxReplicas: 3,
			},
		}},
		SGLang: omev1alpha1.SGLangSpec{
			Image:  "sglang:test",
			GPUs:   4,
			CPU:    "50",
			Memory: "800G",
			Env: map[string]string{
				"SGLANG_CLUSTER": "cks",
			},
			Args: map[string]string{
				"dtype":               "bfloat16",
				"ep-size":             "4",
				"mem-fraction-static": "0.82",
				"tp-size":             "8",
			},
			Components: &omev1alpha1.SGLangComponentOverrides{
				Prefill: &omev1alpha1.SGLangComponentOverride{
					Args: map[string]string{
						"enable-sp": "true",
					},
				},
				Decode: &omev1alpha1.SGLangComponentOverride{
					Args: map[string]string{
						"mem-fraction-static": "0.70",
					},
				},
			},
		},
	}
}

func testInferenceDeploymentSpecWithRouter() omev1alpha1.InferenceDeploymentSpec {
	spec := testInferenceDeploymentSpec()
	spec.Clusters[0].Router = &omev1alpha1.ComponentScale{
		MinReplicas: 2,
		MaxReplicas: 2,
	}
	spec.Router = &omev1alpha1.RouterSpec{
		Image:  "router:test",
		CPU:    "16",
		Memory: "32G",
		Args: map[string]string{
			"model-name": "llama",
		},
	}
	return spec
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, item := range env {
		if item.Name == name {
			return item.Value
		}
	}
	return ""
}

func containsArgPair(args []string, flag, value string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

func containsArg(args []string, flag string) bool {
	for _, arg := range args {
		if arg == flag {
			return true
		}
	}
	return false
}

func startsWithStrings(value, prefix []string) bool {
	if len(value) < len(prefix) {
		return false
	}
	for i := range prefix {
		if value[i] != prefix[i] {
			return false
		}
	}
	return true
}

func inferenceServiceStatus(status corev1.ConditionStatus, reason, message string, observedGeneration int64, extraConditions ...knapis.Condition) omev1beta1.InferenceServiceStatus {
	conditions := duckv1.Conditions{{
		Type:    knapis.ConditionReady,
		Status:  status,
		Reason:  reason,
		Message: message,
	}}
	conditions = append(conditions, extraConditions...)
	return omev1beta1.InferenceServiceStatus{
		Status: duckv1.Status{
			ObservedGeneration: observedGeneration,
			Conditions:         conditions,
		},
	}
}

func isvcCondition(conditionType knapis.ConditionType, status corev1.ConditionStatus, reason, message string) knapis.Condition {
	return knapis.Condition{
		Type:    conditionType,
		Status:  status,
		Reason:  reason,
		Message: message,
	}
}

func findCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}
