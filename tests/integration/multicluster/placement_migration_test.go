package multicluster

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
)

func TestPlacementMigrationBridge(t *testing.T) {
	gomega.RegisterTestingT(t) // Framework teardown asserts the controller write-conflict inventory.
	suite, err := framework.NewSuite(&framework.Framework{WebhookPath: "tests/integration/singlecluster/webhook/inferenceservice/testdata"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := suite.Fwk.Teardown(); err != nil {
			t.Error(err)
		}
	})
	if err := suite.Fwk.StartManager(t.Context(), suite.Cfg, func(_ context.Context, mgr ctrl.Manager) error {
		return components.SetupInferenceServiceWebhook(mgr)
	}); err != nil {
		t.Fatal(err)
	}
	cl := suite.Client
	namespace := "placement-migration"
	if err := cl.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil {
		t.Fatal(err)
	}
	webhook := &admissionv1.ValidatingWebhookConfiguration{}
	if err := cl.Get(t.Context(), client.ObjectKey{Name: "inferenceservice.ome.io"}, webhook); err != nil {
		t.Fatal(err)
	}
	clusters := []v1beta1.WorkloadCluster{
		migrationCluster("member-a", map[string]string{"accelerator": "gpu-a", "region": "west", "metadata.name": "spoofed"}),
		migrationCluster("member-b", map[string]string{"accelerator": "gpu-a", "region": "east"}),
		migrationCluster("member-c", map[string]string{"accelerator": "gpu-b", "region": "west"}),
		migrationCluster("member-d", map[string]string{"region": "west"}),
	}
	for i, tt := range []struct {
		name, old, converted string
		want                 []string
	}{
		{
			name: "label and object-name restrictions remain conjunctive",
			old: `spec:
  placement:
    mode: Single
    requirements: accelerator=gpu-a
    clusterSelector: region=west,metadata.name in (member-a,member-b)
`,
			converted: `spec:
  placement:
    policy: ClusterAffinity
    mode: Single
    clusterAffinity:
    - matchExpressions:
      - {key: accelerator, operator: In, values: [gpu-a]}
      - {key: region, operator: In, values: [west]}
      matchFields:
      - {key: metadata.name, operator: In, values: [member-a, member-b]}
`,
			want: []string{"member-a"},
		},
		{
			name: "annotation-only intent becomes explicit Single",
			old: `metadata:
  annotations:
    ome.io/accelerator-requirements: accelerator==gpu-a
    ome.io/cluster-selector: metadata.name!=member-a
`,
			converted: `spec:
  placement:
    policy: ClusterAffinity
    mode: Single
    clusterAffinity:
    - matchExpressions:
      - {key: accelerator, operator: In, values: [gpu-a]}
      matchFields:
      - {key: metadata.name, operator: NotIn, values: [member-a]}
`,
			want: []string{"member-b"},
		},
		{
			name: "typed block takes precedence over both annotations",
			old: `metadata:
  annotations:
    ome.io/accelerator-requirements: accelerator=gpu-b
    ome.io/cluster-selector: metadata.name=member-c
spec:
  placement:
    mode: All
    requirements: accelerator=gpu-a
`,
			converted: `spec:
  placement:
    policy: ClusterAffinity
    mode: All
    clusterAffinity:
    - matchExpressions:
      - {key: accelerator, operator: In, values: [gpu-a]}
`,
			want: []string{"member-a", "member-b"},
		},
		{
			name: "negative label requirements retain absent-key matches",
			old: `spec:
  placement:
    mode: All
    requirements: accelerator!=gpu-b
    clusterSelector: region=west
`,
			converted: `spec:
  placement:
    policy: ClusterAffinity
    mode: All
    clusterAffinity:
    - matchExpressions:
      - {key: accelerator, operator: NotIn, values: [gpu-b]}
      - {key: region, operator: In, values: [west]}
`,
			want: []string{"member-a", "member-d"},
		},
		{
			name: "spread floor and ceiling survive with routing factors",
			old: `spec:
  placement:
    mode: Split
    requirements: accelerator=gpu-a
    split: {replicas: 6, spread: true, maxReplicasPerCluster: 4}
    capacityFactors: {member-a: "2", member-b: "1"}
`,
			converted: `spec:
  placement:
    policy: ClusterAffinity
    mode: Split
    clusterAffinity:
    - matchExpressions:
      - {key: accelerator, operator: In, values: [gpu-a]}
    split: {replicas: 6, maxReplicasPerCluster: 4}
  routing:
    capacityFactors: {member-a: "2", member-b: "1"}
`,
			want: []string{"member-a", "member-b"},
		},
		{
			name: "saved packing and anti-sliver intent survives a manual ratio choice",
			old: `spec:
  placement:
    mode: Split
    clusterSelector: metadata.name in (member-a,member-b)
    split: {replicas: 6, spread: false, minReplicasPerCluster: 0}
`,
			converted: `spec:
  placement:
    policy: ClusterAffinity
    mode: Split
    clusterAffinity:
    - weight: 2
      matchFields:
      - {key: metadata.name, operator: In, values: [member-a]}
    - weight: 1
      matchFields:
      - {key: metadata.name, operator: In, values: [member-b]}
    split: {replicas: 6}
`,
			want: []string{"member-a", "member-b"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			old := migrationManifest(t, namespace, fmt.Sprintf("service-%d", i), tt.old)
			expectedOld := old.DeepCopy()
			awaitPlacementValidation(t, func() error { return cl.Create(t.Context(), old.DeepCopy(), client.DryRunAll) }, false)
			if err := cl.Create(t.Context(), old); err != nil {
				t.Fatal(err)
			}
			saved := readMigrationManifest(t, cl, old)
			if diff := cmp.Diff(expectedOld.Object["spec"], saved.Object["spec"]); diff != "" {
				t.Fatalf("bridge pruned legacy spec:\n%s", diff)
			}
			if diff := cmp.Diff(expectedOld.GetAnnotations(), saved.GetAnnotations()); diff != "" {
				t.Fatalf("bridge lost legacy annotations:\n%s", diff)
			}
			if diff := cmp.Diff(tt.want, legacyMigrationMatches(t, saved, clusters)); diff != "" {
				t.Fatalf("legacy selection (-want +got):\n%s", diff)
			}
			if err := cl.Update(t.Context(), saved.DeepCopy()); err != nil {
				t.Fatalf("legacy update rejected by active validation: %v", err)
			}
			saved = readMigrationManifest(t, cl, saved)
			converted := migrationManifest(t, namespace, old.GetName(), tt.converted)
			expectedConverted := converted.DeepCopy()
			converted.SetResourceVersion(saved.GetResourceVersion())
			if err := cl.Update(t.Context(), converted); err != nil {
				t.Fatal(err)
			}
			stored := readMigrationManifest(t, cl, converted)
			if diff := cmp.Diff(expectedConverted.Object["spec"], stored.Object["spec"]); diff != "" {
				t.Fatalf("bridge pruned converted spec:\n%s", diff)
			}
			typed := &v1beta1.InferenceService{}
			if err := cl.Get(t.Context(), client.ObjectKeyFromObject(stored), typed); err != nil {
				t.Fatal(err)
			}
			matched, _, err := placement.MatchCandidates(typed, clusters)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, matched); diff != "" {
				t.Fatalf("converted selection (-want +got):\n%s", diff)
			}

			// Policy removal must not let an old writer discard accepted authority.
			restore := saved.DeepCopy()
			restore.SetResourceVersion(stored.GetResourceVersion())
			awaitPlacementValidation(t, func() error { return cl.Update(t.Context(), restore.DeepCopy(), client.DryRunAll) }, true)
			if err := cl.Update(t.Context(), restore.DeepCopy()); err == nil || !strings.Contains(err.Error(), "cannot be removed") {
				t.Fatalf("legacy writer was not rejected by placement validation: %v", err)
			}
			if diff := cmp.Diff(stored, readMigrationManifest(t, cl, stored)); diff != "" {
				t.Fatalf("rejected writer changed the stored source:\n%s", diff)
			}
			if err := cl.Update(t.Context(), typed); err != nil {
				t.Fatalf("converted typed update rejected: %v", err)
			}

			// Schema transition validation remains authoritative without a webhook.
			if err := cl.Delete(t.Context(), webhook); err != nil {
				t.Fatal(err)
			}
			restore.SetResourceVersion(typed.ResourceVersion)
			awaitPlacementValidation(t, func() error { return cl.Update(t.Context(), restore.DeepCopy(), client.DryRunAll) }, true)
			if err := cl.Update(t.Context(), restore.DeepCopy()); err == nil || !strings.Contains(err.Error(), "cannot be removed") {
				t.Fatalf("schema permitted policy removal without the webhook: %v", err)
			}
			if diff := cmp.Diff(typed.Spec, readMigrationSpec(t, cl, restore)); diff != "" {
				t.Fatalf("rejected downgrade changed spec:\n%s", diff)
			}

			// An isolated source with no member allocations can be recreated with
			// saved legacy intent once the opted-in object is deleted.
			if err := cl.Delete(t.Context(), typed); err != nil {
				t.Fatal(err)
			}
			if err := wait.PollUntilContextTimeout(t.Context(), eventuallyPoll, eventuallyTimeout, true, func(context.Context) (bool, error) {
				probe := &v1beta1.InferenceService{}
				err := cl.Get(t.Context(), client.ObjectKeyFromObject(typed), probe)
				return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
			}); err != nil {
				t.Fatal(err)
			}
			webhook = &admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: webhook.Name}, Webhooks: webhook.Webhooks}
			if err := cl.Create(t.Context(), webhook); err != nil {
				t.Fatal(err)
			}
			restore = migrationManifest(t, namespace, old.GetName(), tt.old)
			awaitPlacementValidation(t, func() error { return cl.Create(t.Context(), restore.DeepCopy(), client.DryRunAll) }, false)
			if err := cl.Create(t.Context(), restore); err != nil {
				t.Fatal(err)
			}
			rolledBack := readMigrationManifest(t, cl, restore)
			if diff := cmp.Diff(saved.Object["spec"], rolledBack.Object["spec"]); diff != "" {
				t.Fatalf("rollback lost saved fields:\n%s", diff)
			}
			if diff := cmp.Diff(saved.GetAnnotations(), rolledBack.GetAnnotations()); diff != "" {
				t.Fatalf("rollback lost saved annotations:\n%s", diff)
			}
			if diff := cmp.Diff(tt.want, legacyMigrationMatches(t, rolledBack, clusters)); diff != "" {
				t.Fatalf("rollback changed legacy selection:\n%s", diff)
			}
			r := &placement.Reconciler{Client: cl, APIReader: cl, Log: logr.Discard()}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(restore)}); err != nil {
				t.Fatal(err)
			}
			if err := cl.Get(t.Context(), client.ObjectKeyFromObject(restore), typed); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(true, slices.Contains(typed.Finalizers, placement.PlacementFinalizer)); diff != "" {
				t.Fatalf("legacy source not reconciled:\n%s", diff)
			}
			condition := typed.Status.GetCondition(v1beta1.PlacementInputValid)
			if condition == nil || condition.Status != corev1.ConditionTrue {
				t.Fatalf("restored legacy intent lacks valid-input condition: %+v", condition)
			}
		})
	}
}

func migrationManifest(t *testing.T, namespace, name, data string) *unstructured.Unstructured {
	t.Helper()
	out := &unstructured.Unstructured{}
	encoded, err := yaml.YAMLToJSON([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &out.Object); err != nil {
		t.Fatal(err)
	}
	out.SetAPIVersion(v1beta1.SchemeGroupVersion.String())
	out.SetKind("InferenceService")
	out.SetNamespace(namespace)
	out.SetName(name)
	for _, entry := range []struct {
		field string
		value any
	}{
		{"runtime", map[string]any{"name": "runtime-a", "apiGroup": v1beta1.SchemeGroupVersion.Group, "kind": "ClusterServingRuntime", "autoSync": true}},
		{"deploymentMode", string(constants.OMENative)},
	} {
		if err := unstructured.SetNestedField(out.Object, entry.value, "spec", entry.field); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func readMigrationManifest(t *testing.T, cl client.Client, object client.Object) *unstructured.Unstructured {
	t.Helper()
	out := &unstructured.Unstructured{}
	out.SetGroupVersionKind(v1beta1.SchemeGroupVersion.WithKind("InferenceService"))
	if err := cl.Get(t.Context(), client.ObjectKeyFromObject(object), out); err != nil {
		t.Fatal(err)
	}
	return out
}

func migrationCluster(name string, labels map[string]string) v1beta1.WorkloadCluster {
	return v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}, Status: v1beta1.WorkloadClusterStatus{
		Conditions: []metav1.Condition{{Type: v1beta1.WorkloadClusterReady, Status: metav1.ConditionTrue}},
	}}
}

// legacyMigrationMatches is the saved selector contract: a present typed block
// takes precedence as a whole, and its two selectors form one conjunction.
func legacyMigrationMatches(t *testing.T, source *unstructured.Unstructured, clusters []v1beta1.WorkloadCluster) []string {
	t.Helper()
	inputs := []string{source.GetAnnotations()[constants.AcceleratorRequirements], source.GetAnnotations()[constants.ClusterSelector]}
	if p, found, err := unstructured.NestedMap(source.Object, "spec", "placement"); err != nil {
		t.Fatal(err)
	} else if found {
		inputs[0], _, _ = unstructured.NestedString(p, "requirements")
		inputs[1], _, _ = unstructured.NestedString(p, "clusterSelector")
	}
	selector := labels.Everything()
	constrained := false
	for _, raw := range inputs {
		if raw == "" {
			continue
		}
		parsed, err := labels.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		requirements, _ := parsed.Requirements()
		selector = selector.Add(requirements...)
		constrained = true
	}
	var out []string
	for _, cluster := range clusters {
		values := labels.Set{}
		for key, value := range cluster.Labels {
			values[key] = value
		}
		values[metav1.ObjectNameField] = cluster.Name
		if constrained && selector.Matches(values) {
			out = append(out, cluster.Name)
		}
	}
	return out
}

func awaitPlacementValidation(t *testing.T, write func() error, reject bool) {
	t.Helper()
	err := wait.PollUntilContextTimeout(t.Context(), eventuallyPoll, eventuallyTimeout, true, func(context.Context) (bool, error) {
		err := write()
		if err == nil {
			return !reject, nil
		}
		if strings.Contains(err.Error(), "cannot be removed") {
			return reject, nil
		}
		return false, err
	})
	if err != nil {
		t.Fatalf("waiting for placement validation reject=%v: %v", reject, err)
	}
}

func readMigrationSpec(t *testing.T, cl client.Client, object client.Object) v1beta1.InferenceServiceSpec {
	t.Helper()
	out := &v1beta1.InferenceService{}
	if err := cl.Get(t.Context(), client.ObjectKeyFromObject(object), out); err != nil {
		t.Fatal(err)
	}
	return out.Spec
}
