package inferencereplicaretention

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/util"
)

const (
	waitTimeout  = 2 * time.Minute
	pollInterval = 250 * time.Millisecond
)

// retentionIR returns a minimal single-pod InferenceReplica the IR
// controller accepts without the ISVC controller running: controller-
// write stamped, controller-owned by a synthetic parent ISVC identity
// (envtest runs no garbage collector, so the dangling owner ref is
// inert), one default Runner. The parent name determines the
// ControllerRevision key (`<parent>-<component>-<hash>`).
func retentionIR(parent, namespace string) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      parent + "-engine",
			Namespace: namespace,
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       parent,
				UID:        types.UID(parent + "-isvc-uid"),
				Controller: ptr.To(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{Name: parent},
			Component: v1beta1.EngineComponent,
			Replicas:  ptr.To(int32(1)),
			Runners: []v1beta1.Runner{{
				Name: v1beta1.RunnerNameDefault,
				Size: 1,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "ome-container",
							Image: "registry.example.com/fake-serving:v1",
						}},
					},
				},
			}},
		},
	}
}

// seedNonLiveRevision creates one ControllerRevision under the IR's
// revision key label set — the same trio the controller stamps — so
// the retention sweep's label-selector list picks it up. Owner-ref'd
// to the IR so the Owns(ControllerRevision) watch re-enqueues the IR
// after the seed lands. .Revision numbers well above the controller's
// own history make the seeds the "newest" non-live entries.
func seedNonLiveRevision(ir *v1beta1.InferenceReplica, suffix string, rev int64) *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s-%s", ir.Spec.ParentRef.Name, ir.Spec.Component, suffix),
			Namespace: ir.Namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
				constants.OMEComponentLabel:           string(ir.Spec.Component),
				util.LabelManagedByKey:                util.ManagedByOMENativeValue,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceReplica",
				Name:       ir.Name,
				UID:        ir.UID,
				Controller: ptr.To(true),
			}},
		},
		// The apiserver requires data; retention never reads the payload.
		Data:     runtime.RawExtension{Raw: []byte(`{}`)},
		Revision: rev,
	}
}

// liveRevisionNames returns the union of revision names the sweep must
// never delete, read from the committed IR status.
func liveRevisionNames(ir *v1beta1.InferenceReplica) map[string]struct{} {
	live := map[string]struct{}{}
	for _, name := range []string{ir.Status.CurrentRevision, ir.Status.UpdateRevision} {
		if name != "" {
			live[name] = struct{}{}
		}
	}
	for _, inst := range ir.Status.InstanceStatuses {
		for _, name := range []string{inst.RunningRevision, inst.TargetRevision} {
			if name != "" {
				live[name] = struct{}{}
			}
		}
	}
	return live
}

func listRevisionNames(namespace string) map[string]struct{} {
	list := &appsv1.ControllerRevisionList{}
	Expect(k8sClient.List(suiteCtx, list, client.InNamespace(namespace))).To(Succeed())
	out := make(map[string]struct{}, len(list.Items))
	for _, cr := range list.Items {
		out[cr.Name] = struct{}{}
	}
	return out
}

// awaitLiveRevision waits for the IR controller to mint its own live
// ControllerRevision and commit it to status, returning the fresh IR.
func awaitLiveRevision(key types.NamespacedName) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{}
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(suiteCtx, key, ir)).To(Succeed())
		g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty(),
			"the IR controller must commit its live revision before the seeds land")
	}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
	return ir
}

var _ = ginkgo.Describe("InferenceReplica revision retention", func() {
	ginkgo.It("prunes non-live revisions to the config-driven default and protects live revisions", func() {
		ns := "ir-ret-config"
		Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		})).To(Succeed())

		ir := retentionIR("llama", ns)
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())
		ir = awaitLiveRevision(types.NamespacedName{Name: ir.Name, Namespace: ns})
		live := liveRevisionNames(ir)

		// Seed configuredRetentionLimit+3 non-live CRs, all newer
		// (higher .Revision) than the controller's own history. The
		// sweep must keep only the newest configuredRetentionLimit of
		// the non-live set and delete the rest.
		const extra = 3
		seeded := make([]*appsv1.ControllerRevision, 0, configuredRetentionLimit+extra)
		for i := 0; i < configuredRetentionLimit+extra; i++ {
			cr := seedNonLiveRevision(ir, fmt.Sprintf("nl%02d", i), int64(1000+i))
			Expect(k8sClient.Create(suiteCtx, cr)).To(Succeed())
			seeded = append(seeded, cr)
		}

		Eventually(func(g Gomega) {
			survivors := listRevisionNames(ns)
			for name := range live {
				g.Expect(survivors).To(HaveKey(name),
					"live revision %s must never be deleted regardless of the limit", name)
			}
			for i := 0; i < extra; i++ {
				g.Expect(survivors).NotTo(HaveKey(seeded[i].Name),
					"non-live revision %s beyond the config default must be pruned", seeded[i].Name)
			}
			for i := extra; i < len(seeded); i++ {
				g.Expect(survivors).To(HaveKey(seeded[i].Name),
					"newest %d non-live revisions must be retained", configuredRetentionLimit)
			}
		}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
	})

	ginkgo.It("honors the per-IR spec limit (annotation projection) over the config default", func() {
		ns := "ir-ret-spec"
		Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		})).To(Succeed())

		// Spec.RevisionHistoryLimit is what the ISVC controller projects
		// from the ome.io/revision-history-limit annotation; 1 must win
		// over the (higher) config default.
		ir := retentionIR("mistral", ns)
		ir.Spec.RevisionHistoryLimit = ptr.To(int32(1))
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())
		ir = awaitLiveRevision(types.NamespacedName{Name: ir.Name, Namespace: ns})
		live := liveRevisionNames(ir)

		const seededCount = 4
		seeded := make([]*appsv1.ControllerRevision, 0, seededCount)
		for i := 0; i < seededCount; i++ {
			cr := seedNonLiveRevision(ir, fmt.Sprintf("nl%02d", i), int64(1000+i))
			Expect(k8sClient.Create(suiteCtx, cr)).To(Succeed())
			seeded = append(seeded, cr)
		}

		Eventually(func(g Gomega) {
			survivors := listRevisionNames(ns)
			for name := range live {
				g.Expect(survivors).To(HaveKey(name),
					"live revision %s must never be deleted regardless of the limit", name)
			}
			for i := 0; i < seededCount-1; i++ {
				g.Expect(survivors).NotTo(HaveKey(seeded[i].Name),
					"with limit 1, older non-live revision %s must be pruned", seeded[i].Name)
			}
			g.Expect(survivors).To(HaveKey(seeded[seededCount-1].Name),
				"the single newest non-live revision must be retained")
		}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
	})
})
